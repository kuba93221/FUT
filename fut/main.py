"""Punkt wejścia silnika: pętla async, start, sygnały, __main__."""
import os
import signal
import asyncio
import threading
import aiohttp

from .config import logger, IS_SANDBOX, FUTURES_INSTRUMENTS
from .rate_limiter import TokenBucketRateLimiter
from .telegram import TelegramThrottledDispatcher
from .redis_bridge import UpstashRedisFuturesBridge
from .okx import OKXFuturesClient, OKXSmartMoneyOracle, OKXWebSocketPriceFeed
from .engine import (
    master_bootstrap, master_lock_heartbeat_worker, protection_reconciler_worker,
    independent_4tf_sniper_worker, passive_leader_sentry_worker, spawn_supervised_task,
)
from .http_api import app
from . import state


def background_async_loop():
    state.ctx.background_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(state.ctx.background_loop)

    state.ctx.alpha_lock = asyncio.Lock()
    state.ctx.shutdown_event = asyncio.Event()

    state.ctx.rate_limiter_public = TokenBucketRateLimiter(tokens_per_second=5.0, max_capacity=10.0)
    state.ctx.rate_limiter_trade = TokenBucketRateLimiter(tokens_per_second=4.0, max_capacity=8.0)
    state.ctx.rate_limiter_account = TokenBucketRateLimiter(tokens_per_second=4.0, max_capacity=8.0)

    async def _runner():
        async with aiohttp.ClientSession() as session:
            tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
            tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
            state.ctx.tg = TelegramThrottledDispatcher(tg_token, tg_chat, session)

            upstash_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
            upstash_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
            state.ctx.redis_bridge = UpstashRedisFuturesBridge(upstash_url, upstash_token, session)

            state.ctx.okx_client = OKXFuturesClient(session, state.ctx.rate_limiter_trade, state.ctx.rate_limiter_public, state.ctx.rate_limiter_account, is_sandbox=IS_SANDBOX)
            state.ctx.smart_money_oracle = OKXSmartMoneyOracle(session, state.ctx.rate_limiter_public, is_sandbox=IS_SANDBOX)
            state.ctx.ws_feed = OKXWebSocketPriceFeed(session, is_sandbox=IS_SANDBOX)

            # --- Bootstrap z try/except: crash bootstrapu NIE zabija event loopa ---
            # Bez tego: wyjątek z acquire_master_engine_lock lub master_bootstrap
            # kończy run_until_complete → background_loop.is_running() == False →
            # health check zwraca 503 → Render restartuje w nieskończoność.
            try:
                logger.info("🌐 [STARTUP] Próba przejęcia roli MASTER...")
                acquired = await state.ctx.redis_bridge.acquire_master_engine_lock(state.ctx.worker_id, ttl_seconds=60)

                if acquired:
                    state.ctx.is_master = True
                    logger.info(f"👑 [MASTER-ENGINE-NODE] Proces {state.ctx.worker_id} jest aktywnym MASTER.")
                    symbols = await master_bootstrap(state.ctx.redis_bridge, state.ctx.okx_client, session)
                    spawn_supervised_task(state.ctx.ws_feed.start_listener, "ws_price_feed", symbols, tg=state.ctx.tg)
                    spawn_supervised_task(master_lock_heartbeat_worker, "master_heartbeat_worker", state.ctx.redis_bridge, tg=state.ctx.tg)
                    spawn_supervised_task(protection_reconciler_worker, "reconciler_worker", state.ctx.redis_bridge, state.ctx.tg, state.ctx.okx_client)
                    spawn_supervised_task(independent_4tf_sniper_worker, "sniper_4tf_worker", session, state.ctx.redis_bridge, state.ctx.tg, state.ctx.okx_client, state.ctx.smart_money_oracle, tg=state.ctx.tg)
                    if state.ctx.tg:
                        await state.ctx.tg.push("👑 <b>[MASTER ENGINE ONLINE]</b> Silnik v18.0 (M15 CHoCH) wystartował pomyślnie. Kapitał bazowy zainicjalizowany.")
                else:
                    state.ctx.is_master = False
                    logger.info(f"👀 [PASSIVE-NODE] Proces {state.ctx.worker_id} działa w trybie PASSIVE.")
                    configured_symbols = [item["symbol"] for item in FUTURES_INSTRUMENTS]
                    spawn_supervised_task(state.ctx.ws_feed.start_listener, "ws_price_feed", configured_symbols, tg=state.ctx.tg)
                    spawn_supervised_task(passive_leader_sentry_worker, "passive_sentry_worker", state.ctx.redis_bridge, state.ctx.tg, state.ctx.okx_client, state.ctx.smart_money_oracle, tg=state.ctx.tg)

            except Exception as startup_exc:
                # Logujemy błąd, ale NIE przerywamy pętli — Flask musi żyć.
                # passive_leader_sentry spróbuje przejąć MASTER przy kolejnej próbie.
                logger.critical(f"💥 [STARTUP-CRITICAL] Bootstrap nie powiódł się: {startup_exc}. "
                                f"Flask aktywny — silnik zdegradowany. Sprawdź env vars i logi.")
                try:
                    if state.ctx.tg:
                        await state.ctx.tg.push(f"💥 <b>[STARTUP CRITICAL]</b> Bootstrap silnika nie powiódł się: <code>{startup_exc}</code>\n"
                                                 f"Flask ONLINE, handel WSTRZYMANY do ręcznej interwencji.")
                except Exception:
                    pass
                # Uruchom passive sentry — może odzyska MASTER lock gdy Redis wróci
                try:
                    configured_symbols = [item["symbol"] for item in FUTURES_INSTRUMENTS]
                    spawn_supervised_task(state.ctx.ws_feed.start_listener, "ws_price_feed", configured_symbols, tg=state.ctx.tg)
                    spawn_supervised_task(passive_leader_sentry_worker, "passive_sentry_worker", state.ctx.redis_bridge, state.ctx.tg, state.ctx.okx_client, state.ctx.smart_money_oracle, tg=state.ctx.tg)
                except Exception as spawn_exc:
                    logger.error(f"❌ [STARTUP-DEGRADED] Nie można uruchomić passive sentry: {spawn_exc}")

            while not state.ctx.shutdown_event.is_set():
                await asyncio.sleep(1)

            logger.info("🛑 [ASYNC-SHUTDOWN] Zatrzymywanie zadań w tle...")
            if state.ctx.is_master:
                await state.ctx.redis_bridge.release_master_engine_lock(state.ctx.worker_id)
            for t in list(state.ctx.background_tasks):
                t.cancel()
            await asyncio.gather(*state.ctx.background_tasks, return_exceptions=True)
            logger.info("✅ [ASYNC-SHUTDOWN-COMPLETE] Wszystkie zadania asynchroniczne zostały zatrzymane.")

    state.ctx.background_loop.run_until_complete(_runner())
    state.ctx.shutdown_complete.set()


def start_background_loop():
    t = threading.Thread(target=background_async_loop, daemon=True, name="FuturesAsyncLoop")
    t.start()


def graceful_shutdown_handler(signum, frame):
    logger.warning(f"🛑 [SHUTDOWN-SIGNAL] Odebrano sygnał {signum}. Rozpoczynanie czystego zamykania...")
    state.ctx.process_draining.set()
    if state.ctx.background_loop and state.ctx.shutdown_event:
        state.ctx.background_loop.call_soon_threadsafe(state.ctx.shutdown_event.set)


signal.signal(signal.SIGTERM, graceful_shutdown_handler)
signal.signal(signal.SIGINT, graceful_shutdown_handler)

start_background_loop()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [FLASK-START] Uruchamianie serwera na porcie {port}...")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
