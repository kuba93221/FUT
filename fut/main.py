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
    state.BACKGROUND_LOOP = asyncio.new_event_loop()
    asyncio.set_event_loop(state.BACKGROUND_LOOP)

    state.GLOBAL_ALPHA_LOCK = asyncio.Lock()
    state.ASYNC_SHUTDOWN_EVENT = asyncio.Event()

    state.RATE_LIMITER_PUBLIC = TokenBucketRateLimiter(tokens_per_second=5.0, max_capacity=10.0)
    state.RATE_LIMITER_TRADE = TokenBucketRateLimiter(tokens_per_second=4.0, max_capacity=8.0)
    state.RATE_LIMITER_ACCOUNT = TokenBucketRateLimiter(tokens_per_second=4.0, max_capacity=8.0)

    async def _runner():
        async with aiohttp.ClientSession() as session:
            tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
            tg_chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
            state.GLOBAL_TG = TelegramThrottledDispatcher(tg_token, tg_chat, session)

            upstash_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip()
            upstash_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
            state.GLOBAL_REDIS_BRIDGE = UpstashRedisFuturesBridge(upstash_url, upstash_token, session)

            state.GLOBAL_OKX_CLIENT = OKXFuturesClient(session, state.RATE_LIMITER_TRADE, state.RATE_LIMITER_PUBLIC, state.RATE_LIMITER_ACCOUNT, is_sandbox=IS_SANDBOX)
            state.GLOBAL_SMART_MONEY_ORACLE = OKXSmartMoneyOracle(session, state.RATE_LIMITER_PUBLIC, is_sandbox=IS_SANDBOX)
            state.GLOBAL_WS_FEED = OKXWebSocketPriceFeed(session, is_sandbox=IS_SANDBOX)

            logger.info("🌐 [STARTUP] Próba przejęcia roli MASTER...")
            acquired = await state.GLOBAL_REDIS_BRIDGE.acquire_master_engine_lock(state.ENGINE_WORKER_ID, ttl_seconds=60)

            if acquired:
                state.IS_MASTER_ENGINE_NODE = True
                logger.info(f"👑 [MASTER-ENGINE-NODE] Proces {state.ENGINE_WORKER_ID} jest aktywnym MASTER.")
                symbols = await master_bootstrap(state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_OKX_CLIENT, session)
                spawn_supervised_task(state.GLOBAL_WS_FEED.start_listener, "ws_price_feed", symbols, tg=state.GLOBAL_TG)
                spawn_supervised_task(master_lock_heartbeat_worker, "master_heartbeat_worker", state.GLOBAL_REDIS_BRIDGE, tg=state.GLOBAL_TG)
                spawn_supervised_task(protection_reconciler_worker, "reconciler_worker", state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_TG, state.GLOBAL_OKX_CLIENT)
                spawn_supervised_task(independent_4tf_sniper_worker, "sniper_4tf_worker", session, state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_TG, state.GLOBAL_OKX_CLIENT, state.GLOBAL_SMART_MONEY_ORACLE, tg=state.GLOBAL_TG)
                if state.GLOBAL_TG:
                    await state.GLOBAL_TG.push("👑 <b>[MASTER ENGINE ONLINE]</b> Silnik v18.0 (M15 CHoCH) wystartował pomyślnie. Kapitał bazowy zainicjalizowany.")
            else:
                state.IS_MASTER_ENGINE_NODE = False
                logger.info(f"👀 [PASSIVE-NODE] Proces {state.ENGINE_WORKER_ID} działa w trybie PASSIVE.")
                configured_symbols = [item["symbol"] for item in FUTURES_INSTRUMENTS]
                spawn_supervised_task(state.GLOBAL_WS_FEED.start_listener, "ws_price_feed", configured_symbols, tg=state.GLOBAL_TG)
                spawn_supervised_task(passive_leader_sentry_worker, "passive_sentry_worker", state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_TG, state.GLOBAL_OKX_CLIENT, state.GLOBAL_SMART_MONEY_ORACLE, tg=state.GLOBAL_TG)

            while not state.ASYNC_SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)

            logger.info("🛑 [ASYNC-SHUTDOWN] Zatrzymywanie zadań w tle...")
            if state.IS_MASTER_ENGINE_NODE:
                await state.GLOBAL_REDIS_BRIDGE.release_master_engine_lock(state.ENGINE_WORKER_ID)
            for t in list(state.BACKGROUND_TASKS):
                t.cancel()
            await asyncio.gather(*state.BACKGROUND_TASKS, return_exceptions=True)
            logger.info("✅ [ASYNC-SHUTDOWN-COMPLETE] Wszystkie zadania asynchroniczne zostały zatrzymane.")

    state.BACKGROUND_LOOP.run_until_complete(_runner())
    state.SHUTDOWN_COMPLETE.set()


def start_background_loop():
    t = threading.Thread(target=background_async_loop, daemon=True, name="FuturesAsyncLoop")
    t.start()


def graceful_shutdown_handler(signum, frame):
    logger.warning(f"🛑 [SHUTDOWN-SIGNAL] Odebrano sygnał {signum}. Rozpoczynanie czystego zamykania...")
    state.PROCESS_DRAINING.set()
    if state.BACKGROUND_LOOP and state.ASYNC_SHUTDOWN_EVENT:
        state.BACKGROUND_LOOP.call_soon_threadsafe(state.ASYNC_SHUTDOWN_EVENT.set)


signal.signal(signal.SIGTERM, graceful_shutdown_handler)
signal.signal(signal.SIGINT, graceful_shutdown_handler)

start_background_loop()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 [FLASK-START] Uruchamianie serwera na porcie {port}...")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)