"""HTTP API administracyjne (Flask): status, pauza, wznowienie, e-emergency."""
import json
import hmac
import asyncio
from datetime import datetime, timezone
from flask import Flask, jsonify, request

from .config import logger, EMERGENCY_SECRET, QUOTE_CCY, TARGET_LEVERAGE, CONFIG, FUTURES_INSTRUMENTS, DEFAULT_EQUITY_FALLBACK
from .engine import (
    master_bootstrap, master_lock_heartbeat_worker, protection_reconciler_worker,
    independent_4tf_sniper_worker, spawn_supervised_task,
)
from . import state

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc

app = Flask(__name__)


def require_admin() -> bool:
    """Autoryzacja WYŁĄCZNIE przez nagłówek X-Admin-Secret z bezpiecznym porównaniem bajtów."""
    if not EMERGENCY_SECRET:
        return False
    supplied = request.headers.get("X-Admin-Secret", "").strip()
    if not supplied:
        return False
    return hmac.compare_digest(supplied.encode("utf-8"), EMERGENCY_SECRET.encode("utf-8"))


@app.route('/', methods=['GET'])
def health_check():
    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or state.PROCESS_DRAINING.is_set():
        return "FUTURES_ENGINE_DRAINING", 503
    role = "MASTER" if state.IS_MASTER_ENGINE_NODE else "PASSIVE"
    return f"FUTURES_ENGINE_ONLINE_v18_CHOCH_{QUOTE_CCY} [{role}]", 200


@app.route('/status', methods=['GET'])
def engine_status_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403

    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or not state.GLOBAL_REDIS_BRIDGE or not state.GLOBAL_OKX_CLIENT:
        return jsonify({"status": "starting", "engine": "INIT_PHASE"}), 503

    async def _gather_status():
        wallet = await state.GLOBAL_OKX_CLIENT.get_wallet_balances(QUOTE_CCY)
        active_keys = await state.GLOBAL_REDIS_BRIDGE.get_active_positions()
        daily_loss = await state.GLOBAL_REDIS_BRIDGE.get_daily_loss()

        today_str = datetime.now(UTC).strftime('%Y%m%d')
        start_equity = await state.GLOBAL_REDIS_BRIDGE.get_daily_start_equity(today_str)
        if start_equity is None or start_equity <= 0:
            start_equity = wallet.get("total_equity", DEFAULT_EQUITY_FALLBACK) if wallet else DEFAULT_EQUITY_FALLBACK

        positions_details = []
        for k in active_keys:
            st = await state.GLOBAL_REDIS_BRIDGE.get_position_state(k)
            if st:
                positions_details.append(st)

        is_sl_quarantine = await state.GLOBAL_REDIS_BRIDGE.is_cooldown_active("GLOBAL_PORTFOLIO_QUARANTINE")
        is_stagger_locked = await state.GLOBAL_REDIS_BRIDGE.is_cooldown_active("PORTFOLIO_STAGGER_LOCK")
        is_paused = await state.GLOBAL_REDIS_BRIDGE.is_system_paused_distributed()
        is_cb = await state.GLOBAL_REDIS_BRIDGE.is_circuit_breaker_active()
        prices_snapshot = state.GLOBAL_WS_FEED.get_prices_snapshot() if state.GLOBAL_WS_FEED else {}

        return {
            "status": "ONLINE",
            "version": "v18.0_M15_CHOCH_PROD",
            "engine_role": "MASTER" if state.IS_MASTER_ENGINE_NODE else "PASSIVE",
            "quote_currency": QUOTE_CCY,
            "target_leverage": TARGET_LEVERAGE,
            "trading_paused": is_paused,
            "circuit_breaker_active": is_cb,
            "total_equity": wallet.get("total_equity", 0.0) if wallet else 0.0,
            "daily_start_equity": start_equity,
            "available_cash": wallet.get("available_cash", 0.0) if wallet else 0.0,
            "daily_loss": daily_loss,
            "daily_loss_limit": round(start_equity * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"], 2),
            "portfolio_locks": {
                "global_sl_quarantine": is_sl_quarantine,
                "stagger_lock_active": is_stagger_locked
            },
            "active_slots_count": len(positions_details),
            "max_slots": CONFIG["ALPHA_MAX_ACTIVE_SLOTS"],
            "positions": positions_details,
            "prices": prices_snapshot
        }

    fut = asyncio.run_coroutine_threadsafe(_gather_status(), state.BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=6)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/claim-master', methods=['POST', 'GET'])
def claim_master_endpoint():
    """Wymusza natychmiastowe przejęcie roli Master bez czekania na TTL w Redis."""
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or not state.GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_claim():
        safe_key = state.GLOBAL_REDIS_BRIDGE._enforce_prefix("ENGINE_ACTIVE_LOCK")
        cmd = ["SET", safe_key, state.ENGINE_WORKER_ID, "EX", "60"]
        async with state.GLOBAL_REDIS_BRIDGE.session.post(f"{state.GLOBAL_REDIS_BRIDGE.url}", json=cmd, headers=state.GLOBAL_REDIS_BRIDGE.headers, timeout=4):
            pass
        if not state.IS_MASTER_ENGINE_NODE:
            state.IS_MASTER_ENGINE_NODE = True
            await master_bootstrap(state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_OKX_CLIENT, state.GLOBAL_OKX_CLIENT.session)
            spawn_supervised_task(master_lock_heartbeat_worker, "master_heartbeat_worker", state.GLOBAL_REDIS_BRIDGE, tg=state.GLOBAL_TG)
            spawn_supervised_task(protection_reconciler_worker, "reconciler_worker", state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_TG, state.GLOBAL_OKX_CLIENT)
            spawn_supervised_task(independent_4tf_sniper_worker, "sniper_4tf_worker", state.GLOBAL_OKX_CLIENT.session, state.GLOBAL_REDIS_BRIDGE, state.GLOBAL_TG, state.GLOBAL_OKX_CLIENT, state.GLOBAL_SMART_MONEY_ORACLE, tg=state.GLOBAL_TG)
            logger.info("👑 [FORCE-CLAIM-MASTER] Ręcznie wymuszono rolę MASTER Engine przez administratora!")
        return {"status": "success", "engine_role": "MASTER", "worker_id": state.ENGINE_WORKER_ID}

    fut = asyncio.run_coroutine_threadsafe(_do_claim(), state.BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=10)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/pause-trading', methods=['POST'])
def pause_trading_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or not state.GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_pause():
        async with state.GLOBAL_ALPHA_LOCK:
            state.GLOBAL_TRADING_PAUSED = True
            await state.GLOBAL_REDIS_BRIDGE.set_system_pause(True)
        return {"status": "success", "message": "Handel został wstrzymany (Paused = True)."}

    fut = asyncio.run_coroutine_threadsafe(_do_pause(), state.BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=5)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/resume-trading', methods=['POST'])
def resume_trading_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or not state.GLOBAL_REDIS_BRIDGE or not state.GLOBAL_OKX_CLIENT:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_resume():
        async with state.GLOBAL_ALPHA_LOCK:
            cb_active = await state.GLOBAL_REDIS_BRIDGE.is_circuit_breaker_active()
            if cb_active:
                return {"status": "rejected", "message": "Circuit Breaker jest aktywny w bazie Redis. Zakaz wznowienia handlu przed 00:00 UTC."}

            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                sz_l = await state.GLOBAL_OKX_CLIENT.get_open_position_size(sym, "long")
                sz_s = await state.GLOBAL_OKX_CLIENT.get_open_position_size(sym, "short")
                has_p = await state.GLOBAL_OKX_CLIENT.has_pending_orders(sym)
                algos = await state.GLOBAL_OKX_CLIENT.get_pending_algo_orders(sym, ord_type="oco")

                if sz_l is None or sz_s is None or has_p is None or algos is None:
                    return {"status": "rejected", "message": f"Niepewny stan API dla {sym}. Odmowa wznowienia."}
                if has_p is True:
                    return {"status": "rejected", "message": f"Wykryto aktywne zlecenia oczekujące na {sym}."}
                if (sz_l > 0 or sz_s > 0) and len(algos) == 0:
                    return {"status": "rejected", "message": f"Wykryto nagą pozycję na {sym} bez OCO!"}

            state.GLOBAL_TRADING_PAUSED = False
            await state.GLOBAL_REDIS_BRIDGE.set_system_pause(False)
            logger.info("🟢 [SAFETY-RESUME] Wznowiono handel po pozytywnej weryfikacji pre-flight.")
            return {"status": "success", "message": "Handel wznowiony pomyślnie."}

    fut = asyncio.run_coroutine_threadsafe(_do_resume(), state.BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=10)
        code = 200 if res.get("status") == "success" else 409
        return jsonify(res), code
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/reset-slots', methods=['POST'])
def reset_slots_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or not state.GLOBAL_REDIS_BRIDGE or not state.GLOBAL_OKX_CLIENT:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_flush_slots():
        async with state.GLOBAL_ALPHA_LOCK:
            for item in FUTURES_INSTRUMENTS:
                sz_l = await state.GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], "long")
                sz_s = await state.GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], "short")
                if sz_l is None or sz_s is None:
                    return {"status": "rejected", "message": "Nieznany stan giełdy. Odmowa czyszczenia slotów."}
                if sz_l > 0 or sz_s > 0:
                    return {"status": "rejected", "message": f"Giełda posiada aktywną pozycję na {item['symbol']}."}
            deleted = await state.GLOBAL_REDIS_BRIDGE.reset_all_slots()
            return {"status": "success", "deleted_slots_count": deleted}

    fut = asyncio.run_coroutine_threadsafe(_do_flush_slots(), state.BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=10)
        code = 200 if res.get("status") == "success" else 409
        return jsonify(res), code
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/emergency-liquidate', methods=['POST'])
def emergency_liquidate_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if state.BACKGROUND_LOOP is None or not state.BACKGROUND_LOOP.is_running() or not state.GLOBAL_OKX_CLIENT or not state.GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_emergency():
        async with state.GLOBAL_ALPHA_LOCK:
            state.GLOBAL_TRADING_PAUSED = True
            await state.GLOBAL_REDIS_BRIDGE.set_system_pause(True)

            report = {"closed_positions": [], "canceled_pending": [], "canceled_algos": [], "freed_slots": 0, "verified_flat": False}

            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                try:
                    await state.RATE_LIMITER_TRADE.consume()
                    req_path_p = f"/api/v5/trade/orders-pending?instType=FUTURES&instId={sym}"
                    headers_p = state.GLOBAL_OKX_CLIENT._get_headers("GET", req_path_p)
                    async with state.GLOBAL_OKX_CLIENT.session.get(f"{state.GLOBAL_OKX_CLIENT.base_url}{req_path_p}", headers=headers_p, timeout=4) as resp_p:
                        data_p = await resp_p.json()
                        if data_p.get("code") == "0" and data_p.get("data"):
                            for o in data_p["data"]:
                                ord_id = o.get("ordId")
                                c_body = json.dumps({"instId": sym, "ordId": ord_id})
                                await state.RATE_LIMITER_TRADE.consume()
                                c_headers = state.GLOBAL_OKX_CLIENT._get_headers("POST", "/api/v5/trade/cancel-order", c_body)
                                async with state.GLOBAL_OKX_CLIENT.session.post(f"{state.GLOBAL_OKX_CLIENT.base_url}/api/v5/trade/cancel-order", data=c_body, headers=c_headers, timeout=3):
                                    pass
                                report["canceled_pending"].append(f"{sym}:{ord_id}")
                except Exception as e:
                    logger.error(f"[EMERGENCY-CANCEL-PENDING] {sym}: {e}")

            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                for side in ["long", "short"]:
                    flattened = await state.GLOBAL_OKX_CLIENT.emergency_flatten_position(sym, side)
                    if flattened:
                        report["closed_positions"].append(f"{sym}:{side}")

            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                try:
                    pending_algos = await state.GLOBAL_OKX_CLIENT.get_pending_algo_orders(sym, ord_type="oco")
                    if pending_algos:
                        for al in pending_algos:
                            al_id = al.get("algoId")
                            if al_id:
                                await state.GLOBAL_OKX_CLIENT.cancel_algo_order(sym, al_id)
                                report["canceled_algos"].append(f"{sym}:{al_id}")
                except Exception as e:
                    logger.error(f"[EMERGENCY-CANCEL-ALGO] {sym}: {e}")

            all_flat = True
            for item in FUTURES_INSTRUMENTS:
                for side in ["long", "short"]:
                    rem = await state.GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], side)
                    has_p = await state.GLOBAL_OKX_CLIENT.has_pending_orders(item["symbol"])
                    algos = await state.GLOBAL_OKX_CLIENT.get_pending_algo_orders(item["symbol"], ord_type="oco")
                    if rem is None or rem > 0 or has_p is None or has_p or algos is None or len(algos) > 0:
                        all_flat = False

            report["verified_flat"] = all_flat
            if all_flat:
                freed = await state.GLOBAL_REDIS_BRIDGE.reset_all_slots()
                report["freed_slots"] = freed
                await state.GLOBAL_REDIS_BRIDGE.clear_cooldown("GLOBAL_PORTFOLIO_QUARANTINE")
                await state.GLOBAL_REDIS_BRIDGE.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                logger.info("✅ [EMERGENCY] Wszystkie zasoby FLAT. Sloty zwolnione.")

            if state.GLOBAL_TG:
                await state.GLOBAL_TG.push(
                    f"🚨🚨 <b>[AWARYJNA EWAKUACJA]</b> 🚨🚨\n"
                    f"Zamknięte: <code>{len(report['closed_positions'])}</code>\n"
                    f"Zwolnione sloty: <code>{report['freed_slots']}</code>\n"
                    f"Verified Flat: <b>{all_flat}</b>"
                )
            return report

    fut = asyncio.run_coroutine_threadsafe(_do_emergency(), state.BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=25)
        code = 200 if res.get("verified_flat") else 409
        return jsonify({"status": "COMPLETED" if res.get("verified_flat") else "FAILED_NOT_FLAT", "details": res}), code
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500