"""Workery silnika: sniper, reconciler, heartbeat, sentry, bootstrap, supervisor."""
import time
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from .config import logger, CONFIG, QUOTE_CCY, TARGET_LEVERAGE, FUTURES_INSTRUMENTS, DEFAULT_EQUITY_FALLBACK
from .utils import (
    get_canonical_pos_key, validate_okx_response, round_price_to_tick,
    format_px, format_sz, find_structural_h1_swing_sl, generate_cl_ord_id,
    calc_ema, get_strictly_closed_candles_slice, find_m15_recent_swing_high,
    find_m15_recent_swing_low,
)
from .redis_bridge import UpstashRedisFuturesBridge
from .telegram import TelegramThrottledDispatcher
from . import state

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc


async def reconcile_and_timestop_futures(
    inst: Dict[str, Any],
    pos_side: str,
    redis_trade: UpstashRedisFuturesBridge,
    tg: TelegramThrottledDispatcher
) -> Tuple[bool, Optional[str]]:
    pos_key = get_canonical_pos_key(redis_trade.prefix, inst["symbol"], pos_side)
    pos_data = await redis_trade.get_position_state(pos_key)
    if not pos_data:
        return False, None

    pos_state = pos_data.get("state", "PROTECTED")
    if pos_state in ["ENTRY_SUBMITTED", "ENTRY_UNKNOWN"]:
        return False, None

    actual_pos_on_exchange = await inst["client"].get_open_position_size(inst["symbol"], pos_side)
    if actual_pos_on_exchange is None:
        return False, None

    algo_id = str(pos_data.get("protection", {}).get("algoId") or pos_data.get("algo_id", "EXT_MANUAL"))
    algo_cl_id = str(pos_data.get("protection", {}).get("algoClOrdId") or pos_data.get("algo_cl_ord_id", ""))

    algo_state, actual_px, found_algo_id = await inst["client"].get_algo_order_state(
        algo_id=algo_id, algo_cl_ord_id=algo_cl_id
    )

    opened_at = float(pos_data.get("openedAt") or pos_data.get("time", time.time()))
    entry_p = float(pos_data.get("position", {}).get("avgPx") or pos_data.get("entry_price", 0.0))
    tp_p = float(pos_data.get("protection", {}).get("tpPx") or pos_data.get("tp_price", entry_p))
    sl_p = float(pos_data.get("protection", {}).get("slPx") or pos_data.get("sl_price", entry_p))
    contracts = float(pos_data.get("position", {}).get("realSize") or pos_data.get("contracts", 0.01))
    margin_locked = float(pos_data.get("position", {}).get("marginLocked") or pos_data.get("margin_locked", 1.0))
    be_active = pos_data.get("beActivated") or pos_data.get("be_activated", False)

    max_timeout = CONFIG["TIMEOUTS"].get(pos_data.get("strategy", "M15_CHOCH"), 172800)

    if actual_pos_on_exchange == 0.0:
        pos_data["state"] = "FLAT_PENDING_SETTLEMENT"
        await redis_trade.set_position_state(pos_key, pos_data)

        real_pos_history = None
        for _ in range(3):
            real_pos_history = await inst["client"].get_last_closed_position(inst["symbol"], min_close_time_ms=int(opened_at * 1000))
            if real_pos_history:
                break
            await asyncio.sleep(1.0)

        close_reason = pos_data.get("closeReason", "NORMAL")
        await redis_trade.delete_key(pos_key)

        if real_pos_history and real_pos_history.get("close_avg_px", 0.0) > 0.0:
            exit_p = real_pos_history["close_avg_px"]
            pnl_net = round(real_pos_history["realized_pnl"], 2)
            roe_net = round(real_pos_history["pnl_ratio"], 2)
        else:
            exit_p = pos_data.get("timeStopExitPx") or (actual_px if actual_px and actual_px > 0 else (sl_p if pos_side == "long" else tp_p))
            spec = inst["client"].instruments_cache.get(inst["symbol"], {"ctVal": 1.0, "ctValCcy": ""})
            ct_val = float(spec.get("ctVal", 1.0))
            ct_val_ccy = str(spec.get("ctValCcy", "")).upper()
            mult = 1.0 if pos_side == "long" else -1.0
            if ct_val_ccy in ["USD", "USDC", "USDT"] or inst.get("is_usd_ct", False):
                pnl_gross = ((exit_p - entry_p) / entry_p) * mult * contracts * ct_val if entry_p > 0 else 0.0
            else:
                pnl_gross = (exit_p - entry_p) * mult * contracts * ct_val
            pnl_net = round(pnl_gross - (margin_locked * 0.001), 2)
            roe_net = round((pnl_net / margin_locked) * 100.0, 2) if margin_locked > 0 else 0.0

        if close_reason == "TIME_STOP_48H":
            icon = "⏳ <b>[ZAMKNIĘCIE TIME-STOP 48H]"
        else:
            icon = "🎉 <b>[ZYSK TAKE PROFIT]" if pnl_net >= 0 else "🛑 <b>[STOP LOSS / WYJŚCIE]"

        cooldown_msg = ""
        if pnl_net < 0 and close_reason != "TIME_STOP_48H":
            await redis_trade.set_cooldown("GLOBAL_PORTFOLIO_QUARANTINE", CONFIG["SAFETY_GUARDS"]["SL_QUARANTINE_SECONDS"])
            await redis_trade.set_cooldown(inst["base"], CONFIG["SAFETY_GUARDS"]["SL_COOLDOWN_SECONDS"])
            cooldown_msg = "\n⏳ Nałożono globalną kwarantannę po stracie."

            accum_loss = await redis_trade.add_daily_loss(abs(pnl_net))
            today_str = datetime.now(UTC).strftime('%Y%m%d')
            start_eq = await redis_trade.get_daily_start_equity(today_str)
            if start_eq is None or start_eq <= 0:
                wallet_cb = await inst["client"].get_wallet_balances(QUOTE_CCY)
                start_eq = wallet_cb.get("total_equity", DEFAULT_EQUITY_FALLBACK) if wallet_cb else DEFAULT_EQUITY_FALLBACK

            cb_limit = start_eq * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]
            if accum_loss >= cb_limit:
                await redis_trade.set_circuit_breaker(True)
                await tg.push(f"🚨 <b>[CIRCUIT BREAKER AKTYWNY]</b> Strata dzisiejsza {accum_loss:.2f} >= {cb_limit:.2f} {QUOTE_CCY}. Odcięcie zasilania do 00:00 UTC.")

        strat_display = pos_data.get("strategy", "M15_CHOCH")
        await tg.push(
            f"{icon} • {inst['label']}</b>\n"
            f"Strategia: <b>{strat_display}</b> [{pos_side.upper()}]\n"
            f"Wyjście: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {entry_p})\n"
            f"Wynik netto: <b>{pnl_net} {QUOTE_CCY} ({roe_net}%)</b>{cooldown_msg}"
        )
        return True, pos_key

    if not be_active and actual_pos_on_exchange > 0.0 and entry_p > 0.0 and algo_id != "EXT_MANUAL":
        ticker = await inst["client"].get_market_ticker(inst["symbol"])
        current_market_price = float(ticker.get("last", 0.0)) if ticker else 0.0
        if current_market_price > 0.0:
            be_ratio = CONFIG["DYNAMIC_RISK"].get("BREAK_EVEN_TRIGGER_RATIO", 0.80)
            fee_buffer_pct = CONFIG["DYNAMIC_RISK"].get("BREAK_EVEN_FEE_BUFFER_PCT", 0.0010)
            spec = inst["client"].instruments_cache.get(inst["symbol"], {"tickSz": 0.1})
            tick_sz = spec["tickSz"]
            should_trigger_be = False
            new_sl_px = 0.0

            if pos_side == "long":
                target_dist = tp_p - entry_p
                if target_dist > 0 and (current_market_price - entry_p) >= (target_dist * be_ratio):
                    new_sl_px = round_price_to_tick(entry_p * (1.0 + fee_buffer_pct), tick_sz, direction="UP")
                    if new_sl_px > sl_p and new_sl_px < current_market_price:
                        should_trigger_be = True
            elif pos_side == "short":
                target_dist = entry_p - tp_p
                if target_dist > 0 and (entry_p - current_market_price) >= (target_dist * be_ratio):
                    new_sl_px = round_price_to_tick(entry_p * (1.0 - fee_buffer_pct), tick_sz, direction="DOWN")
                    if new_sl_px < sl_p and new_sl_px > current_market_price:
                        should_trigger_be = True

            if should_trigger_be and new_sl_px > 0.0:
                amend_res = await inst["client"].amend_algo_order(inst["symbol"], algo_id, new_sl_trigger_px=str(new_sl_px))
                ok, err_msg, _ = validate_okx_response(amend_res)
                if ok:
                    pos_data["beActivated"] = True
                    if "protection" in pos_data:
                        pos_data["protection"]["slPx"] = new_sl_px
                    pos_data["sl_price"] = new_sl_px
                    await redis_trade.set_position_state(pos_key, pos_data)
                    await tg.push(f"🛡️ <b>[DYNAMIC BREAK-EVEN: {inst['label']}]</b> SL przesunięty na <code>{new_sl_px} {QUOTE_CCY}</code>")
                else:
                    logger.error(f"❌ [AMEND-BE-FAILED] {inst['label']}: Błąd przestawiania SL na Break-Even ({err_msg})")

    if (time.time() - opened_at) > max_timeout:
        logger.warning(f"⏳ [TIME-STOP] Pozycja {inst['label']} [{pos_side}] przekroczyła {round(max_timeout/3600, 1)}h (48h limit). Zamykanie...")
        ticker = await inst["client"].get_market_ticker(inst["symbol"])
        mkt_px = float(ticker.get("last", entry_p)) if ticker else entry_p

        flatten_ok = await inst["client"].emergency_flatten_position(inst["symbol"], pos_side)
        if flatten_ok:
            if algo_id not in ["EXT_MANUAL", "NONE"]:
                await inst["client"].cancel_algo_order(inst["symbol"], algo_id)
            pos_data["state"] = "FLAT_PENDING_SETTLEMENT"
            pos_data["closeReason"] = "TIME_STOP_48H"
            pos_data["timeStopExitPx"] = mkt_px
            await redis_trade.set_position_state(pos_key, pos_data)
            return True, pos_key
        else:
            logger.critical(f"🔥 [TIME-STOP-FLATTEN-FAILED] Nie udało się zamknąć {inst['label']}. Rezerwacja SOS_LOCKED.")
            pos_data["state"] = "SOS_LOCKED"
            await redis_trade.set_position_state(pos_key, pos_data)
            return False, None

    return False, None


async def protection_reconciler_worker(redis_trade, tg, okx_client):
    logger.info("🛡️ [RECONCILER-WORKER] Dedykowana pętla monitorowania pozycji i ochrony OCO Online (interwał 15s).")
    while not (state.ctx.shutdown_event and state.ctx.shutdown_event.is_set()):
        try:
            if state.ctx.is_master:
                active_keys = await redis_trade.get_active_positions()
                for k in active_keys:
                    pos_data = await redis_trade.get_position_state(k)
                    if not pos_data:
                        continue
                    sym = pos_data.get("instId")
                    pos_side = pos_data.get("posSide", "long").lower()
                    inst = next((item for item in FUTURES_INSTRUMENTS if item["symbol"] == sym), None)
                    if inst:
                        inst_variant = {**inst, "client": okx_client}
                        await reconcile_and_timestop_futures(inst_variant, pos_side, redis_trade, tg)
        except Exception as e:
            logger.error(f"❌ [RECONCILER-LOOP-ERR] {e}")
        finally:
            await interruptible_sleep(15)


async def independent_4tf_sniper_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("🎯 [M15 CHOCH SNIPER] Silnik Strategiczny v18.0 (CHoCH + H1 Swing Low) Online.")
    scan_counter = 0

    while not (state.ctx.shutdown_event and state.ctx.shutdown_event.is_set()):
        try:
            if not state.ctx.is_master:
                await interruptible_sleep(10)
                continue

            if await redis_trade.is_system_paused_distributed() or await redis_trade.is_circuit_breaker_active():
                await interruptible_sleep(15)
                continue

            if await redis_trade.is_cooldown_active("GLOBAL_PORTFOLIO_QUARANTINE") or await redis_trade.is_cooldown_active("PORTFOLIO_STAGGER_LOCK"):
                await interruptible_sleep(15)
                continue

            active_keys = await redis_trade.get_active_positions()
            if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                await interruptible_sleep(15)
                continue

            today_str = datetime.now(UTC).strftime('%Y%m%d')
            daily_loss = await redis_trade.get_daily_loss()
            start_equity = await redis_trade.get_daily_start_equity(today_str)
            if start_equity is None or start_equity <= 0:
                wallet_check = await okx_client.get_wallet_balances(QUOTE_CCY)
                start_equity = wallet_check.get("total_equity", DEFAULT_EQUITY_FALLBACK) if wallet_check else DEFAULT_EQUITY_FALLBACK

            cb_threshold = start_equity * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]
            if daily_loss >= cb_threshold:
                logger.critical(f"🛑 [CIRCUIT-BREAKER] Przekroczono limit straty dobowej: {daily_loss:.2f} >= {cb_threshold:.2f}. Zatrzymanie handlu.")
                await redis_trade.set_circuit_breaker(True)
                await interruptible_sleep(60)
                continue

            candidates: List[Dict[str, Any]] = []
            scan_counter += 1
            is_verbose_cycle = (scan_counter % 4 == 0)

            if is_verbose_cycle:
                logger.info(f"🔍 [M15-CHOCH-SCAN] Skanowanie koszyka (Sloty: {len(active_keys)}/{CONFIG['ALPHA_MAX_ACTIVE_SLOTS']})...")

            now_scan_ts = time.time()

            for conf in FUTURES_INSTRUMENTS:
                if not state.ctx.is_master:
                    break

                sym = conf["symbol"]
                base = conf["base"]

                if any(base in ak for ak in active_keys):
                    if is_verbose_cycle:
                        logger.info(f"   ⮡ [SKIP] {base}: Aktywny slot w portfelu.")
                    continue
                if await redis_trade.is_cooldown_active(base):
                    if is_verbose_cycle:
                        logger.info(f"   ⮡ [SKIP] {base}: Aktywna kwarantanna po stracie.")
                    continue

                spread_ok, spread_val = await okx_client.check_spread_allowed(sym, CONFIG["SAFETY_GUARDS"]["MAX_SPREAD_PCT"])
                if not spread_ok:
                    if is_verbose_cycle:
                        logger.warning(f"   ⮡ [FILTER-SPREAD] {base}: Spread {round(spread_val*100, 3)}% > MAX ({CONFIG['SAFETY_GUARDS']['MAX_SPREAD_PCT']*100}%).")
                    continue

                c4h, c1h, c15m = await asyncio.gather(
                    okx_client.get_macro_candles_cached(sym, bar="4H", limit=250, cache_ttl_sec=900.0),
                    okx_client.get_macro_candles_cached(sym, bar="1H", limit=100, cache_ttl_sec=300.0),
                    okx_client.get_macro_candles_cached(sym, bar="15m", limit=100, cache_ttl_sec=0.0)
                )

                if len(c4h) < 205 or len(c1h) < 60 or len(c15m) < 40:
                    if is_verbose_cycle:
                        logger.warning(f"   ⮡ [DATA-INSUFFICIENT] {base}: Za mało świec (H4:{len(c4h)}, H1:{len(c1h)}, M15:{len(c15m)}).")
                    continue

                c4h_closed, _ = get_strictly_closed_candles_slice(c4h, 14400, now_scan_ts)
                c1h_closed, _ = get_strictly_closed_candles_slice(c1h, 3600, now_scan_ts)
                c15m_closed, _ = get_strictly_closed_candles_slice(c15m, 900, now_scan_ts)

                if not c4h_closed or not c1h_closed or not c15m_closed:
                    if is_verbose_cycle:
                        logger.warning(f"   ⮡ [TIMING-DESYNC] {base}: Rozbieżność znaczników czasu świec zamkniętych.")
                    continue

                closes_4h = [float(c[4]) for c in c4h_closed]
                closes_1h = [float(c[4]) for c in c1h_closed]
                volumes_1h = [float(c[5]) for c in c1h_closed]

                ema50_4h_list = calc_ema(closes_4h, 50)
                ema200_4h_list = calc_ema(closes_4h, 200)
                if not ema50_4h_list or not ema200_4h_list:
                    continue

                ema50_4h = ema50_4h_list[-1]
                ema200_4h = ema200_4h_list[-1]
                h4_bull = closes_4h[-1] > ema50_4h and ema50_4h > ema200_4h
                h4_bear = closes_4h[-1] < ema50_4h and ema50_4h < ema200_4h

                if not h4_bull and not h4_bear:
                    if is_verbose_cycle:
                        logger.info(f"   ⮡ [H4-NEUTRAL] {base}: Brak silnego trendu makro H4.")
                    continue

                ema50_1h = calc_ema(closes_1h, 50)[-1]
                ema20_1h_history = calc_ema(closes_1h, 20)
                ema20_1h = ema20_1h_history[-1]
                ema20_1h_prev = ema20_1h_history[-2]

                h1_bull = closes_1h[-1] > ema50_1h and (ema20_1h > ema20_1h_prev)
                h1_bear = closes_1h[-1] < ema50_1h and (ema20_1h < ema20_1h_prev)
                if (h4_bull and not h1_bull) or (h4_bear and not h1_bear):
                    if is_verbose_cycle:
                        logger.info(f"   ⮡ [H1-DISCORD] {base}: Kierunek H1 niezgodny z H4.")
                    continue

                vol_sma20_1h = sum(volumes_1h[-20:]) / 20.0 if len(volumes_1h) >= 20 else (volumes_1h[-1] if volumes_1h else 0.0)
                last_vol_1h = volumes_1h[-1] if volumes_1h else 0.0
                vsa_panic = last_vol_1h > (1.35 * vol_sma20_1h) if vol_sma20_1h > 0 else False
                if vsa_panic:
                    if is_verbose_cycle:
                        logger.info(f"   ⮡ [H1-VSA-PANIC] {base}: Wolumen paniki H1 ({last_vol_1h:.1f} > 1.35x {vol_sma20_1h:.1f}).")
                    continue

                c15m_close = float(c15m_closed[-1][4])
                c15m_open = float(c15m_closed[-1][1])

                signal_long = False
                signal_short = False
                rank_score = 0.0

                if h4_bull and h1_bull:
                    m15_swing_high = find_m15_recent_swing_high(c15m_closed, lookback=16)
                    if m15_swing_high and c15m_close > m15_swing_high and c15m_close > c15m_open:
                        signal_long = True
                        rank_score = (c15m_close - m15_swing_high) / c15m_close

                elif h4_bear and h1_bear:
                    m15_swing_low = find_m15_recent_swing_low(c15m_closed, lookback=16)
                    if m15_swing_low and c15m_close < m15_swing_low and c15m_close < c15m_open:
                        signal_short = True
                        rank_score = (m15_swing_low - c15m_close) / c15m_close

                if signal_long or signal_short:
                    logger.info(f"🎯 [M15-CHOCH-SIGNAL] {conf['label']} wybił strukturę CHoCH! (Kierunek: {'LONG' if signal_long else 'SHORT'}, Wynik Rank: {round(rank_score*100, 3)}%)")
                    candidates.append({
                        "symbol": sym, "base": base, "conf": conf,
                        "side": "long" if signal_long else "short",
                        "rank_score": rank_score, "c1h_closed": c1h_closed,
                        "strategy": "M15_CHOCH"
                    })

            candidates.sort(key=lambda x: x["rank_score"], reverse=True)

            for best_signal in candidates:
                if not state.ctx.is_master:
                    break

                sym = best_signal["symbol"]
                base = best_signal["base"]
                conf = best_signal["conf"]
                pos_side = best_signal["side"]
                strategy_name = best_signal["strategy"]
                c1h_closed_ref = best_signal["c1h_closed"]

                if CONFIG["SAFETY_GUARDS"].get("SMART_MONEY", {}).get("BYPASS_PULLBACK", True) and strategy_name in ["M15_CHOCH", "TREND_PULLBACK"]:
                    sm_ok, sm_note = True, "BYPASS_CHOCH_STRATEGY_ACTIVE"
                else:
                    sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(base, pos_side)

                if not sm_ok:
                    logger.warning(f"🐳 [SMART-MONEY] Odrzucono sygnał {conf['label']}: {sm_note}")
                    continue

                async with state.ctx.alpha_lock:
                    active_keys = await redis_trade.get_active_positions()
                    if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"] or any(base in ak for ak in active_keys):
                        continue

                    if await redis_trade.is_cooldown_active("PORTFOLIO_STAGGER_LOCK"):
                        continue
                    await redis_trade.set_cooldown("PORTFOLIO_STAGGER_LOCK", CONFIG["SAFETY_GUARDS"]["PORTFOLIO_STAGGER_LOCK_SECONDS"])

                    ticker_live = await okx_client.get_market_ticker(sym)
                    entry_live_price = float(ticker_live.get("last", 0.0)) if ticker_live else 0.0
                    if entry_live_price <= 0.0:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    wallet = await okx_client.get_wallet_balances(QUOTE_CCY)
                    available_cash = wallet.get("available_cash", 0.0) if wallet else 0.0
                    if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    spec = okx_client.instruments_cache.get(sym) or await okx_client.load_instrument_specification(sym)
                    if not spec:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue
                    tick_sz = spec.get("tickSz", 0.1)

                    cushion_val = 0.0025 if base == "BTC" else 0.0015
                    price_sl, price_tp, sl_eff_pct = find_structural_h1_swing_sl(
                        candles_1h=c1h_closed_ref,
                        entry_price=entry_live_price,
                        pos_side=pos_side,
                        tick_sz=tick_sz,
                        lookback_bars=40,
                        pivot_radius=2,
                        cushion_pct=cushion_val,
                        min_sl_pct=0.012,
                        max_sl_pct=0.035,
                        rr_ratio=2.0
                    )

                    risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                    safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                    target_margin = min(
                        risk_capital / (sl_eff_pct * TARGET_LEVERAGE),
                        available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"],
                        safe_cash * 0.95
                    )

                    contracts, actual_margin = okx_client.calculate_contract_size(
                        symbol=sym,
                        current_price=entry_live_price,
                        target_margin_quote=target_margin,
                        max_allowed_margin=safe_cash,
                        is_usd_ct=conf.get("is_usd_ct", False)
                    )
                    if contracts <= 0 or actual_margin > available_cash:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    order_side = "buy" if pos_side == "long" else "sell"
                    my_cl_ord_id = generate_cl_ord_id("E", base)
                    my_algo_cl_id = generate_cl_ord_id("P", base)

                    canonical_pos_key = get_canonical_pos_key(redis_trade.prefix, sym, pos_side)

                    await redis_trade.set_position_state(canonical_pos_key, {
                        "version": 1,
                        "state": "ENTRY_SUBMITTED",
                        "strategy": strategy_name,
                        "instId": sym,
                        "posSide": pos_side,
                        "parent": {"clOrdId": my_cl_ord_id, "targetSize": contracts},
                        "protection": {"algoClOrdId": my_algo_cl_id},
                        "updatedAt": time.time()
                    })

                    order_res = await okx_client.execute_futures_order(
                        sym, side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market",
                        tick_sz=tick_sz, cl_ord_id=my_cl_ord_id, attach_algo_cl_ord_id=my_algo_cl_id,
                        attached_tp=price_tp, attached_sl=price_sl
                    )

                    if order_res is None:
                        resolved_order = await okx_client.resolve_unknown_order_by_cl_id(sym, my_cl_ord_id, max_retries=3)
                        if resolved_order:
                            order_res = {"code": "0", "data": [resolved_order]}
                        else:
                            await redis_trade.set_position_state(canonical_pos_key, {
                                "version": 1, "state": "ENTRY_UNKNOWN", "instId": sym, "posSide": pos_side,
                                "parent": {"clOrdId": my_cl_ord_id}, "updatedAt": time.time()
                            })
                            continue

                    ok, msg, item = validate_okx_response(order_res, expected_id_field="ordId")
                    if not ok or not item:
                        await redis_trade.delete_key(canonical_pos_key)
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    main_ord_id = item.get("ordId", "")

                    for _ in range(7):
                        await asyncio.sleep(0.3)
                        order_info = await okx_client.get_parent_order_state(sym, main_ord_id)
                        if order_info and order_info.get("state") == "filled":
                            break

                    pos_details = await okx_client.get_position_details(sym, pos_side)
                    if pos_details is None or pos_details.get("size", 0.0) <= 0.0:
                        logger.critical(f"🚨 [POSITION-ZERO-UNKNOWN] {sym}: Pozycja zerowa lub brak odpowiedzi. Anulowanie.")
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    real_size = pos_details["size"]
                    real_fill_px = pos_details["avgPx"] if pos_details["avgPx"] > 0 else entry_live_price
                    actual_pos_margin = pos_details["margin"] if pos_details["margin"] > 0 else actual_margin

                    slippage = abs(real_fill_px - entry_live_price) / entry_live_price
                    if slippage > CONFIG["MAX_ENTRY_SLIPPAGE_PCT"]:
                        ct_val = spec.get("ctVal", 1.0)
                        raw_dist = abs(real_fill_px - price_sl)
                        monetary_risk = real_size * raw_dist * ct_val
                        max_allowed_risk = available_cash * CONFIG["HARD_RISK_CAP_ON_SLIPPAGE_PCT"]

                        if monetary_risk > max_allowed_risk:
                            logger.critical(f"🔥 [SLIPPAGE-ABORT] {sym}: Ryzyko ({monetary_risk:.2f}) > limit ({max_allowed_risk:.2f}). Natychmiastowy zrzut!")
                            flatten_ok = await okx_client.emergency_flatten_position(sym, pos_side)
                            if flatten_ok:
                                await redis_trade.delete_key(canonical_pos_key)
                            else:
                                logger.critical(f"🔥 [FLATTEN-FAILED] {sym}: Pozycja nadal na giełdzie! Zapis SOS_LOCKED.")
                                await redis_trade.set_position_state(canonical_pos_key, {
                                    "version": 1, "state": "SOS_LOCKED", "instId": sym, "posSide": pos_side, "updatedAt": time.time()
                                })
                                await redis_trade.set_system_pause(True)
                            await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                            continue
                        else:
                            price_sl, price_tp, sl_eff_pct = find_structural_h1_swing_sl(
                                candles_1h=c1h_closed_ref,
                                entry_price=real_fill_px,
                                pos_side=pos_side,
                                tick_sz=tick_sz,
                                lookback_bars=40,
                                pivot_radius=2,
                                cushion_pct=cushion_val,
                                min_sl_pct=0.012,
                                max_sl_pct=0.035,
                                rr_ratio=2.0
                            )

                    prot_state, real_algo_id = await okx_client.ensure_position_protection(
                        symbol=sym,
                        pos_side=pos_side,
                        real_size=real_size,
                        avg_px=real_fill_px,
                        price_tp=price_tp,
                        price_sl=price_sl,
                        tick_sz=tick_sz,
                        attach_algo_cl_ord_id=my_algo_cl_id
                    )

                    if prot_state != "PROTECTED" or not real_algo_id:
                        logger.critical(f"🔥 [UNPROTECTED-ABORT] {sym}: Ochrona OCO niepotwierdzona. Awaryjny zrzut pozycji.")
                        flatten_ok = await okx_client.emergency_flatten_position(sym, pos_side)
                        if flatten_ok:
                            await redis_trade.delete_key(canonical_pos_key)
                        else:
                            logger.critical(f"🔥 [FLATTEN-FAILED] {sym}: Pozycja bez ochrony nadal otwarta! Zapis SOS_LOCKED.")
                            await redis_trade.set_position_state(canonical_pos_key, {
                                "version": 1, "state": "SOS_LOCKED", "instId": sym, "posSide": pos_side, "updatedAt": time.time()
                            })
                            await redis_trade.set_system_pause(True)
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    if slippage > CONFIG["MAX_ENTRY_SLIPPAGE_PCT"]:
                        await okx_client.amend_algo_order(sym, real_algo_id, new_sl_trigger_px=format_px(price_sl, tick_sz))

                    canonical_record = {
                        "version": 1,
                        "state": "PROTECTED",
                        "strategy": strategy_name,
                        "instId": sym,
                        "posSide": pos_side,
                        "clOrdId": my_cl_ord_id,
                        "ordId": main_ord_id,
                        "algoClOrdId": my_algo_cl_id,
                        "algoId": real_algo_id,
                        "targetSize": contracts,
                        "realSize": real_size,
                        "entryPx": real_fill_px,
                        "tpPx": price_tp,
                        "slPx": price_sl,
                        "marginLocked": actual_pos_margin,
                        "beActivated": False,
                        "openedAt": time.time(),
                        "updatedAt": time.time(),
                        "parent": {"clOrdId": my_cl_ord_id, "ordId": main_ord_id, "state": "filled", "accFillSz": real_size, "avgPx": real_fill_px},
                        "position": {"realSize": real_size, "avgPx": real_fill_px, "marginLocked": actual_pos_margin},
                        "protection": {"algoClOrdId": my_algo_cl_id, "algoId": real_algo_id, "type": "oco", "slPx": price_sl, "tpPx": price_tp, "state": "effective"}
                    }
                    await redis_trade.set_position_state(canonical_pos_key, canonical_record)

                    await tg.push(
                        f"🎯 <b>[M15 CHOCH ENTRY: {conf['label']}]</b>\n"
                        f"Taktyka: <b>{strategy_name}</b> [{pos_side.upper()} 3x Izolowany]\n"
                        f"Kurs: <b>{real_fill_px} {QUOTE_CCY}</b> | Margines: ~{actual_pos_margin} {QUOTE_CCY}\n"
                        f"Kontrakty: <b>{format_sz(real_size)} sz</b>\n"
                        f"🎯 TP (2.0R): <code>{price_tp}</code> | 🛑 SL (H1 Swing): <code>{price_sl}</code>\n"
                        f"🛡️ <b>Stan: PROTECTED (OKX OCO Algo ID: {real_algo_id})</b>\n"
                        f"🐳 Smart Money: <code>{sm_note}</code>"
                    )
                    break
        except Exception as e:
            logger.error(f"❌ [M15 CHOCH SNIPER ERROR] {e}")
        finally:
            now_ts = time.time()
            rem_sec = now_ts % 900
            sleep_sec = (900.0 - rem_sec) + 4.0
            await interruptible_sleep(sleep_sec)


async def master_lock_heartbeat_worker(redis_trade):
    logger.info("👑 [MASTER-HEARTBEAT] Dedykowany heartbeat lidera uruchomiony.")
    while not (state.ctx.shutdown_event and state.ctx.shutdown_event.is_set()):
        await interruptible_sleep(15)
        if state.ctx.is_master:
            renewed = await redis_trade.renew_master_engine_lock(state.ctx.worker_id, ttl_seconds=60)
            if not renewed:
                logger.critical("🚨🚨🚨 [MASTER-LOCK-LOST] Utracono blokadę lidera! Przechodzenie w stan PASSIVE.")
                state.ctx.is_master = False
                if state.ctx.tg:
                    await state.ctx.tg.push("🚨🚨🚨 <b>[SPLIT-BRAIN PREVENTED]</b> Utracono blokadę Master — przechodzenie w stan PASSIVE.")
                spawn_supervised_task(passive_leader_sentry_worker, "passive_sentry_worker", state.ctx.redis_bridge, state.ctx.tg, state.ctx.okx_client, state.ctx.smart_money_oracle, tg=state.ctx.tg)
                break


async def master_bootstrap(redis_trade, okx_client, session):
    logger.info("🚀 [MASTER-BOOTSTRAP] Inicjalizacja środowiska Master Engine (M15 CHoCH Baseline)...")
    await redis_trade.init_sync()
    await okx_client.set_position_mode("long_short_mode")

    symbols_to_stream = []
    for item in FUTURES_INSTRUMENTS:
        resolved = await okx_client.auto_resolve_xperp_symbol(item["symbol"])
        item["symbol"] = resolved
        symbols_to_stream.append(resolved)
        await okx_client.load_instrument_specification(resolved)
        await okx_client.set_leverage(resolved, TARGET_LEVERAGE, "long")
        await okx_client.set_leverage(resolved, TARGET_LEVERAGE, "short")

    wallet_init = await okx_client.get_wallet_balances(QUOTE_CCY)
    init_eq = wallet_init.get("total_equity", DEFAULT_EQUITY_FALLBACK) if wallet_init else DEFAULT_EQUITY_FALLBACK
    today_str = datetime.now(UTC).strftime('%Y%m%d')
    base_eq_key = redis_trade._enforce_prefix(f"DAILY_START_EQUITY:{today_str}")
    try:
        cmd = ["SET", base_eq_key, str(init_eq), "EX", "172800", "NX"]
        async with session.post(f"{redis_trade.url}", json=cmd, headers=redis_trade.headers, timeout=4):
            pass
        logger.info(f"⚓ [EQUITY-BASELINE-SAVED] Kotwica kapitału dobowego: {init_eq} {QUOTE_CCY}")
    except Exception as e:
        logger.error(f"⚠️ [BASELINE-SAVE-ERR] {e}")

    try:
        req_pos_path = "/api/v5/account/positions?instType=FUTURES"
        headers_p = okx_client._get_headers("GET", req_pos_path)
        await okx_client.limiter_account.consume()
        async with session.get(f"{okx_client.base_url}{req_pos_path}", headers=headers_p, timeout=6) as r_p:
            p_data = await r_p.json()
            if p_data.get("code") == "0" and p_data.get("data") is not None:
                configured_symbols = {item["symbol"] for item in FUTURES_INSTRUMENTS}
                configured_symbols_with_open_pos = []
                for pos_item in p_data["data"]:
                    pos_sz = abs(float(pos_item.get("pos", 0.0) or 0.0))
                    pos_inst = pos_item.get("instId")
                    if pos_inst not in configured_symbols:
                        logger.warning(f"⚠️ [REHYDRATION-SKIP] Pomijanie obcego instrumentu: {pos_inst}")
                        continue

                    raw_side = pos_item.get("posSide", "long").lower()
                    raw_pos_float = float(pos_item.get("pos", 0.0) or 0.0)
                    pos_side = "long" if (raw_side == "long" or (raw_side == "net" and raw_pos_float > 0)) else "short"
                    avg_px = float(pos_item.get("avgPx", 0.0) or 0.0)
                    margin_val = float(pos_item.get("margin", 50.0) or 50.0)

                    if pos_sz > 0.0 and pos_inst:
                        configured_symbols_with_open_pos.append(pos_inst)
                        canonical_key = get_canonical_pos_key(redis_trade.prefix, pos_inst, pos_side)
                        existing_st = await redis_trade.get_position_state(canonical_key)
                        if not existing_st:
                            logger.info(f"🔍 [REHYDRATING-CANONICAL] Odtwarzanie stanu dla {pos_inst}:{pos_side}...")
                            pending_algos = await okx_client.get_pending_algo_orders(pos_inst, ord_type="oco")
                            spec = okx_client.instruments_cache.get(pos_inst, {"tickSz": 0.1})
                            tick_sz = spec["tickSz"]

                            matching_algos = [a for a in pending_algos if a.get("posSide", "").lower() == pos_side.lower()] if pending_algos else []
                            detected_algo_id = None
                            detected_tp, detected_sl = avg_px * 1.025, avg_px * 0.985

                            if matching_algos:
                                detected_algo_id = matching_algos[0].get("algoId")
                                detected_tp = float(matching_algos[0].get("tpTriggerPx", avg_px * 1.025))
                                detected_sl = float(matching_algos[0].get("slTriggerPx", avg_px * 0.985))
                            else:
                                logger.critical(f"🚨 [NAKED-REHYDRATION] Pozycja {pos_inst}:{pos_side} bez OCO! Uzbrajanie...")
                                p_sl = round_price_to_tick(avg_px * (1.0 - 0.018), tick_sz, "DOWN") if pos_side == "long" else round_price_to_tick(avg_px * (1.0 + 0.018), tick_sz, "UP")
                                p_tp = round_price_to_tick(avg_px * (1.0 + 0.018 * 2.0), tick_sz, "UP") if pos_side == "long" else round_price_to_tick(avg_px * (1.0 - 0.018 * 2.0), tick_sz, "DOWN")
                                oco_res = await okx_client.execute_futures_oco(pos_inst, pos_side, pos_sz, p_tp, p_sl, tick_sz=tick_sz)
                                if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                    detected_algo_id = oco_res["data"][0].get("algoId")
                                    detected_tp, detected_sl = p_tp, p_sl
                                else:
                                    await okx_client.emergency_flatten_position(pos_inst, pos_side)
                                    continue

                            rehydrated_record = {
                                "version": 1,
                                "state": "PROTECTED",
                                "strategy": "REHYDRATED_RECOVERY",
                                "instId": pos_inst,
                                "posSide": pos_side,
                                "clOrdId": "REHYD",
                                "ordId": "REHYD",
                                "algoClOrdId": "REHYD_ALGO",
                                "algoId": detected_algo_id,
                                "targetSize": pos_sz,
                                "realSize": pos_sz,
                                "entryPx": avg_px,
                                "tpPx": detected_tp,
                                "slPx": detected_sl,
                                "marginLocked": margin_val,
                                "beActivated": False,
                                "openedAt": time.time(),
                                "updatedAt": time.time()
                            }
                            await redis_trade.set_position_state(canonical_key, rehydrated_record)

                if len(configured_symbols_with_open_pos) == 0:
                    await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                    logger.info("🧹 [CLEAN-START] Portfel jest FLAT. Zdjęto blokadę Stagger Lock po restarcie.")
            elif p_data.get("code") == "0" and (not p_data.get("data") or len(p_data.get("data")) == 0):
                await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                logger.info("🧹 [CLEAN-START] Portfel jest FLAT. Zdjęto blokadę Stagger Lock po restarcie.")
    except Exception as exc_rehyd:
        logger.error(f"⚠️ [REHYDRATION-FAILED]: {exc_rehyd}")

    return symbols_to_stream


async def passive_leader_sentry_worker(redis_trade, tg, okx_client, smart_money_oracle):
    if state.ctx.is_master:
        return
    logger.info("👀 [PASSIVE-SENTRY] Węzeł pasywny nasłuchuje wolnego miejsca lidera...")
    while not (state.ctx.shutdown_event and state.ctx.shutdown_event.is_set()) and not state.ctx.is_master:
        await interruptible_sleep(10)
        if state.ctx.is_master or (state.ctx.shutdown_event and state.ctx.shutdown_event.is_set()):
            break
        acquired = await redis_trade.acquire_master_engine_lock(state.ctx.worker_id, ttl_seconds=60)
        if acquired:
            logger.info(f"👑 [LEADER-PROMOTION] Węzeł pasywny {state.ctx.worker_id} przejął rolę aktywnego MASTER Engine!")
            try:
                await master_bootstrap(redis_trade, okx_client, okx_client.session)
                state.ctx.is_master = True
                spawn_supervised_task(master_lock_heartbeat_worker, "master_heartbeat_worker", redis_trade, tg=tg)
                spawn_supervised_task(protection_reconciler_worker, "reconciler_worker", redis_trade, tg, okx_client)
                spawn_supervised_task(independent_4tf_sniper_worker, "sniper_4tf_worker", okx_client.session, redis_trade, tg, okx_client, smart_money_oracle, tg=tg)
                if tg:
                    await tg.push("👑 <b>[LEADER PROMOTION]</b> Proces przejmuje rolę aktywnego MASTER Engine po zwolnieniu blokady.")
                break
            except Exception as e:
                logger.error(f"❌ [PROMOTION-BOOTSTRAP-ERR] {e}")
                state.ctx.is_master = False


def spawn_supervised_task(coro_fn, name: str, *args, tg: Optional[TelegramThrottledDispatcher] = None) -> asyncio.Task:
    async def _supervisor_wrapper():
        restart_count = 0
        while not (state.ctx.shutdown_event and state.ctx.shutdown_event.is_set()):
            try:
                logger.info(f"🛡️ [SUPERVISOR] Uruchamianie '{name}' (cykl #{restart_count})...")
                await coro_fn(*args)
                if state.ctx.shutdown_event and state.ctx.shutdown_event.is_set():
                    break
                if name == "passive_sentry_worker" and state.ctx.is_master:
                    logger.info("🛡️ [SUPERVISOR] 'passive_sentry_worker' zakończył zadanie po pomyślnym awansie.")
                    break
                logger.warning(f"⚠️ [SUPERVISOR] '{name}' zakończył działanie. Wznowienie za 3s...")
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                logger.info(f"🛑 [SUPERVISOR] '{name}' zatrzymany sygnałem shutdown.")
                break
            except Exception as exc:
                restart_count += 1
                logger.critical(f"💥 [SUPERVISOR] Awaria w '{name}' (#{restart_count}): {exc}. Restart za 5s...")
                if tg:
                    await tg.push(f"💥 <b>[SUPERVISOR ALERT]</b> Awaria zadania <code>{name}</code>: {exc}")
                await asyncio.sleep(5)

    task = asyncio.create_task(_supervisor_wrapper(), name=name)
    state.ctx.background_tasks.add(task)
    task.add_done_callback(state.ctx.background_tasks.discard)
    return task


async def interruptible_sleep(seconds: float):
    if seconds <= 0:
        return
    end_time = time.monotonic() + seconds
    while time.monotonic() < end_time:
        if state.ctx.shutdown_event and state.ctx.shutdown_event.is_set():
            break
        step = min(1.0, end_time - time.monotonic())
        if step > 0:
            await asyncio.sleep(step)