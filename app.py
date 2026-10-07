import os
import asyncio
import math
import time
import logging
import json
import aiohttp
import msgpack
import signal
import threading
import hmac
import hashlib
import base64
import sys
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from typing import Dict, Any, List, Optional, Tuple, Set, Callable

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

class FlushStreamHandler(logging.StreamHandler):
    """Gwarantuje natychmiastowe wypychanie logow do konsoli Rendera bez buforowania."""
    def emit(self, record):
        super().emit(record)
        self.flush()

LOG_LEVEL_CONFIG = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("FuturesEngine_OKX_3X")
logger.setLevel(getattr(logging, LOG_LEVEL_CONFIG, logging.INFO))
logger.handlers.clear()

_stream_handler = FlushStreamHandler(sys.stdout)
_stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(_stream_handler)
logger.propagate = False

IS_SANDBOX = os.environ.get("OKX_IS_SANDBOX", "False").strip().lower() in ("true", "1", "yes")

DEFAULT_CCY = "USDC" if not IS_SANDBOX else "USD"
QUOTE_CCY = os.environ.get("QUOTE_CCY", DEFAULT_CCY).strip().upper()
TARGET_LEVERAGE = 3
TARGET_MARGIN_MODE = "isolated"

EMERGENCY_SECRET = os.environ.get("EMERGENCY_SECRET", "safe-kill-secret-2026").strip()

REDIS_PREFIX = "FUTURES_3X_DEMO_" if IS_SANDBOX else "FUTURES_3X_LIVE_"

logger.info(f"⚙️ [SYSTEM-INIT] Silnik Futures 3x v17.8-FINAL (SAFETY-REBUILD) Online [QUOTE: {QUOTE_CCY} | PREFIKS: {REDIS_PREFIX} | SANDBOX: {IS_SANDBOX}]")

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
GLOBAL_ALPHA_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
BACKGROUND_TASKS: Set[asyncio.Task] = set()

RATE_LIMITER_PUBLIC: Optional[Any] = None
RATE_LIMITER_TRADE: Optional[Any] = None
RATE_LIMITER_ACCOUNT: Optional[Any] = None

GLOBAL_WS_FEED: Optional[Any] = None
GLOBAL_OKX_CLIENT: Optional[Any] = None
GLOBAL_REDIS_BRIDGE: Optional[Any] = None
GLOBAL_TG: Optional[Any] = None
GLOBAL_SMART_MONEY_ORACLE: Optional[Any] = None

IS_MASTER_ENGINE_NODE = False
ENGINE_WORKER_ID = f"WORKER_{os.getpid()}_{int(time.time())}"

SHUTDOWN_COMPLETE = threading.Event()
PROCESS_DRAINING = threading.Event()
GLOBAL_TRADING_PAUSED = False

FUTURES_INSTRUMENTS = [
    {
        "symbol": "BTC-USD_UM_XPERP-310404" if not IS_SANDBOX else "BTC-USD_UM_XPERP-310328",
        "family": "BTC-USD_UM_XPERP",
        "base": "BTC",
        "label": "BTC_USD_XPERP",
        "price_round": 2
    },
    {
        "symbol": "ETH-USD_UM_XPERP-310404" if not IS_SANDBOX else "ETH-USD_UM_XPERP-310328",
        "family": "ETH-USD_UM_XPERP",
        "base": "ETH",
        "label": "ETH_USD_XPERP",
        "price_round": 2
    },
    {
        "symbol": "SOL-USD_UM_XPERP-310404" if not IS_SANDBOX else "SOL-USD_UM-260925",
        "family": "SOL-USD_UM_XPERP",
        "base": "SOL",
        "label": "SOL_USD_FUT",
        "price_round": 2
    },
    {
        "symbol": "XRP-USD_UM_XPERP-310404" if not IS_SANDBOX else "XRP-USD_UM_XPERP-310801",
        "family": "XRP-USD_UM_XPERP",
        "base": "XRP",
        "label": "XRP_USD_XPERP",
        "price_round": 4
    }
]

CONFIG = {
    "ALPHA_MAX_ACTIVE_SLOTS": 3,
    "MIN_ORDER_VALUE_QUOTE": 11.0,
    "RESERVE_CASH_BUFFER_QUOTE": 3.0,
    "RISK_PER_TRADE_PCT": 0.004,
    "MAX_POSITION_PORTFOLIO_RATIO": 0.15,
    "MAX_ENTRY_SLIPPAGE_PCT": 0.0030,
    "HARD_RISK_CAP_ON_SLIPPAGE_PCT": 0.0050,
    "DYNAMIC_RISK": {
        "MIN_SL_PCT": 0.006,
        "MAX_SL_HARD_CAP": 0.015,
        "DEFAULT_SL_PCT": 0.010,
        "VOLATILITY_CUSHION_PCT": 0.0015,
        "BREAK_EVEN_TRIGGER_RATIO": 0.75,
        "BREAK_EVEN_FEE_BUFFER_PCT": 0.0010
    },
    "TIMEOUTS": {
        "DIGITAL_TWIN_SNIPER": 8 * 3600,
        "REHYDRATED_RECOVERY": 8 * 3600,
        "TREND_PULLBACK": 6 * 3600,
        "VOLATILITY_BREAKOUT": 3 * 3600,
        "MEAN_REVERSION": 8 * 3600,
        "4TF_SNIPER_CORE": 8 * 3600
    },
    "SAFETY_GUARDS": {
        "PORTFOLIO_STAGGER_LOCK_SECONDS": 30 * 60,
        "SL_QUARANTINE_SECONDS": 75 * 60,
        "SL_COOLDOWN_SECONDS": 90 * 60,
        "MAX_SPREAD_PCT": 0.0020,
        "DAILY_CIRCUIT_BREAKER_PCT": 0.03,
        "SMART_MONEY": {
            "ENABLED": True,
            "MAX_TAKER_IMBALANCE_RATIO": 1.35,
            "CACHE_TTL_SECONDS": 180
        }
    }
}

# ==============================================================================
# NARZĘDZIA MATEMATYCZNE, FORMATOWANIE I WALIDATORY API
# ==============================================================================

def generate_cl_ord_id(prefix: str, base_symbol: str) -> str:
    """Generuje scisle alfanumeryczny identyfikator zgodny z OKX v5 ^[a-zA-Z0-9]{1,32}$."""
    clean_base = "".join(filter(str.isalnum, base_symbol))[:4].upper()
    ts_compact = hex(int(time.time() * 1000))[2:].upper()
    salt = os.urandom(2).hex().upper()
    return f"{prefix}{clean_base}{ts_compact}{salt}"[:32]

def get_canonical_pos_key(prefix: str, inst_id: str, pos_side: str) -> str:
    """Generuje kanoniczny klucz dla pozycji w trybie long_short_mode."""
    return f"{prefix}POS:{inst_id}:{pos_side.lower()}"

def validate_okx_response(
    res_json: Optional[Dict[str, Any]], 
    expected_id_field: Optional[str] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Rygorystyczny walidator API v5: wymaga spelnienia koniunkcji:
    HTTP 200 and code == '0' and data exists and data[0].sCode == '0'.
    """
    if not res_json or not isinstance(res_json, dict):
        return False, "EMPTY_OR_NON_JSON_RESPONSE", None
    
    top_code = str(res_json.get("code", "-1"))
    if top_code != "0":
        return False, f"TOP_LEVEL_ERROR_{top_code}: {res_json.get('msg')}", None
        
    data = res_json.get("data", [])
    if not data or not isinstance(data, list):
        return False, "EMPTY_DATA_PAYLOAD", None
        
    item = data[0]
    sub_code = str(item.get("sCode", "0"))
    if sub_code != "0":
        return False, f"SUB_LEVEL_SCODE_{sub_code}: {item.get('sMsg')}", item
        
    if expected_id_field and not item.get(expected_id_field):
        return False, f"MISSING_EXPECTED_FIELD_{expected_id_field}", item
        
    return True, "SUCCESS", item

def floor_to_lot(val: float, lot_sz: float) -> float:
    if lot_sz <= 0.0 or val <= 0.0:
        return 0.0
    v = Decimal(str(val))
    lot = Decimal(str(lot_sz))
    steps = (v / lot).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * lot)

def round_price_to_tick(price: float, tick_sz: float, direction: str = "NEAREST") -> float:
    if price <= 0.0 or tick_sz <= 0.0:
        return 0.0
    p = Decimal(str(price))
    t = Decimal(str(tick_sz))
    if direction == "DOWN":
        rounding = ROUND_DOWN
    elif direction == "UP":
        rounding = ROUND_UP
    else:
        rounding = ROUND_HALF_UP
    steps = (p / t).to_integral_value(rounding=rounding)
    return float(steps * t)

def format_sz(quantity: float) -> str:
    return f"{Decimal(str(quantity)):.8f}".rstrip('0').rstrip('.')

def format_px(price: float, tick_sz: float) -> str:
    if price <= 0.0 or tick_sz <= 0.0:
        return "0"
    p = Decimal(str(price))
    t = Decimal(str(tick_sz))
    steps = (p / t).to_integral_value(rounding=ROUND_DOWN)
    res = steps * t
    decimals = abs(t.as_tuple().exponent)
    return f"{res:.{decimals}f}"

def calc_ema(prices: List[float], period: int) -> List[float]:
    if not prices or len(prices) < period:
        return []
    emas = [sum(prices[:period]) / period]
    k = 2.0 / (period + 1.0)
    for p in prices[period:]:
        emas.append(p * k + emas[-1] * (1.0 - k))
    return emas

def calc_atr(candles: List[List[str]], period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    tr_list = []
    for i in range(1, len(candles)):
        h = float(candles[i][2])
        l = float(candles[i][3])
        prev_c = float(candles[i - 1][4])
        tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    if len(tr_list) < period:
        return 0.0
    return sum(tr_list[-period:]) / period

def calculate_clamped_sl_tp(
    current_price: float,
    atr: float,
    atr_mult: float,
    rr_ratio: float,
    tick_sz: float,
    pos_side: str = "long"
) -> Tuple[float, float, float]:
    """Wylicza poziomy SL i TP, po czym zwraca dokladny, efektywny procent SL po zaokragleniu do tickSz."""
    min_sl = CONFIG["DYNAMIC_RISK"]["MIN_SL_PCT"]
    max_sl = CONFIG["DYNAMIC_RISK"]["MAX_SL_HARD_CAP"]
    def_sl = CONFIG["DYNAMIC_RISK"]["DEFAULT_SL_PCT"]
    cushion_pct = CONFIG["DYNAMIC_RISK"].get("VOLATILITY_CUSHION_PCT", 0.0015)

    if atr > 0 and current_price > 0:
        base_sl_pct = (atr * atr_mult) / current_price
    else:
        base_sl_pct = def_sl

    raw_sl_pct = base_sl_pct + cushion_pct
    sl_pct = max(min_sl, min(raw_sl_pct, max_sl))
    tp_pct = sl_pct * rr_ratio

    if pos_side == "long":
        raw_sl = current_price * (1.0 - sl_pct)
        raw_tp = current_price * (1.0 + tp_pct)
        price_sl = round_price_to_tick(raw_sl, tick_sz, direction="DOWN")
        price_tp = round_price_to_tick(raw_tp, tick_sz, direction="UP")
    else:
        raw_sl = current_price * (1.0 + sl_pct)
        raw_tp = current_price * (1.0 - tp_pct)
        price_sl = round_price_to_tick(raw_sl, tick_sz, direction="UP")
        price_tp = round_price_to_tick(raw_tp, tick_sz, direction="DOWN")

    eff_sl_pct = abs(current_price - price_sl) / current_price if current_price > 0 else sl_pct
    eff_sl_pct = max(min_sl, min(eff_sl_pct, max_sl))

    return price_sl, price_tp, eff_sl_pct

# ==============================================================================
# FLASK & ZARZADZANIE OPERACYJNE
# ==============================================================================

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

def require_admin() -> bool:
    if not EMERGENCY_SECRET:
        return False
    supplied = request.headers.get("X-Admin-Secret", "").strip()
    if not supplied:
        supplied = request.args.get("secret", "").strip() or request.form.get("secret", "").strip()
    return hmac.compare_digest(supplied, EMERGENCY_SECRET)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or PROCESS_DRAINING.is_set():
        return "FUTURES_ENGINE_DRAINING", 503
    role = "MASTER" if IS_MASTER_ENGINE_NODE else "PASSIVE"
    return f"FUTURES_ENGINE_ONLINE_3X_{QUOTE_CCY} [{role}]", 200

@app.route('/status', methods=['GET'])
def engine_status_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403

    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE or not GLOBAL_OKX_CLIENT:
        return jsonify({"status": "starting", "engine": "INIT_PHASE"}), 503

    async def _gather_status():
        wallet = await GLOBAL_OKX_CLIENT.get_wallet_balances(QUOTE_CCY)
        active_keys = await GLOBAL_REDIS_BRIDGE.get_active_positions()
        daily_loss = await GLOBAL_REDIS_BRIDGE.get_daily_loss()
        
        today_str = datetime.now(UTC).strftime('%Y%m%d')
        start_equity = await GLOBAL_REDIS_BRIDGE.get_daily_start_equity(today_str)
        if start_equity is None or start_equity <= 0:
            start_equity = wallet.get("total_equity", 360.0) if wallet else 360.0

        positions_details = []
        for k in active_keys:
            st = await GLOBAL_REDIS_BRIDGE.get_position_state(k)
            if st:
                positions_details.append(st)

        is_sl_quarantine = await GLOBAL_REDIS_BRIDGE.is_cooldown_active("GLOBAL_PORTFOLIO_QUARANTINE")
        is_stagger_locked = await GLOBAL_REDIS_BRIDGE.is_cooldown_active("PORTFOLIO_STAGGER_LOCK")
        is_paused = await GLOBAL_REDIS_BRIDGE.is_system_paused_distributed()
        is_cb = await GLOBAL_REDIS_BRIDGE.is_circuit_breaker_active()
        prices_snapshot = GLOBAL_WS_FEED.get_prices_snapshot() if GLOBAL_WS_FEED else {}

        return {
            "status": "ONLINE",
            "version": "v17.8_FINAL_PROD",
            "engine_role": "MASTER" if IS_MASTER_ENGINE_NODE else "PASSIVE",
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

    fut = asyncio.run_coroutine_threadsafe(_gather_status(), BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=6)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/pause-trading', methods=['POST'])
def pause_trading_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_pause():
        global GLOBAL_TRADING_PAUSED
        async with GLOBAL_ALPHA_LOCK:
            GLOBAL_TRADING_PAUSED = True
            await GLOBAL_REDIS_BRIDGE.set_system_pause(True)
        return {"status": "success", "message": "Handel zostal wstrzymany (Paused = True)."}

    fut = asyncio.run_coroutine_threadsafe(_do_pause(), BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=5)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/resume-trading', methods=['POST'])
def resume_trading_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized"}), 403
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE or not GLOBAL_OKX_CLIENT:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_resume():
        async with GLOBAL_ALPHA_LOCK:
            # Weryfikacja Circuit Breakera w Redis
            cb_active = await GLOBAL_REDIS_BRIDGE.is_circuit_breaker_active()
            if cb_active:
                return {"status": "rejected", "message": "Circuit Breaker jest aktywny w bazie Redis. Zakaz wznowienia handlu przed 00:00 UTC."}

            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                sz_l = await GLOBAL_OKX_CLIENT.get_open_position_size(sym, "long")
                sz_s = await GLOBAL_OKX_CLIENT.get_open_position_size(sym, "short")
                has_p = await GLOBAL_OKX_CLIENT.has_pending_orders(sym)
                algos = await GLOBAL_OKX_CLIENT.get_pending_algo_orders(sym, ord_type="oco")

                if sz_l is None or sz_s is None or has_p is None or algos is None:
                    return {"status": "rejected", "message": f"Niepewny stan API dla {sym}. Odmowa wznowienia."}
                if has_p is True:
                    return {"status": "rejected", "message": f"Wykryto aktywne zlecenia oczekujace na {sym}."}
                if (sz_l > 0 or sz_s > 0) and len(algos) == 0:
                    return {"status": "rejected", "message": f"Wykryto naga pozycje na {sym} bez OCO!"}

            global GLOBAL_TRADING_PAUSED
            GLOBAL_TRADING_PAUSED = False
            await GLOBAL_REDIS_BRIDGE.set_system_pause(False)
            logger.info("🟢 [SAFETY-RESUME] Wznowiono handel po pozytywnej weryfikacji pre-flight.")
            return {"status": "success", "message": "Handel wznowiony pomyslnie."}

    fut = asyncio.run_coroutine_threadsafe(_do_resume(), BACKGROUND_LOOP)
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
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE or not GLOBAL_OKX_CLIENT:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_flush_slots():
        async with GLOBAL_ALPHA_LOCK:
            for item in FUTURES_INSTRUMENTS:
                sz_l = await GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], "long")
                sz_s = await GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], "short")
                if sz_l is None or sz_s is None:
                    return {"status": "rejected", "message": "Nieznany stan gieldy. Odmowa czyszczenia slotow."}
                if sz_l > 0 or sz_s > 0:
                    return {"status": "rejected", "message": f"Gielda posiada aktywna pozycje na {item['symbol']}."}
            deleted = await GLOBAL_REDIS_BRIDGE.reset_all_slots()
            return {"status": "success", "deleted_slots_count": deleted}

    fut = asyncio.run_coroutine_threadsafe(_do_flush_slots(), BACKGROUND_LOOP)
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
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_OKX_CLIENT or not GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Engine not ready"}), 503

    async def _do_emergency():
        async with GLOBAL_ALPHA_LOCK:
            global GLOBAL_TRADING_PAUSED
            GLOBAL_TRADING_PAUSED = True
            await GLOBAL_REDIS_BRIDGE.set_system_pause(True)

            report = {"closed_positions": [], "canceled_pending": [], "canceled_algos": [], "freed_slots": 0, "verified_flat": False}
            
            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                try:
                    await RATE_LIMITER_TRADE.consume()
                    req_path_p = f"/api/v5/trade/orders-pending?instType=FUTURES&instId={sym}"
                    headers_p = GLOBAL_OKX_CLIENT._get_headers("GET", req_path_p)
                    async with GLOBAL_OKX_CLIENT.session.get(f"{GLOBAL_OKX_CLIENT.base_url}{req_path_p}", headers=headers_p, timeout=4) as resp_p:
                        data_p = await resp_p.json()
                        if data_p.get("code") == "0" and data_p.get("data"):
                            for o in data_p["data"]:
                                ord_id = o.get("ordId")
                                c_body = json.dumps({"instId": sym, "ordId": ord_id})
                                await RATE_LIMITER_TRADE.consume()
                                c_headers = GLOBAL_OKX_CLIENT._get_headers("POST", "/api/v5/trade/cancel-order", c_body)
                                async with GLOBAL_OKX_CLIENT.session.post(f"{GLOBAL_OKX_CLIENT.base_url}/api/v5/trade/cancel-order", data=c_body, headers=c_headers, timeout=3):
                                    pass
                                report["canceled_pending"].append(f"{sym}:{ord_id}")
                except Exception as e:
                    logger.error(f"[EMERGENCY-CANCEL-PENDING] {sym}: {e}")

                try:
                    pending_algos = await GLOBAL_OKX_CLIENT.get_pending_algo_orders(sym, ord_type="oco")
                    if pending_algos:
                        for al in pending_algos:
                            al_id = al.get("algoId")
                            if al_id:
                                await GLOBAL_OKX_CLIENT.cancel_algo_order(sym, al_id)
                                report["canceled_algos"].append(f"{sym}:{al_id}")
                except Exception as e:
                    logger.error(f"[EMERGENCY-CANCEL-ALGO] {sym}: {e}")

            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                for side in ["long", "short"]:
                    flattened = await GLOBAL_OKX_CLIENT.emergency_flatten_position(sym, side)
                    if flattened:
                        report["closed_positions"].append(f"{sym}:{side}")

            all_flat = True
            for item in FUTURES_INSTRUMENTS:
                for side in ["long", "short"]:
                    rem = await GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], side)
                    has_p = await GLOBAL_OKX_CLIENT.has_pending_orders(item["symbol"])
                    algos = await GLOBAL_OKX_CLIENT.get_pending_algo_orders(item["symbol"], ord_type="oco")
                    if rem is None or rem > 0 or has_p is None or has_p or algos is None or len(algos) > 0:
                        all_flat = False

            report["verified_flat"] = all_flat
            if all_flat:
                freed = await GLOBAL_REDIS_BRIDGE.reset_all_slots()
                report["freed_slots"] = freed
                await GLOBAL_REDIS_BRIDGE.clear_cooldown("GLOBAL_PORTFOLIO_QUARANTINE")
                await GLOBAL_REDIS_BRIDGE.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                logger.info("✅ [EMERGENCY] Wszystkie zasoby FLAT. Sloty zwolnione.")

            if GLOBAL_TG:
                await GLOBAL_TG.push(
                    f"🚨🚨 <b>[AWARYJNA EWAKUACJA]</b> 🚨🚨\n"
                    f"Zamkniete: <code>{len(report['closed_positions'])}</code>\n"
                    f"Zwolnione sloty: <code>{report['freed_slots']}</code>\n"
                    f"Verified Flat: <b>{all_flat}</b>"
                )
            return report

    fut = asyncio.run_coroutine_threadsafe(_do_emergency(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=25)
        code = 200 if res.get("verified_flat") else 409
        return jsonify({"status": "COMPLETED" if res.get("verified_flat") else "FAILED_NOT_FLAT", "details": res}), code
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ==============================================================================
# TOKEN BUCKET RATE LIMITER
# ==============================================================================

class TokenBucketRateLimiter:
    def __init__(self, tokens_per_second: float = 4.0, max_capacity: float = 8.0):
        self.rate = tokens_per_second
        self.capacity = max_capacity
        self.tokens = max_capacity
        self.last_check = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None

    def _ensure_lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()

    async def consume(self):
        self._ensure_lock()
        wait_time = 0.0
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last_check) * self.rate)
            self.last_check = now
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                self.tokens = 0.0
                self.last_check = now + wait_time
            else:
                self.tokens -= 1.0
        if wait_time > 0.0:
            await asyncio.sleep(wait_time)

# ==============================================================================
# UPSTASH REDIS: KANONICZNY STAN, COMPARE-AND-RENEW I CIRCUIT BREAKER
# ==============================================================================

class UpstashRedisFuturesBridge:
    def __init__(self, url: str, token: str, session: aiohttp.ClientSession):
        self.url = url.rstrip('/') if url else ""
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        } if token else {}
        self.session = session
        self.prefix = REDIS_PREFIX

        self._local_cooldowns: Dict[str, float] = {}
        self._local_daily_loss: float = 0.0
        self._local_positions: Dict[str, Dict[str, Any]] = {}
        self._initialized: bool = False
        self._last_midnight_check: str = ""

    def _enforce_prefix(self, key: str) -> str:
        return key if key.startswith(self.prefix) else f"{self.prefix}{key}"

    def _safe_unpack_hex(self, hex_string: str) -> Optional[Dict[str, Any]]:
        if not hex_string or hex_string in ["None", "NULL", "none", "null"]:
            return None
        try:
            return msgpack.unpackb(bytes.fromhex(hex_string.strip()), strict_map_key=False)
        except Exception:
            return None

    def _check_midnight_rollover(self):
        current_day = datetime.now(UTC).strftime('%Y%m%d')
        if self._last_midnight_check and self._last_midnight_check != current_day:
            logger.info(f"📅 [MIDNIGHT-ROLLOVER] Zmiana doby UTC z {self._last_midnight_check} na {current_day}. Reset dziennej straty.")
            self._local_daily_loss = 0.0
        self._last_midnight_check = current_day

    async def init_sync(self):
        if self._initialized or not self.url:
            return
        try:
            self._check_midnight_rollover()
            today_str = datetime.now(UTC).strftime('%Y%m%d')
            safe_key = self._enforce_prefix(f"DAILY_LOSS:{today_str}")
            async with self.session.get(f"{self.url}/get/{safe_key}", headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    if res:
                        self._local_daily_loss = float(res)

            self._local_positions.clear()
            self._local_cooldowns.clear()

            # Kanoniczne skanowanie kluczy {PREFIX}POS:*
            pattern = f"{self.prefix}POS:*"
            async with self.session.get(f"{self.url}/keys/{pattern}", headers=self.headers, timeout=4) as r_k:
                if r_k.status == 200:
                    keys = (await r_k.json()).get("result", [])
                    for k in keys:
                        clean_k = k.replace(self.prefix, "")
                        async with self.session.get(f"{self.url}/get/{k}", headers=self.headers, timeout=4) as r_p:
                            if r_p.status == 200:
                                val_hex = (await r_p.json()).get("result")
                                if val_hex:
                                    pos_obj = self._safe_unpack_hex(val_hex)
                                    if pos_obj:
                                        self._local_positions[clean_k] = pos_obj

            self._initialized = True
            logger.info(f"💾 [REDIS-CACHE-INIT] Zsynchronizowano: Strata={self._local_daily_loss} | Pozycje Kanoniczne={len(self._local_positions)}")
        except Exception as e:
            logger.error(f"⚠️ [REDIS-INIT-SYNC-ERROR] {e}")

    async def acquire_master_engine_lock(self, worker_id: str, ttl_seconds: int = 180) -> bool:
        if not self.url:
            return True
        safe_key = self._enforce_prefix("ENGINE_ACTIVE_LOCK")
        try:
            cmd = ["SET", safe_key, worker_id, "EX", str(ttl_seconds), "NX"]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                data = await resp.json()
                return data.get("result") == "OK"
        except Exception as e:
            logger.error(f"[REDIS-ACQUIRE-LOCK-ERR] {e}")
            return False

    async def renew_master_engine_lock(self, worker_id: str, ttl_seconds: int = 180) -> bool:
        if not self.url:
            return True
        safe_key = self._enforce_prefix("ENGINE_ACTIVE_LOCK")
        lua_script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('EXPIRE', KEYS[1], ARGV[2]) "
            "else return 0 end"
        )
        cmd = ["EVAL", lua_script, "1", safe_key, worker_id, str(ttl_seconds)]
        try:
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                data = await resp.json()
                return data.get("result") == 1
        except Exception as e:
            logger.error(f"[REDIS-RENEW-LOCK-ERR] {e}")
            return False

    async def set_system_pause(self, paused: bool) -> bool:
        if not self.url:
            return True
        safe_key = self._enforce_prefix("SYSTEM_STATE:TRADING_PAUSED")
        val = "TRUE" if paused else "FALSE"
        try:
            cmd = ["SET", safe_key, val]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"[REDIS-SYSTEM-PAUSE-ERR] {e}")
            return False

    async def is_system_paused_distributed(self) -> bool:
        if not self.url:
            return GLOBAL_TRADING_PAUSED
        safe_key = self._enforce_prefix("SYSTEM_STATE:TRADING_PAUSED")
        try:
            cmd = ["GET", safe_key]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    return str(res).upper() == "TRUE"
        except Exception:
            if not IS_SANDBOX:
                return True
        return GLOBAL_TRADING_PAUSED

    async def set_circuit_breaker(self, active: bool) -> bool:
        if not self.url:
            return True
        safe_key = self._enforce_prefix("SYSTEM_STATE:CIRCUIT_BREAKER")
        val = "TRUE" if active else "FALSE"
        try:
            cmd = ["SET", safe_key, val, "EX", "86400"]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"[REDIS-CB-SET-ERR] {e}")
            return False

    async def is_circuit_breaker_active(self) -> bool:
        if not self.url:
            return False
        safe_key = self._enforce_prefix("SYSTEM_STATE:CIRCUIT_BREAKER")
        try:
            cmd = ["GET", safe_key]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    return str(res).upper() == "TRUE"
        except Exception:
            pass
        return False

    async def get_daily_start_equity(self, day_str: str) -> Optional[float]:
        if not self.url:
            return None
        safe_key = self._enforce_prefix(f"DAILY_START_EQUITY:{day_str}")
        try:
            cmd = ["GET", safe_key]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    if res is not None:
                        return float(res)
        except Exception as e:
            logger.error(f"[REDIS-GET-DAILY-EQUITY-ERR] {e}")
        return None

    async def set_position_state(self, pos_key: str, state_data: Dict[str, Any]) -> bool:
        clean_key = pos_key.replace(self.prefix, "")
        if not self.url:
            self._local_positions[clean_key] = state_data
            return True
        safe_key = self._enforce_prefix(pos_key)
        try:
            hex_str = msgpack.packb(state_data, use_bin_type=True).hex()
            cmd = ["SET", safe_key, hex_str, "EX", "604800"]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=5) as resp:
                if resp.status == 200:
                    self._local_positions[clean_key] = state_data
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS-POS-SAVE-ERROR] {e}")
            return False

    async def get_position_state(self, pos_key: str) -> Optional[Dict[str, Any]]:
        clean_key = pos_key.replace(self.prefix, "")
        return self._local_positions.get(clean_key)

    async def delete_key(self, key: str) -> bool:
        clean_key = key.replace(self.prefix, "")
        self._local_positions.pop(clean_key, None)
        if not self.url:
            return True
        safe_key = self._enforce_prefix(key)
        try:
            cmd = ["DEL", safe_key]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-DEL-ERROR] {e}")
            return False

    async def get_active_positions(self) -> List[str]:
        return [self._enforce_prefix(k) for k in self._local_positions.keys() if "POS:" in k]

    async def reset_all_slots(self) -> int:
        deleted = 0
        keys_to_delete = list(self._local_positions.keys())
        for k in keys_to_delete:
            if "POS:" in k:
                ok = await self.delete_key(k)
                if ok:
                    deleted += 1
        return deleted

    async def set_cooldown(self, base_symbol_or_key: str, ttl_seconds: int = 4500) -> bool:
        self._local_cooldowns[base_symbol_or_key] = time.time() + ttl_seconds
        if not self.url:
            return True
        safe_key = self._enforce_prefix(f"COOLDOWN:{base_symbol_or_key}")
        try:
            cmd = ["SET", safe_key, "ACTIVE", "EX", str(ttl_seconds)]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-COOLDOWN-ERROR] {e}")
            return False

    async def clear_cooldown(self, base_symbol_or_key: str) -> bool:
        self._local_cooldowns.pop(base_symbol_or_key, None)
        if not self.url:
            return True
        safe_key = self._enforce_prefix(f"COOLDOWN:{base_symbol_or_key}")
        try:
            cmd = ["DEL", safe_key]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-CLEAR-COOLDOWN] {e}")
            return False

    async def is_cooldown_active(self, base_symbol_or_key: str) -> bool:
        expiry = self._local_cooldowns.get(base_symbol_or_key, 0.0)
        if time.time() < expiry:
            return True
        if expiry > 0.0:
            self._local_cooldowns.pop(base_symbol_or_key, None)
        return False

    async def add_daily_loss(self, loss_amount: float) -> float:
        self._check_midnight_rollover()
        if loss_amount <= 0:
            return self._local_daily_loss
        self._local_daily_loss = round(self._local_daily_loss + loss_amount, 4)
        if not self.url:
            return self._local_daily_loss
        today_str = datetime.now(UTC).strftime('%Y%m%d')
        safe_key = self._enforce_prefix(f"DAILY_LOSS:{today_str}")
        try:
            cmd = ["SET", safe_key, str(self._local_daily_loss), "EX", "86400"]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4):
                pass
        except Exception as e:
            logger.error(f"❌ [REDIS-CIRCUIT-ERROR] {e}")
        return self._local_daily_loss

    async def get_daily_loss(self) -> float:
        self._check_midnight_rollover()
        return self._local_daily_loss

# ==============================================================================
# TELEGRAM DISPATCHER
# ==============================================================================

class TelegramThrottledDispatcher:
    def __init__(self, token: str, chat_id: str, session: aiohttp.ClientSession):
        self.token = token
        self.chat_id = chat_id
        self.session = session

    async def push(self, text: str):
        if not self.token or not self.chat_id:
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with self.session.post(url, json=payload, timeout=10) as response:
                await response.read()
        except Exception as e:
            logger.error(f"❌ [TELEGRAM-ERROR] {e}")

# ==============================================================================
# OKX SMART MONEY ORACLE
# ==============================================================================

class OKXSmartMoneyOracle:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter, is_sandbox: bool = False):
        self.session = session
        self.rate_limiter = rate_limiter
        self.is_sandbox = is_sandbox
        self.primary_url = os.environ.get("OKX_API_URL", "https://eea.okx.com").rstrip('/')
        self.fallback_url = "https://www.okx.com"
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.ttl = CONFIG["SAFETY_GUARDS"].get("SMART_MONEY", {}).get("CACHE_TTL_SECONDS", 180)
        self.enabled = CONFIG["SAFETY_GUARDS"].get("SMART_MONEY", {}).get("ENABLED", True)
        self.max_imbalance = CONFIG["SAFETY_GUARDS"].get("SMART_MONEY", {}).get("MAX_TAKER_IMBALANCE_RATIO", 1.35)

    async def _fetch_rubik_data(self, endpoint: str) -> Optional[List[Any]]:
        urls = [f"{self.primary_url}{endpoint}", f"{self.fallback_url}{endpoint}"]
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"

        for url in urls:
            try:
                await self.rate_limiter.consume()
                async with self.session.get(url, headers=headers, timeout=4) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("code") == "0" and data.get("data"):
                            return data.get("data")
            except Exception:
                continue
        return None

    async def get_taker_volume_flow(self, base_ccy: str) -> Dict[str, Any]:
        now = time.monotonic()
        cache_key = f"TAKER_{base_ccy}"
        if cache_key in self._cache and (now - self._cache[cache_key]["ts"] < self.ttl):
            return self._cache[cache_key]["data"]

        endpoint = f"/api/v5/rubik/stat/taker-volume?ccy={base_ccy}&instType=CONTRACTS&period=5m"
        raw_data = await self._fetch_rubik_data(endpoint)
        parsed = {"buy_vol": 0.0, "sell_vol": 0.0, "ratio": 1.0, "dominant": "NEUTRAL"}

        if raw_data and isinstance(raw_data, list) and len(raw_data) > 0:
            latest = raw_data[0]
            try:
                if isinstance(latest, list) and len(latest) >= 3:
                    buy_v = float(latest[1])
                    sell_v = float(latest[2])
                elif isinstance(latest, dict):
                    buy_v = float(latest.get("buyVol", 0.0))
                    sell_v = float(latest.get("sellVol", 0.0))
                else:
                    buy_v, sell_v = 0.0, 0.0

                total_v = buy_v + sell_v
                if total_v > 0:
                    ratio = round(buy_v / sell_v, 2) if sell_v > 0 else 2.0
                    dominant = "BUYERS" if ratio > 1.1 else ("SELLERS" if ratio < 0.9 else "NEUTRAL")
                    parsed = {"buy_vol": buy_v, "sell_vol": sell_v, "ratio": ratio, "dominant": dominant}
            except Exception:
                pass

        self._cache[cache_key] = {"data": parsed, "ts": now}
        return parsed

    async def check_smart_money_alignment(self, base_ccy: str, target_pos_side: str) -> Tuple[bool, str, Dict[str, Any]]:
        if not self.enabled:
            return True, "SM_BYPASS_DISABLED", {}

        flow = await self.get_taker_volume_flow(base_ccy)
        buy_v = flow.get("buy_vol", 0.0)
        sell_v = flow.get("sell_vol", 0.0)

        if buy_v == 0.0 and sell_v == 0.0:
            return True, "SM_DATA_NEUTRAL", flow

        if target_pos_side.lower() == "long":
            if sell_v > (buy_v * self.max_imbalance):
                return False, f"Aggressive Institutional Sell Pressure (Sell: {round(sell_v, 1)} > Buy: {round(buy_v, 1)})", flow
            return True, f"ZGODNY Z PRZEPLYWEM (Ratio: {flow.get('ratio')})", flow
        elif target_pos_side.lower() == "short":
            if buy_v > (sell_v * self.max_imbalance):
                return False, f"Aggressive Institutional Buy Absorption (Buy: {round(buy_v, 1)} > Sell: {round(sell_v, 1)})", flow
            return True, f"ZGODNY Z PRZEPLYWEM (Ratio: {flow.get('ratio')})", flow

        return True, "SM_ALIGNED", flow

# ==============================================================================
# OKX WEBSOCKET PRICE FEED
# ==============================================================================

class OKXWebSocketPriceFeed:
    def __init__(self, session: aiohttp.ClientSession, is_sandbox: bool = False):
        self.session = session
        self.is_sandbox = is_sandbox
        self.ws_endpoints = [
            "wss://wseeapap.okx.com:8443/ws/v5/public" if is_sandbox else "wss://wseea.okx.com:8443/ws/v5/public",
            "wss://wspap.okx.com:8443/ws/v5/public" if is_sandbox else "wss://ws.okx.com:8443/ws/v5/public"
        ]
        self.current_ep_index = 0
        self.latest_prices: Dict[str, Dict[str, Any]] = {}
        self._price_lock = threading.Lock()
        self.last_msg_time = time.monotonic()
        self._running: bool = False

    async def _ping_worker(self, ws):
        try:
            while not ws.closed and self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
                await asyncio.sleep(20)
                if not ws.closed:
                    await ws.send_str("ping")
        except Exception:
            pass

    async def start_listener(self, symbols: list):
        self._running = True
        sub_args = [{"channel": "tickers", "instId": sym} for sym in symbols]
        subscribe_msg = json.dumps({"op": "subscribe", "args": sub_args})

        while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            ws_url = self.ws_endpoints[self.current_ep_index % len(self.ws_endpoints)]
            try:
                logger.info(f"🌐 [WS-CONNECT] Laczenie z OKX SWAP EEA: {ws_url}...")
                async with self.session.ws_connect(ws_url, heartbeat=None) as ws:
                    await ws.send_str(subscribe_msg)
                    logger.info(f"📡 [WS-SUBSCRIBED] Subskrypcja aktywna dla {symbols}")
                    self.last_msg_time = time.monotonic()
                    ping_task = asyncio.create_task(self._ping_worker(ws))

                    try:
                        while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=45.0)
                            except asyncio.TimeoutError:
                                logger.warning("⚠️ [WS-WATCHDOG] Brak pakietow przez 45s. Przelaczanie serwera...")
                                self.current_ep_index += 1
                                break

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self.last_msg_time = time.monotonic()
                                if msg.data == "pong":
                                    continue
                                try:
                                    data = json.loads(msg.data)
                                except Exception:
                                    continue

                                if "data" in data and len(data["data"]) > 0:
                                    ticker = data["data"][0]
                                    inst_id = ticker.get("instId")
                                    last_price = ticker.get("last")
                                    if inst_id and last_price:
                                        with self._price_lock:
                                            self.latest_prices[inst_id] = {
                                                "price": float(last_price),
                                                "ts": time.monotonic()
                                            }
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning("⚠️ [WS-DISCONNECTED] Gniazdo zamkniete. Nastepny serwer...")
                                self.current_ep_index += 1
                                break
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logger.error(f"❌ [WS-ERROR] Blad strumienia ({ws_url}): {e}. Wznawianie za 5s...")
                self.current_ep_index += 1
                await asyncio.sleep(5)

    def get_last_price(self, symbol: str) -> Optional[float]:
        with self._price_lock:
            data = self.latest_prices.get(symbol)
        if not data:
            return None
        if (time.monotonic() - data["ts"]) > 5.0 and not self.is_sandbox:
            return None
        return data["price"]

    def get_prices_snapshot(self) -> Dict[str, float]:
        with self._price_lock:
            return {k: v["price"] for k, v in self.latest_prices.items()}

# ==============================================================================
# OKX FUTURES CLIENT: API V5 Z WALIDACJA SCODE I DETERMINISTYCZNA OCHRONA
# ==============================================================================

class OKXFuturesClient:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        limiter_trade: TokenBucketRateLimiter,
        limiter_public: TokenBucketRateLimiter,
        limiter_account: TokenBucketRateLimiter,
        is_sandbox: bool = False
    ):
        self.base_url = os.environ.get("OKX_API_URL", "https://eea.okx.com").rstrip('/')
        self.session = session
        self.limiter_trade = limiter_trade
        self.limiter_public = limiter_public
        self.limiter_account = limiter_account
        self.is_sandbox = is_sandbox

        self.api_key = os.environ.get("OKX_API_KEY", "").strip()
        self.secret_key = os.environ.get("OKX_SECRET_KEY", "").strip()
        self.passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()

        self.instruments_cache: Dict[str, Dict[str, Any]] = {}
        self.TARGET_LEVERAGE = TARGET_LEVERAGE
        self.MARGIN_MODE = TARGET_MARGIN_MODE

    def _generate_timestamp(self) -> str:
        return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = f"{timestamp}{method.upper()}{request_path}{body}"
        mac = hmac.new(self.secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode('utf-8')

    def _get_headers(self, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        timestamp = self._generate_timestamp()
        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase
        }
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        return headers

    async def set_position_mode(self, pos_mode: str = "long_short_mode") -> bool:
        await self.limiter_trade.consume()
        request_path = "/api/v5/account/set-position-mode"
        body = json.dumps({"posMode": pos_mode})
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body)
        try:
            async with self.session.post(url, data=body, headers=headers, timeout=5) as resp:
                data = await resp.json()
                ok, msg, _ = validate_okx_response(data)
                return ok or "already" in str(data.get("msg", "")).lower()
        except Exception as e:
            logger.error(f"[FUTURES-CONFIG] Blad trybu pozycji: {e}")
            return False

    async def auto_resolve_xperp_symbol(self, family_or_symbol: str) -> str:
        await self.limiter_public.consume()
        request_path = "/api/v5/public/instruments?instType=FUTURES"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        try:
            async with self.session.get(url, headers=headers, timeout=6) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    clean_target = family_or_symbol.split('-')[0].upper()
                    for item in data["data"]:
                        inst_id = item.get("instId", "")
                        state = item.get("state", "")
                        if state == "live" and clean_target in inst_id.upper() and "XPERP" in inst_id.upper():
                            logger.info(f"🎯 [AUTO-DISCOVERY] Dopasowano symbol Live: {inst_id}")
                            return inst_id
        except Exception as e:
            logger.warning(f"⚠️ [AUTO-DISCOVERY-FALLBACK] {e}")
        return family_or_symbol

    async def load_instrument_specification(self, symbol: str) -> Optional[Dict[str, Any]]:
        types = ["FUTURES", "SWAP"]
        for inst_type in types:
            await self.limiter_public.consume()
            request_path = f"/api/v5/public/instruments?instType={inst_type}&instId={symbol}"
            url = f"{self.base_url}{request_path}"
            headers = {"Content-Type": "application/json"}
            if self.is_sandbox:
                headers["x-simulated-trading"] = "1"
            try:
                async with self.session.get(url, headers=headers, timeout=5) as resp:
                    data = await resp.json()
                    if data.get("code") == "0" and data.get("data"):
                        item = data["data"][0]
                        spec = {
                            "instId": item.get("instId"),
                            "instType": item.get("instType", inst_type),
                            "ctVal": float(item.get("ctVal", 1.0)),
                            "ctValCcy": item.get("ctValCcy", ""),
                            "minSz": float(item.get("minSz", 0.01)),
                            "lotSz": float(item.get("lotSz", 0.01)),
                            "tickSz": float(item.get("tickSz", 0.1)),
                            "settleCcy": item.get("settleCcy", QUOTE_CCY)
                        }
                        self.instruments_cache[symbol] = spec
                        logger.info(f"📋 [SPEC-LOADED] {symbol} | tickSz: {spec['tickSz']} | lotSz: {spec['lotSz']}")
                        return spec
            except Exception as e:
                logger.error(f"[FUTURES-SPEC] {symbol}: {e}")
        return None

    async def set_leverage(self, symbol: str, leverage: int = 3, pos_side: str = "long") -> bool:
        await self.limiter_trade.consume()
        request_path = "/api/v5/account/set-leverage"
        body = json.dumps({"instId": symbol, "lever": str(leverage), "mgnMode": self.MARGIN_MODE, "posSide": pos_side})
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body)
        try:
            async with self.session.post(url, data=body, headers=headers, timeout=5) as resp:
                data = await resp.json()
                ok, msg, _ = validate_okx_response(data)
                return ok or "not modified" in str(data.get("msg", "")).lower()
        except Exception as e:
            logger.error(f"[FUTURES-LEVERAGE] {symbol}: {e}")
            return False

    async def get_wallet_balances(self, preferred_ccy: str = QUOTE_CCY) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_account.consume()
        request_path = "/api/v5/account/balance"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=6) as resp:
                data = await resp.json()
                ok, msg, item = validate_okx_response(data)
                if not ok or not item:
                    return None
                
                total_eq = float(item.get("totalEq", 0.0) or 0.0)
                balances_map = {}
                for b in item.get("details", []):
                    c = b.get("ccy", "").upper()
                    avail_e = float(b.get("availEq", 0.0) or 0.0)
                    avail_b = float(b.get("availBal", 0.0) or 0.0)
                    cash_b = float(b.get("cashBal", 0.0) or 0.0)
                    best_avail = avail_e if avail_e > 0 else (avail_b if avail_b > 0 else cash_b)
                    balances_map[c] = {"availBal": best_avail, "eq": float(b.get("eq", 0.0) or 0.0)}

                avail_cash = 0.0
                for check_c in [preferred_ccy, "USDC", "USD", "USDT"]:
                    if check_c in balances_map and balances_map[check_c]["availBal"] > 0:
                        avail_cash = balances_map[check_c]["availBal"]
                        break
                return {"total_equity": round(total_eq, 2), "available_cash": round(avail_cash, 2), "balances": balances_map}
        except Exception as e:
            logger.error(f"[WALLET-EXCEPTION] {e}")
        return None

    def calculate_contract_size(
        self,
        symbol: str,
        current_price: float,
        target_margin_quote: float,
        max_allowed_margin: float
    ) -> Tuple[float, float]:
        spec = self.instruments_cache.get(symbol)
        if not spec or current_price <= 0:
            return 0.0, 0.0

        ct_val = float(spec.get("ctVal", 1.0))
        ct_val_ccy = str(spec.get("ctValCcy", "")).upper()
        min_sz = float(spec.get("minSz", 0.01))
        lot_sz = float(spec.get("lotSz", 0.01))

        base_ccy = symbol.split('-')[0].upper()
        if ct_val_ccy in [base_ccy, "BTC", "ETH", "SOL", "XRP"]:
            contract_nominal_quote = ct_val * current_price
        elif ct_val_ccy in ["USD", "USDC", "USDT"]:
            contract_nominal_quote = ct_val
        else:
            return 0.0, 0.0

        if contract_nominal_quote <= 0:
            return 0.0, 0.0

        single_contract_margin = contract_nominal_quote / self.TARGET_LEVERAGE
        min_margin = min_sz * single_contract_margin
        if min_margin > target_margin_quote or min_margin > max_allowed_margin:
            return 0.0, 0.0

        target_nominal = target_margin_quote * self.TARGET_LEVERAGE
        raw_contracts = target_nominal / contract_nominal_quote
        contracts = floor_to_lot(raw_contracts, lot_sz)

        if contracts < min_sz:
            return 0.0, 0.0

        actual_margin = (contracts * contract_nominal_quote) / self.TARGET_LEVERAGE
        if actual_margin > target_margin_quote or actual_margin > max_allowed_margin:
            return 0.0, 0.0

        return contracts, round(actual_margin, 2)

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        if GLOBAL_WS_FEED:
            ws_price = GLOBAL_WS_FEED.get_last_price(symbol)
            if ws_price and ws_price > 0.0:
                return {"source": "OKX_WS", "symbol": symbol, "last": ws_price}
        await self.limiter_public.consume()
        request_path = f"/api/v5/market/ticker?instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return {"source": "OKX_REST", "symbol": symbol, "last": float(data["data"][0].get("last", 0.0))}
        except Exception as e:
            logger.error(f"[OKX-TICKER] {e}")
        return None

    async def get_macro_candles_raw(self, symbol: str, bar: str = "15m", limit: int = 100) -> List[List[str]]:
        await self.limiter_public.consume()
        request_path = f"/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return list(reversed(data["data"]))
        except Exception as e:
            logger.error(f"[OKX-CANDLES] {e}")
        return []

    async def check_spread_allowed(self, symbol: str, max_spread_pct: float = 0.0020) -> Tuple[bool, float]:
        await self.limiter_public.consume()
        request_path = f"/api/v5/market/ticker?instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        try:
            async with self.session.get(url, headers=headers, timeout=4) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    t = data["data"][0]
                    bid = float(t.get("bidPx", 0.0) or 0.0)
                    ask = float(t.get("askPx", 0.0) or 0.0)
                    if bid <= 0 or ask <= 0:
                        return False, 0.0
                    spread_pct = (ask - bid) / bid
                    return (spread_pct <= max_spread_pct), spread_pct
        except Exception:
            pass
        return False, 0.0

    async def execute_futures_order(
        self,
        symbol: str,
        side: str,
        pos_side: str,
        quantity: float,
        ord_type: str = "market",
        price: Optional[float] = None,
        reduce_only: bool = False,
        tick_sz: float = 0.1,
        attached_tp: Optional[float] = None,
        attached_sl: Optional[float] = None,
        cl_ord_id: Optional[str] = None,
        attach_algo_cl_ord_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/order"

        order_cl_id = cl_ord_id or generate_cl_ord_id("E", symbol.split('-')[0])

        body_dict: Dict[str, Any] = {
            "instId": symbol,
            "tdMode": self.MARGIN_MODE,
            "side": side.lower(),
            "posSide": pos_side.lower(),
            "ordType": ord_type.lower(),
            "sz": format_sz(quantity),
            "reduceOnly": reduce_only,
            "clOrdId": order_cl_id
        }
        if ord_type == "limit" and price is not None:
            body_dict["px"] = str(price)

        if attached_tp is not None and attached_sl is not None:
            protection_cl_id = attach_algo_cl_ord_id or generate_cl_ord_id("P", symbol.split('-')[0])
            body_dict["attachAlgoOrds"] = [{
                "attachAlgoClOrdId": protection_cl_id,
                "tpTriggerPx": format_px(attached_tp, tick_sz),
                "tpTriggerPxType": "last",
                "tpOrdPx": "-1",
                "slTriggerPx": format_px(attached_sl, tick_sz),
                "slTriggerPxType": "mark",
                "slOrdPx": "-1"
            }]

        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res_json = await r.json()
                ok, msg, _ = validate_okx_response(res_json, expected_id_field="ordId")
                if not ok:
                    logger.error(f"❌ [OKX-ORDER-REJECTED] {symbol}: {msg}")
                return res_json
        except Exception as e:
            logger.error(f"❌ [OKX-ORDER-ERROR] {symbol}: {e}")
            return None

    async def execute_futures_oco(
        self,
        symbol: str,
        pos_side: str,
        quantity: float,
        price_tp: float,
        price_sl: float,
        tick_sz: float = 0.1,
        algo_cl_ord_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/order-algo"
        exit_side = "sell" if pos_side == "long" else "buy"
        
        algo_cl_id = algo_cl_ord_id or generate_cl_ord_id("P", symbol.split('-')[0])
        body_dict = {
            "instId": symbol,
            "tdMode": self.MARGIN_MODE,
            "side": exit_side,
            "posSide": pos_side,
            "ordType": "oco",
            "sz": format_sz(quantity),
            "reduceOnly": True,
            "algoClOrdId": algo_cl_id,
            "tpTriggerPx": format_px(price_tp, tick_sz),
            "tpTriggerPxType": "last",
            "tpOrdPx": "-1",
            "slTriggerPx": format_px(price_sl, tick_sz),
            "slTriggerPxType": "mark",
            "slOrdPx": "-1"
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res_json = await r.json()
                ok, msg, _ = validate_okx_response(res_json, expected_id_field="algoId")
                if not ok:
                    logger.error(f"❌ [OKX-OCO-REJECTED] {symbol}: {msg}")
                return res_json
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-ERROR] {symbol}: {e}")
            return None

    async def amend_algo_order(
        self,
        symbol: str,
        algo_id: str,
        new_sl_trigger_px: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not self.api_key or algo_id == "EXT_MANUAL":
            return None
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/amend-algos"
        body_dict: Dict[str, Any] = {"instId": symbol, "algoId": str(algo_id)}
        if new_sl_trigger_px is not None:
            body_dict["newSlTriggerPx"] = str(new_sl_trigger_px)
            body_dict["newSlOrdPx"] = "-1"

        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"❌ [AMEND-ALGO-ERROR] {e}")
            return None

    async def cancel_algo_order(self, symbol: str, algo_id: str) -> bool:
        if not self.api_key or algo_id == "EXT_MANUAL":
            return False
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/cancel-algos"
        body = json.dumps([{"instId": symbol, "algoId": str(algo_id)}])
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body)
        try:
            async with self.session.post(url, data=body, headers=headers, timeout=5) as r:
                data = await r.json()
                ok, _, _ = validate_okx_response(data)
                return ok
        except Exception:
            return False

    async def cancel_regular_order(self, symbol: str, ord_id: str) -> bool:
        if not self.api_key:
            return False
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/cancel-order"
        body = json.dumps({"instId": symbol, "ordId": str(ord_id)})
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body)
        try:
            async with self.session.post(url, data=body, headers=headers, timeout=4) as r:
                data = await r.json()
                ok, _, _ = validate_okx_response(data)
                return ok
        except Exception:
            return False

    async def get_open_position_size(self, symbol: str, pos_side: str) -> Optional[float]:
        if not self.api_key:
            return None
        await self.limiter_account.consume()
        request_path = f"/api/v5/account/positions?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") != "0":
                    return None
                for p in data.get("data", []):
                    raw_pos = float(p.get("pos", 0.0) or 0.0)
                    raw_side = p.get("posSide", "").lower()
                    if raw_side == pos_side.lower():
                        return abs(raw_pos)
                    if raw_side == "net":
                        if pos_side.lower() == "long" and raw_pos > 0:
                            return raw_pos
                        elif pos_side.lower() == "short" and raw_pos < 0:
                            return abs(raw_pos)
                return 0.0
        except Exception as e:
            logger.critical(f"[POSITION-CHECK-UNKNOWN] {symbol}: {e}")
            return None

    async def get_position_details(self, symbol: str, pos_side: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_account.consume()
        request_path = f"/api/v5/account/positions?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") != "0":
                    return None
                for p in data.get("data", []):
                    raw_pos = float(p.get("pos", 0.0) or 0.0)
                    raw_side = p.get("posSide", "").lower()
                    if raw_side == pos_side.lower() or (raw_side == "net" and ((pos_side.lower()=="long" and raw_pos>0) or (pos_side.lower()=="short" and raw_pos<0))):
                        return {
                            "size": abs(raw_pos),
                            "avgPx": float(p.get("avgPx", 0.0) or 0.0),
                            "margin": float(p.get("margin", 0.0) or 0.0)
                        }
                return {"size": 0.0, "avgPx": 0.0, "margin": 0.0}
        except Exception:
            return None

    async def has_pending_orders(self, symbol: str) -> Optional[bool]:
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = f"/api/v5/trade/orders-pending?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data") is not None:
                    return len(data["data"]) > 0
                return None
        except Exception:
            return None

    async def get_pending_algo_orders(self, symbol: str, ord_type: str = "oco") -> Optional[List[Dict[str, Any]]]:
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = f"/api/v5/trade/orders-algo-pending?instType=FUTURES&instId={symbol}&ordType={ord_type}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data") is not None:
                    return data["data"]
                return None
        except Exception:
            return None

    async def get_algo_order_state(
        self,
        algo_id: Optional[str] = None,
        algo_cl_ord_id: Optional[str] = None
    ) -> Tuple[str, Optional[float], Optional[str]]:
        """
        Zwraca krotke trzystanowa: (stan, cena_wyzwalania, algo_id).
        Stany mozliwe: live, canceled, order_failed, effective, NOT_FOUND, UNKNOWN.
        """
        if not self.api_key:
            return "UNKNOWN", None, None
        await self.limiter_trade.consume()

        if algo_cl_ord_id:
            request_path = f"/api/v5/trade/order-algo?algoClOrdId={algo_cl_ord_id}"
        elif algo_id and algo_id != "EXT_MANUAL":
            request_path = f"/api/v5/trade/order-algo?algoId={algo_id}"
        else:
            return "UNKNOWN", None, None

        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=4) as resp:
                res = await resp.json()
                code = str(res.get("code", "-1"))
                if code in ("51400", "51401"):
                    return "NOT_FOUND", None, None
                if code == "0" and res.get("data"):
                    item = res["data"][0]
                    state = str(item.get("state", "live")).lower()
                    found_id = str(item.get("algoId", ""))
                    px_str = item.get("actualPx") or item.get("tpTriggerPx") or item.get("slTriggerPx") or "0"
                    actual_px = float(px_str) if px_str else 0.0
                    return state, actual_px, found_id
                return "UNKNOWN", None, None
        except Exception:
            return "UNKNOWN", None, None

    async def get_last_closed_position(self, symbol: str, min_close_time_ms: int = 0) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_account.consume()
        request_path = f"/api/v5/account/positions-history?instType=FUTURES&instId={symbol}&limit=1"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data") and len(data["data"]) > 0:
                    p = data["data"][0]
                    c_time = int(p.get("uTime") or p.get("cTime") or 0)
                    if min_close_time_ms > 0 and c_time < (min_close_time_ms - 5000):
                        return None
                    return {
                        "close_avg_px": float(p.get("closeAvgPx", 0.0) or 0.0),
                        "realized_pnl": float(p.get("realizedPnl", 0.0) or 0.0),
                        "pnl_ratio": float(p.get("pnlRatio", 0.0) or 0.0) * 100.0
                    }
        except Exception:
            pass
        return None

    async def get_parent_order_state(self, symbol: str, ord_id: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = f"/api/v5/trade/order?instId={symbol}&ordId={ord_id}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=4) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data") and len(data["data"]) > 0:
                    item = data["data"][0]
                    return {
                        "state": item.get("state", "live"),
                        "accFillSz": float(item.get("accFillSz", 0.0) or 0.0),
                        "avgPx": float(item.get("avgPx", 0.0) or 0.0)
                    }
                return None
        except Exception:
            return None

    async def resolve_unknown_order_by_cl_id(self, symbol: str, cl_ord_id: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        for attempt in range(max_retries):
            await self.limiter_trade.consume()
            request_path = f"/api/v5/trade/order?instId={symbol}&clOrdId={cl_ord_id}"
            url = f"{self.base_url}{request_path}"
            headers = self._get_headers("GET", request_path)
            try:
                async with self.session.get(url, headers=headers, timeout=4) as resp:
                    data = await resp.json()
                    if data.get("code") == "0" and data.get("data") and len(data["data"]) > 0:
                        return data["data"][0]
            except Exception:
                pass
            await asyncio.sleep(0.5 * (attempt + 1))
        return None

    async def emergency_flatten_position(self, symbol: str, pos_side: str, max_attempts: int = 3) -> bool:
        for attempt in range(max_attempts):
            rem = await self.get_open_position_size(symbol, pos_side)
            if rem is None:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            if rem <= 0.0:
                return True
            exit_side = "sell" if pos_side == "long" else "buy"
            await self.execute_futures_order(symbol, side=exit_side, pos_side=pos_side, quantity=rem, ord_type="market", reduce_only=True)
            await asyncio.sleep(0.5)

        final_sz = await self.get_open_position_size(symbol, pos_side)
        return final_sz == 0.0

    async def ensure_position_protection(
        self,
        symbol: str,
        pos_side: str,
        real_size: float,
        avg_px: float,
        price_tp: float,
        price_sl: float,
        tick_sz: float,
        attach_algo_cl_ord_id: str
    ) -> Tuple[str, Optional[str]]:
        """
        Deterministyczny zarzadca ochrony pozycji.
        Zwraca krotke: (status_ochrony, real_algo_id).
        Nigdy nie sklada Standalone OCO w stanie UNKNOWN!
        """
        for delay in (0.25, 0.50, 1.00):
            await asyncio.sleep(delay)
            algo_state, actual_px, found_id = await self.get_algo_order_state(
                algo_cl_ord_id=attach_algo_cl_ord_id
            )
            if algo_state not in ["UNKNOWN", "NOT_FOUND"] and found_id:
                return "PROTECTED", found_id

        pending_algos = await self.get_pending_algo_orders(symbol, ord_type="oco")
        if pending_algos is None:
            logger.critical(f"🚨 [PROTECTION-UNKNOWN] {symbol}: Stan OCO nieznany (Timeout/Blad API). Zakaz skladania drugiego OCO!")
            return "PROTECTION_UNKNOWN", None

        matched = next((a for a in pending_algos if a.get("algoClOrdId") == attach_algo_cl_ord_id), None)
        if matched and matched.get("algoId"):
            return "PROTECTED", matched.get("algoId")

        logger.warning(f"⚠️ [ATTACHED-NOT-FOUND] {symbol}: Brak attached OCO potwierdzony. Skladanie Standalone OCO...")
        standalone_res = await self.execute_futures_oco(
            symbol=symbol,
            pos_side=pos_side,
            quantity=real_size,
            price_tp=price_tp,
            price_sl=price_sl,
            tick_sz=tick_sz,
            algo_cl_ord_id=attach_algo_cl_ord_id
        )
        ok, msg, item = validate_okx_response(standalone_res, expected_id_field="algoId")
        if ok and item:
            return "PROTECTED", item.get("algoId")
        
        return "RECOVERY_REQUIRED", None

# ==============================================================================
# NADZOR I RECONCILER (PROTECTION RECONCILER WORKER)
# ==============================================================================

async def reconcile_and_timestop_futures(
    inst: Dict[str, Any],
    pos_side: str,
    redis_trade: UpstashRedisFuturesBridge,
    tg: TelegramThrottledDispatcher
) -> Tuple[bool, Optional[str]]:
    """Obsluguje kanoniczny rekord pozycji per instId oraz posSide."""
    pos_key = get_canonical_pos_key(redis_trade.prefix, inst["symbol"], pos_side)
    pos_data = await redis_trade.get_position_state(pos_key)
    if not pos_data:
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

    max_timeout = CONFIG["TIMEOUTS"].get(pos_data.get("strategy", "4TF_SNIPER_CORE"), 28800)

    # 1. POGROMCA WIDM LUB ZAMKNIECIE NA OKX
    if actual_pos_on_exchange == 0.0:
        pos_data["state"] = "FLAT_PENDING_SETTLEMENT"
        await redis_trade.set_position_state(pos_key, pos_data)

        real_pos_history = None
        for _ in range(3):
            real_pos_history = await inst["client"].get_last_closed_position(inst["symbol"], min_close_time_ms=int(opened_at * 1000))
            if real_pos_history:
                break
            await asyncio.sleep(1.0)

        await redis_trade.delete_key(pos_key)

        if real_pos_history and real_pos_history.get("close_avg_px", 0.0) > 0.0:
            exit_p = real_pos_history["close_avg_px"]
            pnl_net = round(real_pos_history["realized_pnl"], 2)
            roe_net = round(real_pos_history["pnl_ratio"], 2)
        else:
            exit_p = actual_px if actual_px and actual_px > 0 else (sl_p if pos_side == "long" else tp_p)
            spec = inst["client"].instruments_cache.get(inst["symbol"], {"ctVal": 1.0})
            ct_val = spec["ctVal"]
            mult = 1.0 if pos_side == "long" else -1.0
            pnl_gross = (exit_p - entry_p) * mult * contracts * ct_val
            pnl_net = round(pnl_gross - (margin_locked * 0.001), 2)
            roe_net = round((pnl_net / margin_locked) * 100.0, 2) if margin_locked > 0 else 0.0

        icon = "🎉 <b>[ZYSK TAKE PROFIT]" if pnl_net >= 0 else "🛑 <b>[STOP LOSS / WYJSCIE]"
        cooldown_msg = ""
        if pnl_net < 0:
            await redis_trade.set_cooldown("GLOBAL_PORTFOLIO_QUARANTINE", CONFIG["SAFETY_GUARDS"]["SL_QUARANTINE_SECONDS"])
            await redis_trade.set_cooldown(inst["base"], CONFIG["SAFETY_GUARDS"]["SL_COOLDOWN_SECONDS"])
            cooldown_msg = "\n⏳ Nalozono globalna kwarantanne po stracie."
            
            accum_loss = await redis_trade.add_daily_loss(abs(pnl_net))
            today_str = datetime.now(UTC).strftime('%Y%m%d')
            start_eq = await redis_trade.get_daily_start_equity(today_str)
            if start_eq is None or start_eq <= 0:
                wallet_cb = await inst["client"].get_wallet_balances(QUOTE_CCY)
                start_eq = wallet_cb.get("total_equity", 360.0) if wallet_cb else 360.0

            cb_limit = start_eq * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]
            if accum_loss >= cb_limit:
                await redis_trade.set_circuit_breaker(True)
                await tg.push(f"🚨 <b>[CIRCUIT BREAKER AKTYWNY]</b> Strata dzisiejsza {accum_loss:.2f} >= {cb_limit:.2f} {QUOTE_CCY} (baza: {start_eq:.2f} {QUOTE_CCY}). Odciecie zasilania do 00:00 UTC.")

        strat_display = pos_data.get("strategy", "4TF_SNIPER_CORE")
        await tg.push(
            f"{icon} • {inst['label']}</b>\n"
            f"Strategia: <b>{strat_display}</b> [{pos_side.upper()}]\n"
            f"Wyjscie: <b>{exit_p} {QUOTE_CCY}</b> (Wejscie: {entry_p})\n"
            f"Wynik netto: <b>{pnl_net} {QUOTE_CCY} ({roe_net}%)</b>{cooldown_msg}"
        )
        return True, pos_key

    # 2. DYNAMIC BREAK-EVEN GUARD (75% TP)
    if not be_active and actual_pos_on_exchange > 0.0 and entry_p > 0.0 and algo_id != "EXT_MANUAL":
        ticker = await inst["client"].get_market_ticker(inst["symbol"])
        current_market_price = float(ticker.get("last", 0.0)) if ticker else 0.0
        if current_market_price > 0.0:
            be_ratio = CONFIG["DYNAMIC_RISK"].get("BREAK_EVEN_TRIGGER_RATIO", 0.75)
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
                if amend_res and amend_res.get("code") == "0":
                    pos_data["beActivated"] = True
                    if "protection" in pos_data:
                        pos_data["protection"]["slPx"] = new_sl_px
                    pos_data["sl_price"] = new_sl_px
                    await redis_trade.set_position_state(pos_key, pos_data)
                    await tg.push(f"🛡️ <b>[DYNAMIC BREAK-EVEN: {inst['label']}]</b> SL przesuniety na <code>{new_sl_px} {QUOTE_CCY}</code>")

    # 3. STRAZNIK CZASU (TIME-STOP TTL)
    if (time.time() - opened_at) > max_timeout:
        logger.warning(f"⏳ [TIME-STOP] Pozycja {inst['label']} [{pos_side}] przekroczyla {round(max_timeout/3600, 1)}h. Likwidacja...")
        if algo_id != "EXT_MANUAL":
            await inst["client"].cancel_algo_order(inst["symbol"], algo_id)
        await asyncio.sleep(0.3)
        await inst["client"].emergency_flatten_position(inst["symbol"], pos_side)
        pos_data["state"] = "FLAT_PENDING_SETTLEMENT"
        await redis_trade.set_position_state(pos_key, pos_data)
        return True, pos_key

    return False, None

# ==============================================================================
# SNIPER ENTRY WORKER (PETLA WEJSC 4-TF)
# ==============================================================================

async def independent_4tf_sniper_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("🎯 [4-TF SNIPER] Centralny Arbiter Portfelowy v17.8 Online.")

    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        try:
            if not IS_MASTER_ENGINE_NODE:
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

            # Weryfikacja bazy kapitalu z trwalej kotwicy dobowej w Redis
            today_str = datetime.now(UTC).strftime('%Y%m%d')
            daily_loss = await redis_trade.get_daily_loss()
            start_equity = await redis_trade.get_daily_start_equity(today_str)
            if start_equity is None or start_equity <= 0:
                wallet_check = await okx_client.get_wallet_balances(QUOTE_CCY)
                start_equity = wallet_check.get("total_equity", 360.0) if wallet_check else 360.0

            cb_threshold = start_equity * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]
            if daily_loss >= cb_threshold:
                logger.critical(f"🛑 [CIRCUIT-BREAKER] Przekroczono limit straty dobowej: {daily_loss:.2f} >= {cb_threshold:.2f} ({start_equity:.2f} USDC bazowe). Zatrzymanie handlu.")
                await redis_trade.set_circuit_breaker(True)
                await interruptible_sleep(60)
                continue

            best_signal = None

            for conf in FUTURES_INSTRUMENTS:
                if not IS_MASTER_ENGINE_NODE:
                    break

                sym = conf["symbol"]
                base = conf["base"]

                if any(base in ak for ak in active_keys):
                    continue
                if await redis_trade.is_cooldown_active(base):
                    continue

                spread_ok, _ = await okx_client.check_spread_allowed(sym, CONFIG["SAFETY_GUARDS"]["MAX_SPREAD_PCT"])
                if not spread_ok:
                    continue

                c4h, c1h, c15m, c5m = await asyncio.gather(
                    okx_client.get_macro_candles_raw(sym, bar="4H", limit=250),
                    okx_client.get_macro_candles_raw(sym, bar="1H", limit=100),
                    okx_client.get_macro_candles_raw(sym, bar="15m", limit=150),
                    okx_client.get_macro_candles_raw(sym, bar="5m", limit=100)
                )

                if len(c4h) < 205 or len(c1h) < 60 or len(c15m) < 40 or len(c5m) < 20:
                    continue

                closes_4h = [float(c[4]) for c in c4h[:-1]]
                closes_1h = [float(c[4]) for c in c1h[:-1]]
                closes_15m = [float(c[4]) for c in c15m[:-1]]

                ema50_4h_list = calc_ema(closes_4h, 50)
                ema200_4h_list = calc_ema(closes_4h, 200)
                if not ema50_4h_list or not ema200_4h_list:
                    continue

                ema50_4h = ema50_4h_list[-1]
                ema200_4h = ema200_4h_list[-1]
                h4_bull = closes_4h[-1] > ema50_4h and ema50_4h > ema200_4h
                h4_bear = closes_4h[-1] < ema50_4h and ema50_4h < ema200_4h
                if not h4_bull and not h4_bear:
                    continue

                ema50_1h = calc_ema(closes_1h, 50)[-1]
                ema20_1h_history = calc_ema(closes_1h, 20)
                ema20_1h = ema20_1h_history[-1]
                ema20_1h_prev = ema20_1h_history[-2]

                h1_bull = closes_1h[-1] > ema50_1h and (ema20_1h > ema20_1h_prev)
                h1_bear = closes_1h[-1] < ema50_1h and (ema20_1h < ema20_1h_prev)
                if (h4_bull and not h1_bull) or (h4_bear and not h1_bear):
                    continue

                ema20_15m = calc_ema(closes_15m, 20)[-1]
                dist_to_ema_15m = abs(closes_15m[-1] - ema20_15m) / closes_15m[-1]
                if dist_to_ema_15m >= 0.0035:
                    continue

                m5_closed = c5m[-2]
                m5_o, m5_h, m5_l, m5_c = float(m5_closed[1]), float(m5_closed[2]), float(m5_closed[3]), float(m5_closed[4])
                range_m5 = m5_h - m5_l
                dominance = abs(m5_c - m5_o) / range_m5 if range_m5 > 0 else 0.0
                if dominance <= 0.60:
                    continue

                sig_long = h4_bull and h1_bull and (m5_c > m5_o)
                sig_short = h4_bear and h1_bear and (m5_c < m5_o)

                if sig_long or sig_short:
                    tactical = "TREND_PULLBACK" if dist_to_ema_15m <= 0.0018 else "4TF_SNIPER_CORE"
                    if best_signal is None or dominance > best_signal["dominance"]:
                        best_signal = {
                            "symbol": sym, "base": base, "conf": conf,
                            "side": "long" if sig_long else "short",
                            "dominance": dominance, "c5m_full": c5m[:-1],
                            "strategy": tactical
                        }

            if best_signal and IS_MASTER_ENGINE_NODE:
                sym = best_signal["symbol"]
                base = best_signal["base"]
                conf = best_signal["conf"]
                pos_side = best_signal["side"]
                strategy_name = best_signal["strategy"]

                sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(base, pos_side)
                if not sm_ok:
                    logger.warning(f"🐳 [SMART-MONEY] Odrzucono sygnal {conf['label']}: {sm_note}")
                    await interruptible_sleep(20)
                    continue

                async with GLOBAL_ALPHA_LOCK:
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

                    atr_5m = calc_atr(best_signal["c5m_full"], 14)
                    price_sl, price_tp, sl_eff_pct = calculate_clamped_sl_tp(
                        entry_live_price, atr_5m, atr_mult=1.5, rr_ratio=2.0,
                        tick_sz=tick_sz, pos_side=pos_side
                    )

                    risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                    safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                    target_margin = min(
                        risk_capital / (sl_eff_pct * TARGET_LEVERAGE),
                        available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"],
                        safe_cash * 0.95
                    )

                    contracts, actual_margin = okx_client.calculate_contract_size(sym, entry_live_price, target_margin, safe_cash)
                    if contracts <= 0 or actual_margin > available_cash:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    order_side = "buy" if pos_side == "long" else "sell"
                    my_cl_ord_id = generate_cl_ord_id("E", base)
                    my_algo_cl_id = generate_cl_ord_id("P", base)

                    canonical_pos_key = get_canonical_pos_key(redis_trade.prefix, sym, pos_side)

                    # KROK 1: Rejestracja stanu ENTRY_SUBMITTED w Redis
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
                    
                    # Weryfikacja terminalnego stanu parent orderu
                    parent_state = "UNKNOWN"
                    for _ in range(7):
                        await asyncio.sleep(0.3)
                        order_info = await okx_client.get_parent_order_state(sym, main_ord_id)
                        if order_info:
                            parent_state = order_info.get("state", "UNKNOWN")
                            if parent_state == "filled":
                                break

                    pos_details = await okx_client.get_position_details(sym, pos_side)
                    if pos_details is None or pos_details.get("size", 0.0) <= 0.0:
                        logger.critical(f"🚨 [POSITION-DETAILS-UNKNOWN] {sym}: Pozycja zerowa lub brak odpowiedzi. Anulowanie.")
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    real_size = pos_details["size"]
                    real_fill_px = pos_details["avgPx"] if pos_details["avgPx"] > 0 else entry_live_price
                    actual_pos_margin = pos_details["margin"] if pos_details["margin"] > 0 else actual_margin

                    # OBSLUGA POLITYKI POSLIZGU CENOWEGO (Dr Nowak Slippage Model)
                    slippage = abs(real_fill_px - entry_live_price) / entry_live_price
                    if slippage > CONFIG["MAX_ENTRY_SLIPPAGE_PCT"]:
                        ct_val = spec.get("ctVal", 1.0)
                        raw_dist = abs(real_fill_px - price_sl)
                        monetary_risk = real_size * raw_dist * ct_val
                        max_allowed_risk = available_cash * CONFIG["HARD_RISK_CAP_ON_SLIPPAGE_PCT"]

                        if monetary_risk > max_allowed_risk:
                            logger.critical(f"🔥 [SLIPPAGE-ABORT] {sym}: Ryzyko dolarowe ({monetary_risk:.2f}) > limit ({max_allowed_risk:.2f}). Natychmiastowy zrzut!")
                            await okx_client.emergency_flatten_position(sym, pos_side)
                            await redis_trade.delete_key(canonical_pos_key)
                            await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                            continue
                        else:
                            price_sl, price_tp, sl_eff_pct = calculate_clamped_sl_tp(
                                real_fill_px, atr_5m, 1.5, 2.0, tick_sz, pos_side
                            )
                            logger.warning(f"⚠️ [SLIPPAGE-ADAPTED] {sym}: Przeliczono SL/TP bezposrednio od {real_fill_px}.")

                    # Weryfikacja ochrony OCO przez ensure_position_protection
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
                        logger.critical(f"🔥 [UNPROTECTED-ABORT] {sym}: Ochrona OCO nie zostala potwierdzona. Awaryjny zrzut pozycji.")
                        await okx_client.emergency_flatten_position(sym, pos_side)
                        await redis_trade.delete_key(canonical_pos_key)
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    # KROK FINALNY: Zapis pelnego kanonicznego rekordu stanu PROTECTED
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
                        f"🎯 <b>[4-TF SNIPER ENTRY: {conf['label']}]</b>\n"
                        f"Taktyka: <b>{strategy_name}</b> [{pos_side.upper()} 3x Izolowany]\n"
                        f"Kurs: <b>{real_fill_px} {QUOTE_CCY}</b> | Margines: ~{actual_pos_margin} {QUOTE_CCY}\n"
                        f"Kontrakty: <b>{format_sz(real_size)} sz</b>\n"
                        f"🎯 TP: <code>{price_tp}</code> | 🛑 SL: <code>{price_sl}</code>\n"
                        f"🛡️ <b>Stan: PROTECTED (OKX OCO Algo ID: {real_algo_id})</b>\n"
                        f"🐳 Smart Money: <code>{sm_note}</code>"
                    )
        except Exception as e:
            logger.error(f"❌ [4-TF SNIPER ERROR] {e}")

        await interruptible_sleep(45)

# ==============================================================================
# LEADER ELECTION, HEARTBEAT I UNIFIED MASTER BOOTSTRAP
# ==============================================================================

async def master_lock_heartbeat_worker(redis_trade):
    global IS_MASTER_ENGINE_NODE
    logger.info("👑 [MASTER-HEARTBEAT] Uruchomiono dedykowany heartbeat odnawiania blokady lidera.")
    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        await interruptible_sleep(15)
        if IS_MASTER_ENGINE_NODE:
            renewed = await redis_trade.renew_master_engine_lock(ENGINE_WORKER_ID, ttl_seconds=180)
            if not renewed:
                logger.critical("🚨🚨🚨 [MASTER-LOCK-LOST] Utracono rozproszona blokade lidera! Przechodzenie w stan PASSIVE.")
                IS_MASTER_ENGINE_NODE = False
                if GLOBAL_TG:
                    await GLOBAL_TG.push("🚨🚨🚨 <b>[SPLIT-BRAIN PREVENTED]</b> Utracono blokade Master — silnik przechodzi w stan PASSIVE.")
                spawn_supervised_task(lambda: passive_leader_sentry_worker(GLOBAL_REDIS_BRIDGE, GLOBAL_TG, GLOBAL_OKX_CLIENT, GLOBAL_SMART_MONEY_ORACLE), "passive_sentry_worker")
                break

async def master_bootstrap(redis_trade, okx_client, session):
    """Zunifikowana procedura rozruchu dla Mastera (wykonywana na starcie i po awansie)."""
    logger.info("🚀 [MASTER-BOOTSTRAP] Inicjalizacja pelnego srodowiska Master Engine...")
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

    # Zapis trwalej bazy dobowej DAILY_START_EQUITY:YYYYMMDD komenda SET NX EX 172800
    wallet_init = await okx_client.get_wallet_balances(QUOTE_CCY)
    init_eq = wallet_init.get("total_equity", 360.0) if wallet_init else 360.0
    today_str = datetime.now(UTC).strftime('%Y%m%d')
    base_eq_key = redis_trade._enforce_prefix(f"DAILY_START_EQUITY:{today_str}")
    try:
        cmd = ["SET", base_eq_key, str(init_eq), "EX", "172800", "NX"]
        async with session.post(f"{redis_trade.url}", json=cmd, headers=redis_trade.headers, timeout=4):
            pass
        logger.info(f"⚓ [EQUITY-BASELINE-SAVED] Kotwica kapitalu dobowego w Redis: {init_eq} {QUOTE_CCY}")
    except Exception as e:
        logger.error(f"⚠️ [BASELINE-SAVE-ERR] {e}")

    # Rehydratacja pozycji bezposrednio z endpointow OKX
    try:
        req_pos_path = "/api/v5/account/positions?instType=FUTURES"
        headers_p = okx_client._get_headers("GET", req_pos_path)
        await okx_client.limiter_account.consume()
        async with session.get(f"{okx_client.base_url}{req_pos_path}", headers=headers_p, timeout=6) as r_p:
            p_data = await r_p.json()
            if p_data.get("code") == "0" and p_data.get("data"):
                for pos_item in p_data["data"]:
                    pos_sz = abs(float(pos_item.get("pos", 0.0)))
                    pos_inst = pos_item.get("instId")
                    raw_side = pos_item.get("posSide", "long").lower()
                    raw_pos_float = float(pos_item.get("pos", 0.0))
                    pos_side = "long" if (raw_side == "long" or (raw_side == "net" and raw_pos_float > 0)) else "short"
                    avg_px = float(pos_item.get("avgPx", 0.0) or 0.0)
                    margin_val = float(pos_item.get("margin", 50.0) or 50.0)

                    if pos_sz > 0.0 and pos_inst:
                        canonical_key = get_canonical_pos_key(redis_trade.prefix, pos_inst, pos_side)
                        existing_st = await redis_trade.get_position_state(canonical_key)
                        if not existing_st:
                            logger.info(f"🔍 [REHYDRATING-CANONICAL] Odtwarzanie stanu dla {pos_inst}:{pos_side}...")
                            pending_algos = await okx_client.get_pending_algo_orders(pos_inst, ord_type="oco")
                            spec = okx_client.instruments_cache.get(pos_inst, {"tickSz": 0.1})
                            tick_sz = spec["tickSz"]

                            detected_algo_id = None
                            detected_tp, detected_sl = avg_px * 1.02, avg_px * 0.98

                            if pending_algos:
                                detected_algo_id = pending_algos[0].get("algoId")
                                detected_tp = float(pending_algos[0].get("tpTriggerPx", avg_px * 1.02))
                                detected_sl = float(pending_algos[0].get("slTriggerPx", avg_px * 0.98))
                            else:
                                logger.critical(f"🚨 [NAKED-REHYDRATION] Pozycja {pos_inst}:{pos_side} bez OCO! Uzbrajanie...")
                                p_sl, p_tp, _ = calculate_clamped_sl_tp(avg_px, 0.0, 0.0, 1.5, tick_sz, pos_side)
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
    except Exception as exc_rehyd:
        logger.error(f"⚠️ [REHYDRATION-FAILED]: {exc_rehyd}")

    return symbols_to_stream

async def passive_leader_sentry_worker(redis_trade, tg, okx_client, smart_money_oracle):
    global IS_MASTER_ENGINE_NODE
    if IS_MASTER_ENGINE_NODE:
        return
    logger.info("👀 [PASSIVE-SENTRY] Wezel pasywny nasluchuje wolnego miejsca lidera...")
    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()) and not IS_MASTER_ENGINE_NODE:
        await interruptible_sleep(15)
        if IS_MASTER_ENGINE_NODE or (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            break
        acquired = await redis_trade.acquire_master_engine_lock(ENGINE_WORKER_ID, ttl_seconds=180)
        if acquired:
            logger.info(f"👑 [LEADER-PROMOTION] Wezel pasywny {ENGINE_WORKER_ID} przejal role aktywnego MASTER Engine!")
            try:
                await master_bootstrap(redis_trade, okx_client, okx_client.session)
                IS_MASTER_ENGINE_NODE = True
                spawn_supervised_task(lambda: master_lock_heartbeat_worker(redis_trade), "master_heartbeat_worker", tg)
                spawn_supervised_task(lambda: independent_4tf_sniper_worker(okx_client.session, redis_trade, tg, okx_client, smart_money_oracle), "sniper_4tf_worker", tg)
                if tg:
                    await tg.push("👑 <b>[LEADER PROMOTION]</b> Proces przejmuje role aktywnego MASTER Engine po awarii poprzedniego lidera.")
                break
            except Exception as e:
                logger.error(f"❌ [PROMOTION-BOOTSTRAP-ERR] Blad pelnego bootstrapu po awansie: {e}")
                IS_MASTER_ENGINE_NODE = False

def spawn_supervised_task(coro_factory: Callable[[], Any], name: str, tg: Optional[TelegramThrottledDispatcher] = None) -> asyncio.Task:
    async def _wrapper():
        while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                await coro_factory()
                break  # Czyste zakonczenie zadania - brak ponawiania w petli bez bledu
            except asyncio.CancelledError:
                logger.info(f"🛑 [TASK-CANCELLED] Zadanie '{name}' zatrzymane.")
                break
            except Exception as exc:
                logger.critical(f"💥 [FATAL-TASK-CRASH] Zadanie '{name}' padlo z wyjatkiem: {exc}")
                if tg and BACKGROUND_LOOP and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
                    asyncio.run_coroutine_threadsafe(
                        tg.push(f"🚨 <b>AWARIA WORKERA</b> 🚨\nProces <code>{name}</code> ulegl awarii: {exc}. Restart za 5s."),
                        BACKGROUND_LOOP
                    )
                await asyncio.sleep(5)

    task = asyncio.create_task(_wrapper(), name=name)
    BACKGROUND_TASKS.add(task)
    def _on_done(t: asyncio.Task):
        BACKGROUND_TASKS.discard(t)
    task.add_done_callback(_on_done)
    return task

# ==============================================================================
# GLOWNA PETLA CYKLU I ZARZADZANIE PROCESEM
# ==============================================================================

async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT, RATE_LIMITER_PUBLIC, RATE_LIMITER_TRADE, RATE_LIMITER_ACCOUNT
    global GLOBAL_WS_FEED, GLOBAL_ALPHA_LOCK, GLOBAL_OKX_CLIENT, GLOBAL_REDIS_BRIDGE, GLOBAL_TG, GLOBAL_SMART_MONEY_ORACLE, IS_MASTER_ENGINE_NODE

    logger.info(f"⚡ [ENGINE-START] Uruchamianie Silnika Futures 3x v17.8-FINAL ({QUOTE_CCY})...")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    GLOBAL_ALPHA_LOCK = asyncio.Lock()

    RATE_LIMITER_PUBLIC = TokenBucketRateLimiter(tokens_per_second=8.0, max_capacity=16.0)
    RATE_LIMITER_TRADE = TokenBucketRateLimiter(tokens_per_second=4.0, max_capacity=8.0)
    RATE_LIMITER_ACCOUNT = TokenBucketRateLimiter(tokens_per_second=2.0, max_capacity=4.0)

    async with aiohttp.ClientSession() as session:
        redis_trade = UpstashRedisFuturesBridge(
            os.environ.get("UPSTASH_REDIS_REST_URL", ""),
            os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
            session
        )
        tg = TelegramThrottledDispatcher(
            os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            os.environ.get("TELEGRAM_CHANNEL_ID", ""),
            session
        )
        okx_client = OKXFuturesClient(
            session, RATE_LIMITER_TRADE, RATE_LIMITER_PUBLIC, RATE_LIMITER_ACCOUNT, is_sandbox=IS_SANDBOX
        )
        smart_money_oracle = OKXSmartMoneyOracle(session, RATE_LIMITER_PUBLIC, is_sandbox=IS_SANDBOX)
        ws_feed = OKXWebSocketPriceFeed(session, is_sandbox=IS_SANDBOX)

        GLOBAL_WS_FEED = ws_feed
        GLOBAL_OKX_CLIENT = okx_client
        GLOBAL_REDIS_BRIDGE = redis_trade
        GLOBAL_TG = tg
        GLOBAL_SMART_MONEY_ORACLE = smart_money_oracle

        acquired = await redis_trade.acquire_master_engine_lock(ENGINE_WORKER_ID, ttl_seconds=180)
        if acquired or not os.environ.get("UPSTASH_REDIS_REST_URL"):
            IS_MASTER_ENGINE_NODE = True
            logger.info(f"👑 [MASTER-ENGINE-NODE] Proces {ENGINE_WORKER_ID} przejal role aktywnego silnika tradingu.")
            symbols_to_stream = await master_bootstrap(redis_trade, okx_client, session)
            spawn_supervised_task(lambda: master_lock_heartbeat_worker(redis_trade), "master_heartbeat_worker", tg)
            spawn_supervised_task(lambda: independent_4tf_sniper_worker(session, redis_trade, tg, okx_client, smart_money_oracle), "sniper_4tf_worker", tg)
        else:
            IS_MASTER_ENGINE_NODE = False
            logger.info(f"👀 [PASSIVE-HTTP-NODE] Proces {ENGINE_WORKER_ID} dziala jako wezel pasywny.")
            symbols_to_stream = [item["symbol"] for item in FUTURES_INSTRUMENTS]
            spawn_supervised_task(lambda: passive_leader_sentry_worker(redis_trade, tg, okx_client, smart_money_oracle), "passive_sentry_worker", tg)

        spawn_supervised_task(lambda: ws_feed.start_listener(symbols_to_stream), "ws_feed_listener", tg)

        await tg.push(
            f"🚀 <b>Silnik Futures 3x v17.8-FINAL Online ({QUOTE_CCY})</b>\n"
            f"Rola: <b>{'MASTER' if IS_MASTER_ENGINE_NODE else 'PASSIVE'}</b> | Tryb: <b>Safety-Rebuild Verified</b>"
        )

        instruments_for_reconciler = [
            {"client": okx_client, "symbol": item["symbol"], "base": item["base"], "label": item["label"], "price_round": item["price_round"]}
            for item in FUTURES_INSTRUMENTS
        ]

        while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                if IS_MASTER_ENGINE_NODE:
                    for base_inst in instruments_for_reconciler:
                        for side in ["long", "short"]:
                            await reconcile_and_timestop_futures(base_inst, side, redis_trade, tg)

                wallet_data = await okx_client.get_wallet_balances(QUOTE_CCY)
                eq_total = wallet_data.get("total_equity", 0.0) if wallet_data else 0.0
                cash_avail = wallet_data.get("available_cash", 0.0) if wallet_data else 0.0

                today_str = datetime.now(UTC).strftime('%Y%m%d')
                start_equity = await redis_trade.get_daily_start_equity(today_str)
                if start_equity is None or start_equity <= 0:
                    start_equity = eq_total

                active_keys = await redis_trade.get_active_positions()
                today_loss = await redis_trade.get_daily_loss()
                max_loss_limit = round(start_equity * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"], 2)

                logger.info(
                    f"💓 [HEARTBEAT-v17.8] Rola: {'MASTER' if IS_MASTER_ENGINE_NODE else 'PASSIVE'} | "
                    f"Kapital: {eq_total} {QUOTE_CCY} | Baza CB: {start_equity} | Wolne: {cash_avail} | "
                    f"Sloty: {len(active_keys)}/{CONFIG['ALPHA_MAX_ACTIVE_SLOTS']} | "
                    f"Strata: {today_loss}/{max_loss_limit} | Pauza: {GLOBAL_TRADING_PAUSED}"
                )
            except Exception as e:
                logger.error(f"[HEARTBEAT-ERROR] {e}")

            await interruptible_sleep(60)

async def interruptible_sleep(seconds: float):
    if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
        return
    try:
        if ASYNC_SHUTDOWN_EVENT:
            await asyncio.wait_for(ASYNC_SHUTDOWN_EVENT.wait(), timeout=seconds)
        else:
            await asyncio.sleep(seconds)
    except asyncio.TimeoutError:
        pass

def _shutdown_watchdog():
    SHUTDOWN_COMPLETE.wait(timeout=25)
    if not SHUTDOWN_COMPLETE.is_set():
        logger.critical("🛑 [SHUTDOWN-TIMEOUT] Wymuszone zakonczenie procesu przez watchdog.")
    os._exit(0)

def handle_exit_signal(sig, frame):
    PROCESS_DRAINING.set()
    logger.warning(f"🛑 [SHUTDOWN-SIGNAL] Odebrano sygnal {sig}. Rozpoczynanie czystego zamykania...")
    if BACKGROUND_LOOP and BACKGROUND_LOOP.is_running() and ASYNC_SHUTDOWN_EVENT:
        BACKGROUND_LOOP.call_soon_threadsafe(ASYNC_SHUTDOWN_EVENT.set)
    threading.Thread(target=_shutdown_watchdog, daemon=True).start()

signal.signal(signal.SIGTERM, handle_exit_signal)
signal.signal(signal.SIGINT, handle_exit_signal)

def start_background_loop():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    BACKGROUND_LOOP = loop
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e:
        logger.critical(f"💥 [FATAL-CRASH] Petla bota padla: {e}")
        os._exit(1)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
            SHUTDOWN_COMPLETE.set()

bg_thread = threading.Thread(target=start_background_loop, daemon=True, name="FuturesEngineThread")
bg_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
