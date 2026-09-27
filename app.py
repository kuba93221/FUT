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
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from collections import deque
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from typing import Dict, Any, List, Optional, Tuple, Set

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
    """Gwarantuje natychmiastowe wypychanie logów do konsoli Rendera bez buforowania."""
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

# [OpSec #20] Wymuszenie sekretu administracyjnego bez domyślnych haseł
EMERGENCY_SECRET = os.environ.get("EMERGENCY_SECRET", "").strip()
if not EMERGENCY_SECRET:
    logger.critical("🚨 [FATAL-CONFIG] Brak EMERGENCY_SECRET w zmiennych środowiskowych! Endpointy administracyjne zablokowane.")

REDIS_PREFIX = "FUTURES_3X_DEMO_" if IS_SANDBOX else "FUTURES_3X_LIVE_"

logger.info(f"⚙️ [SYSTEM-INIT] Silnik Futures 3x v17.1 PROD-HARDENED Online [QUOTE: {QUOTE_CCY} | PREFIKS: {REDIS_PREFIX} | SANDBOX: {IS_SANDBOX}]")

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
GLOBAL_ALPHA_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
BACKGROUND_TASKS: Set[asyncio.Task] = set()

# [Concurrency #11] Niezależne pule Rate Limiterów dla eliminacji starvation
RATE_LIMITER_PUBLIC: Optional[Any] = None
RATE_LIMITER_TRADE: Optional[Any] = None
RATE_LIMITER_ACCOUNT: Optional[Any] = None

GLOBAL_WS_FEED: Optional[Any] = None
GLOBAL_OKX_CLIENT: Optional[Any] = None
GLOBAL_REDIS_BRIDGE: Optional[Any] = None
GLOBAL_TG: Optional[Any] = None

SHUTDOWN_COMPLETE = threading.Event()
PROCESS_DRAINING = threading.Event()

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
    "RISK_PER_TRADE_PCT": 0.004,         # Aptekarskie 0.4% kapitału ryzykowane per trade
    "MAX_POSITION_PORTFOLIO_RATIO": 0.15,  # Max 15% wolnej gotówki na pojedynczy margines
    "DYNAMIC_RISK": {
        "MIN_SL_PCT": 0.006,                # 0.6% ruchu bazowego = 1.8% na dźwigni 3x
        "MAX_SL_HARD_CAP": 0.015,          # 1.5% ruchu bazowego = 4.5% na dźwigni 3x
        "DEFAULT_SL_PCT": 0.010,           # 1.0% ruchu bazowego = 3.0% na dźwigni 3x
        "VOLATILITY_CUSHION_PCT": 0.0015,  # Poduszka anty-szpilkowa (+0.15% bufora)
        "BREAK_EVEN_TRIGGER_RATIO": 0.75,  # Aktywacja BE po 75% drogi do TP
        "BREAK_EVEN_FEE_BUFFER_PCT": 0.0010 # +0.10% buforu na prowizje maklerskie OKX
    },
    "TIMEOUTS": {
        "DIGITAL_TWIN_SNIPER": 8 * 3600,
        "4TF_SNIPER_CORE": 8 * 3600,
        "REHYDRATED_RECOVERY": 8 * 3600,
        "TREND_PULLBACK": 6 * 3600,
        "VOLATILITY_BREAKOUT": 3 * 3600,
        "MEAN_REVERSION": 8 * 3600
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

def floor_to_lot(val: float, lot_sz: float) -> float:
    """Rygorystyczne obcinanie Decimal w dół do wielokrotności lotSz."""
    if lot_sz <= 0.0 or val <= 0.0:
        return 0.0
    v = Decimal(str(val))
    lot = Decimal(str(lot_sz))
    steps = (v / lot).to_integral_value(rounding=ROUND_DOWN)
    return float(steps * lot)

def round_price_to_tick(price: float, tick_sz: float, direction: str = "NEAREST") -> float:
    """Zaokrągla cenę zgodnie z minimalnym krokiem notowania tickSz giełdy OKX."""
    if price <= 0.0 or tick_sz <= 0.0:
        return 0.0
    p = Decimal(str(price))
    t = Decimal(str(tick_sz))
    if direction == "DOWN":
        rounding = ROUND_DOWN
    elif direction == "UP":
        rounding = ROUND_UP
    else:
        rounding = ROUND_DOWN
    steps = (p / t).to_integral_value(rounding=rounding)
    return float(steps * t)

def format_sz(quantity: float) -> str:
    return f"{Decimal(str(quantity)):.8f}".rstrip('0').rstrip('.')

def format_px(price: float, tick_sz: float) -> str:
    """Formatuje cenę jako string bez anomalii zmiennoprzecinkowych float."""
    if price <= 0.0 or tick_sz <= 0.0:
        return "0"
    p = Decimal(str(price))
    t = Decimal(str(tick_sz))
    steps = (p / t).to_integral_value(rounding=ROUND_DOWN)
    res = steps * t
    t_str = str(t).rstrip('0')
    decimals = len(t_str.split('.')[1]) if '.' in t_str else 0
    return f"{res:.{decimals}f}" if decimals > 0 else f"{int(res)}"

def calc_ema(prices: List[float], period: int) -> List[float]:
    """Wektorowa średnia wykładnicza EMA z pełnym seedingiem."""
    if not prices or len(prices) < period:
        return []
    emas = [sum(prices[:period]) / period]
    k = 2.0 / (period + 1.0)
    for p in prices[period:]:
        emas.append(p * k + emas[-1] * (1.0 - k))
    return emas

def calc_atr(candles: List[List[str]], period: int = 14) -> float:
    """Kalkulator średniego rzeczywistego zasięgu ATR z listy świec."""
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
    """
    Wyznacza SL i TP z uwzględnieniem bufora szumu i dopasowaniem do tickSz.
    Poduszka zmienności jest dodawana przed sizingiem, zachowując ścisłe ryzyko 0.4%.
    """
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

    return price_sl, price_tp, sl_pct

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

def require_admin() -> bool:
    """[OpSec #7, #20] Ścisła weryfikacja nagłówka X-Admin-Secret w stałym czasie (brak parametrów URL!)."""
    if not EMERGENCY_SECRET:
        return False
    supplied = request.headers.get("X-Admin-Secret", "").strip()
    return hmac.compare_digest(supplied, EMERGENCY_SECRET)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or PROCESS_DRAINING.is_set():
        return "FUTURES_ENGINE_DRAINING", 503
    return f"FUTURES_ENGINE_ONLINE_3X_{QUOTE_CCY}", 200

@app.route('/status', methods=['GET'])
def engine_status_endpoint():
    """Raport telemetryczny silnika transakcyjnego dla systemów monitoringu."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE or not GLOBAL_OKX_CLIENT:
        return jsonify({"status": "starting", "engine": "INIT_PHASE"}), 503

    async def _gather_status():
        wallet = await GLOBAL_OKX_CLIENT.get_wallet_balances(QUOTE_CCY)
        active_keys = await GLOBAL_REDIS_BRIDGE.get_active_positions()
        daily_loss = await GLOBAL_REDIS_BRIDGE.get_daily_loss()
        positions_details = []
        for k in active_keys:
            st = await GLOBAL_REDIS_BRIDGE.get_position_state(k)
            if st:
                positions_details.append(st)

        is_sl_quarantine = await GLOBAL_REDIS_BRIDGE.is_cooldown_active("GLOBAL_PORTFOLIO_QUARANTINE")
        is_stagger_locked = await GLOBAL_REDIS_BRIDGE.is_cooldown_active("PORTFOLIO_STAGGER_LOCK")
        prices_snapshot = GLOBAL_WS_FEED.get_prices_snapshot() if GLOBAL_WS_FEED else {}

        tot_eq = wallet.get("total_equity", 0.0) if wallet else 0.0
        avail_c = wallet.get("available_cash", 0.0) if wallet else 0.0

        return {
            "status": "ONLINE",
            "version": "v17.1_PROD_HARDENED",
            "quote_currency": QUOTE_CCY,
            "target_leverage": TARGET_LEVERAGE,
            "total_equity": tot_eq,
            "available_cash": avail_c,
            "daily_loss": daily_loss,
            "circuit_breaker_threshold": round(tot_eq * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"], 2),
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

@app.route('/reset-circuit-breaker', methods=['POST'])
def reset_circuit_breaker_endpoint():
    if not require_admin():
        return jsonify({"error": "Unauthorized. Wymagany nagłówek X-Admin-Secret."}), 403
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE:
        return jsonify({"status": "error", "message": "Pętla bota nie jest gotowa."}), 503

    async def _do_reset():
        today_str = datetime.now(UTC).strftime('%Y%m%d')
        safe_key = f"DAILY_LOSS:{today_str}"
        GLOBAL_REDIS_BRIDGE._local_daily_loss = 0.0
        await GLOBAL_REDIS_BRIDGE.delete_key(safe_key)
        return {"status": "success", "message": f"Zresetowano klucz dziennej straty {safe_key}."}

    fut = asyncio.run_coroutine_threadsafe(_do_reset(), BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=10)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/reset-slots', methods=['POST'])
def reset_slots_endpoint():
    """[OpSec & Integrity #6 - BOMBA 2 NAPRAWIONA] Twarda odmowa czyszczenia slotów przy stanie None."""
    if not require_admin():
        return jsonify({"error": "Unauthorized. Wymagany nagłówek X-Admin-Secret."}), 403
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_REDIS_BRIDGE or not GLOBAL_OKX_CLIENT:
        return jsonify({"status": "error", "message": "Pętla bota nie jest gotowa."}), 503

    async def _do_flush_slots():
        # Weryfikacja czy giełda jest w 100% FLAT
        for item in FUTURES_INSTRUMENTS:
            sz_l = await GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], "long")
            sz_s = await GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], "short")
            
            # BOMBA 2 FIX: Stan nieznany (None) bezwzględnie blokuje reset!
            if sz_l is None or sz_s is None:
                return {
                    "status": "rejected",
                    "message": f"Błąd komunikacji z OKX dla {item['symbol']} (stan nieznany). Odmowa czyszczenia slotów ze względów bezpieczeństwa!"
                }
            if sz_l > 0.0 or sz_s > 0.0:
                return {
                    "status": "rejected",
                    "message": f"Giełda nadal posiada otwartą pozycję na {item['symbol']} (L:{sz_l}, S:{sz_s})! Odmowa czyszczenia slotów."
                }
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
    """
    [OpSec & Concurrency #6, #12] Pancerna ewakuacja konta:
    1. Konsumpcja tokenów z właściwego limitera RATE_LIMITER_TRADE.
    2. Anulowanie wszystkich oczekujących zleceń i algosów.
    3. Zamykanie pozycji rynkowo z weryfikacją FLAT.
    4. Reset slotów dopiero po potwierdzonym 0.0 na koncie.
    """
    if not require_admin():
        return jsonify({"error": "Unauthorized. Wymagany nagłówek X-Admin-Secret."}), 403

    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_OKX_CLIENT or not GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Silnik bota nie jest w pełni zainicjalizowany."}), 503

    async def _do_emergency():
        report = {"closed_positions": [], "canceled_pending": [], "canceled_algos": [], "freed_slots": 0, "verified_flat": False}
        
        # 1. Anulowanie oczekujących zleceń zwykłych
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
                            await GLOBAL_OKX_CLIENT.session.post(f"{GLOBAL_OKX_CLIENT.base_url}/api/v5/trade/cancel-order", data=c_body, headers=c_headers, timeout=3)
                            report["canceled_pending"].append(f"{sym}:{ord_id}")
            except Exception as e:
                logger.error(f"[EMERGENCY-CANCEL-PENDING-ERR] {sym}: {e}")

            # 2. Anulowanie zleceń algo
            try:
                pending_algos = await GLOBAL_OKX_CLIENT.get_pending_algo_orders(sym)
                for al in pending_algos:
                    al_id = al.get("algoId")
                    if al_id:
                        await GLOBAL_OKX_CLIENT.cancel_algo_order(sym, al_id)
                        report["canceled_algos"].append(f"{sym}:{al_id}")
            except Exception as e:
                logger.error(f"[EMERGENCY-CANCEL-ALGO-ERR] {sym}: {e}")

        # 3. Zamykanie rynkowe pozycji
        for item in FUTURES_INSTRUMENTS:
            sym = item["symbol"]
            for side in ["long", "short"]:
                flattened = await GLOBAL_OKX_CLIENT.emergency_flatten_position(sym, side)
                if flattened:
                    report["closed_positions"].append(f"{sym}:{side}")

        # 4. Twarda weryfikacja FLAT przed dotknięciem slotów w RAM/Redis
        all_flat = True
        for item in FUTURES_INSTRUMENTS:
            for side in ["long", "short"]:
                rem = await GLOBAL_OKX_CLIENT.get_open_position_size(item["symbol"], side)
                if rem is None or rem > 0:
                    all_flat = False
                    logger.critical(f"🔥 [EMERGENCY-NOT-FLAT] Pozycja {item['symbol']} [{side}] nadal aktywna: {rem} sz!")

        report["verified_flat"] = all_flat
        if all_flat:
            freed = await GLOBAL_REDIS_BRIDGE.reset_all_slots()
            report["freed_slots"] = freed
            await GLOBAL_REDIS_BRIDGE.clear_cooldown("GLOBAL_PORTFOLIO_QUARANTINE")
            await GLOBAL_REDIS_BRIDGE.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
            logger.info("✅ [EMERGENCY] Wszystkie pozycje FLAT. Zwolniono sloty i kwarantanny.")
        else:
            logger.critical("⚠️ [EMERGENCY] Stan nie jest w 100% FLAT. Sloty w RAM nie zostały skasowane!")

        if GLOBAL_TG:
            status_text = "100% kapitału zabezpieczone w gotówce." if all_flat else "UWAGA: Część pozycji wymaga ręcznej weryfikacji!"
            await GLOBAL_TG.push(
                f"🚨🚨 <b>[AWARYJNA EWAKUACJA KONTA]</b> 🚨🚨\n"
                f"Zamknięte pozycje: <code>{len(report['closed_positions'])}</code>\n"
                f"Anulowane OCO: <code>{len(report['canceled_algos'])}</code>\n"
                f"Zwolnione sloty: <code>{report['freed_slots']}</code>\n"
                f"Stan FLAT potwierdzony: <b>{all_flat}</b>\n"
                f"Status: <b>{status_text}</b>"
            )
        return report

    fut = asyncio.run_coroutine_threadsafe(_do_emergency(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=20)
        return jsonify({"status": "COMPLETED", "details": res}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

class TokenBucketRateLimiter:
    """[Concurrency #10, #11] Token Bucket z deterministyczną inicjalizacją Locka w konstruktorze."""
    def __init__(self, tokens_per_second: float = 4.0, max_capacity: float = 8.0):
        self.rate = tokens_per_second
        self.capacity = max_capacity
        self.tokens = max_capacity
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def consume(self):
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last_check) * self.rate)
            self.last_check = now
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
                self.last_check = time.monotonic()
            else:
                self.tokens -= 1.0

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

    def _enforce_prefix(self, key: str) -> str:
        return key if key.startswith(self.prefix) else f"{self.prefix}{key}"

    def _safe_unpack_hex(self, hex_string: str) -> Optional[Dict[str, Any]]:
        if not hex_string or hex_string in ["None", "NULL", "none", "null"]:
            return None
        try:
            clean_hex = hex_string.strip()
            return msgpack.unpackb(bytes.fromhex(clean_hex), strict_map_key=False)
        except Exception as e:
            logger.error(f"[MSGPACK-DECODE-ERR] Uszkodzony rekord w Redis: {e}")
            return None

    async def init_sync(self):
        """[Rehydration #7, #13] Pełna rehydratacja stanu z Redis, włącznie z kwarantannami."""
        if self._initialized or not self.url:
            return
        try:
            today_str = datetime.now(UTC).strftime('%Y%m%d')
            safe_key = self._enforce_prefix(f"DAILY_LOSS:{today_str}")
            url_dl = f"{self.url}/get/{safe_key}"
            async with self.session.get(url_dl, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    if res:
                        self._local_daily_loss = float(res)

            # Rehydratacja pozycji
            pattern = f"{self.prefix}POS_ACTIVE:ALPHA:*"
            url_k = f"{self.url}/keys/{pattern}"
            async with self.session.get(url_k, headers=self.headers, timeout=4) as r_k:
                if r_k.status == 200:
                    keys = (await r_k.json()).get("result", [])
                    for k in keys:
                        clean_k = k.replace(self.prefix, "")
                        url_pos = f"{self.url}/lrange/{k}/0/0"
                        async with self.session.get(url_pos, headers=self.headers, timeout=4) as r_p:
                            if r_p.status == 200:
                                h_list = (await r_p.json()).get("result", [])
                                if h_list:
                                    pos_obj = self._safe_unpack_hex(h_list[0])
                                    if pos_obj:
                                        self._local_positions[clean_k] = pos_obj

            # [Rehydration #7] Rehydratacja kwarantann z Redis do RAM
            pattern_cd = f"{self.prefix}COOLDOWN:*"
            url_cd = f"{self.url}/keys/{pattern_cd}"
            async with self.session.get(url_cd, headers=self.headers, timeout=4) as r_cd:
                if r_cd.status == 200:
                    cd_keys = (await r_cd.json()).get("result", [])
                    for k in cd_keys:
                        clean_cd = k.replace(f"{self.prefix}COOLDOWN:", "")
                        url_ttl = f"{self.url}/ttl/{k}"
                        async with self.session.get(url_ttl, headers=self.headers, timeout=4) as r_t:
                            if r_t.status == 200:
                                ttl_val = (await r_t.json()).get("result", 0)
                                if ttl_val and int(ttl_val) > 0:
                                    self._local_cooldowns[clean_cd] = time.time() + int(ttl_val)

            self._initialized = True
            logger.info(f"💾 [REDIS-CACHE-INIT] Zsynchronizowano: Strata={self._local_daily_loss} | Pozycje={len(self._local_positions)} | Cooldowny={len(self._local_cooldowns)}")
        except Exception as e:
            logger.error(f"⚠️ [REDIS-INIT-SYNC-ERROR] Błąd synchronizacji: {e}")

    async def set_position_state(self, pos_key: str, state_data: Dict[str, Any]) -> bool:
        clean_key = pos_key.replace(self.prefix, "")
        self._local_positions[clean_key] = state_data
        if not self.url:
            return True
        safe_key = self._enforce_prefix(pos_key)
        try:
            hex_str = msgpack.packb(state_data, use_bin_type=True).hex()
            pipeline_payload = [
                ["LPUSH", safe_key, hex_str],
                ["LTRIM", safe_key, "0", "0"],
                ["EXPIRE", safe_key, "604800"]
            ]
            url = f"{self.url}/pipeline"
            async with self.session.post(url, json=pipeline_payload, headers=self.headers, timeout=5) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-POS-SAVE-ERROR] Błąd zapisu pozycji {pos_key}: {e}")
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
            url = f"{self.url}/del/{safe_key}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-DEL-ERROR] Błąd usuwania klucza {key}: {e}")
            return False

    async def get_active_positions(self) -> List[str]:
        return [self._enforce_prefix(k) for k in self._local_positions.keys() if "POS_ACTIVE:ALPHA:" in k]

    async def reset_all_slots(self) -> int:
        deleted = 0
        keys_to_delete = list(self._local_positions.keys())
        for k in keys_to_delete:
            if "POS_ACTIVE:ALPHA:" in k:
                await self.delete_key(k)
                deleted += 1
        return deleted

    async def set_cooldown(self, base_symbol_or_key: str, ttl_seconds: int = 4500) -> bool:
        self._local_cooldowns[base_symbol_or_key] = time.time() + ttl_seconds
        if not self.url:
            return True
        safe_key = self._enforce_prefix(f"COOLDOWN:{base_symbol_or_key}")
        try:
            url = f"{self.url}/set/{safe_key}/ACTIVE/EX/{ttl_seconds}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-COOLDOWN-ERROR] {base_symbol_or_key}: {e}")
            return False

    async def clear_cooldown(self, base_symbol_or_key: str) -> bool:
        """[Rollback #19] Precyzyjne usuwanie kwarantanny z RAM i Redis bez psuwania slotów."""
        self._local_cooldowns.pop(base_symbol_or_key, None)
        if not self.url:
            return True
        safe_key = self._enforce_prefix(f"COOLDOWN:{base_symbol_or_key}")
        try:
            url = f"{self.url}/del/{safe_key}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-CLEAR-COOLDOWN-ERR] {base_symbol_or_key}: {e}")
            return False

    async def is_cooldown_active(self, base_symbol_or_key: str) -> bool:
        expiry = self._local_cooldowns.get(base_symbol_or_key, 0.0)
        if time.time() < expiry:
            return True
        if expiry > 0.0:
            self._local_cooldowns.pop(base_symbol_or_key, None)
        return False

    async def add_daily_loss(self, loss_amount: float) -> float:
        if loss_amount <= 0:
            return self._local_daily_loss
        self._local_daily_loss = round(self._local_daily_loss + loss_amount, 4)
        if not self.url:
            return self._local_daily_loss
        today_str = datetime.now(UTC).strftime('%Y%m%d')
        safe_key = self._enforce_prefix(f"DAILY_LOSS:{today_str}")
        try:
            url_set = f"{self.url}/set/{safe_key}/{self._local_daily_loss}/EX/86400"
            async with self.session.get(url_set, headers=self.headers, timeout=4):
                pass
        except Exception as e:
            logger.error(f"❌ [REDIS-CIRCUIT-ERROR] Błąd zapisu straty: {e}")
        return self._local_daily_loss

    async def get_daily_loss(self) -> float:
        return self._local_daily_loss

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
            logger.error(f"❌ [TELEGRAM-ERROR] Błąd powiadomienia: {e}")

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
                reason = f"Aggressive Institutional Sell Pressure (Taker Sell: {round(sell_v, 1)} > Buy: {round(buy_v, 1)})"
                return False, reason, flow
            return True, f"ZGODNY Z PRZEPŁYWEM (Taker Ratio: {flow.get('ratio')})", flow
        elif target_pos_side.lower() == "short":
            if buy_v > (sell_v * self.max_imbalance):
                reason = f"Aggressive Institutional Buy Absorption (Taker Buy: {round(buy_v, 1)} > Sell: {round(sell_v, 1)})"
                return False, reason, flow
            return True, f"ZGODNY Z PRZEPŁYWEM (Taker Ratio: {flow.get('ratio')})", flow

        return True, "SM_ALIGNED", flow

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
                logger.info(f"🌐 [WS-CONNECT] Łączenie z OKX SWAP EEA: {ws_url}...")
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
                                logger.warning("⚠️ [WS-WATCHDOG] Brak pakietów przez 45s. Przełączanie serwera...")
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
                                logger.warning("⚠️ [WS-DISCONNECTED] Gniazdo zamknięte. Następny klaster...")
                                self.current_ep_index += 1
                                break
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logger.error(f"❌ [WS-ERROR] Błąd strumienia ({ws_url}): {e}. Wznawianie za 5s...")
                self.current_ep_index += 1
                await asyncio.sleep(5)

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Zwraca cenę tylko jeśli jest świeża (< 5 sekund w trybie Live)."""
        with self._price_lock:
            data = self.latest_prices.get(symbol)
        if not data:
            return None
        if (time.monotonic() - data["ts"]) > 5.0 and not self.is_sandbox:
            return None
        return data["price"]

    def get_prices_snapshot(self) -> Dict[str, float]:
        """Bezpieczna migawka cen dla serwera Flask."""
        with self._price_lock:
            return {k: v["price"] for k, v in self.latest_prices.items()}

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
                code = data.get("code")
                return code == "0" or code == "51000" or "already" in data.get("msg", "").lower()
        except Exception as e:
            logger.error(f"[FUTURES-CONFIG] Błąd trybu pozycji: {e}")
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
            logger.warning(f"⚠️ [AUTO-DISCOVERY-FALLBACK] Błąd skanowania: {e}")
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
                        logger.info(f"📋 [SPEC-LOADED] {symbol} | ctVal: {spec['ctVal']} {spec['ctValCcy']} | tickSz: {spec['tickSz']} | lotSz: {spec['lotSz']}")
                        return spec
            except Exception as e:
                logger.error(f"[FUTURES-SPEC] Błąd specyfikacji {symbol}: {e}")
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
                code = data.get("code")
                return code == "0" or code == "51000" or "not modified" in data.get("msg", "").lower()
        except Exception as e:
            logger.error(f"[FUTURES-LEVERAGE] Błąd lewaru {symbol}: {e}")
            return False

    async def get_wallet_balances(self, preferred_ccy: str = QUOTE_CCY) -> Optional[Dict[str, Any]]:
        """[Audit #5 Fix] Zwraca None w razie błędu API/timeoutu, eliminując fałszywy fail-open."""
        if not self.api_key:
            return None
        await self.limiter_account.consume()
        request_path = "/api/v5/account/balance"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=6) as resp:
                data = await resp.json()
                if data.get("code") != "0":
                    logger.error(f"[WALLET-REJECTED] {data.get('msg')} (kod: {data.get('code')})")
                    return None
                if data.get("data"):
                    acc = data["data"][0]
                    total_eq = float(acc.get("totalEq", 0.0) or 0.0)
                    balances_map = {}
                    for b in acc.get("details", []):
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
                    if avail_cash <= 0.0:
                        avail_cash = total_eq
                    return {"total_equity": round(total_eq, 2), "available_cash": round(avail_cash, 2), "balances": balances_map}
        except Exception as e:
            logger.error(f"[WALLET-EXCEPTION] {e}")
        return None

    async def get_position_details(self, symbol: str, pos_side: str) -> Dict[str, Any]:
        """[Order Management #2] Odczytuje fizyczną cenę wypełnienia avgPx i margines z giełdy."""
        if not self.api_key:
            return {"size": 0.0, "avgPx": 0.0, "margin": 0.0}
        await self.limiter_account.consume()
        request_path = f"/api/v5/account/positions?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    for p in data["data"]:
                        raw_side = p.get("posSide", "").lower()
                        raw_pos = float(p.get("pos", 0.0) or 0.0)
                        if raw_side == pos_side.lower() or (raw_side == "net" and ((pos_side == "long" and raw_pos > 0) or (pos_side == "short" and raw_pos < 0))):
                            return {
                                "size": abs(raw_pos),
                                "avgPx": float(p.get("avgPx", 0.0) or 0.0),
                                "margin": float(p.get("margin", 0.0) or 0.0)
                            }
        except Exception as e:
            logger.error(f"[POS-DETAILS-ERR] {symbol}: {e}")
        return {"size": 0.0, "avgPx": 0.0, "margin": 0.0}

    def calculate_contract_size(
        self,
        symbol: str,
        current_price: float,
        target_margin_quote: float,
        max_allowed_margin: float
    ) -> Tuple[float, float]:
        """
        [Quant Math #17, #18] Rygorystyczny sizing:
        Jeśli minimalny kontrakt minSz przekracza budżet ryzyka, funkcja twardo odrzuca zlecenie.
        """
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
            logger.critical(f"🚨 [SPEC-UNKNOWN-CCY] {symbol}: nierozpoznana waluta {ct_val_ccy}. Odmowa sizingu.")
            return 0.0, 0.0

        if contract_nominal_quote <= 0:
            return 0.0, 0.0

        single_contract_margin = contract_nominal_quote / self.TARGET_LEVERAGE

        # Twarda odmowa naruszenia budżetu ryzyka przez minSz
        min_margin = min_sz * single_contract_margin
        if min_margin > target_margin_quote or min_margin > max_allowed_margin:
            logger.warning(f"🛡️ [RISK-BLOCK-MINSZ] {symbol}: Wymagany margines minSz ({min_margin:.2f}) przekracza budżet ({target_margin_quote:.2f}).")
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
            logger.error(f"[OKX-TICKER] Błąd kursu {symbol}: {e}")
        return None

    async def get_macro_candles_raw(self, symbol: str, bar: str = "15m", limit: int = 100) -> List[List[str]]:
        """[Quant Math #15] Pobiera świece z gwarancją limitu 250 dla rozgrzewki EMA 200."""
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
            logger.error(f"[OKX-CANDLES] Błąd świec {symbol} ({bar}): {e}")
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
        attached_tp: Optional[float] = None,
        attached_sl: Optional[float] = None,
        tick_sz: float = 0.1,
        cl_ord_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        [Order Management #1] ATOMOWA EGZEKUCJA ZLECENIA Z ATTACHED OCO.
        Eliminuje okno nagiej pozycji – zlecenie wejścia niesie ze sobą parametry obrony.
        """
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/order"

        if not cl_ord_id:
            base_clean = symbol.split('-')[0].replace('_', '')[:4]
            ms_now = str(int(time.time() * 1000))[-9:]
            cl_ord_id = f"A{base_clean}{ms_now}{os.urandom(2).hex()}"[:32]

        body_dict: Dict[str, Any] = {
            "instId": symbol,
            "tdMode": self.MARGIN_MODE,
            "side": side.lower(),
            "posSide": pos_side.lower(),
            "ordType": ord_type.lower(),
            "sz": format_sz(quantity),
            "reduceOnly": reduce_only,
            "clOrdId": cl_ord_id
        }
        if ord_type == "limit" and price is not None:
            body_dict["px"] = format_px(price, tick_sz)

        # Dołączenie atomowej ochrony algo z formatowaniem tickSz
        if attached_tp is not None and attached_sl is not None:
            body_dict["attachAlgoOrds"] = [{
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
                if res_json.get("code") != "0":
                    logger.error(f"❌ [OKX-ORDER-REJECTED] {symbol}: {res_json.get('msg')} (kod: {res_json.get('code')})")
                return res_json
        except Exception as e:
            logger.error(f"❌ [OKX-ORDER-ERROR] Zlecenie {symbol}: {e}")
            return None

    async def execute_futures_oco(
        self,
        symbol: str,
        pos_side: str,
        quantity: float,
        price_tp: float,
        price_sl: float,
        tick_sz: float = 0.1
    ) -> Optional[Dict[str, Any]]:
        """Samodzielne zlecenie algo OCO (używane w rehydratacji i obronie awaryjnej)."""
        if not self.api_key:
            return None
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/order-algo"
        exit_side = "sell" if pos_side == "long" else "buy"
        body_dict = {
            "instId": symbol,
            "tdMode": self.MARGIN_MODE,
            "side": exit_side,
            "posSide": pos_side,
            "ordType": "oco",
            "sz": format_sz(quantity),
            "reduceOnly": True,
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
                return await r.json()
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-ERROR] {symbol}: {e}")
            return None

    async def amend_algo_order(self, symbol: str, algo_id: str, new_sl_trigger_px: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not self.api_key or algo_id in ["EXT_MANUAL", "ATTACHED_PENDING", "ATTACHED_OKX", "NONE"]:
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
            logger.error(f"❌ [AMEND-ALGO-ERROR] algo {algo_id}: {e}")
            return None

    async def cancel_algo_order(self, symbol: str, algo_id: str) -> bool:
        if not self.api_key or algo_id in ["EXT_MANUAL", "ATTACHED_PENDING", "ATTACHED_OKX", "NONE"]:
            return False
        await self.limiter_trade.consume()
        request_path = "/api/v5/trade/cancel-algos"
        body = json.dumps([{"instId": symbol, "algoId": str(algo_id)}])
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body)
        try:
            async with self.session.post(url, data=body, headers=headers, timeout=5) as r:
                data = await r.json()
                return data.get("code") == "0"
        except Exception:
            return False

    async def get_open_position_size(self, symbol: str, pos_side: str) -> Optional[float]:
        """
        [Fail-Open Elimination #3] Zwraca None w razie błędu sieci/API.
        Zero (0.0) jest zwracane WYŁĄCZNIE, gdy giełda twardo potwierdzi brak pozycji.
        """
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
                    raw_pos = float(p.get("pos", 0.0))
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
            logger.critical(f"[POSITION-CHECK-UNKNOWN] {symbol}/{pos_side}: {e}")
            return None

    async def get_pending_algo_orders(self, symbol: str) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        await self.limiter_trade.consume()
        request_path = f"/api/v5/trade/orders-algo-pending?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return data["data"]
        except Exception:
            pass
        return []

    async def get_last_closed_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            return None
        await self.limiter_account.consume()
        request_path = f"/api/v5/account/positions-history?instType=FUTURES&instId={symbol}&limit=1"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    p = data["data"][0]
                    return {
                        "close_avg_px": float(p.get("closeAvgPx", 0.0) or 0.0),
                        "realized_pnl": float(p.get("realizedPnl", 0.0) or 0.0),
                        "pnl_ratio": float(p.get("pnlRatio", 0.0) or 0.0) * 100.0
                    }
        except Exception:
            pass
        return None

    async def get_algo_order_state(self, algo_id: str) -> Tuple[Optional[str], Optional[float]]:
        if not self.api_key or algo_id in ["EXT_MANUAL", "ATTACHED_PENDING", "ATTACHED_OKX", "NONE"]:
            return None, None
        await self.limiter_trade.consume()
        request_path = f"/api/v5/trade/order-algo?algoId={algo_id}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    item = data["data"][0]
                    state = item.get("state")
                    actual_px_str = item.get("actualPx") or item.get("tpTriggerPx") or item.get("slTriggerPx") or "0"
                    try:
                        actual_px = float(actual_px_str)
                    except ValueError:
                        actual_px = None
                    return state, actual_px
        except Exception:
            pass
        return None, None

    async def emergency_flatten_position(self, symbol: str, pos_side: str, max_attempts: int = 5) -> bool:
        """Pętla rynkowego zrzutu z twardą weryfikacją FLAT."""
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
        return final_sz is not None and final_sz == 0.0

async def reconcile_and_timestop_futures(
    inst: Dict[str, Any],
    strategy_type: str,
    redis_trade: UpstashRedisFuturesBridge,
    tg: TelegramThrottledDispatcher
) -> Tuple[bool, Optional[str]]:
    pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
    pos_data = await redis_trade.get_position_state(pos_key)
    if not pos_data:
        return False, None

    pos_side = pos_data.get("pos_side", "long")
    contracts = float(pos_data.get("contracts", 0.01))
    entry_p = float(pos_data.get("entry_price", 0.0))
    tp_p = float(pos_data.get("tp_price", entry_p))
    sl_p = float(pos_data.get("sl_price", entry_p))
    margin_locked = float(pos_data.get("margin_locked", 1.0))
    be_active = pos_data.get("be_activated", False)
    algo_id = str(pos_data.get("algo_id", "ATTACHED_PENDING"))

    actual_pos_on_exchange = await inst["client"].get_open_position_size(inst["symbol"], pos_side)

    # Jeśli API ma czkawkę, nie wolno modyfikować stanu w RAM!
    if actual_pos_on_exchange is None:
        logger.warning(f"⚠️ [RECONCILE-BLOCKED] Stan pozycji {inst['label']} nieznany (Błąd API). Oczekiwanie...")
        return False, None

    # [Bomba 1 FIX] Jeśli algoId nadal oczekuje na powiązanie, spróbuj go odkryć z giełdy
    if algo_id in ["ATTACHED_PENDING", "ATTACHED_OKX", "EXT_MANUAL", "NONE"] and actual_pos_on_exchange > 0.0:
        pending_algos = await inst["client"].get_pending_algo_orders(inst["symbol"])
        matched_algo = next(
            (a for a in pending_algos
             if a.get("instId") == inst["symbol"]
             and a.get("posSide", "").lower() == pos_side.lower()
             and abs(float(a.get("sz", 0.0)) - contracts) < 1e-8),
            None
        )
        if matched_algo:
            found_algo_id = matched_algo.get("algoId")
            if found_algo_id:
                algo_id = found_algo_id
                pos_data["algo_id"] = found_algo_id
                await redis_trade.set_position_state(pos_key, pos_data)
                logger.info(f"🎯 [ALGO-BOUND] Pomyślnie powiązano algoId {found_algo_id} dla pozycji {inst['label']}")

    algo_state, actual_px = await inst["client"].get_algo_order_state(algo_id)

    # 1. POGROMCA POZYCJI WIDM (TYLKO JEŚLI NIE BYŁO NIGDY RZECZYWISTEJ TRANSAKCJI)
    # [Krytyczne #1 FIX]: Jeśli pozycja zamknęła się na giełdzie, NIE purguj natychmiast! Przejdź do sekcji 4 (Rozliczenie PnL)!
    if actual_pos_on_exchange == 0.0 and algo_state in ["canceled", "order_failed"] and (time.time() - float(pos_data.get("time", 0.0))) < 10.0:
        logger.warning(f"🧹 [GHOST-PURGE] Pozycja {inst['label']} usunięta z Redis (odrzucona/anulowana przed wejściem).")
        await redis_trade.delete_key(pos_key)
        return True, pos_key

    # 2. [State Integrity #5 - BOMBA 3 FIX] AKTYWNA RE-OCHRONA W RAZIE ZNIKNIĘCIA OCO
    if actual_pos_on_exchange > 0.0 and algo_state in ["canceled", "order_failed"]:
        logger.critical(f"🚨 [OCO-VANISHED] Pozycja {inst['label']} otwarta ({actual_pos_on_exchange} sz), lecz OCO padło! Re-wystawianie...")
        spec = inst["client"].instruments_cache.get(inst["symbol"], {"tickSz": 0.1})
        tick_sz = spec["tickSz"]
        p_sl, p_tp, _ = calculate_clamped_sl_tp(entry_p, 0.0, 0.0, 1.5, tick_sz, pos_side)
        new_oco = await inst["client"].execute_futures_oco(inst["symbol"], pos_side, actual_pos_on_exchange, p_tp, p_sl, tick_sz=tick_sz)
        if new_oco and new_oco.get("code") == "0" and new_oco.get("data"):
            pos_data["algo_id"] = new_oco["data"][0].get("algoId", "")
            await redis_trade.set_position_state(pos_key, pos_data)
            await tg.push(f"🛡️ [OCO-RESTORED] {inst['label']}: Przywrócono zlecenie obronne na giełdzie.")
        else:
            logger.critical(f"🔥 [RE-OCO-FAILED] Nie udało się odtworzyć OCO. Natychmiastowe zamykanie rynkowe...")
            flattened = await inst["client"].emergency_flatten_position(inst["symbol"], pos_side)
            if flattened:
                await redis_trade.delete_key(pos_key)
                await tg.push(f"🚨 [EMERGENCY-CLOSED] Pozycja {inst['label']} zrzucona po awarii OCO.")
                return True, pos_key
            else:
                logger.critical(f"🔥 [CRITICAL-SOS] Pozycja {inst['label']} nadal wisi na OKX! Slot NIE zostaje skasowany!")
                await tg.push(
                    f"🚨🚨🚨 <b>[ALARM SOS: {inst['label']}]</b> 🚨🚨🚨\n"
                    f"Nie udało się zamknąć pozycji rynkowo po odrzuceniu OCO!\n"
                    f"Wymagana interwencja przez /emergency-liquidate!"
                )
                return False, None

    # 3. DYNAMIC BREAK-EVEN GUARD (75% drogi do TP)
    if not be_active and actual_pos_on_exchange > 0.0 and entry_p > 0.0 and algo_id not in ["EXT_MANUAL", "ATTACHED_PENDING", "ATTACHED_OKX", "NONE"]:
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
                logger.info(f"🛡️ [BREAK-EVEN] {inst['label']} przesuwa SL na {new_sl_px} (algoId: {algo_id})...")
                amend_res = await inst["client"].amend_algo_order(inst["symbol"], algo_id, new_sl_trigger_px=format_px(new_sl_px, tick_sz))
                if amend_res and amend_res.get("code") == "0":
                    pos_data["be_activated"] = True
                    pos_data["sl_price"] = new_sl_px
                    await redis_trade.set_position_state(pos_key, pos_data)
                    await tg.push(
                        f"🛡️ <b>[DYNAMIC BREAK-EVEN: {inst['label']}]</b>\n"
                        f"Pozycja: <b>{pos_side.upper()} 3x</b> | Kurs: <b>{current_market_price}</b>\n"
                        f"🔒 Nowy SL: <code>{new_sl_px} {QUOTE_CCY}</code> (Zabezpieczona na 75% TP)"
                    )

    # 4. [State Integrity #4 & Audit #1 FIX] ROZLICZENIE: USUNIĘCIE STANU TYLKO GDY EXCHANGE == 0.0
    if actual_pos_on_exchange == 0.0:
        logger.info(f"🧹 [FUTURES-RECONCILE] Pozycja {inst['label']} potwierdzona FLAT. Zwalnianie...")
        await redis_trade.delete_key(pos_key)

        real_pos_history = await inst["client"].get_last_closed_position(inst["symbol"])
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

        icon = "🎉 <b>[ZYSK TAKE PROFIT]" if pnl_net >= 0 else "🛑 <b>[STOP LOSS / WYJŚCIE]"
        cooldown_msg = ""
        if pnl_net < 0:
            quarantine_sec = CONFIG["SAFETY_GUARDS"]["SL_QUARANTINE_SECONDS"]
            await redis_trade.set_cooldown("GLOBAL_PORTFOLIO_QUARANTINE", quarantine_sec)
            await redis_trade.set_cooldown(inst["base"], CONFIG["SAFETY_GUARDS"]["SL_COOLDOWN_SECONDS"])
            cooldown_msg = f"\n⏳ Nałożono 75 min globalnej kwarantanny po stracie."

            accum_loss = await redis_trade.add_daily_loss(abs(pnl_net))
            wallet_cb = await inst["client"].get_wallet_balances(QUOTE_CCY)
            eq_cb = wallet_cb.get("total_equity", 360.0) if wallet_cb else 360.0
            max_daily_loss = eq_cb * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]
            if accum_loss >= max_daily_loss:
                await tg.push(f"🚨 <b>[CIRCUIT BREAKER]</b> Dzienna strata: -{round(accum_loss, 2)} {QUOTE_CCY}. Blokada handlu!")

        strat_display = pos_data.get("strategy", strategy_type)
        await tg.push(
            f"{icon} • {inst['label']}</b>\n"
            f"Strategia: <b>{strat_display}</b> [{pos_side.upper()}]\n"
            f"Wyjście: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {entry_p})\n"
            f"Wynik netto: <b>{pnl_net} {QUOTE_CCY} ({roe_net}%)</b>{cooldown_msg}"
        )
        return True, pos_key

    # 5. [BOMBA 3 & Audit #6 FIX] STRAŻNIK CZASU (TIME-STOP TTL)
    opened_at = float(pos_data.get("time", time.time()))
    current_strat = pos_data.get("strategy", strategy_type)
    max_timeout = CONFIG["TIMEOUTS"].get(current_strat, 28800)
    if (time.time() - opened_at) > max_timeout:
        logger.warning(f"⏳ [TIME-STOP] Pozycja {inst['label']} ({current_strat}) przekroczyła {round(max_timeout/3600, 1)}h. Likwidacja...")
        if algo_id not in ["EXT_MANUAL", "ATTACHED_PENDING", "ATTACHED_OKX", "NONE"]:
            cancel_ok = await inst["client"].cancel_algo_order(inst["symbol"], algo_id)
            if not cancel_ok:
                logger.critical(f"🚨 [ORPHAN-ALGO-RISK] Nie udało się anulować algo {algo_id} dla {inst['symbol']}!")
                await tg.push(f"⚠️ [ORPHAN-ALGO] {inst['label']}: zweryfikuj ręcznie orders-algo-pending dla {inst['symbol']}.")

        flattened = await inst["client"].emergency_flatten_position(inst["symbol"], pos_side)
        if flattened:
            await redis_trade.delete_key(pos_key)
            await tg.push(f"⏳ <b>[STRAŻNIK CZASU: {inst['label']}]</b> Zlikwidowano pozycję po przekroczeniu limitu czasu ({current_strat}).")
            return True, pos_key
        else:
            logger.critical(f"🔥 [TIME-STOP-FLATTEN-FAILED] Nie udało się zamknąć pozycji {inst['label']} po Time-Stop! Zachowuję slot.")
            await tg.push(f"🚨🚨🚨 [TIME-STOP-FAILED] {inst['label']}: zrzut rynkowy po Time-Stop zawiódł!")
            return False, None

    return False, None

async def independent_4tf_sniper_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("🎯 [4-TF SNIPER] Centralny Arbiter Portfelowy v17.1 Online.")

    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        try:
            if await redis_trade.is_cooldown_active("GLOBAL_PORTFOLIO_QUARANTINE") or await redis_trade.is_cooldown_active("PORTFOLIO_STAGGER_LOCK"):
                await interruptible_sleep(15)
                continue

            active_keys = await redis_trade.get_active_positions()
            if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                await interruptible_sleep(15)
                continue

            daily_loss = await redis_trade.get_daily_loss()
            wallet_check = await okx_client.get_wallet_balances(QUOTE_CCY)
            if wallet_check is None:
                logger.warning("⚠️ [WALLET-CHECK-NONE] Brak salda konta. Usypianie na 20s przed decyzją...")
                await interruptible_sleep(20)
                continue

            equity_check = wallet_check.get("total_equity", 360.0)
            if equity_check > 0 and daily_loss >= (equity_check * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]):
                await interruptible_sleep(60)
                continue

            best_signal = None

            for conf in FUTURES_INSTRUMENTS:
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

                # Zamknięte świece bez Look-Ahead Bias
                closes_4h = [float(c[4]) for c in c4h[:-1]]
                closes_1h = [float(c[4]) for c in c1h[:-1]]
                closes_15m = [float(c[4]) for c in c15m[:-1]]

                # EKRAN 1: H4
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

                # EKRAN 2: H1
                ema50_1h = calc_ema(closes_1h, 50)[-1]
                ema20_1h_history = calc_ema(closes_1h, 20)
                ema20_1h = ema20_1h_history[-1]
                ema20_1h_prev = ema20_1h_history[-2]

                h1_bull = closes_1h[-1] > ema50_1h and (ema20_1h > ema20_1h_prev)
                h1_bear = closes_1h[-1] < ema50_1h and (ema20_1h < ema20_1h_prev)
                if (h4_bull and not h1_bull) or (h4_bear and not h1_bear):
                    continue

                vol_1h = float(c1h[-2][5])
                vol_sma_1h = sum(float(c[5]) for c in c1h[-21:-1]) / 20.0
                if vol_sma_1h > 0 and vol_1h > (1.35 * vol_sma_1h):
                    is_bear_candle = float(c1h[-2][4]) < float(c1h[-2][1])
                    if (h4_bull and is_bear_candle) or (h4_bear and not is_bear_candle):
                        continue

                # EKRAN 3: M15
                ema20_15m = calc_ema(closes_15m, 20)[-1]
                dist_to_ema_15m = abs(closes_15m[-1] - ema20_15m) / closes_15m[-1]
                if dist_to_ema_15m >= 0.0035:
                    continue

                # EKRAN 4: M5 (SPUST SNAJPERA)
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

            if best_signal:
                sym = best_signal["symbol"]
                base = best_signal["base"]
                conf = best_signal["conf"]
                pos_side = best_signal["side"]
                strategy_name = best_signal["strategy"]

                sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(base, pos_side)
                if not sm_ok:
                    logger.warning(f"🐳 [SMART-MONEY] Odrzucono sygnał {conf['label']}: {sm_note}")
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
                    if wallet is None:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    available_cash = wallet.get("available_cash", 0.0)
                    if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    spec = okx_client.instruments_cache.get(sym) or await okx_client.load_instrument_specification(sym)
                    tick_sz = spec.get("tickSz", 0.1) if spec else 0.1

                    atr_5m = calc_atr(best_signal["c5m_full"], 14)
                    price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                        entry_live_price, atr_5m, atr_mult=1.5, rr_ratio=2.0,
                        tick_sz=tick_sz, pos_side=pos_side
                    )

                    risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                    safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                    target_margin = min(
                        risk_capital / (sl_pct * TARGET_LEVERAGE),
                        available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"],
                        safe_cash * 0.95
                    )

                    contracts, actual_margin = okx_client.calculate_contract_size(sym, entry_live_price, target_margin, safe_cash)
                    if contracts <= 0 or actual_margin > available_cash:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                        continue

                    # [Order Management #1] Atomowe wystawienie zlecenia z dołączonym SL/TP
                    order_side = "buy" if pos_side == "long" else "sell"
                    order_res = await okx_client.execute_futures_order(
                        sym, side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market",
                        attached_tp=price_tp, attached_sl=price_sl, tick_sz=tick_sz
                    )

                    if order_res and order_res.get("code") == "0":
                        main_ord_id = order_res["data"][0].get("ordId", "")
                        now_ts = time.time()
                        pos_key = f"POS_ACTIVE:ALPHA:{conf['label']}_SNIPER"
                        
                        # [Audit #1 FIX] Czekamy 0.4s na zarejestrowanie algo w silniku OKX
                        await asyncio.sleep(0.4)

                        pending_algos = await okx_client.get_pending_algo_orders(sym)
                        matched_algo = next(
                            (a for a in pending_algos
                             if a.get("instId") == sym
                             and a.get("posSide", "").lower() == pos_side.lower()
                             and abs(float(a.get("sz", 0.0)) - contracts) < 1e-8),
                            None
                        )
                        real_algo_id = matched_algo.get("algoId") if matched_algo else None

                        # Fallback jeśli attached algo nie pojawiło się w orders-algo-pending
                        if not real_algo_id:
                            logger.critical(f"🚨 [ATTACH-DISCOVERY-FAILED] {sym}: brak algoId w orders-algo-pending! Próba awaryjnego OCO...")
                            fallback_oco = await okx_client.execute_futures_oco(sym, pos_side, contracts, price_tp, price_sl, tick_sz=tick_sz)
                            if fallback_oco and fallback_oco.get("code") == "0" and fallback_oco.get("data"):
                                real_algo_id = fallback_oco["data"][0].get("algoId")
                            else:
                                logger.critical(f"🔥 [KILL-NAKED] Odrzucono awaryjne OCO! Natychmiastowe zamykanie pozycji rynkowo...")
                                flatten_ok = await okx_client.emergency_flatten_position(sym, pos_side)
                                if not flatten_ok:
                                    logger.critical(f"🔥🔥🔥 [CRITICAL-SOS] {sym}: Nie udało się zrzucić pozycji po odrzuceniu OCO!")
                                    await tg.push(f"🚨🚨🚨 [CRITICAL-SOS] {sym}: Pozycja otwarta bez OCO i nie udało się jej zamknąć!")
                                await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
                                continue

                        # [Order Management #2] Zasilenie stanu rzeczywistą ceną wykonania avgPx z pozycji
                        pos_details = await okx_client.get_position_details(sym, pos_side)
                        real_fill_px = pos_details.get("avgPx", 0.0)
                        actual_entry_price = real_fill_px if real_fill_px > 0.0 else entry_live_price
                        actual_pos_margin = pos_details.get("margin", actual_margin)

                        await redis_trade.set_position_state(pos_key, {
                            "status": "OPEN", "inst_id": sym, "algo_id": real_algo_id, "main_ord_id": main_ord_id,
                            "contracts": contracts, "pos_side": pos_side, "margin_locked": actual_pos_margin,
                            "entry_price": actual_entry_price, "tp_price": price_tp, "sl_price": price_sl,
                            "time": now_ts, "strategy": strategy_name, "be_activated": False
                        })

                        await tg.push(
                            f"🎯 <b>[4-TF SNIPER ENTRY: {conf['label']}]</b>\n"
                            f"Taktyka: <b>{strategy_name}</b> [{pos_side.upper()} 3x Izolowany]\n"
                            f"Kurs wejścia: <b>{actual_entry_price} {QUOTE_CCY}</b> | Margines: ~{actual_pos_margin} {QUOTE_CCY}\n"
                            f"Kontrakty: <b>{format_sz(contracts)} sz</b>\n"
                            f"🎯 TP: <code>{price_tp}</code> | 🛑 SL: <code>{price_sl}</code> (-{round(sl_pct*100, 2)}%)\n"
                            f"🛡️ <b>Tarcza SL: MARK PRICE (AlgoId: {real_algo_id})</b>\n"
                            f"🐳 Smart Money: <code>{sm_note}</code>\n"
                            f"🔒 Zamek Portfela aktywny przez 30 minut."
                        )
                    else:
                        await redis_trade.clear_cooldown("PORTFOLIO_STAGGER_LOCK")
        except Exception as e:
            logger.error(f"❌ [4-TF SNIPER ERROR] {e}")

        await interruptible_sleep(45)

def spawn_supervised_task(coro_fn, name: str, *args, tg: Optional[TelegramThrottledDispatcher] = None) -> asyncio.Task:
    """[Bomba 4 FIX & Supervisor #9] Nadzór nad zadaniami w tle z automatyczną pętlą restartu i alertem Telegram."""
    async def _supervisor_wrapper():
        restart_count = 0
        while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                logger.info(f"🛡️ [SUPERVISOR] Uruchamianie workera '{name}' (cykl #{restart_count})...")
                await coro_fn(*args)
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break
                logger.warning(f"⚠️ [SUPERVISOR] Worker '{name}' zakończył działanie. Wznowienie za 3s...")
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                logger.info(f"🛑 [SUPERVISOR] Worker '{name}' zatrzymany sygnałem shutdown.")
                break
            except Exception as exc:
                restart_count += 1
                logger.critical(f"💥 [SUPERVISOR-CRASH] Worker '{name}' padł z błędem: {exc}. Samoczynny restart za 5s...")
                if tg and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
                    try:
                        await tg.push(
                            f"🚨 <b>[SUPERVISOR: AWARIA & RESTART]</b>\n"
                            f"Worker: <code>{name}</code>\n"
                            f"Wyjątek: <code>{exc}</code>\n"
                            f"Restart #{restart_count} za 5 sekund."
                        )
                    except Exception:
                        pass
                await asyncio.sleep(5)

    task = asyncio.create_task(_supervisor_wrapper(), name=name)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task

async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT, RATE_LIMITER_PUBLIC, RATE_LIMITER_TRADE, RATE_LIMITER_ACCOUNT
    global GLOBAL_WS_FEED, GLOBAL_ALPHA_LOCK, GLOBAL_OKX_CLIENT, GLOBAL_REDIS_BRIDGE, GLOBAL_TG

    logger.info(f"⚡ [ENGINE-START] Uruchamianie Silnika Futures 3x v17.1 ({QUOTE_CCY})...")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    GLOBAL_ALPHA_LOCK = asyncio.Lock()

    # [Concurrency #11] Inicjalizacja niezależnych pul rate limitera
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

        try:
            logger.info("🔍 [REHYDRATION] Sprawdzanie otwartych pozycji po restarcie...")
            req_pos_path = "/api/v5/account/positions?instType=FUTURES"
            headers_p = okx_client._get_headers("GET", req_pos_path)
            await okx_client.limiter_account.consume()
            async with session.get(f"{okx_client.base_url}{req_pos_path}", headers=headers_p, timeout=6) as r_p:
                p_data = await r_p.json()
                if p_data.get("code") == "0" and p_data.get("data"):
                    KNOWN_SUFFIXES = ["_SNIPER", "_REHYDRATED", "_MR", "_MOM", "_BRK", "_PB", "_SOS_LOCKED"]
                    for pos_item in p_data["data"]:
                        pos_sz = abs(float(pos_item.get("pos", 0.0)))
                        pos_inst = pos_item.get("instId")
                        raw_side = pos_item.get("posSide", "long").lower()
                        raw_pos_float = float(pos_item.get("pos", 0.0))
                        pos_side = "long" if (raw_side == "long" or (raw_side == "net" and raw_pos_float > 0)) else "short"
                        avg_px = float(pos_item.get("avgPx", 0.0) or 0.0)
                        margin_val = float(pos_item.get("margin", 50.0) or 50.0)

                        if pos_sz > 0.0 and pos_inst:
                            matched = next((x for x in FUTURES_INSTRUMENTS if x["symbol"] == pos_inst), None)
                            if matched:
                                already_tracked = False
                                for sfx in KNOWN_SUFFIXES:
                                    chk_key = f"POS_ACTIVE:ALPHA:{matched['label']}{sfx}"
                                    if await redis_trade.get_position_state(chk_key):
                                        already_tracked = True
                                        logger.info(f"✅ [REHYDRATION-SKIP] {pos_inst} jest już śledzony jako {chk_key}.")
                                        break

                                if not already_tracked:
                                    pending_algos = await okx_client.get_pending_algo_orders(pos_inst)
                                    spec = okx_client.instruments_cache.get(pos_inst, {"tickSz": 0.1})
                                    tick_sz = spec["tickSz"]

                                    # [Audit #2 FIX] Dopasowujemy po stronie i rozmiarze, a nie ślepym [0]
                                    matching_algos = [
                                        a for a in pending_algos
                                        if a.get("posSide", "").lower() == pos_side.lower()
                                        and abs(float(a.get("sz", 0.0)) - pos_sz) < 1e-8
                                    ]
                                    detected_algo = matching_algos[0] if matching_algos else None

                                    if detected_algo:
                                        detected_algo_id = detected_algo.get("algoId")
                                        detected_tp = float(detected_algo.get("tpTriggerPx", avg_px * 1.02))
                                        detected_sl = float(detected_algo.get("slTriggerPx", avg_px * 0.98))
                                    else:
                                        # [Rehydration #14] TWARDE ZABEZPIECZENIE NAGIEJ POZYCJI
                                        logger.critical(f"🚨 [NAKED-REHYDRATION] Pozycja {pos_inst} bez OCO! Próba natychmiastowego zabezpieczenia...")
                                        p_sl, p_tp, _ = calculate_clamped_sl_tp(avg_px, 0.0, 0.0, 1.5, tick_sz, pos_side)
                                        oco_res = await okx_client.execute_futures_oco(pos_inst, pos_side, pos_sz, p_tp, p_sl, tick_sz=tick_sz)
                                        if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                            detected_algo_id = oco_res["data"][0].get("algoId")
                                            detected_tp, detected_sl = p_tp, p_sl
                                        else:
                                            # [Audit #3 & #4 FIX] Bezpieczne zamykanie lub zapis stanu SOS
                                            logger.critical(f"🔥 [KILL-NAKED] Odrzucono OCO! Zamykanie rynkowe pozycji...")
                                            flatten_ok = await okx_client.emergency_flatten_position(pos_inst, pos_side)
                                            if not flatten_ok:
                                                # Zapisujemy twardy rekord blokujący slot, aby snajper nie otworzył 2. pozycji!
                                                redis_pos_key = f"POS_ACTIVE:ALPHA:{matched['label']}_SOS_LOCKED"
                                                await redis_trade.set_position_state(redis_pos_key, {
                                                    "status": "SOS_MANUAL_REQUIRED", "inst_id": pos_inst, "algo_id": "NONE",
                                                    "contracts": pos_sz, "pos_side": pos_side, "margin_locked": margin_val,
                                                    "entry_price": avg_px, "tp_price": avg_px, "sl_price": avg_px,
                                                    "time": time.time(), "strategy": "SOS_ALERT", "be_activated": False
                                                })
                                                await tg.push(f"🚨🚨🚨 [SOS-UNTRACKED-POSITION] {pos_inst}: ręczna interwencja WYMAGANA NATYCHMIAST!")
                                            continue

                                    redis_pos_key = f"POS_ACTIVE:ALPHA:{matched['label']}_REHYDRATED"
                                    await redis_trade.set_position_state(redis_pos_key, {
                                        "status": "OPEN", "inst_id": pos_inst, "algo_id": detected_algo_id,
                                        "contracts": pos_sz, "pos_side": pos_side, "margin_locked": margin_val,
                                        "entry_price": avg_px, "tp_price": detected_tp, "sl_price": detected_sl,
                                        "time": time.time(), "strategy": "REHYDRATED_RECOVERY", "be_activated": False
                                    })
        except Exception as e:
            logger.error(f"⚠️ [REHYDRATION-FAILED] {e}")

        await tg.push(
            f"🚀 <b>Silnik Transakcyjny v17.1 PROD-HARDENED Online ({QUOTE_CCY})</b>\n"
            f"🛡️ Architektura: <b>Atomic Attached OCO + Auto-Restart Supervisor</b>\n"
            f"Sizing: 0.4% | Dźwignia: 3x Izolowana | Rozbrojone bomby: 4/4"
        )

        # [Supervisor #9] Uruchomienie workera i feedera z pętlą samonaprawiającą
        spawn_supervised_task(ws_feed.start_listener, "ws_feed_listener", symbols_to_stream, tg=tg)
        spawn_supervised_task(
            independent_4tf_sniper_worker, "sniper_4tf_worker",
            session, redis_trade, tg, okx_client, smart_money_oracle, tg=tg
        )

        instruments_for_reconciler = [
            {"client": okx_client, "symbol": item["symbol"], "base": item["base"], "label": item["label"]}
            for item in FUTURES_INSTRUMENTS
        ]

        while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                for base_inst in instruments_for_reconciler:
                    for strat_suffix in ["_SNIPER", "_REHYDRATED", "_MR", "_MOM", "_BRK", "_PB", "_SOS_LOCKED"]:
                        inst_variant = {**base_inst, "label": f"{base_inst['label']}{strat_suffix}"}
                        await reconcile_and_timestop_futures(inst_variant, "CRON_RECONCILE", redis_trade, tg)

                wallet_data = await okx_client.get_wallet_balances(QUOTE_CCY)
                eq_total = wallet_data.get("total_equity", 0.0) if wallet_data else 0.0
                cash_avail = wallet_data.get("available_cash", 0.0) if wallet_data else 0.0

                active_keys = await redis_trade.get_active_positions()
                today_loss = await redis_trade.get_daily_loss()
                max_loss_limit = round(eq_total * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"], 2)

                logger.info(
                    f"💓 [HEARTBEAT] Kapitał: {eq_total} {QUOTE_CCY} | Wolne: {cash_avail} | "
                    f"Sloty: {len(active_keys)}/{CONFIG['ALPHA_MAX_ACTIVE_SLOTS']} | Strata: {today_loss}/{max_loss_limit}"
                )
            except Exception as e:
                logger.error(f"[HEARTBEAT-ERROR] {e}")

            # [Shutdown #8] Przerywalny sen reagujący na sygnały OS
            await interruptible_sleep(60)

        # [Graceful Shutdown #8] Anulowanie i oczekiwanie na taski w tle
        logger.info("🛑 [DRAINING] Anulowanie zadań w tle przed wyłączeniem sesji...")
        for task in list(BACKGROUND_TASKS):
            if not task.done():
                task.cancel()
        if BACKGROUND_TASKS:
            await asyncio.gather(*BACKGROUND_TASKS, return_exceptions=True)
        logger.info("✅ [DRAINING-FINISHED] Wszystkie zadania w tle zostały zamknięte.")

async def interruptible_sleep(seconds: float):
    """Przerywalny sen – budzi się natychmiast po nadejściu sygnału wyłączenia."""
    if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
        return
    try:
        if ASYNC_SHUTDOWN_EVENT:
            await asyncio.wait_for(ASYNC_SHUTDOWN_EVENT.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass

def _shutdown_watchdog():
    """Strażnik czasu: daje pętli 25s na czyste wyjście, zapobiegając ucięciu SIGKILL od Rendera."""
    SHUTDOWN_COMPLETE.wait(timeout=25)
    if not SHUTDOWN_COMPLETE.is_set():
        logger.critical("🛑 [SHUTDOWN-TIMEOUT] Wymuszone zakończenie procesu przez watchdog.")
    os._exit(0)

def handle_exit_signal(sig, frame):
    PROCESS_DRAINING.set()
    logger.warning(f"🛑 [SHUTDOWN-SIGNAL] Odebrano sygnał {sig}. Rozpoczynanie czystego zamykania...")
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
        logger.critical(f"💥 [FATAL-CRASH] Pętla bota padła: {e}")
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
