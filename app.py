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
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from typing import Dict, Any, List, Optional, Tuple

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

# INTELIGENTNY PRZEŁĄCZNIK ŚRODOWISKOWY:
DEFAULT_CCY = "USDC" if not IS_SANDBOX else "USD"
QUOTE_CCY = os.environ.get("QUOTE_CCY", DEFAULT_CCY).strip().upper()
TARGET_LEVERAGE = 3
TARGET_MARGIN_MODE = "isolated"
EMERGENCY_SECRET = os.environ.get("EMERGENCY_SECRET", "safe-kill-secret-2026").strip()

# IZOLACJA PREFIKSU REDIS: LIVE nie widzi śmieci z DEMO!
REDIS_PREFIX = "FUTURES_3X_DEMO_" if IS_SANDBOX else "FUTURES_3X_LIVE_"

logger.info(f"⚙️ [SYSTEM-INIT] Silnik Futures 3x Online [QUOTE: {QUOTE_CCY} | PREFIKS: {REDIS_PREFIX} | SANDBOX: {IS_SANDBOX}]")

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
GLOBAL_ALPHA_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
RATE_LIMITER: Optional[Any] = None
GLOBAL_WS_FEED: Optional[Any] = None
GLOBAL_OKX_CLIENT: Optional[Any] = None
GLOBAL_REDIS_BRIDGE: Optional[Any] = None
GLOBAL_TG: Optional[Any] = None

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
    "RISK_PER_TRADE_PCT": 0.01,
    "MAX_POSITION_PORTFOLIO_RATIO": 0.18,  # max ~18% wolnej gotówki na margines izolowany
    "DYNAMIC_RISK": {
        "MIN_SL_PCT": 0.008,      # 0.8% ruchu bazowego = 2.4% straty na 3x
        "MAX_SL_HARD_CAP": 0.020, # 2.0% ruchu bazowego = 6.0% straty na 3x
        "DEFAULT_SL_PCT": 0.015,  # 1.5% ruchu bazowego = 4.5% straty na 3x
        "BREAK_EVEN_TRIGGER_RATIO": 0.50, # Aktywacja BE po osiągnięciu 50% dystansu do TP
        "BREAK_EVEN_FEE_BUFFER_PCT": 0.0010 # +0.10% buforu na prowizje maklerskie OKX
    },
    "TIMEOUTS": {
        "MOMENTUM": 3 * 3600,        # 3h dla strategii impulsowych
        "BREAKOUT": 3 * 3600,        # 3h dla wybicia zmienności
        "TREND_PULLBACK": 6 * 3600,  # 6h dla wejścia z trendem
        "MEAN_REVERSION": 8 * 3600   # 8h dla powrotu do średniej
    },
    "SAFETY_GUARDS": {
        "SL_COOLDOWN_SECONDS": 90 * 60,      # TARCZA 3: 90 minut kwarantanny po uderzeniu w Stop Loss (5400s)
        "MAX_SPREAD_PCT": 0.0020,            # 0.20% maksymalnego spreadu Bid/Ask (Spread Guard)
        "DAILY_CIRCUIT_BREAKER_PCT": 0.03,   # Max 3.0% dziennej straty kapitału (Circuit Breaker)
        "SMART_MONEY": {
            "ENABLED": True,
            "MAX_TAKER_IMBALANCE_RATIO": 1.35, # Blokada gdy wolumen przeciwny instytucji > 135%
            "CACHE_TTL_SECONDS": 180           # Pamięć podręczna sentymentu (3 minuty)
        }
    },
    "STRATEGY_PARAMS": {
        "MEAN_REVERSION": {
            "Z_BUY_LONG": -1.5,
            "Z_SELL_SHORT": 1.5,
            "RSI_LONG_MAX": 35.0,
            "RSI_SHORT_MIN": 65.0,
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 1.8
        },
        "MOMENTUM": {
            "ROC_PERIOD": 10,
            "ROC_TRIGGER": 1.5,       # Zoptymalizowano do 1.5% dla lepszej responsywności
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 1.6
        },
        "BREAKOUT": {
            "BB_PERIOD": 20,
            "COMPRESSION_BANDWIDTH": 0.018,
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 2.0
        },
        "TREND_PULLBACK": {
            "EMA_FAST": 20,
            "EMA_SLOW": 50,
            "TOLERANCE_PCT": 0.0045,  # 0.45% strefy retestu EMA-20
            "ATR_SL_MULT": 1.2,
            "RR_RATIO": 2.2
        }
    }
}

# ==============================================================================
# TARCZA 4: MATEMATYCZNY FLOOR TO LOT (Bezwzględne ucinanie w dół)
# ==============================================================================
def floor_to_lot(val: float, lot_sz: float, precision: int = 8) -> float:
    """
    Rygorystycznie obcina wartość w dół do wielokrotności kroku lot_sz bez zaokrągleń w górę.
    Całkowicie eliminuje błąd braku depozytu (kod 51008) spowodowany zaokrągleniami arytmetycznymi.
    """
    if lot_sz <= 0.0 or val <= 0.0:
        return 0.0
    factor = 1.0 / lot_sz
    floored = math.floor(val * factor + 1e-12) / factor
    return round(floored, precision)

def floor_to_precision(value: float, precision: int) -> float:
    factor = 10 ** precision
    return math.floor(value * factor) / factor

def format_sz(quantity: float) -> str:
    return f"{quantity:.8f}".rstrip('0').rstrip('.')

def calculate_clamped_sl_tp(
    current_price: float,
    atr: float,
    atr_mult: float,
    rr_ratio: float,
    price_round: int,
    pos_side: str = "long"
) -> Tuple[float, float, float]:
    min_sl = CONFIG["DYNAMIC_RISK"]["MIN_SL_PCT"]
    max_sl = CONFIG["DYNAMIC_RISK"]["MAX_SL_HARD_CAP"]
    def_sl = CONFIG["DYNAMIC_RISK"]["DEFAULT_SL_PCT"]

    if atr > 0 and current_price > 0:
        raw_sl_pct = (atr * atr_mult) / current_price
    else:
        raw_sl_pct = def_sl

    sl_pct = max(min_sl, min(raw_sl_pct, max_sl))
    tp_pct = sl_pct * rr_ratio

    if pos_side == "long":
        price_sl = round(current_price * (1.0 - sl_pct), price_round)
        price_tp = round(current_price * (1.0 + tp_pct), price_round)
    else:
        price_sl = round(current_price * (1.0 + sl_pct), price_round)
        price_tp = round(current_price * (1.0 - tp_pct), price_round)

    return price_sl, price_tp, sl_pct

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return "FUTURES_ENGINE_STANDBY", 503
    return f"FUTURES_ENGINE_ONLINE_3X_{QUOTE_CCY}", 200

@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest aktywna."}), 503
    return jsonify({
        "status": "success",
        "message": f"Silnik Futures 3x ({QUOTE_CCY}) działa w pełni autonomicznie w tle.",
        "engine": f"ONLINE_3X_{QUOTE_CCY}"
    }), 200

@app.route('/reset-circuit-breaker', methods=['GET', 'POST'])
def reset_circuit_breaker_endpoint():
    """Pozwala natychmiast zresetować licznik dziennej straty w Redis."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest gotowa."}), 503

    async def _do_reset():
        async with aiohttp.ClientSession() as sess:
            redis_trade = UpstashRedisFuturesBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                sess
            )
            today_str = datetime.now(UTC).strftime('%Y%m%d')
            safe_key = f"DAILY_LOSS:{today_str}"
            await redis_trade.delete_key(safe_key)
            return {"status": "success", "message": f"Zresetowano klucz dziennej straty {safe_key} w Redis."}

    fut = asyncio.run_coroutine_threadsafe(_do_reset(), BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=10)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/reset-slots', methods=['GET', 'POST'])
def reset_slots_endpoint():
    """Usuwa wszystkie wiszące klucze slotów z Redis (odblokowanie zamrożonych pozycji)."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest gotowa."}), 503

    async def _do_flush_slots():
        async with aiohttp.ClientSession() as sess:
            redis_trade = UpstashRedisFuturesBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                sess
            )
            pattern = f"{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
            url = f"{redis_trade.url}/keys/{pattern}"
            async with sess.get(url, headers=redis_trade.headers, timeout=5) as r:
                keys = (await r.json()).get("result", []) if r.status == 200 else []
            deleted = 0
            for k in keys:
                clean_k = k.replace(redis_trade.prefix, "")
                await redis_trade.delete_key(clean_k)
                deleted += 1
            return {"status": "success", "deleted_slots_count": deleted}

    fut = asyncio.run_coroutine_threadsafe(_do_flush_slots(), BACKGROUND_LOOP)
    try:
        return jsonify(fut.result(timeout=10)), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ==============================================================================
# TARCZA 2: CZERWONY PRZYCISK AWARYJNY (/emergency-liquidate)
# ==============================================================================
@app.route('/emergency-liquidate', methods=['GET', 'POST'])
def emergency_liquidate_endpoint():
    """
    Atomowa ewakuacja konta jednym kliknięciem:
    1. Weryfikuje token autoryzacyjny secret.
    2. Anuluje wszystkie aktywne zlecenia OCO i oczekujące w arkuszu.
    3. Rynkowo zamyka wszystkie aktywne pozycje Futures (reduceOnly=True).
    4. Czyści klucze slotów w Upstash Redis.
    5. Raportuje zdarzenie na Telegram.
    """
    secret = request.args.get("secret", "").strip() or request.form.get("secret", "").strip()
    if secret != EMERGENCY_SECRET:
        return jsonify({"error": "Unauthorized. Błędny parametr secret."}), 403

    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running() or not GLOBAL_OKX_CLIENT or not GLOBAL_REDIS_BRIDGE:
        return jsonify({"error": "Silnik bota nie jest w pełni zainicjalizowany."}), 503

    async def _do_emergency():
        report = {"closed_positions": [], "canceled_pending": [], "canceled_algos": [], "freed_slots": 0}
        
        # 1. Anuluj zlecenia oczekujące i algo dla każdego instrumentu
        for item in FUTURES_INSTRUMENTS:
            sym = item["symbol"]
            try:
                req_path_p = f"/api/v5/trade/orders-pending?instType=FUTURES&instId={sym}"
                headers_p = GLOBAL_OKX_CLIENT._get_headers("GET", req_path_p)
                async with GLOBAL_OKX_CLIENT.session.get(f"{GLOBAL_OKX_CLIENT.base_url}{req_path_p}", headers=headers_p, timeout=4) as resp_p:
                    data_p = await resp_p.json()
                    if data_p.get("code") == "0" and data_p.get("data"):
                        for o in data_p["data"]:
                            ord_id = o.get("ordId")
                            c_body = json.dumps({"instId": sym, "ordId": ord_id})
                            c_headers = GLOBAL_OKX_CLIENT._get_headers("POST", "/api/v5/trade/cancel-order", c_body)
                            await GLOBAL_OKX_CLIENT.session.post(f"{GLOBAL_OKX_CLIENT.base_url}/api/v5/trade/cancel-order", data=c_body, headers=c_headers, timeout=3)
                            report["canceled_pending"].append(f"{sym}:{ord_id}")
            except Exception as e:
                logger.error(f"[EMERGENCY-CANCEL-PENDING-ERR] {sym}: {e}")

            try:
                pending_algos = await GLOBAL_OKX_CLIENT.get_pending_algo_orders(sym)
                for al in pending_algos:
                    al_id = al.get("algoId")
                    if al_id:
                        await GLOBAL_OKX_CLIENT.cancel_algo_order(sym, al_id)
                        report["canceled_algos"].append(f"{sym}:{al_id}")
            except Exception as e:
                logger.error(f"[EMERGENCY-CANCEL-ALGO-ERR] {sym}: {e}")

        # 2. Rynkowe zamykanie wszystkich aktywnych pozycji na koncie
        try:
            req_path_pos = "/api/v5/account/positions?instType=FUTURES"
            headers_pos = GLOBAL_OKX_CLIENT._get_headers("GET", req_path_pos)
            async with GLOBAL_OKX_CLIENT.session.get(f"{GLOBAL_OKX_CLIENT.base_url}{req_path_pos}", headers=headers_pos, timeout=5) as resp_pos:
                data_pos = await resp_pos.json()
                if data_pos.get("code") == "0" and data_pos.get("data"):
                    for p in data_pos["data"]:
                        raw_sz = float(p.get("pos", 0.0))
                        if abs(raw_sz) > 0.0:
                            s_id = p.get("instId")
                            p_side_raw = p.get("posSide", "net").lower()
                            is_long = (p_side_raw == "long" or (p_side_raw == "net" and raw_sz > 0))
                            close_side = "sell" if is_long else "buy"
                            actual_side = "long" if is_long else "short"
                            close_res = await GLOBAL_OKX_CLIENT.execute_futures_order(
                                symbol=s_id,
                                side=close_side,
                                pos_side=actual_side,
                                quantity=abs(raw_sz),
                                ord_type="market",
                                reduce_only=True
                            )
                            report["closed_positions"].append({
                                "symbol": s_id,
                                "size": abs(raw_sz),
                                "side": actual_side,
                                "code": close_res.get("code") if close_res else "-1"
                            })
        except Exception as e:
            logger.error(f"[EMERGENCY-POS-CLOSE-ERR] {e}")

        # 3. Czyszczenie kluczy w Upstash Redis
        try:
            pattern = f"{GLOBAL_REDIS_BRIDGE.prefix}POS_ACTIVE:ALPHA:*"
            url = f"{GLOBAL_REDIS_BRIDGE.url}/keys/{pattern}"
            async with GLOBAL_REDIS_BRIDGE.session.get(url, headers=GLOBAL_REDIS_BRIDGE.headers, timeout=5) as r:
                keys = (await r.json()).get("result", []) if r.status == 200 else []
            for k in keys:
                clean_k = k.replace(GLOBAL_REDIS_BRIDGE.prefix, "")
                await GLOBAL_REDIS_BRIDGE.delete_key(clean_k)
                report["freed_slots"] += 1
        except Exception as e:
            logger.error(f"[EMERGENCY-REDIS-CLEAR-ERR] {e}")

        # 4. Alert Telegram
        if GLOBAL_TG:
            await GLOBAL_TG.push(
                f"🚨🚨 <b>[AWARYJNA EWAKUACJA KONTA]</b> 🚨🚨\n"
                f"Zamknięte pozycje: <code>{len(report['closed_positions'])}</code>\n"
                f"Anulowane OCO: <code>{len(report['canceled_algos'])}</code>\n"
                f"Anulowane zlecenia: <code>{len(report['canceled_pending'])}</code>\n"
                f"Uwolnione sloty: <code>{report['freed_slots']}</code>\n"
                f"Status: <b>100% kapitału ewakuowane do bezpiecznego USDC</b>."
            )
        logger.critical(f"🚨 [EMERGENCY-LIQUIDATE] Wykonano natychmiastową ewakuację: {report}")
        return report

    fut = asyncio.run_coroutine_threadsafe(_do_emergency(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=15)
        return jsonify({"status": "EMERGENCY_LIQUIDATION_COMPLETED", "details": res, "timestamp": int(time.time())}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

class TokenBucketRateLimiter:
    def __init__(self, tokens_per_second: float = 4.0, max_capacity: float = 8.0):
        self.rate = tokens_per_second
        self.capacity = max_capacity
        self.tokens = max_capacity
        self.last_check = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None

    async def consume(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
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
        self._pipeline_cache: Dict[str, List[Dict[str, Any]]] = {}

    def _enforce_prefix(self, key: str) -> str:
        return key if key.startswith(self.prefix) else f"{self.prefix}{key}"

    def _safe_unpack_hex(self, hex_string: str) -> Optional[Dict[str, Any]]:
        if not hex_string or hex_string in ["None", "NULL", "none", "null"]:
            return None
        try:
            clean_hex = hex_string.strip()
            return msgpack.unpackb(bytes.fromhex(clean_hex), strict_map_key=False)
        except Exception:
            return None

    async def ping_check(self) -> bool:
        if not self.url or not self.headers:
            return False
        try:
            url = f"{self.url}/ping"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                data = await resp.json()
                return data.get("result") == "PONG"
        except Exception as e:
            logger.error(f"[REDIS-PING-ERROR] Błąd testu połączenia z Redis: {e}")
            return False

    async def push_historical_tick(self, market_id: str, tick_data: Dict[str, Any], max_elements: int = 50) -> bool:
        if not self.url:
            return False
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            hex_str = msgpack.packb(tick_data, use_bin_type=True).hex()
            pipeline_payload = [
                ["LPUSH", safe_key, hex_str],
                ["LTRIM", safe_key, "0", str(max_elements - 1)],
                ["LRANGE", safe_key, "0", str(max_elements - 1)],
                ["EXPIRE", safe_key, "604800"]
            ]
            url = f"{self.url}/pipeline"
            async with self.session.post(url, json=pipeline_payload, headers=self.headers, timeout=5) as resp:
                if resp.status != 200:
                    return False
                results = await resp.json()
                if isinstance(results, list) and len(results) >= 3:
                    cmd_res = results[2]
                    hex_list = cmd_res.get("result", []) if isinstance(cmd_res, dict) else []
                    parsed_ticks = []
                    for h in hex_list:
                        unpacked = self._safe_unpack_hex(h)
                        if unpacked:
                            parsed_ticks.append(unpacked)
                    self._pipeline_cache[market_id] = parsed_ticks
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS-PIPELINE-ERROR] Błąd zapisu historii {market_id}: {e}")
            return False

    async def get_historical_ticks(self, market_id: str, max_elements: int = 50) -> List[Dict[str, Any]]:
        cached_data = self._pipeline_cache.pop(market_id, None)
        if cached_data is not None:
            return cached_data
        if not self.url:
            return []
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            url = f"{self.url}/lrange/{safe_key}/0/{max_elements - 1}"
            async with self.session.get(url, headers=self.headers, timeout=4) as response:
                if response.status != 200:
                    return []
                res_json = await response.json()
                hex_list = res_json.get("result", []) if isinstance(res_json, dict) else []
                parsed_ticks = []
                for h in hex_list:
                    unpacked = self._safe_unpack_hex(h)
                    if unpacked:
                        parsed_ticks.append(unpacked)
                return parsed_ticks
        except Exception as e:
            logger.error(f"❌ [REDIS-READ-ERROR] Błąd odczytu historii {market_id}: {e}")
            return []

    async def set_position_state(self, pos_key: str, state_data: Dict[str, Any]) -> bool:
        if not self.url:
            return False
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
        if not self.url:
            return None
        safe_key = self._enforce_prefix(pos_key)
        try:
            url = f"{self.url}/lrange/{safe_key}/0/0"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                if resp.status != 200:
                    return None
                res_json = await resp.json()
                hex_list = res_json.get("result", []) if isinstance(res_json, dict) else []
                if hex_list:
                    return self._safe_unpack_hex(hex_list[0])
                return None
        except Exception as e:
            logger.error(f"❌ [REDIS-POS-READ-ERROR] Błąd odczytu stanu pozycji {pos_key}: {e}")
            return None

    async def delete_key(self, key: str) -> bool:
        if not self.url:
            return False
        safe_key = self._enforce_prefix(key)
        try:
            url = f"{self.url}/del/{safe_key}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-DEL-ERROR] Błąd usuwania klucza {key}: {e}")
            return False

    async def set_cooldown(self, base_symbol: str, ttl_seconds: int = 5400) -> bool:
        """TARCZA 3: Kwarantanna 90 minut po Stop Lossie (5400s)."""
        if not self.url:
            return False
        safe_key = self._enforce_prefix(f"COOLDOWN:{base_symbol}")
        try:
            url = f"{self.url}/set/{safe_key}/ACTIVE/EX/{ttl_seconds}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS-COOLDOWN-ERROR] Błąd kwarantanny {base_symbol}: {e}")
            return False

    async def is_cooldown_active(self, base_symbol: str) -> bool:
        if not self.url:
            return False
        safe_key = self._enforce_prefix(f"COOLDOWN:{base_symbol}")
        try:
            url = f"{self.url}/get/{safe_key}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
                return data.get("result") is not None and data.get("result") != ""
        except Exception:
            return False

    async def add_daily_loss(self, loss_amount: float) -> float:
        if not self.url or loss_amount <= 0:
            return 0.0
        today_str = datetime.now(UTC).strftime('%Y%m%d')
        safe_key = self._enforce_prefix(f"DAILY_LOSS:{today_str}")
        try:
            url_get = f"{self.url}/get/{safe_key}"
            current_loss = 0.0
            async with self.session.get(url_get, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    if res:
                        current_loss = float(res)
            new_total = round(current_loss + loss_amount, 4)
            url_set = f"{self.url}/set/{safe_key}/{new_total}/EX/86400"
            async with self.session.get(url_set, headers=self.headers, timeout=4):
                pass
            return new_total
        except Exception as e:
            logger.error(f"❌ [REDIS-CIRCUIT-ERROR] Błąd rejestracji straty: {e}")
            return 0.0

    async def get_daily_loss(self) -> float:
        if not self.url:
            return 0.0
        today_str = datetime.now(UTC).strftime('%Y%m%d')
        safe_key = self._enforce_prefix(f"DAILY_LOSS:{today_str}")
        try:
            url = f"{self.url}/get/{safe_key}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    if res:
                        return float(res)
            return 0.0
        except Exception:
            return 0.0

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
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML"
            }
            async with self.session.post(url, json=payload, timeout=10) as response:
                await response.read()
        except Exception as e:
            logger.error(f"❌ [TELEGRAM-ERROR] Błąd wysyłania powiadomienia: {e}")

class AlgorithmicQuantCore:
    @staticmethod
    def _calculate_ema(prices: List[float], period: int = 15) -> float:
        if len(prices) < period:
            return prices[-1] if prices else 0.0
        k = 2.0 / (period + 1.0)
        ema = sum(prices[:period]) / period
        for p in prices[period:]:
            ema = (p * k) + (ema * (1.0 - k))
        return ema

    @staticmethod
    def _calculate_rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(1, period + 1):
            change = prices[-i] - prices[-(i + 1)]
            if change > 0:
                gains += change
            else:
                losses -= change
        if losses == 0.0:
            return 100.0
        rs = (gains / period) / (losses / period)
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def calculate_z_score(ticks: List[Dict[str, Any]], macro_prices: List[float]) -> Optional[Dict[str, Any]]:
        prices = [float(t.get("last", 0)) for t in ticks if t.get("last")]
        n = len(prices)
        if n < 20:
            return None

        sma = sum(prices) / n
        variance = sum((x - sma) ** 2 for x in prices) / n
        std_dev = math.sqrt(variance)
        if std_dev == 0:
            std_dev = 1e-6

        current_price = prices[0]
        z_score = (current_price - sma) / std_dev

        use_prices = macro_prices if len(macro_prices) >= 15 else list(reversed(prices))
        ema_trend = AlgorithmicQuantCore._calculate_ema(use_prices, period=15)
        trend_direction = "LONG_ONLY" if current_price >= ema_trend else "SHORT_ONLY"

        use_rsi_prices = macro_prices if len(macro_prices) >= 15 else list(reversed(prices))
        rsi_val = AlgorithmicQuantCore._calculate_rsi(use_rsi_prices, period=14)

        bandwidth = (std_dev * 4.0) / sma if sma != 0 else 0.0
        atr_estimated = std_dev * 0.5

        return {
            "current": current_price,
            "sma": round(sma, 6),
            "z_score": round(z_score, 4),
            "trend": trend_direction,
            "rsi": round(rsi_val, 2),
            "bandwidth": round(bandwidth, 4),
            "atr": round(atr_estimated, 6)
        }

class MomentumQuantCore:
    @staticmethod
    def calculate_momentum(candles: List[List[str]], period: int = 10) -> Optional[Dict[str, Any]]:
        if len(candles) < period + 2:
            return None
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]

        current_price = closes[-1]
        past_price = closes[-period - 1]
        if past_price == 0:
            return None
        roc = ((current_price - past_price) / past_price) * 100.0

        tr_list = []
        for i in range(1, min(15, len(candles))):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(tr_list) / len(tr_list) if tr_list else 0.0

        roc_thresh = CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ROC_TRIGGER"]
        return {
            "roc": round(roc, 2),
            "current": current_price,
            "atr": atr,
            "signal_long": roc > roc_thresh,
            "signal_short": roc < -roc_thresh
        }

class BreakoutQuantCore:
    @staticmethod
    def calculate_breakout(candles: List[List[str]], period: int = 20) -> Optional[Dict[str, Any]]:
        if len(candles) < period:
            return None
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]

        current_price = closes[-1]
        sma = sum(closes[-period:]) / period
        variance = sum((x - sma) ** 2 for x in closes[-period:]) / period
        std_dev = math.sqrt(variance) if variance > 0 else 1e-6

        upper_band = sma + (2.0 * std_dev)
        lower_band = sma - (2.0 * std_dev)
        bandwidth = (upper_band - lower_band) / sma if sma > 0 else 0.0

        comp_thresh = CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["COMPRESSION_BANDWIDTH"]
        is_compression = bandwidth < comp_thresh

        tr_list = []
        for i in range(1, min(15, len(candles))):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(tr_list) / len(tr_list) if tr_list else 0.0

        return {
            "bandwidth": round(bandwidth, 4),
            "upper_band": round(upper_band, 4),
            "lower_band": round(lower_band, 4),
            "current": current_price,
            "atr": atr,
            "signal_long": is_compression and (current_price > upper_band),
            "signal_short": is_compression and (current_price < lower_band)
        }

class PullbackQuantCore:
    @staticmethod
    def calculate_pullback(candles: List[List[str]]) -> Optional[Dict[str, Any]]:
        if len(candles) < 50:
            return None
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        current_price = closes[-1]
        if current_price <= 0:
            return None

        ema_20 = AlgorithmicQuantCore._calculate_ema(closes, period=20)
        ema_50 = AlgorithmicQuantCore._calculate_ema(closes, period=50)

        tr_list = []
        for i in range(1, min(15, len(candles))):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(tr_list) / len(tr_list) if tr_list else 0.0

        tol = CONFIG["STRATEGY_PARAMS"]["TREND_PULLBACK"]["TOLERANCE_PCT"]
        is_uptrend = ema_20 > ema_50
        is_downtrend = ema_20 < ema_50

        dist_to_ema = abs(current_price - ema_20) / current_price
        near_ema = dist_to_ema <= tol

        signal_long = is_uptrend and near_ema and (current_price >= ema_20 * 0.998)
        signal_short = is_downtrend and near_ema and (current_price <= ema_20 * 1.002)

        return {
            "current": current_price,
            "ema_20": round(ema_20, 4),
            "ema_50": round(ema_50, 4),
            "atr": atr,
            "is_uptrend": is_uptrend,
            "is_downtrend": is_downtrend,
            "signal_long": signal_long,
            "signal_short": signal_short
        }

class MarketRegimeArbitrator:
    _cache: Dict[str, Dict[str, Any]] = {}
    _TTL: float = 60.0

    @classmethod
    async def get_candles(cls, okx_client, symbol: str) -> List[List[str]]:
        now = time.monotonic()
        if symbol in cls._cache and (now - cls._cache[symbol]["time"] < cls._TTL):
            return cls._cache[symbol]["data"]

        candles = await okx_client.get_macro_candles_raw(symbol, bar="15m", limit=100)
        if candles:
            cls._cache[symbol] = {"data": candles, "time": now}
        return candles or []

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
        urls_to_try = [f"{self.primary_url}{endpoint}", f"{self.fallback_url}{endpoint}"]
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"

        for url in urls_to_try:
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
        self.latest_prices: Dict[str, float] = {}
        self.last_msg_time = time.monotonic()
        self._running: bool = False

    async def _ping_worker(self, ws):
        try:
            while not ws.closed and self._running:
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
                logger.info(f"🌐 [WS-CONNECT] Łączenie ze strumieniem cen OKX SWAP EEA: {ws_url}...")
                async with self.session.ws_connect(ws_url, heartbeat=None) as ws:
                    await ws.send_str(subscribe_msg)
                    logger.info(f"📡 [WS-SUBSCRIBED] Wysłano subskrypcję SWAP dla {symbols}")
                    self.last_msg_time = time.monotonic()

                    ping_task = asyncio.create_task(self._ping_worker(ws))

                    try:
                        while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
                            try:
                                msg = await asyncio.wait_for(ws.receive(), timeout=45.0)
                            except asyncio.TimeoutError:
                                logger.warning("⚠️ [WS-WATCHDOG] Brak pakietów przez 45s. Przełączanie serwera WebSocket...")
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

                                if "event" in data:
                                    ev = data.get("event")
                                    if ev == "subscribe":
                                        logger.info(f"✅ [OKX-WS-CONFIRMED] Potwierdzono kanał: {data.get('arg')}")
                                    elif ev == "error":
                                        logger.error(f"❌ [OKX-WS-ERROR] Odpowiedź błędu z OKX: {data}")
                                    continue

                                if "data" in data and len(data["data"]) > 0:
                                    ticker = data["data"][0]
                                    inst_id = ticker.get("instId")
                                    last_price = ticker.get("last")
                                    if inst_id and last_price:
                                        prev_p = self.latest_prices.get(inst_id)
                                        self.latest_prices[inst_id] = float(last_price)
                                        if prev_p is None:
                                            logger.info(f"📡 [WS-FEED] Kurs SWAP {inst_id}: {last_price} {QUOTE_CCY}")
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                logger.warning("⚠️ [WS-DISCONNECTED] Gniazdo zamknięte. Następny serwer...")
                                self.current_ep_index += 1
                                break
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logger.error(f"❌ [WS-ERROR] Awaria strumienia SWAP ({ws_url}): {e}. Wznawianie za 5s...")
                self.current_ep_index += 1
                await asyncio.sleep(5)

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self.latest_prices.get(symbol)

class OKXFuturesClient:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter, is_sandbox: bool = False):
        self.base_url = os.environ.get("OKX_API_URL", "https://eea.okx.com").rstrip('/')
        self.session = session
        self.rate_limiter = rate_limiter
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

    async def test_auth_handshake(self) -> Dict[str, Any]:
        await self.rate_limiter.consume()
        request_path = "/api/v5/account/config"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=6) as resp:
                data = await resp.json()
                return {
                    "http_status": resp.status,
                    "code": data.get("code"),
                    "msg": data.get("msg"),
                    "pos_mode": data.get("data", [{}])[0].get("posMode") if data.get("data") else None
                }
        except Exception as e:
            return {"error": str(e)}

    async def set_position_mode(self, pos_mode: str = "long_short_mode") -> bool:
        await self.rate_limiter.consume()
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
        await self.rate_limiter.consume()
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
                        inst_fam = item.get("instFamily", "")
                        state = item.get("state", "")
                        if state == "live" and clean_target in inst_id.upper() and "XPERP" in inst_id.upper():
                            logger.info(f"🎯 [AUTO-DISCOVERY] Dopasowano symbol Live: {inst_id} (rodzina: {inst_fam})")
                            return inst_id
        except Exception as e:
            logger.warning(f"⚠️ [AUTO-DISCOVERY-FALLBACK] Błąd skanowania: {e}")
        return family_or_symbol

    async def load_instrument_specification(self, symbol: str) -> Optional[Dict[str, Any]]:
        types_to_check = ["FUTURES", "SWAP"]
        for inst_type in types_to_check:
            for attempt in range(2):
                await self.rate_limiter.consume()
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
                            logger.info(f"📋 [SPEC-LOADED] {symbol} | ctVal: {spec['ctVal']} {spec['ctValCcy']} | minSz: {spec['minSz']} | lotSz: {spec['lotSz']}")
                            return spec
                        elif data.get("code") == "50011":
                            await asyncio.sleep(0.5 * (attempt + 1))
                            continue
                except Exception as e:
                    logger.error(f"[FUTURES-SPEC] Błąd specyfikacji {symbol}: {e}")
                    await asyncio.sleep(0.3)
        return None

    async def set_leverage(self, symbol: str, leverage: int = 3, pos_side: str = "long") -> bool:
        for attempt in range(3):
            await self.rate_limiter.consume()
            request_path = "/api/v5/account/set-leverage"
            body_dict = {
                "instId": symbol,
                "lever": str(leverage),
                "mgnMode": self.MARGIN_MODE,
                "posSide": pos_side
            }
            body = json.dumps(body_dict)
            url = f"{self.base_url}{request_path}"
            headers = self._get_headers("POST", request_path, body)
            try:
                async with self.session.post(url, data=body, headers=headers, timeout=5) as resp:
                    data = await resp.json()
                    code = data.get("code")
                    if code == "0" or code == "51000" or "not modified" in data.get("msg", "").lower():
                        return True
                    if code == "50011":
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue

                    body_dict_fb = {
                        "instId": symbol,
                        "lever": str(leverage),
                        "mgnMode": self.MARGIN_MODE
                    }
                    body_fb = json.dumps(body_dict_fb)
                    headers_fb = self._get_headers("POST", request_path, body_fb)
                    async with self.session.post(url, data=body_fb, headers=headers_fb, timeout=5) as resp_fb:
                        data_fb = await resp_fb.json()
                        code_fb = data_fb.get("code")
                        if code_fb == "0" or code_fb == "51000" or "not modified" in data_fb.get("msg", "").lower():
                            return True
                        logger.error(f"❌ [FUTURES-LEVERAGE-ERROR] {symbol} [{pos_side}]: {data_fb.get('msg')} (kod: {code_fb})")
                        return False
            except Exception as e:
                logger.error(f"[FUTURES-LEVERAGE] Błąd lewaru {symbol}: {e}")
                await asyncio.sleep(0.5)
        return False

    async def get_wallet_balances(self, preferred_ccy: str = QUOTE_CCY) -> Dict[str, Any]:
        """Pancerny odczyt salda wspierający Single-Currency, Multi-Currency i Unified Margin na OKX Europe."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}}
        await self.rate_limiter.consume()
        request_path = "/api/v5/account/balance"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=6) as resp:
                data = await resp.json()
                code = str(data.get("code", "-1"))
                if code != "0":
                    err_msg = data.get("msg", "Nieznany błąd autoryzacji salda")
                    logger.error(f"❌ [OKX-WALLET-REJECTED] Błąd salda: {err_msg} (kod: {code}) | HTTP: {resp.status}")
                    return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}, "error": err_msg}

                if data.get("data") and len(data["data"]) > 0:
                    acc = data["data"][0]
                    total_eq_raw = acc.get("totalEq")
                    try:
                        total_eq = float(total_eq_raw) if total_eq_raw and str(total_eq_raw).strip() != "" else 0.0
                    except (ValueError, TypeError):
                        total_eq = 0.0

                    balances_map = {}
                    sum_eq = 0.0
                    sum_avail = 0.0

                    def _parse_num(val) -> float:
                        if val is None or str(val).strip() == "":
                            return 0.0
                        try:
                            return float(val)
                        except (ValueError, TypeError):
                            return 0.0

                    for b in acc.get("details", []):
                        c = b.get("ccy", "").upper()
                        avail_b = _parse_num(b.get("availBal"))
                        avail_e = _parse_num(b.get("availEq"))
                        cash_b = _parse_num(b.get("cashBal"))
                        eq_b = _parse_num(b.get("eq"))

                        best_avail = avail_e if avail_e > 0 else (avail_b if avail_b > 0 else cash_b)
                        best_eq = eq_b if eq_b > 0 else best_avail

                        balances_map[c] = {
                            "availBal": best_avail,
                            "eq": best_eq,
                            "cashBal": cash_b,
                            "availEq": avail_e
                        }
                        sum_eq += best_eq
                        sum_avail += best_avail

                    # Jeśli w trybie Single-Currency totalEq w USD było puste (""), używamy sumy z details:
                    if total_eq <= 0.0 and sum_eq > 0.0:
                        total_eq = sum_eq

                    # Poszukiwanie gotówki w preferowanych walutach:
                    avail_cash = 0.0
                    for check_c in [preferred_ccy, "USDC", "USD", "USDT"]:
                        if check_c in balances_map and balances_map[check_c]["availBal"] > 0:
                            avail_cash = balances_map[check_c]["availBal"]
                            break

                    if avail_cash <= 0.0:
                        if sum_avail > 0.0:
                            avail_cash = sum_avail
                        elif total_eq > 0.0:
                            avail_cash = total_eq

                    return {
                        "total_equity": round(total_eq, 2),
                        "available_cash": round(avail_cash, 2),
                        "preferred_ccy": preferred_ccy,
                        "balances": balances_map
                    }
                else:
                    logger.warning(f"⚠️ [OKX-WALLET-EMPTY] Odpowiedź /api/v5/account/balance ma pustą tablicę 'data': {data}")
                    return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}}
        except Exception as e:
            logger.error(f"❌ [OKX-WALLET-EXCEPTION] Błąd pobierania salda: {e}")
            return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}}

    def calculate_contract_size(
        self,
        symbol: str,
        current_price: float,
        target_margin_quote: float,
        max_allowed_margin: float
    ) -> Tuple[float, float]:
        """TARCZA 4: Ścisłe ucinanie lotów w dół za pomocą floor_to_lot."""
        spec = self.instruments_cache.get(symbol)
        if not spec or current_price <= 0:
            return 0.0, 0.0

        ct_val = float(spec.get("ctVal", 1.0))
        min_sz = float(spec.get("minSz", 0.01))
        lot_sz = float(spec.get("lotSz", 0.01))

        if lot_sz <= 0:
            lot_sz = 0.01
        if min_sz <= 0:
            min_sz = lot_sz

        contract_nominal_quote = ct_val * current_price
        if contract_nominal_quote <= 0:
            return 0.0, 0.0

        single_contract_margin = contract_nominal_quote / self.TARGET_LEVERAGE

        if (min_sz * single_contract_margin) > max_allowed_margin:
            return 0.0, 0.0

        target_nominal = target_margin_quote * self.TARGET_LEVERAGE
        raw_contracts = target_nominal / contract_nominal_quote

        lot_str = f"{lot_sz:.8f}".rstrip('0')
        decimals = len(lot_str.split('.')[1]) if '.' in lot_str else 0

        # TARCZA 4: Użycie floor_to_lot bez zaokrąglenia w górę
        contracts = floor_to_lot(raw_contracts, lot_sz, decimals)

        if contracts < min_sz:
            if (min_sz * single_contract_margin) <= max_allowed_margin:
                contracts = min_sz
            else:
                return 0.0, 0.0

        actual_margin = (contracts * contract_nominal_quote) / self.TARGET_LEVERAGE
        if actual_margin > max_allowed_margin:
            return 0.0, 0.0

        return contracts, round(actual_margin, 2)

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        if GLOBAL_WS_FEED:
            ws_price = GLOBAL_WS_FEED.get_last_price(symbol)
            if ws_price and ws_price > 0.0:
                return {"source": "OKX_WS", "symbol": symbol, "last": ws_price}
        await self.rate_limiter.consume()
        request_path = f"/api/v5/market/ticker?instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    item = data["data"][0]
                    return {"source": "OKX_REST", "symbol": symbol, "last": float(item.get("last", 0.0))}
                return None
        except Exception as e:
            logger.error(f"[OKX-TICKER] Błąd kursu {symbol}: {e}")
            return None

    async def get_macro_candles_raw(self, symbol: str, bar: str = "15m", limit: int = 100) -> List[List[str]]:
        await self.rate_limiter.consume()
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
                return []
        except Exception as e:
            logger.error(f"[OKX-CANDLES] Błąd świec {symbol}: {e}")
            return []

    async def execute_futures_order(
        self,
        symbol: str,
        side: str,
        pos_side: str,
        quantity: float,
        ord_type: str = "market",
        price: Optional[float] = None,
        reduce_only: bool = False,
        cl_ord_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        request_path = "/api/v5/trade/order"

        if not cl_ord_id:
            base_clean = symbol.split('-')[0].replace('_', '')[:4]
            ms_now = str(int(time.time() * 1000))[-9:]
            rand_salt = os.urandom(2).hex()
            cl_ord_id = f"A{base_clean}{ms_now}{rand_salt}"[:32]

        body_dict = {
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
            body_dict["px"] = str(price)

        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res_json = await r.json()
                if res_json.get("code") != "0":
                    err_msg = res_json.get('msg', 'Nieznany błąd')
                    data_list = res_json.get('data', [])
                    if data_list and isinstance(data_list, list) and len(data_list) > 0:
                        sub_msg = data_list[0].get('sMsg')
                        sub_code = data_list[0].get('sCode')
                        if sub_msg:
                            err_msg = f"{err_msg} [{sub_code}: {sub_msg}]"
                    logger.error(f"❌ [OKX-ORDER-REJECTED] {symbol} [{pos_side}]: {err_msg} (kod: {res_json.get('code')}) | clOrdId: {cl_ord_id}")
                return res_json
        except Exception as e:
            logger.error(f"❌ [OKX-ORDER-ERROR] Zlecenie {symbol} [{pos_side}]: {e}")
            return None

    async def execute_futures_oco(
        self,
        symbol: str,
        pos_side: str,
        quantity: float,
        price_tp: float,
        price_sl: float
    ) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
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
            "tpTriggerPx": str(price_tp),
            "tpOrdPx": "-1",
            "slTriggerPx": str(price_sl),
            "slOrdPx": "-1"
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res_json = await r.json()
                if res_json.get("code") != "0":
                    logger.error(f"❌ [OKX-OCO-REJECTED] {symbol} [{pos_side}]: {res_json.get('msg')} (kod: {res_json.get('code')})")
                return res_json
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-ERROR] Błąd OCO dla {symbol}: {e}")
            return None

    async def amend_algo_order(
        self,
        symbol: str,
        algo_id: str,
        new_sl_trigger_px: Optional[str] = None,
        new_tp_trigger_px: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        request_path = "/api/v5/trade/amend-algos"
        body_dict: Dict[str, Any] = {
            "instId": symbol,
            "algoId": str(algo_id)
        }
        if new_sl_trigger_px is not None:
            body_dict["newSlTriggerPx"] = str(new_sl_trigger_px)
            body_dict["newSlOrdPx"] = "-1"
        if new_tp_trigger_px is not None:
            body_dict["newTpTriggerPx"] = str(new_tp_trigger_px)
            body_dict["newTpOrdPx"] = "-1"

        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res_json = await r.json()
                if res_json.get("code") != "0":
                    logger.warning(f"⚠️ [AMEND-ALGO-REJECTED] {symbol} algo {algo_id}: {res_json.get('msg')} (kod: {res_json.get('code')})")
                return res_json
        except Exception as e:
            logger.error(f"❌ [AMEND-ALGO-ERROR] Błąd modyfikacji algo {algo_id}: {e}")
            return None

    async def cancel_algo_order(self, symbol: str, algo_id: str) -> bool:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return False
        await self.rate_limiter.consume()
        request_path = "/api/v5/trade/cancel-algo-orders"
        body = json.dumps([{"instId": symbol, "algoId": str(algo_id)}])
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body)
        try:
            async with self.session.post(url, data=body, headers=headers, timeout=5) as r:
                data = await r.json()
                return data.get("code") == "0"
        except Exception as e:
            logger.error(f"❌ [OKX-CANCEL-ALGO] Błąd anulowania OCO {algo_id}: {e}")
            return False

    async def get_open_position_size(self, symbol: str, pos_side: str) -> float:
        """Pobiera wielkość otwartej pozycji z pełnym wsparciem trybu Net i Long/Short."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return 0.0
        await self.rate_limiter.consume()
        request_path = f"/api/v5/account/positions?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    for p in data["data"]:
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
            logger.error(f"[OKX-POS-CHECK] Błąd pozycji {symbol}: {e}")
            return 0.0

    async def has_pending_orders(self, symbol: str) -> bool:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return False
        await self.rate_limiter.consume()
        request_path = f"/api/v5/trade/orders-pending?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return len(data["data"]) > 0
                return False
        except Exception:
            return False

    async def get_pending_algo_orders(self, symbol: str) -> List[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return []
        await self.rate_limiter.consume()
        request_path = f"/api/v5/trade/orders-algo-pending?instType=FUTURES&instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return data["data"]
                return []
        except Exception:
            return []

    async def get_last_closed_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        request_path = f"/api/v5/account/positions-history?instType=FUTURES&instId={symbol}&limit=1"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data") and len(data["data"]) > 0:
                    p = data["data"][0]
                    return {
                        "close_avg_px": float(p.get("closeAvgPx", 0.0)),
                        "realized_pnl": float(p.get("realizedPnl", 0.0)),
                        "pnl_ratio": float(p.get("pnlRatio", 0.0)) * 100.0,
                        "fee": float(p.get("fee", 0.0))
                    }
                return None
        except Exception:
            return None

    async def check_spread_allowed(self, symbol: str, max_spread_pct: float = 0.0020) -> Tuple[bool, float]:
        await self.rate_limiter.consume()
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
                    raw_bid = t.get("bidPx")
                    raw_ask = t.get("askPx")

                    if not raw_bid or not raw_ask or str(raw_bid).strip() == "" or str(raw_ask).strip() == "":
                        return False, 0.0

                    bid = float(raw_bid)
                    ask = float(raw_ask)
                    if bid <= 0 or ask <= 0:
                        return False, 0.0

                    spread_pct = (ask - bid) / bid
                    if spread_pct > max_spread_pct:
                        return False, spread_pct
                    return True, spread_pct
                return False, 0.0
        except Exception:
            return False, 0.0

    async def get_algo_order_state(self, algo_id: str) -> Tuple[Optional[str], Optional[float]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None, None
        await self.rate_limiter.consume()
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
                return None, None
        except Exception:
            return None, None

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

    if pos_data.get("status") in ["WAITING_OCO", "OPEN"] and "algo_id" in pos_data:
        algo_id = str(pos_data["algo_id"])
        pos_side = pos_data.get("pos_side", "long")
        contracts = float(pos_data.get("contracts", 0.01))
        entry_p = float(pos_data.get("entry_price", 0.0))
        tp_p = float(pos_data.get("tp_price", entry_p))
        sl_p = float(pos_data.get("sl_price", entry_p))
        margin_locked = float(pos_data.get("margin_locked", 1.0))
        be_active = pos_data.get("be_activated", False)

        actual_pos_on_exchange = await inst["client"].get_open_position_size(inst["symbol"], pos_side)
        algo_state, actual_px = await inst["client"].get_algo_order_state(algo_id)

        opened_at = float(pos_data.get("time", time.time()))
        elapsed_time = time.time() - opened_at
        max_timeout = CONFIG["TIMEOUTS"].get(strategy_type, 21600)

        # 1. POGROMCA GHOST-POSITIONS: Usuwanie widm (np. gdy pozycja na giełdzie to 0 i algo nie istnieje)
        if actual_pos_on_exchange == 0.0 and algo_state is None:
            logger.warning(f"🧹 [GHOST-PURGE] Pozycja widmo {inst['label']} usunięta z Redis (0 pozycji na giełdzie, brak algoId). Zwolniono slot.")
            await redis_trade.delete_key(pos_key)
            return True, pos_key

        # 2. DYNAMIC BREAK-EVEN GUARD
        if not be_active and actual_pos_on_exchange > 0.0 and entry_p > 0.0:
            ticker = await inst["client"].get_market_ticker(inst["symbol"])
            current_market_price = float(ticker.get("last", 0.0)) if ticker else 0.0

            if current_market_price > 0.0:
                be_ratio = CONFIG["DYNAMIC_RISK"].get("BREAK_EVEN_TRIGGER_RATIO", 0.50)
                fee_buffer_pct = CONFIG["DYNAMIC_RISK"].get("BREAK_EVEN_FEE_BUFFER_PCT", 0.0010)
                should_trigger_be = False
                new_sl_px = 0.0

                if pos_side == "long":
                    target_dist = tp_p - entry_p
                    if target_dist > 0 and (current_market_price - entry_p) >= (target_dist * be_ratio):
                        new_sl_px = round(entry_p * (1.0 + fee_buffer_pct), inst["price_round"])
                        if new_sl_px > sl_p and new_sl_px < current_market_price:
                            should_trigger_be = True
                elif pos_side == "short":
                    target_dist = entry_p - tp_p
                    if target_dist > 0 and (entry_p - current_market_price) >= (target_dist * be_ratio):
                        new_sl_px = round(entry_p * (1.0 - fee_buffer_pct), inst["price_round"])
                        if new_sl_px < sl_p and new_sl_px > current_market_price:
                            should_trigger_be = True

                if should_trigger_be and new_sl_px > 0.0:
                    logger.info(f"🛡️ [BREAK-EVEN TRIGGER] {inst['label']} osiągnął 50% TP ({current_market_price})! Przesuwanie SL na {new_sl_px}...")
                    amend_res = await inst["client"].amend_algo_order(
                        symbol=inst["symbol"],
                        algo_id=algo_id,
                        new_sl_trigger_px=str(new_sl_px)
                    )
                    if amend_res and amend_res.get("code") == "0":
                        pos_data["be_activated"] = True
                        pos_data["sl_price"] = new_sl_px
                        await redis_trade.set_position_state(pos_key, pos_data)
                        await tg.push(
                            f"🛡️ <b>[DYNAMIC BREAK-EVEN: {inst['label']}]</b>\n"
                            f"──────────────────────────────\n"
                            f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                            f"Kurs: <b>{current_market_price} {QUOTE_CCY}</b>\n"
                            f"🔒 <b>Nowy Stop Loss:</b> <code>{new_sl_px} {QUOTE_CCY}</code>\n"
                            f"✨ <b>STATUS: RYZYKO = 0.00 USD (Pozycja Darmowa)</b>"
                        )

        # 3. ROZLICZENIE ZAMKNIĘCIA POZYCJI
        is_algo_executed = algo_state in ["effective", "filled"]
        is_algo_aborted = algo_state in ["canceled", "order_failed"]

        if is_algo_executed or (is_algo_aborted and actual_pos_on_exchange == 0.0):
            logger.info(f"🧹 [FUTURES-RECONCILE] Pozycja {inst['label']} zakończona (pos={actual_pos_on_exchange}, algo={algo_state}). Zwalnianie slotu...")
            await redis_trade.delete_key(pos_key)

            real_pos_history = await inst["client"].get_last_closed_position(inst["symbol"])
            if real_pos_history and real_pos_history.get("close_avg_px", 0.0) > 0.0:
                exit_p = real_pos_history["close_avg_px"]
                pnl_net = round(real_pos_history["realized_pnl"], 2)
                roe_net = round(real_pos_history["pnl_ratio"], 2)
            else:
                tp_p_val = float(pos_data.get("tp_price", entry_p))
                sl_p_val = float(pos_data.get("sl_price", entry_p))
                exit_p = actual_px if actual_px and actual_px > 0 else (sl_p_val if pos_side == "long" else tp_p_val)
                spec = inst["client"].instruments_cache.get(inst["symbol"], {"ctVal": 1.0})
                ct_val = spec["ctVal"]

                if pos_side == "long":
                    pnl_gross = (exit_p - entry_p) * contracts * ct_val
                else:
                    pnl_gross = (entry_p - exit_p) * contracts * ct_val

                pnl_net = round(pnl_gross - (margin_locked * 0.001), 2)
                roe_net = round((pnl_net / margin_locked) * 100.0, 2) if margin_locked > 0 else 0.0

            icon = "🎉 <b>[ZYSK TAKE PROFIT]" if pnl_net >= 0 else "🛑 <b>[STOP LOSS / WYJŚCIE]"
            if be_active and abs(pnl_net) <= (margin_locked * 0.005):
                icon = "🛡️ <b>[BREAK-EVEN WYJŚCIE 0.00 USD]"

            cooldown_msg = ""
            if pnl_net < 0:
                cooldown_sec = CONFIG["SAFETY_GUARDS"]["SL_COOLDOWN_SECONDS"]
                await redis_trade.set_cooldown(inst["base"], cooldown_sec)
                cooldown_msg = f"\n⏳ <b>Kwarantanna:</b> Nałożono {int(cooldown_sec/60)} min blokady na {inst['base']}."

                accum_loss = await redis_trade.add_daily_loss(abs(pnl_net))
                wallet_cb = await inst["client"].get_wallet_balances(QUOTE_CCY)
                eq_cb = wallet_cb.get("total_equity", 360.0)
                max_daily_loss = eq_cb * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]
                if accum_loss >= max_daily_loss:
                    await tg.push(
                        f"🚨 <b>[CIRCUIT BREAKER: AKTYWACJA]</b>\n"
                        f"Dzienna strata: <b>-{round(accum_loss, 2)} {QUOTE_CCY}</b> (Limit: {round(max_daily_loss, 2)} {QUOTE_CCY}).\n"
                        f"Nowe wejścia zablokowane do jutra."
                    )

            await tg.push(
                f"{icon} • {inst['label']}</b>\n"
                f"──────────────────────────────\n"
                f"Strategia: <b>{strategy_type}</b> [{pos_side.upper()}]\n"
                f"Wyjście: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {entry_p})\n"
                f"Kontrakty: <b>{format_sz(contracts)} sz</b> (Margines: {margin_locked} {QUOTE_CCY})\n"
                f"Wynik netto: <b>{pnl_net} {QUOTE_CCY} ({roe_net}%)</b>{cooldown_msg}"
            )
            return True, pos_key

        # 4. STRAŻNIK CZASU (TIME-STOP TTL)
        if elapsed_time > max_timeout:
            logger.warning(f"⏳ [TIME-STOP] Pozycja {inst['label']} przekroczyła {round(max_timeout/3600, 1)}h. Likwidacja...")
            await inst["client"].cancel_algo_order(inst["symbol"], algo_id)
            await asyncio.sleep(0.3)

            exit_side = "sell" if pos_side == "long" else "buy"
            liq_order_res = await inst["client"].execute_futures_order(
                symbol=inst["symbol"],
                side=exit_side,
                pos_side=pos_side,
                quantity=contracts,
                ord_type="market",
                reduce_only=True
            )

            if liq_order_res and liq_order_res.get("code") == "0":
                await redis_trade.delete_key(pos_key)
                await tg.push(
                    f"⏳ <b>[STRAŻNIK CZASU: {inst['label']}] • TTL</b>\n"
                    f"Zlikwidowano pozycję po {round(elapsed_time/3600, 1)}h. Slot zwolniony."
                )
                return True, pos_key

    return False, None

async def independent_mean_reversion_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("🌊 [MEAN-REV-WORKER] Start wątku Mean Reversion.")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "base": item["base"], "label": f"{item['label']}_MR", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "MEAN_REVERSION", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                if await redis_trade.get_position_state(pos_key):
                    continue

                if await inst["client"].has_pending_orders(inst["symbol"]):
                    continue

                if await redis_trade.is_cooldown_active(inst["base"]):
                    continue

                daily_loss = await redis_trade.get_daily_loss()
                wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                equity_check = wallet_check.get("total_equity", 360.0)
                if equity_check > 0 and daily_loss >= (equity_check * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]):
                    continue

                spread_ok, _ = await inst["client"].check_spread_allowed(inst["symbol"], CONFIG["SAFETY_GUARDS"]["MAX_SPREAD_PCT"])
                if not spread_ok:
                    continue

                ticker = await inst["client"].get_market_ticker(inst["symbol"])
                if not ticker:
                    continue

                current_price = ticker.get("last", 0.0)
                await redis_trade.push_historical_tick(inst["label"], ticker, max_elements=50)
                history = await redis_trade.get_historical_ticks(inst["label"], max_elements=50)
                if len(history) < 20:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                macro_prices = [float(c[4]) for c in candles_raw] if candles_raw else []
                metrics = AlgorithmicQuantCore.calculate_z_score(history, macro_prices)
                if not metrics or metrics["bandwidth"] < 0.001:
                    continue

                z = metrics["z_score"]
                rsi = metrics["rsi"]
                trend = metrics["trend"]
                atr = metrics["atr"]

                signal_long = (z <= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["Z_BUY_LONG"] and trend == "LONG_ONLY" and rsi <= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RSI_LONG_MAX"])
                signal_short = (z >= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["Z_SELL_SHORT"] and trend == "SHORT_ONLY" and rsi >= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RSI_SHORT_MIN"])

                if signal_long or signal_short:
                    pos_side = "long" if signal_long else "short"
                    order_side = "buy" if signal_long else "sell"

                    sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(inst["base"], pos_side)
                    if not sm_ok:
                        logger.warning(f"🐳 [SMART-MONEY-GUARD] {inst['label']} [{pos_side.upper()}] odrzucone: {sm_note}")
                        continue

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        is_coin_open = any(inst["base"] in ak for ak in active_keys)
                        if is_coin_open:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, atr,
                            CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RR_RATIO"],
                            inst["price_round"], pos_side=pos_side
                        )

                        risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(inst["symbol"], current_price, target_margin, safe_cash)
                        if contracts <= 0 or actual_margin > available_cash:
                            continue

                        order_res = await inst["client"].execute_futures_order(inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market")
                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO", "inst_id": inst["symbol"], "algo_id": algo_id,
                                    "contracts": contracts, "pos_side": pos_side, "margin_locked": actual_margin,
                                    "entry_price": current_price, "tp_price": price_tp, "sl_price": price_sl,
                                    "time": now_ts, "strategy": "MEAN_REVERSION", "be_activated": False
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • MEAN REVERSION</b>\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"Kurs: <b>{current_price} {QUOTE_CCY}</b> | Margines: ~{actual_margin} {QUOTE_CCY}\n"
                                    f"🎯 TP: <code>{price_tp}</code> | 🛑 SL: <code>{price_sl}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"🐳 Smart Money: <code>{sm_note}</code>"
                                )
                            else:
                                # TARCZA 1: FAIL-SAFE KILL (Natychmiastowe wyjście reduceOnly przy odrzuceniu OCO)
                                exit_side = "sell" if pos_side == "long" else "buy"
                                logger.critical(f"🚨 [FAIL-SAFE-KILL] Odrzucono zlecenie OCO dla {inst['label']}! Awaryjna likwidacja rynkowa...")
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=exit_side, pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
                                err_msg = oco_res.get("msg") if isinstance(oco_res, dict) else "Brak odpowiedzi OCO"
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: {inst['label']}]</b>\n"
                                    f"Odrzucono zlecenie OCO: <code>{err_msg}</code>\n"
                                    f"⚡ <b>Pozycja natychmiast zamknięta rynkowo (Zero ryzyka 'gołej' pozycji).</b>"
                                )
        except Exception as e:
            logger.error(f"❌ [MEAN-REV-ERROR] {e}")

        await asyncio.sleep(60)

async def independent_momentum_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("🚀 [MOMENTUM-WORKER] Start wątku Momentum.")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "base": item["base"], "label": f"{item['label']}_MOM", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "MOMENTUM", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                if await redis_trade.get_position_state(pos_key):
                    continue

                if await inst["client"].has_pending_orders(inst["symbol"]):
                    continue

                if await redis_trade.is_cooldown_active(inst["base"]):
                    continue

                daily_loss = await redis_trade.get_daily_loss()
                wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                equity_check = wallet_check.get("total_equity", 360.0)
                if equity_check > 0 and daily_loss >= (equity_check * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]):
                    continue

                spread_ok, _ = await inst["client"].check_spread_allowed(inst["symbol"], CONFIG["SAFETY_GUARDS"]["MAX_SPREAD_PCT"])
                if not spread_ok:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                mom = MomentumQuantCore.calculate_momentum(candles_raw)
                if not mom:
                    continue

                if mom["signal_long"] or mom["signal_short"]:
                    pos_side = "long" if mom["signal_long"] else "short"
                    order_side = "buy" if mom["signal_long"] else "sell"
                    current_price = mom["current"]

                    sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(inst["base"], pos_side)
                    if not sm_ok:
                        logger.warning(f"🐳 [SMART-MONEY-GUARD] {inst['label']} [{pos_side.upper()}] odrzucone: {sm_note}")
                        continue

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        is_coin_open = any(inst["base"] in ak for ak in active_keys)
                        if is_coin_open:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, mom["atr"],
                            CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["RR_RATIO"],
                            inst["price_round"], pos_side=pos_side
                        )

                        risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(inst["symbol"], current_price, target_margin, safe_cash)
                        if contracts <= 0 or actual_margin > available_cash:
                            continue

                        order_res = await inst["client"].execute_futures_order(inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market")
                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO", "inst_id": inst["symbol"], "algo_id": algo_id,
                                    "contracts": contracts, "pos_side": pos_side, "margin_locked": actual_margin,
                                    "entry_price": current_price, "tp_price": price_tp, "sl_price": price_sl,
                                    "time": now_ts, "strategy": "MOMENTUM", "be_activated": False
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • MOMENTUM</b>\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"Kurs: <b>{current_price} {QUOTE_CCY}</b> (ROC: {mom['roc']}%)\n"
                                    f"Kontrakty: <b>{format_sz(contracts)} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 TP: <code>{price_tp}</code> | 🛑 SL: <code>{price_sl}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"🐳 Smart Money: <code>{sm_note}</code>"
                                )
                            else:
                                # TARCZA 1: FAIL-SAFE KILL (Natychmiastowe wyjście reduceOnly przy odrzuceniu OCO)
                                exit_side = "sell" if pos_side == "long" else "buy"
                                logger.critical(f"🚨 [FAIL-SAFE-KILL] Odrzucono zlecenie OCO dla {inst['label']}! Awaryjna likwidacja rynkowa...")
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=exit_side, pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
                                err_msg = oco_res.get("msg") if isinstance(oco_res, dict) else "Brak odpowiedzi OCO"
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: {inst['label']}]</b>\n"
                                    f"Odrzucono zlecenie OCO: <code>{err_msg}</code>\n"
                                    f"⚡ <b>Pozycja natychmiast zamknięta rynkowo (Zero ryzyka 'gołej' pozycji).</b>"
                                )
        except Exception as e:
            logger.error(f"❌ [MOMENTUM-ERROR] {e}")

        await asyncio.sleep(60)

async def independent_breakout_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("💥 [BREAKOUT-WORKER] Start wątku Breakout.")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "base": item["base"], "label": f"{item['label']}_BRK", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "BREAKOUT", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                if await redis_trade.get_position_state(pos_key):
                    continue

                if await inst["client"].has_pending_orders(inst["symbol"]):
                    continue

                if await redis_trade.is_cooldown_active(inst["base"]):
                    continue

                daily_loss = await redis_trade.get_daily_loss()
                wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                equity_check = wallet_check.get("total_equity", 360.0)
                if equity_check > 0 and daily_loss >= (equity_check * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]):
                    continue

                spread_ok, _ = await inst["client"].check_spread_allowed(inst["symbol"], CONFIG["SAFETY_GUARDS"]["MAX_SPREAD_PCT"])
                if not spread_ok:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                brk = BreakoutQuantCore.calculate_breakout(candles_raw)
                if not brk:
                    continue

                if brk["signal_long"] or brk["signal_short"]:
                    pos_side = "long" if brk["signal_long"] else "short"
                    order_side = "buy" if brk["signal_long"] else "sell"
                    current_price = brk["current"]

                    sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(inst["base"], pos_side)
                    if not sm_ok:
                        logger.warning(f"🐳 [SMART-MONEY-GUARD] {inst['label']} [{pos_side.upper()}] odrzucone: {sm_note}")
                        continue

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        is_coin_open = any(inst["base"] in ak for ak in active_keys)
                        if is_coin_open:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, brk["atr"],
                            CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["RR_RATIO"],
                            inst["price_round"], pos_side=pos_side
                        )

                        risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(inst["symbol"], current_price, target_margin, safe_cash)
                        if contracts <= 0 or actual_margin > available_cash:
                            continue

                        order_res = await inst["client"].execute_futures_order(inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market")
                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO", "inst_id": inst["symbol"], "algo_id": algo_id,
                                    "contracts": contracts, "pos_side": pos_side, "margin_locked": actual_margin,
                                    "entry_price": current_price, "tp_price": price_tp, "sl_price": price_sl,
                                    "time": now_ts, "strategy": "BREAKOUT", "be_activated": False
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • BREAKOUT</b>\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"Kurs: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"Kontrakty: <b>{format_sz(contracts)} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 TP: <code>{price_tp}</code> | 🛑 SL: <code>{price_sl}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"🐳 Smart Money: <code>{sm_note}</code>"
                                )
                            else:
                                # TARCZA 1: FAIL-SAFE KILL (Natychmiastowe wyjście reduceOnly przy odrzuceniu OCO)
                                exit_side = "sell" if pos_side == "long" else "buy"
                                logger.critical(f"🚨 [FAIL-SAFE-KILL] Odrzucono zlecenie OCO dla {inst['label']}! Awaryjna likwidacja rynkowa...")
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=exit_side, pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
                                err_msg = oco_res.get("msg") if isinstance(oco_res, dict) else "Brak odpowiedzi OCO"
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: {inst['label']}]</b>\n"
                                    f"Odrzucono zlecenie OCO: <code>{err_msg}</code>\n"
                                    f"⚡ <b>Pozycja natychmiast zamknięta rynkowo (Zero ryzyka 'gołej' pozycji).</b>"
                                )
        except Exception as e:
            logger.error(f"❌ [BREAKOUT-ERROR] {e}")

        await asyncio.sleep(60)

async def independent_pullback_worker(session, redis_trade, tg, okx_client, smart_money_oracle):
    logger.info("🎯 [PULLBACK-WORKER] Start wątku Trend Pullback.")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "base": item["base"], "label": f"{item['label']}_PB", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "TREND_PULLBACK", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                if await redis_trade.get_position_state(pos_key):
                    continue

                if await inst["client"].has_pending_orders(inst["symbol"]):
                    continue

                if await redis_trade.is_cooldown_active(inst["base"]):
                    continue

                daily_loss = await redis_trade.get_daily_loss()
                wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                equity_check = wallet_check.get("total_equity", 360.0)
                if equity_check > 0 and daily_loss >= (equity_check * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"]):
                    continue

                spread_ok, _ = await inst["client"].check_spread_allowed(inst["symbol"], CONFIG["SAFETY_GUARDS"]["MAX_SPREAD_PCT"])
                if not spread_ok:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw or len(candles_raw) < 50:
                    continue

                pb = PullbackQuantCore.calculate_pullback(candles_raw)
                if not pb:
                    continue

                if pb["signal_long"] or pb["signal_short"]:
                    pos_side = "long" if pb["signal_long"] else "short"
                    order_side = "buy" if pb["signal_long"] else "sell"
                    current_price = pb["current"]

                    sm_ok, sm_note, _ = await smart_money_oracle.check_smart_money_alignment(inst["base"], pos_side)
                    if not sm_ok:
                        logger.warning(f"🐳 [SMART-MONEY-GUARD] {inst['label']} [{pos_side.upper()}] odrzucone: {sm_note}")
                        continue

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        is_coin_open = any(inst["base"] in ak for ak in active_keys)
                        if is_coin_open:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, pb["atr"],
                            CONFIG["STRATEGY_PARAMS"]["TREND_PULLBACK"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["TREND_PULLBACK"]["RR_RATIO"],
                            inst["price_round"], pos_side=pos_side
                        )

                        risk_capital = available_cash * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, available_cash * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(inst["symbol"], current_price, target_margin, safe_cash)
                        if contracts <= 0 or actual_margin > available_cash:
                            continue

                        logger.info(f"🎯 [PULLBACK-TRIGGER] {inst['label']} [{pos_side.upper()}] Kontrakty: {format_sz(contracts)} | Margines: {actual_margin} {QUOTE_CCY}")
                        order_res = await inst["client"].execute_futures_order(inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market")
                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO", "inst_id": inst["symbol"], "algo_id": algo_id,
                                    "contracts": contracts, "pos_side": pos_side, "margin_locked": actual_margin,
                                    "entry_price": current_price, "tp_price": price_tp, "sl_price": price_sl,
                                    "time": now_ts, "strategy": "TREND_PULLBACK", "be_activated": False
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • TREND PULLBACK</b>\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"Kurs: <b>{current_price} {QUOTE_CCY}</b> (EMA-20: {pb['ema_20']})\n"
                                    f"Kontrakty: <b>{format_sz(contracts)} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 TP: <code>{price_tp}</code> | 🛑 SL: <code>{price_sl}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"🐳 Smart Money: <code>{sm_note}</code>"
                                )
                            else:
                                # TARCZA 1: FAIL-SAFE KILL (Natychmiastowe wyjście reduceOnly przy odrzuceniu OCO)
                                exit_side = "sell" if pos_side == "long" else "buy"
                                logger.critical(f"🚨 [FAIL-SAFE-KILL] Odrzucono zlecenie OCO dla {inst['label']}! Awaryjna likwidacja rynkowa...")
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=exit_side, pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
                                err_msg = oco_res.get("msg") if isinstance(oco_res, dict) else "Brak odpowiedzi OCO"
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: {inst['label']}]</b>\n"
                                    f"Odrzucono zlecenie OCO: <code>{err_msg}</code>\n"
                                    f"⚡ <b>Pozycja natychmiast zamknięta rynkowo (Zero ryzyka 'gołej' pozycji).</b>"
                                )
        except Exception as e:
            logger.error(f"❌ [PULLBACK-ERROR] {e}")

        await asyncio.sleep(60)

async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT, RATE_LIMITER, GLOBAL_WS_FEED, GLOBAL_ALPHA_LOCK, GLOBAL_OKX_CLIENT, GLOBAL_REDIS_BRIDGE, GLOBAL_TG
    logger.info(f"⚡ [ENGINE ONLINE] Uruchamianie Silnika Futures 3x ({QUOTE_CCY} / Prefiks: {REDIS_PREFIX} / Sandbox: {IS_SANDBOX})...")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    GLOBAL_ALPHA_LOCK = asyncio.Lock()
    if RATE_LIMITER is None:
        RATE_LIMITER = TokenBucketRateLimiter()

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
        okx_client = OKXFuturesClient(session, RATE_LIMITER, is_sandbox=IS_SANDBOX)
        smart_money_oracle = OKXSmartMoneyOracle(session, RATE_LIMITER, is_sandbox=IS_SANDBOX)
        ws_feed = OKXWebSocketPriceFeed(session, is_sandbox=IS_SANDBOX)
        GLOBAL_WS_FEED = ws_feed
        GLOBAL_OKX_CLIENT = okx_client
        GLOBAL_REDIS_BRIDGE = redis_trade
        GLOBAL_TG = tg

        await okx_client.set_position_mode("long_short_mode")
        await asyncio.sleep(0.5)

        for item in FUTURES_INSTRUMENTS:
            resolved_id = await okx_client.auto_resolve_xperp_symbol(item["symbol"])
            if resolved_id and resolved_id != item["symbol"]:
                logger.info(f"🔄 [SYMBOL-RESOLVED] Podmieniono {item['symbol']} -> {resolved_id}")
                item["symbol"] = resolved_id

        symbols_to_stream = [item["symbol"] for item in FUTURES_INSTRUMENTS]
        for sym in symbols_to_stream:
            spec = await okx_client.load_instrument_specification(sym)
            await asyncio.sleep(0.2)
            lev_l = await okx_client.set_leverage(sym, TARGET_LEVERAGE, "long")
            await asyncio.sleep(0.2)
            lev_s = await okx_client.set_leverage(sym, TARGET_LEVERAGE, "short")
            await asyncio.sleep(0.3)
            if spec:
                logger.info(f"🛡️ [LEVERAGE-STATUS] {sym} | Dźwignia 3x [LONG: {lev_l}, SHORT: {lev_s}]")

        # REHYDRATION Z PEŁNĄ DIAGNOSTYKĄ
        try:
            logger.info("🔍 [REHYDRATION] Sprawdzanie otwartych pozycji na giełdzie po restarcie...")
            await okx_client.rate_limiter.consume()
            pos_req_path = "/api/v5/account/positions?instType=FUTURES"
            headers_p = okx_client._get_headers("GET", pos_req_path)
            async with session.get(f"{okx_client.base_url}{pos_req_path}", headers=headers_p, timeout=6) as r_p:
                p_data = await r_p.json()
                if p_data.get("code") != "0":
                    logger.error(f"⚠️ [REHYDRATION-API-ERROR] Błąd sprawdzania pozycji: {p_data.get('msg')} (kod: {p_data.get('code')})")
                elif p_data.get("code") == "0" and p_data.get("data"):
                    for pos_item in p_data["data"]:
                        pos_sz = abs(float(pos_item.get("pos", 0.0)))
                        pos_inst = pos_item.get("instId")
                        raw_side = pos_item.get("posSide", "long").lower()
                        raw_pos_float = float(pos_item.get("pos", 0.0))
                        pos_side = "long" if (raw_side == "long" or (raw_side == "net" and raw_pos_float > 0)) else "short"
                        avg_px = float(pos_item.get("avgPx", 0.0))
                        margin_val = float(pos_item.get("margin", 50.0))

                        if pos_sz > 0.0 and pos_inst:
                            matched_inst = next((x for x in FUTURES_INSTRUMENTS if x["symbol"] == pos_inst), None)
                            if matched_inst:
                                redis_pos_key = f"POS_ACTIVE:ALPHA:{matched_inst['label']}_REHYDRATED"
                                existing = await redis_trade.get_position_state(redis_pos_key)

                                if not existing:
                                    pending_algos = await okx_client.get_pending_algo_orders(pos_inst)
                                    detected_algo_id = pending_algos[0].get("algoId") if pending_algos else "EXT_MANUAL"
                                    detected_tp = float(pending_algos[0].get("tpTriggerPx", avg_px * 1.02)) if pending_algos else (avg_px * 1.02 if pos_side == "long" else avg_px * 0.98)
                                    detected_sl = float(pending_algos[0].get("slTriggerPx", avg_px * 0.98)) if pending_algos else (avg_px * 0.98 if pos_side == "long" else avg_px * 1.02)

                                    await redis_trade.set_position_state(redis_pos_key, {
                                        "status": "WAITING_OCO", "inst_id": pos_inst, "algo_id": detected_algo_id,
                                        "contracts": pos_sz, "pos_side": pos_side, "margin_locked": margin_val,
                                        "entry_price": avg_px, "tp_price": round(detected_tp, matched_inst["price_round"]),
                                        "sl_price": round(detected_sl, matched_inst["price_round"]), "time": time.time(),
                                        "strategy": "REHYDRATED_RECOVERY", "be_activated": False
                                    })
                                    logger.info(f"🔄 [REHYDRATION-SUCCESS] Wskrzeszono {pos_inst} [{pos_side.upper()}]: {pos_sz} sz")
        except Exception as exc_rehyd:
            logger.error(f"⚠️ [REHYDRATION-FAILED] {exc_rehyd}")

        await tg.push(
            f"🚀 <b>Silnik Futures 3x Online ({QUOTE_CCY})! [PREFIKS: {REDIS_PREFIX}]</b>\n"
            f"Sloty: {CONFIG['ALPHA_MAX_ACTIVE_SLOTS']} | Dźwignia: 3x Izolowana\n"
            f"Gotowy do handlu."
        )

        asyncio.create_task(ws_feed.start_listener(symbols_to_stream))
        asyncio.create_task(independent_mean_reversion_worker(session, redis_trade, tg, okx_client, smart_money_oracle))
        asyncio.create_task(independent_momentum_worker(session, redis_trade, tg, okx_client, smart_money_oracle))
        asyncio.create_task(independent_breakout_worker(session, redis_trade, tg, okx_client, smart_money_oracle))
        asyncio.create_task(independent_pullback_worker(session, redis_trade, tg, okx_client, smart_money_oracle))

        while not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                wallet_data = await okx_client.get_wallet_balances(QUOTE_CCY)
                eq_total = wallet_data.get("total_equity", 0.0)
                cash_avail = wallet_data.get("available_cash", 0.0)

                url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                async with session.get(url_keys, headers=redis_trade.headers, timeout=4) as r_k:
                    active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                today_loss = await redis_trade.get_daily_loss()
                max_loss_limit = round(eq_total * CONFIG["SAFETY_GUARDS"]["DAILY_CIRCUIT_BREAKER_PCT"], 2)

                status_flag = "🟢 OK"
                if today_loss >= max_loss_limit and max_loss_limit > 0:
                    status_flag = "🛑 CIRCUIT-BREAKER"
                elif cash_avail < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                    status_flag = "⚠️ NISKI KAPITAŁ"

                logger.info(
                    f"💓 [HEARTBEAT] Strumień: 4/4 | Kapitał: {eq_total} USD | Wolne: {cash_avail} | "
                    f"Sloty: {len(active_keys)}/{CONFIG['ALPHA_MAX_ACTIVE_SLOTS']} | "
                    f"Strata dziś: {today_loss}/{max_loss_limit} [{status_flag}]"
                )
            except Exception as e:
                logger.error(f"[HEARTBEAT-CHECK-ERROR] {e}")

            await asyncio.sleep(60)

def start_background_loop():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    BACKGROUND_LOOP = loop
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e:
        logger.critical(f"💥 [CRITICAL-FATAL] Pętla bota padła: {e}")
    finally:
        loop.close()

bg_thread = threading.Thread(target=start_background_loop, daemon=True, name="FuturesEngineThread")
bg_thread.start()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
