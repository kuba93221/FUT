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
from datetime import datetime, UTC
from flask import Flask, jsonify, request
from typing import Dict, Any, List, Optional, Tuple
from urllib.request import Request, urlopen

# =========================================================================
# SYSTEMOWY MODUŁ OBSERVABILITY & TELEMETRII (v11.3 FUTURES 3X)
# =========================================================================
# Wymuszenie natychmiastowego zrzutu logów w kontenerze Render (brak buforowania)
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

class FlushStreamHandler(logging.StreamHandler):
    """Gwarantuje natychmiastowe wypychanie logów do konsoli Rendera bez czekania na bufor."""
    def emit(self, record):
        super().emit(record)
        self.flush()

LOG_LEVEL_CONFIG = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("FuturesEngine_OKX_SANDBOX_3X")
logger.setLevel(getattr(logging, LOG_LEVEL_CONFIG, logging.INFO))
logger.handlers.clear()

_stream_handler = FlushStreamHandler(sys.stdout)
_stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(_stream_handler)
logger.propagate = False

print("🚀 [BOOT] Silnik Futures 3x (Sandbox) inicjalizuje telemetrie na Renderze...", flush=True)

# Wymuszenie trybu Demo / Sandbox (domyślnie True dla pełnego bezpieczeństwa środków)
IS_SANDBOX = os.environ.get("OKX_IS_SANDBOX", "True").strip().lower() in ("true", "1", "yes")
logger.info(f"⚙️ [SYSTEM-INIT] Silnik Futures 3x Online [ATOMOWY LOCK | DUAL TIME-STOP | LEWAR: 3x IZOLOWANY | SANDBOX: {IS_SANDBOX}]")

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
GLOBAL_ALPHA_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
RATE_LIMITER: Optional[Any] = None
GLOBAL_WS_FEED: Optional[Any] = None

# Waluta kwotowana kontraktów perpetual SWAP na OKX (oficjalnie USDT dla rynku liniowego SWAP)
QUOTE_CCY = os.environ.get("QUOTE_CCY", "USDT").strip().upper()
TARGET_LEVERAGE = 3
TARGET_MARGIN_MODE = "isolated"

# Oficjalne instrumenty SWAP na OKX: BTC-USDT-SWAP, ETH-USDT-SWAP, SOL-USDT-SWAP, XRP-USDT-SWAP
FUTURES_INSTRUMENTS = [
    {"symbol": f"BTC-{QUOTE_CCY}-SWAP", "base": "BTC", "label": f"BTC_{QUOTE_CCY}", "price_round": 2},
    {"symbol": f"ETH-{QUOTE_CCY}-SWAP", "base": "ETH", "label": f"ETH_{QUOTE_CCY}", "price_round": 2},
    {"symbol": f"SOL-{QUOTE_CCY}-SWAP", "base": "SOL", "label": f"SOL_{QUOTE_CCY}", "price_round": 2},
    {"symbol": f"XRP-{QUOTE_CCY}-SWAP", "base": "XRP", "label": f"XRP_{QUOTE_CCY}", "price_round": 4}
]

# =========================================================================
# CENTRALNA KONFIGURACJA PARAMETRYCZNA (FUTURES 3X)
# =========================================================================
CONFIG = {
    "ALPHA_MAX_ACTIVE_SLOTS": 3,
    "MIN_ORDER_VALUE_QUOTE": 11.0,
    "RESERVE_CASH_BUFFER_QUOTE": 3.0,
    "RISK_PER_TRADE_PCT": 0.01,
    "MAX_POSITION_PORTFOLIO_RATIO": 0.18,  # max ~18% portfela na margines izolowany
    "DYNAMIC_RISK": {
        "MIN_SL_PCT": 0.008,      # 0.8% ruchu bazowego = 2.4% straty na 3x
        "MAX_SL_HARD_CAP": 0.020, # 2.0% ruchu bazowego = 6.0% straty na 3x
        "DEFAULT_SL_PCT": 0.015   # 1.5% ruchu bazowego = 4.5% straty na 3x
    },
    "TIMEOUTS": {
        "MOMENTUM": 6 * 3600,        # 6 godzin dla strategii impulsowych
        "BREAKOUT": 6 * 3600,        # 6 godzin dla wybicia zmienności
        "TREND_PULLBACK": 6 * 3600,  # 6 godzin dla wejścia z trendem
        "MEAN_REVERSION": 8 * 3600   # 8 godzin dla powrotu do średniej
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
            "ROC_TRIGGER": 2.0,
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 1.6
        },
        "BREAKOUT": {
            "BB_PERIOD": 20,
            "COMPRESSION_BANDWIDTH": 0.015,
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 2.0
        },
        "TREND_PULLBACK": {
            "EMA_FAST": 20,
            "EMA_SLOW": 50,
            "TOLERANCE_PCT": 0.0035, # Wejście w strefie 0.35% od EMA-20
            "ATR_SL_MULT": 1.2,
            "RR_RATIO": 2.2
        }
    }
}

def floor_to_precision(value: float, precision: int) -> float:
    """Rygorystyczne obcinanie wartości w dół bez ryzyka zaokrąglenia w górę."""
    factor = 10 ** precision
    return math.floor(value * factor) / factor

def calculate_clamped_sl_tp(
    current_price: float,
    atr: float,
    atr_mult: float,
    rr_ratio: float,
    price_round: int,
    pos_side: str = "long"
) -> Tuple[float, float, float]:
    """Wylicza adaptacyjny Stop Loss i Take Profit dla pozycji LONG lub SHORT na lewarze 3x."""
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
    else: # short
        price_sl = round(current_price * (1.0 + sl_pct), price_round)
        price_tp = round(current_price * (1.0 - tp_pct), price_round)

    return price_sl, price_tp, sl_pct

# =========================================================================
# SERWER MONITORINGU FLASK (ALWAYS-ON NA RENDERZE)
# =========================================================================
app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

@app.route('/', methods=['GET'])
def health_check():
    """Główny ping sprawdzający stan życia usługi."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return "FUTURES_ENGINE_STANDBY", 503
    return "FUTURES_ENGINE_ONLINE_3X_SANDBOX", 200

@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    """Endpoint dla zewnętrznych budzików (CronJob/UptimeRobot)."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest aktywna."}), 503
    return jsonify({
        "status": "success",
        "message": "Silnik Futures 3x działa w pełni autonomicznie w tle.",
        "engine": "ONLINE_3X_SANDBOX"
    }), 200

# =========================================================================
# REGULATOR PRZEPŁYWU SIECIOWEGO (TOKEN BUCKET RATE LIMITER)
# =========================================================================
class TokenBucketRateLimiter:
    """Rygorystyczny regulator przepustowości zapytań do API OKX (max 4 req/s)."""
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

# =========================================================================
# POMOST UPSTASH REDIS (SEPARACJA: DEDYKOWANY PREFIKS FUTURES_3X_)
# =========================================================================
class UpstashRedisFuturesBridge:
    """Dedykowany mostek Upstash Redis z kompresją binarną MessagePack do formatu HEX."""
    def __init__(self, url: str, token: str, session: aiohttp.ClientSession):
        self.url = url.rstrip('/') if url else ""
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        } if token else {}
        self.session = session
        self.prefix = "FUTURES_3X_"
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

# =========================================================================
# DYSPOZYTOR POWIADOMIEŃ TELEGRAM
# =========================================================================
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
        except Exception:
            pass

# =========================================================================
# RDZENIE OBLICZENIOWE QUANT (MEAN REV, MOMENTUM, BREAKOUT, TREND PULLBACK)
# =========================================================================
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
    """Strategia Trend Pullback: wejścia na retestach EMA-20 w trendzie."""
    @staticmethod
    def calculate_pullback(candles: List[List[str]]) -> Optional[Dict[str, Any]]:
        if len(candles) < 55:
            return None
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        current_price = closes[-1]

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

        # Retest średniej w trendzie wzrostowym (Kupno LONG)
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
    _TTL: float = 120.0

    @classmethod
    async def get_candles(cls, okx_client, symbol: str) -> List[List[str]]:
        now = time.monotonic()
        if symbol in cls._cache and (now - cls._cache[symbol]["time"] < cls._TTL):
            return cls._cache[symbol]["data"]

        candles = await okx_client.get_macro_candles_raw(symbol, bar="15m", limit=60)
        if candles:
            cls._cache[symbol] = {"data": candles, "time": now}
        return candles or []

    @staticmethod
    def get_regime(candles: List[List[str]]) -> str:
        if len(candles) < 20:
            return "NEUTRAL"
        closes = [float(c[4]) for c in candles]
        sma = sum(closes[-20:]) / 20.0
        variance = sum((x - sma) ** 2 for x in closes[-20:]) / 20.0
        std_dev = math.sqrt(variance) if variance > 0 else 1e-6
        bandwidth = (std_dev * 4.0) / sma if sma > 0 else 0.0

        if bandwidth > 0.020:
            return "TRENDING"
        elif bandwidth <= 0.015:
            return "RANGING"
        return "NEUTRAL"

# =========================================================================
# KLIENT ASYNCHRONICZNY WEBSOCKET Z FAILOVER I DEDYKOWANYM PINGIEM OKX
# =========================================================================
class OKXWebSocketPriceFeed:
    def __init__(self, session: aiohttp.ClientSession, is_sandbox: bool = True):
        self.session = session
        self.is_sandbox = is_sandbox
        # Lista serwerów z automatycznym przełączaniem awaryjnym (Failover)
        self.ws_endpoints = [
            "wss://wseea.okx.com:8443/ws/v5/public",
            "wss://wsaws.okx.com:8443/ws/v5/public",
            "wss://ws.okx.com:8443/ws/v5/public"
        ]
        self.current_ep_index = 0
        self.latest_prices: Dict[str, float] = {}
        self.last_msg_time = time.monotonic()
        self._running: bool = False

    async def _ping_worker(self, ws):
        """Wysyła tekstowy ping co 20 sekund zgodnie ze specyfikacją OKX WebSocket."""
        try:
            while not ws.closed and self._running:
                await asyncio.sleep(20)
                if not ws.closed:
                    await ws.send_str("ping")
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def start_listener(self, symbols: list):
        self._running = True
        sub_args = [{"channel": "tickers", "instId": sym} for sym in symbols]
        subscribe_msg = json.dumps({"op": "subscribe", "args": sub_args})

        while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            ws_url = self.ws_endpoints[self.current_ep_index % len(self.ws_endpoints)]
            try:
                logger.info(f"🌐 [WS-CONNECT] Łączenie ze strumieniem cen OKX SWAP: {ws_url}...")
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
                                            logger.info(f"📡 [WS-FEED] Odebrano pierwszy kurs SWAP {inst_id}: {last_price} {QUOTE_CCY}")
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

# =========================================================================
# SYSTEMOWY KLIENT GIEŁDY OKX FUTURES / SWAP (API V5 REST)
# =========================================================================
class OKXFuturesClient:
    """Wyspecjalizowany klient OKX API V5 dla rynku SWAP z dźwignią 3x i marginesem izolowanym."""
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter, is_sandbox: bool = True):
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
        """Weryfikacja autoryzacji z OKX Sandbox."""
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
        """Wymusza tryb pozycji dwukierunkowej (long_short_mode)."""
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

    async def load_instrument_specification(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Pobiera parametry kontraktu (ctVal, lotSz, minSz, tickSz) dla SWAP."""
        await self.rate_limiter.consume()
        request_path = f"/api/v5/public/instruments?instType=SWAP&instId={symbol}"
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
                        "ctVal": float(item.get("ctVal", 1.0)),
                        "ctValCcy": item.get("ctValCcy", ""),
                        "minSz": float(item.get("minSz", 1.0)),
                        "lotSz": float(item.get("lotSz", 1.0)),
                        "tickSz": float(item.get("tickSz", 0.1)),
                        "settleCcy": item.get("settleCcy", QUOTE_CCY)
                    }
                    self.instruments_cache[symbol] = spec
                    logger.info(f"📋 [SPEC-LOADED] {symbol} | ctVal: {spec['ctVal']} {spec['ctValCcy']} | minSz: {spec['minSz']} | lotSz: {spec['lotSz']}")
                    return spec
                logger.warning(f"⚠️ [SPEC-FAILED] Brak specyfikacji dla {symbol}: {data}")
                return None
        except Exception as e:
            logger.error(f"[FUTURES-SPEC] Błąd specyfikacji {symbol}: {e}")
            return None

    async def set_leverage(self, symbol: str, leverage: int = 3, pos_side: str = "long") -> bool:
        """Wymusza dźwignię 3x i margines izolowany."""
        await self.rate_limiter.consume()
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
            logger.error(f"[FUTURES-LEVERAGE] Błąd lewaru {symbol} [{pos_side}]: {e}")
            return False

    async def get_wallet_balances(self, preferred_ccy: str = "USDT") -> Dict[str, Any]:
        """Pobiera kapitał i wolny depozyt, obsługując automatycznie USDT oraz USDC."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}}
        await self.rate_limiter.consume()
        request_path = "/api/v5/account/balance"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)
        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    acc = data["data"][0]
                    total_eq = float(acc.get("totalEq", 0.0))
                    balances_map = {}
                    for b in acc.get("details", []):
                        c = b.get("ccy", "")
                        balances_map[c] = {
                            "availBal": float(b.get("availBal", 0.0)),
                            "eq": float(b.get("eq", 0.0))
                        }
                    
                    # Sprawdzenie dostępnej gotówki dla preferowanej waluty (USDT), fallback na USDC
                    avail_cash = 0.0
                    if preferred_ccy in balances_map:
                        avail_cash = balances_map[preferred_ccy]["availBal"]
                    elif "USDT" in balances_map and balances_map["USDT"]["availBal"] > 0:
                        avail_cash = balances_map["USDT"]["availBal"]
                    elif "USDC" in balances_map and balances_map["USDC"]["availBal"] > 0:
                        avail_cash = balances_map["USDC"]["availBal"]
                    
                    return {
                        "total_equity": total_eq,
                        "available_cash": avail_cash,
                        "preferred_ccy": preferred_ccy,
                        "balances": balances_map
                    }
                return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}}
        except Exception as e:
            logger.error(f"❌ [OKX-WALLET] Błąd salda: {e}")
            return {"total_equity": 0.0, "available_cash": 0.0, "balances": {}}

    def calculate_contract_size(
        self,
        symbol: str,
        current_price: float,
        target_margin_quote: float,
        max_allowed_margin: float
    ) -> Tuple[int, float]:
        """Przelicza zaplanowany margines na liczbę kontraktów całkowitych sz."""
        spec = self.instruments_cache.get(symbol)
        if not spec or current_price <= 0:
            return 0, 0.0

        ct_val = spec["ctVal"]
        min_sz = int(spec["minSz"])
        lot_sz = int(spec["lotSz"])

        contract_nominal_quote = ct_val * current_price
        single_contract_margin = contract_nominal_quote / self.TARGET_LEVERAGE

        if (min_sz * single_contract_margin) > max_allowed_margin:
            logger.warning(f"🛡️ [SIZING-REJECTED] {symbol}: 1 lot wymaga {round(min_sz * single_contract_margin, 2)} {QUOTE_CCY} > limit {round(max_allowed_margin, 2)} {QUOTE_CCY}.")
            return 0, 0.0

        target_nominal = target_margin_quote * self.TARGET_LEVERAGE
        raw_contracts = target_nominal / contract_nominal_quote
        contracts = math.floor(raw_contracts / lot_sz) * lot_sz

        if contracts < min_sz:
            if (min_sz * single_contract_margin) <= max_allowed_margin:
                contracts = min_sz
            else:
                return 0, 0.0

        actual_margin = (contracts * contract_nominal_quote) / self.TARGET_LEVERAGE
        return int(contracts), round(actual_margin, 2)

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Pobiera kurs SWAP z WebSocket lub REST fallback."""
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

    async def get_macro_candles_raw(self, symbol: str, bar: str = "15m", limit: int = 60) -> List[List[str]]:
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
        quantity: int,
        ord_type: str = "market",
        price: Optional[float] = None,
        reduce_only: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Składa zlecenie na rynku SWAP (izolowany margines)."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        request_path = "/api/v5/trade/order"
        body_dict = {
            "instId": symbol,
            "tdMode": self.MARGIN_MODE,
            "side": side.lower(),
            "posSide": pos_side.lower(),
            "ordType": ord_type.lower(),
            "sz": str(quantity),
            "reduceOnly": reduce_only
        }
        if ord_type == "limit" and price is not None:
            body_dict["px"] = str(price)

        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)
        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"❌ [OKX-ORDER-ERROR] Zlecenie {symbol} [{pos_side}]: {e}")
            return None

    async def execute_futures_oco(
        self,
        symbol: str,
        pos_side: str,
        quantity: int,
        price_tp: float,
        price_sl: float
    ) -> Optional[Dict[str, Any]]:
        """Uzbraja powiązane zlecenie Algo OCO (Take Profit Limit i Stop Loss Market)."""
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
            "sz": str(quantity),
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
                return await r.json()
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-ERROR] Błąd OCO dla {symbol} [{pos_side}]: {e}")
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
        except Exception as e:
            return None, None

# =========================================================================
# WSPÓLNA PROCEDURA RECONCILIACJI I DUAL TIME-STOP DLA FUTURES 3X
# =========================================================================
async def reconcile_and_timestop_futures(
    inst: Dict[str, Any],
    strategy_type: str,
    redis_trade: UpstashRedisFuturesBridge,
    tg: TelegramThrottledDispatcher
) -> Tuple[bool, Optional[str]]:
    """Uniwersalny strażnik czasu pozycji lewarowanych LONG i SHORT."""
    pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
    pos_data = await redis_trade.get_position_state(pos_key)
    if not pos_data:
        return False, None

    if pos_data.get("status") == "WAITING_OCO" and "algo_id" in pos_data:
        algo_id = pos_data["algo_id"]
        pos_side = pos_data.get("pos_side", "long")
        contracts = int(pos_data.get("contracts", 1))
        algo_state, actual_px = await inst["client"].get_algo_order_state(algo_id)

        opened_at = float(pos_data.get("time", time.time()))
        elapsed_time = time.time() - opened_at
        max_timeout = CONFIG["TIMEOUTS"].get(strategy_type, 28800)

        # 1. Dual Time-Stop Interwencja
        if algo_state not in ["filled", "canceled", "order_failed"] and elapsed_time > max_timeout:
            logger.warning(f"⏳ [TIME-STOP] Pozycja {inst['label']} ({strategy_type} [{pos_side}]) przekroczyła {round(max_timeout/3600, 1)}h. Awaryjna likwidacja...")
            await inst["client"].cancel_algo_order(inst["symbol"], algo_id)
            await asyncio.sleep(0.3)

            exit_side = "sell" if pos_side == "long" else "buy"
            await inst["client"].execute_futures_order(
                symbol=inst["symbol"],
                side=exit_side,
                pos_side=pos_side,
                quantity=contracts,
                ord_type="market",
                reduce_only=True
            )
            await redis_trade.delete_key(pos_key)
            logger.info(f"🔓 [SLOT-FREED] Zwolniono slot ALFA dla {inst['label']}.")

            await tg.push(
                f"⏳ <b>[STRAŻNIK CZASU: {inst['label']}] • WYGASZENIE TTL</b>\n"
                f"──────────────────────────────\n"
                f"📈 Strategia: <b>{strategy_type}</b> [{pos_side.upper()}]\n"
                f"⌛ Czas: <b>{round(elapsed_time/3600, 1)}h</b> / Limit: {round(max_timeout/3600, 1)}h\n"
                f"📦 Kontrakty: <b>{contracts} sz</b> (Lewar: 3x)\n"
                f"Pozycja zlikwidowana rynkowo z reduceOnly. Slot uwolniony."
            )
            return True, pos_key

        # 2. Zrealizowane wyjście (TP lub SL)
        if algo_state in ["filled", "canceled", "order_failed"]:
            logger.info(f"🧹 [FUTURES-RECONCILE] Zlecenie Algo dla {inst['label']} zakończone ({algo_state}).")
            await redis_trade.delete_key(pos_key)

            if algo_state == "filled":
                entry_p = float(pos_data.get("entry_price", 0.0))
                tp_p = float(pos_data.get("tp_price", entry_p))
                sl_p = float(pos_data.get("sl_price", entry_p))
                exit_p = actual_px if actual_px and actual_px > 0 else (tp_p if pos_side == "long" else sl_p)

                spec = inst["client"].instruments_cache.get(inst["symbol"], {"ctVal": 1.0})
                ct_val = spec["ctVal"]

                if pos_side == "long":
                    pnl_gross = (exit_p - entry_p) * contracts * ct_val
                else: # short
                    pnl_gross = (entry_p - exit_p) * contracts * ct_val

                margin_locked = float(pos_data.get("margin_locked", 1.0))
                pnl_net = round(pnl_gross - (margin_locked * 0.001), 2)
                roe_net = round((pnl_net / margin_locked) * 100.0, 2) if margin_locked > 0 else 0.0

                icon = "🎉 <b>[ZYSK TAKE PROFIT]" if pnl_net >= 0 else "🛑 <b>[STOP LOSS]"
                await tg.push(
                    f"{icon} • {inst['label']}</b>\n"
                    f"──────────────────────────────\n"
                    f"📈 Strategia: <b>{strategy_type}</b> [{pos_side.upper()}]\n"
                    f"💰 Wyjście: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {entry_p} {QUOTE_CCY})\n"
                    f"📦 Kontrakty: <b>{contracts} sz</b> (Margines: {margin_locked} {QUOTE_CCY} 3x)\n"
                    f"💵 Wynik netto: <b>{pnl_net} {QUOTE_CCY} ({roe_net}%)</b>\n"
                    f"Slot ALFA zwolniony."
                )
            return True, pos_key

    return False, None

# =========================================================================
# WORKER 1: MEAN REVERSION DUAL-DIRECTION (LONG & SHORT)
# =========================================================================
async def independent_mean_reversion_worker(session, redis_trade, tg, okx_client):
    logger.info("🌊 [MEAN-REV-WORKER] Start autonomicznego wątku Mean Reversion (Futures 3x).")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "label": f"{item['label']}_MR", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "MEAN_REVERSION", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
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

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, atr,
                            CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RR_RATIO"],
                            inst["price_round"],
                            pos_side=pos_side
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(
                            inst["symbol"], current_price, target_margin, safe_cash
                        )
                        if contracts < 1 or actual_margin > available_cash:
                            continue

                        logger.info(f"🚨 [MEAN-REV-TRIGGER] Otwarcie SWAP {inst['label']} [{pos_side.upper()}] | Kontrakty: {contracts} | Margines: {actual_margin} {QUOTE_CCY}")
                        order_res = await inst["client"].execute_futures_order(
                            inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market"
                        )

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(
                                inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl
                            )
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "contracts": contracts,
                                    "pos_side": pos_side,
                                    "margin_locked": actual_margin,
                                    "entry_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "time": now_ts,
                                    "strategy": "MEAN_REVERSION"
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • MEAN REVERSION</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"💰 Kurs wejścia: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Kontrakty: <b>{contracts} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"Strażnik Czasu: 8h | OCO: AKTYWNE"
                                )
                            else:
                                logger.critical(f"🚨 [FAIL-SAFE] Odrzucono OCO dla {inst['label']}! Natychmiastowe zamknięcie pozycji...")
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=("sell" if pos_side == "long" else "buy"),
                                    pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
        except Exception as e:
            logger.error(f"❌ [MEAN-REV-ERROR] Błąd workera: {e}")

        await asyncio.sleep(60)

# =========================================================================
# WORKER 2: MOMENTUM DUAL-DIRECTION (LONG & SHORT)
# =========================================================================
async def independent_momentum_worker(session, redis_trade, tg, okx_client):
    logger.info("🚀 [MOMENTUM-WORKER] Start autonomicznego wątku Momentum (Futures 3x).")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "label": f"{item['label']}_MOM", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "MOMENTUM", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                regime = MarketRegimeArbitrator.get_regime(candles_raw)
                if regime == "RANGING":
                    continue

                mom = MomentumQuantCore.calculate_momentum(candles_raw, period=CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ROC_PERIOD"])
                if not mom:
                    continue

                if mom["signal_long"] or mom["signal_short"]:
                    pos_side = "long" if mom["signal_long"] else "short"
                    order_side = "buy" if mom["signal_long"] else "sell"
                    current_price = mom["current"]

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, mom["atr"],
                            CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["RR_RATIO"],
                            inst["price_round"],
                            pos_side=pos_side
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(
                            inst["symbol"], current_price, target_margin, safe_cash
                        )
                        if contracts < 1 or actual_margin > available_cash:
                            continue

                        logger.info(f"🚨 [MOMENTUM-TRIGGER] Otwarcie SWAP {inst['label']} [{pos_side.upper()}] ROC: {mom['roc']}% | Kontrakty: {contracts}")
                        order_res = await inst["client"].execute_futures_order(
                            inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market"
                        )

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(
                                inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl
                            )
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "contracts": contracts,
                                    "pos_side": pos_side,
                                    "margin_locked": actual_margin,
                                    "entry_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "time": now_ts,
                                    "strategy": "MOMENTUM"
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • MOMENTUM</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"💰 Kurs: <b>{current_price} {QUOTE_CCY}</b> (ROC: {mom['roc']}%)\n"
                                    f"📦 Kontrakty: <b>{contracts} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"Strażnik Czasu: 6h | OCO: AKTYWNE"
                                )
                            else:
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=("sell" if pos_side == "long" else "buy"),
                                    pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
        except Exception as e:
            logger.error(f"❌ [MOMENTUM-ERROR] Błąd workera: {e}")

        await asyncio.sleep(180)

# =========================================================================
# WORKER 3: BREAKOUT DUAL-DIRECTION (LONG & SHORT)
# =========================================================================
async def independent_breakout_worker(session, redis_trade, tg, okx_client):
    logger.info("💥 [BREAKOUT-WORKER] Start autonomicznego wątku Breakout (Futures 3x).")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "label": f"{item['label']}_BRK", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "BREAKOUT", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                brk = BreakoutQuantCore.calculate_breakout(candles_raw, period=CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["BB_PERIOD"])
                if not brk:
                    continue

                if brk["signal_long"] or brk["signal_short"]:
                    pos_side = "long" if brk["signal_long"] else "short"
                    order_side = "buy" if brk["signal_long"] else "sell"
                    current_price = brk["current"]

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, brk["atr"],
                            CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["RR_RATIO"],
                            inst["price_round"],
                            pos_side=pos_side
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(
                            inst["symbol"], current_price, target_margin, safe_cash
                        )
                        if contracts < 1 or actual_margin > available_cash:
                            continue

                        logger.info(f"🚨 [BREAKOUT-TRIGGER] Otwarcie SWAP {inst['label']} [{pos_side.upper()}] Bw: {brk['bandwidth']} | Kontrakty: {contracts}")
                        order_res = await inst["client"].execute_futures_order(
                            inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market"
                        )

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(
                                inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl
                            )
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "contracts": contracts,
                                    "pos_side": pos_side,
                                    "margin_locked": actual_margin,
                                    "entry_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "time": now_ts,
                                    "strategy": "BREAKOUT"
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • BREAKOUT</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"💰 Kurs: <b>{current_price} {QUOTE_CCY}</b> (Banda: {brk['upper_band'] if pos_side == 'long' else brk['lower_band']})\n"
                                    f"📦 Kontrakty: <b>{contracts} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"Strażnik Czasu: 6h | OCO: AKTYWNE"
                                )
                            else:
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=("sell" if pos_side == "long" else "buy"),
                                    pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
        except Exception as e:
            logger.error(f"❌ [BREAKOUT-ERROR] Błąd workera: {e}")

        await asyncio.sleep(180)

# =========================================================================
# WORKER 4: TREND PULLBACK (RETEST EMA-20 - ZASTĄPIENIE GRIDU)
# =========================================================================
async def independent_pullback_worker(session, redis_trade, tg, okx_client):
    logger.info("🎯 [PULLBACK-WORKER] Start autonomicznego wątku Trend Pullback (retest EMA-20).")
    instruments = [
        {"client": okx_client, "symbol": item["symbol"], "label": f"{item['label']}_PB", "price_round": item["price_round"]}
        for item in FUTURES_INSTRUMENTS
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop_futures(inst, "TREND_PULLBACK", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw or len(candles_raw) < 55:
                    continue

                pb = PullbackQuantCore.calculate_pullback(candles_raw)
                if not pb:
                    continue

                if pb["signal_long"] or pb["signal_short"]:
                    pos_side = "long" if pb["signal_long"] else "short"
                    order_side = "buy" if pb["signal_long"] else "sell"
                    current_price = pb["current"]

                    async with GLOBAL_ALPHA_LOCK:
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_QUOTE"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, pb["atr"],
                            CONFIG["STRATEGY_PARAMS"]["TREND_PULLBACK"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["TREND_PULLBACK"]["RR_RATIO"],
                            inst["price_round"],
                            pos_side=pos_side
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_QUOTE"])
                        target_margin = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        contracts, actual_margin = inst["client"].calculate_contract_size(
                            inst["symbol"], current_price, target_margin, safe_cash
                        )
                        if contracts < 1 or actual_margin > available_cash:
                            continue

                        logger.info(f"🎯 [PULLBACK-TRIGGER] Wejście z trendem {inst['label']} [{pos_side.upper()}] EMA-20: {pb['ema_20']} | Kontrakty: {contracts}")
                        order_res = await inst["client"].execute_futures_order(
                            inst["symbol"], side=order_side, pos_side=pos_side, quantity=contracts, ord_type="market"
                        )

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            oco_res = await inst["client"].execute_futures_oco(
                                inst["symbol"], pos_side=pos_side, quantity=contracts, price_tp=price_tp, price_sl=price_sl
                            )
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "contracts": contracts,
                                    "pos_side": pos_side,
                                    "margin_locked": actual_margin,
                                    "entry_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "time": now_ts,
                                    "strategy": "TREND_PULLBACK"
                                })
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • TREND PULLBACK</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja: <b>{pos_side.upper()} (3x Izolowany)</b>\n"
                                    f"💰 Kurs: <b>{current_price} {QUOTE_CCY}</b> (EMA-20: {pb['ema_20']})\n"
                                    f"📦 Kontrakty: <b>{contracts} sz</b> (Margines: ~{actual_margin} {QUOTE_CCY})\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"Strażnik Czasu: 6h | OCO: AKTYWNE"
                                )
                            else:
                                await inst["client"].execute_futures_order(
                                    inst["symbol"], side=("sell" if pos_side == "long" else "buy"),
                                    pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                                )
                                await redis_trade.delete_key(pos_key)
        except Exception as e:
            logger.error(f"❌ [PULLBACK-ERROR] Błąd workera: {e}")

        await asyncio.sleep(120)

# =========================================================================
# GŁÓWNA PĘTLA ASYNCHRONICZNA WIELOZADANIOWA (CRON + WEBSOCKET)
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT, RATE_LIMITER, GLOBAL_WS_FEED, GLOBAL_ALPHA_LOCK
    logger.info("⚡ [ENGINE ONLINE] Uruchamianie Silnika Futures 3x (Sandbox)...")
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
        ws_feed = OKXWebSocketPriceFeed(session, is_sandbox=IS_SANDBOX)
        GLOBAL_WS_FEED = ws_feed

        # 1. Konfiguracja konta SWAP przy starcie
        await okx_client.set_position_mode("long_short_mode")
        symbols_to_stream = [item["symbol"] for item in FUTURES_INSTRUMENTS]
        for sym in symbols_to_stream:
            await okx_client.load_instrument_specification(sym)
            await okx_client.set_leverage(sym, TARGET_LEVERAGE, "long")
            await okx_client.set_leverage(sym, TARGET_LEVERAGE, "short")

        # 2. Start workerów asynchronicznych
        tasks = [
            asyncio.create_task(ws_feed.start_listener(symbols_to_stream)),
            asyncio.create_task(independent_mean_reversion_worker(session, redis_trade, tg, okx_client)),
            asyncio.create_task(independent_momentum_worker(session, redis_trade, tg, okx_client)),
            asyncio.create_task(independent_breakout_worker(session, redis_trade, tg, okx_client)),
            asyncio.create_task(independent_pullback_worker(session, redis_trade, tg, okx_client))
        ]

        heartbeat_timer = 0
        try:
            while not ASYNC_SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)
                heartbeat_timer += 1
                if heartbeat_timer >= 60:
                    heartbeat_timer = 0
                    prices_count = len(ws_feed.latest_prices)
                    logger.info(f"💓 [ENGINE-HEARTBEAT] 4 workery aktywne | Strumień SWAP: {prices_count}/4 par | Sandbox: {IS_SANDBOX}")
        except Exception as e:
            logger.error(f"❌ [CRON-FATAL] Awaria pętli: {e}")
        finally:
            logger.info("🛑 [SHUTDOWN] Zatrzymywanie workerów...")
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

def background_scheduler_thread():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    BACKGROUND_LOOP = loop
    try:
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e:
        logger.error(f"[THREAD FAILURE] Awaria: {e}")
    finally:
        loop.close()

# =========================================================================
# PUBLICZNE ENDPOINTY KONTROLNO-DIAGNOSTYCZNE FLASK
# =========================================================================
@app.route('/test-futures-env', methods=['GET'])
def web_test_futures_environment():
    """WIZJER DIAGNOSTYCZNY DLA DYREKTORA: Weryfikacja połączenia, salda i dźwigni 3x."""
    async def _run_diag():
        async with aiohttp.ClientSession() as session:
            limiter = TokenBucketRateLimiter()
            client = OKXFuturesClient(session, limiter, is_sandbox=IS_SANDBOX)
            redis_bridge = UpstashRedisFuturesBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                session
            )
            redis_pong = await redis_bridge.ping_check()
            auth_status = await client.test_auth_handshake()
            balance = await client.get_wallet_balances(QUOTE_CCY)

            instruments_report = []
            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                spec = await client.load_instrument_specification(sym)
                ticker = await client.get_market_ticker(sym)
                lev_l = await client.set_leverage(sym, TARGET_LEVERAGE, "long")
                lev_s = await client.set_leverage(sym, TARGET_LEVERAGE, "short")
                instruments_report.append({
                    "symbol": sym,
                    "spec_loaded": spec is not None,
                    "ct_val": spec["ctVal"] if spec else None,
                    "settle_ccy": spec["settleCcy"] if spec else None,
                    "current_price": ticker["last"] if ticker else 0.0,
                    "leverage_3x_locked": (lev_l and lev_s)
                })

            return {
                "system": "OKX_FUTURES_3X_MULTI_AGENT_ENGINE",
                "mode": "SANDBOX (DEMO)" if IS_SANDBOX else "LIVE_SUBACCOUNT",
                "quote_ccy": QUOTE_CCY,
                "redis_connected": redis_pong,
                "redis_prefix": redis_bridge.prefix,
                "okx_auth": auth_status,
                "futures_balance": balance,
                "instruments": instruments_report
            }

    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest gotowa."}), 503

    fut = asyncio.run_coroutine_threadsafe(_run_diag(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=15)
        return jsonify({"status": "success", "diagnostics": res}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/emergency-liquidate', methods=['GET', 'POST'])
def emergency_liquidate_to_cash():
    """Awaryjne odwołanie wszystkich zleceń, rynkowy zrzut kontraktów (reduceOnly) i czyszczenie Redis."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest aktywna."}), 500

    async def _execute_flush():
        async with aiohttp.ClientSession() as session:
            client = OKXFuturesClient(session, RATE_LIMITER, is_sandbox=IS_SANDBOX)
            redis_trade = UpstashRedisFuturesBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                session
            )
            report = {"cancelled_orders": [], "closed_positions": [], "redis_cleaned": False}

            # 1. Anulowanie wszystkich zleceń oczekujących
            for item in FUTURES_INSTRUMENTS:
                sym = item["symbol"]
                try:
                    await client.rate_limiter.consume()
                    req_p = f"/api/v5/trade/orders-pending?instId={sym}"
                    headers = client._get_headers("GET", req_p)
                    async with session.get(f"{client.base_url}{req_p}", headers=headers, timeout=4) as r_pend:
                        d_pend = await r_pend.json()
                        for ord_item in d_pend.get("data", []):
                            o_id = ord_item.get("ordId")
                            cancel_req = "/api/v5/trade/cancel-order"
                            b_c = json.dumps({"instId": sym, "ordId": o_id})
                            await session.post(f"{client.base_url}{cancel_req}", data=b_c, headers=client._get_headers("POST", cancel_req, b_c))
                            report["cancelled_orders"].append({"symbol": sym, "ordId": o_id})
                except Exception as ex:
                    logger.error(f"⚠️ [EMERGENCY] Błąd anulowania {sym}: {ex}")

            # 2. Zamknięcie wszystkich aktywnych pozycji kontraktowych z bazy Redis
            url_pos = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:*"
            all_pos_keys = []
            async with session.get(url_pos, headers=redis_trade.headers) as r_pos:
                if r_pos.status == 200:
                    all_pos_keys = (await r_pos.json()).get("result", [])
                    for p_key in all_pos_keys:
                        raw_data = await redis_trade.get_position_state(p_key.replace(redis_trade.prefix, ""))
                        if raw_data:
                            sym = raw_data.get("inst_id") or f"{p_key.split(':')[-1].split('_')[0]}-{QUOTE_CCY}-SWAP"
                            pos_side = raw_data.get("pos_side", "long")
                            contracts = int(raw_data.get("contracts", 1))
                            exit_side = "sell" if pos_side == "long" else "buy"

                            if "algo_id" in raw_data:
                                await client.cancel_algo_order(sym, raw_data["algo_id"])

                            res = await client.execute_futures_order(
                                symbol=sym, side=exit_side, pos_side=pos_side, quantity=contracts, ord_type="market", reduce_only=True
                            )
                            report["closed_positions"].append({"symbol": sym, "side": pos_side, "contracts": contracts, "result": res})

            # 3. Wyczyszczenie kluczy pozycji w Upstash Redis
            if all_pos_keys:
                del_payload = [["DEL"] + all_pos_keys]
                await session.post(f"{redis_trade.url}/pipeline", json=del_payload, headers=redis_trade.headers)
                report["redis_cleaned"] = True

            return report

    fut = asyncio.run_coroutine_threadsafe(_execute_flush(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=25)
        return jsonify({"status": "success", "report": res}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# =========================================================================
# GŁÓWNY PUNKT STARTU (SIGNAL HANDLER DLA RENDERA)
# =========================================================================
if __name__ == "__main__":
    worker_thread = threading.Thread(target=background_scheduler_thread, daemon=True)
    worker_thread.start()

    def main_thread_shutdown_handler(signum, frame):
        logger.warning(f"🛑 [SIGTERM/SIGINT] Zatrzymywanie Silnika Futures 3x (sygnał {signum})...")
        if BACKGROUND_LOOP and ASYNC_SHUTDOWN_EVENT:
            BACKGROUND_LOOP.call_soon_threadsafe(ASYNC_SHUTDOWN_EVENT.set)
        time.sleep(1.5)
        sys.exit(0)

    signal.signal(signal.SIGTERM, main_thread_shutdown_handler)
    signal.signal(signal.SIGINT, main_thread_shutdown_handler)

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False)
