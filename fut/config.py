"""Konfiguracja, logger, instrumenty i stałe silnika."""
import os
import sys
import logging
from datetime import datetime, timezone

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

VERSION = "v18.0"
DEFAULT_EQUITY_FALLBACK = 332.63

EMERGENCY_SECRET = os.environ.get("EMERGENCY_SECRET", "").strip()
if not EMERGENCY_SECRET or len(EMERGENCY_SECRET) < 16:
    if not IS_SANDBOX:
        logger.critical("🚨 [FATAL-CONFIG] Brak bezpiecznego EMERGENCY_SECRET (min. 16 znaków) w zmiennych środowiskowych!")
        sys.exit(1)
    else:
        logger.warning("⚠️ [SANDBOX-CONFIG] Użycie awaryjnego sekretu testowego w trybie sandbox.")
        EMERGENCY_SECRET = "sandbox-safe-kill-secret-2026"

REDIS_PREFIX = "FUTURES_3X_DEMO_" if IS_SANDBOX else "FUTURES_3X_LIVE_"


def get_seconds_until_midnight_utc() -> int:
    now = datetime.now(UTC)
    midnight = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    diff = int((midnight - now).total_seconds()) + 1
    return max(60, diff)


FUTURES_INSTRUMENTS = [
    {
        "symbol": "BTC-USD_UM_XPERP-310404" if not IS_SANDBOX else "BTC-USD_UM_XPERP-310328",
        "family": "BTC-USD_UM_XPERP",
        "base": "BTC",
        "label": "BTC_USD_XPERP",
        "price_round": 2,
        "is_usd_ct": False,  # FIXED: ctValCcy=BTC, use price*ctVal for nominal
        "ct_val": 1.0         
    },
    {
        "symbol": "ETH-USD_UM_XPERP-310404" if not IS_SANDBOX else "ETH-USD_UM_XPERP-310328",
        "family": "ETH-USD_UM_XPERP",
        "base": "ETH",
        "label": "ETH_USD_XPERP",
        "price_round": 2,
        "is_usd_ct": False,
        "ct_val": 1.0
    },
    {
        "symbol": "SOL-USD_UM_XPERP-310404" if not IS_SANDBOX else "SOL-USD_UM-260925",
        "family": "SOL-USD_UM_XPERP",
        "base": "SOL",
        "label": "SOL_USD_FUT",
        "price_round": 2,
        "is_usd_ct": False,
        "ct_val": 1.0
    },
    {
        "symbol": "XRP-USD_UM_XPERP-310404" if not IS_SANDBOX else "XRP-USD_UM_XPERP-310801",
        "family": "XRP-USD_UM_XPERP",
        "base": "XRP",
        "label": "XRP_USD_XPERP",
        "price_round": 4,
        "is_usd_ct": False,
        "ct_val": 1.0
    }
]

CONFIG = {
    "ALPHA_MAX_ACTIVE_SLOTS": 3,
    "MIN_ORDER_VALUE_QUOTE": 11.0,
    "RESERVE_CASH_BUFFER_QUOTE": 3.0,
    "RISK_PER_TRADE_PCT": 0.020,
    "MAX_POSITION_PORTFOLIO_RATIO": 0.15,
    "MAX_ENTRY_SLIPPAGE_PCT": 0.0030,
    "HARD_RISK_CAP_ON_SLIPPAGE_PCT": 0.0050,
    "DYNAMIC_RISK": {
        "MIN_SL_PCT": 0.012,
        "MAX_SL_HARD_CAP": 0.035,
        "DEFAULT_SL_PCT": 0.018,
        "VOLATILITY_CUSHION_PCT": 0.0015,
        "BREAK_EVEN_TRIGGER_RATIO": 0.80,
        "BREAK_EVEN_FEE_BUFFER_PCT": 0.0010,
        "RR_RATIO": 2.0
    },
    "TIMEOUTS": {
        "M15_CHOCH": 48 * 3600,
        "4TF_SNIPER_CORE": 48 * 3600,
        "TREND_PULLBACK": 48 * 3600,
        "REHYDRATED_RECOVERY": 48 * 3600
    },
    "SAFETY_GUARDS": {
        "PORTFOLIO_STAGGER_LOCK_SECONDS": 30 * 60,
        "SL_QUARANTINE_SECONDS": 75 * 60,
        "SL_COOLDOWN_SECONDS": 90 * 60,
        "MAX_SPREAD_PCT": 0.0020,
        "DAILY_CIRCUIT_BREAKER_PCT": 0.03,
        "SMART_MONEY": {
            "ENABLED": True,
            "BYPASS_PULLBACK": True,
            "MAX_TAKER_IMBALANCE_RATIO": 1.35,
            "CACHE_TTL_SECONDS": 180
        }
    }
}


# Bezpiecznik reżimu (regime filter): trend BTC na daily EMA50 vs EMA200 (crossover).
# LONG tylko gdy EMA50 > EMA200; SHORT tylko gdy EMA50 < EMA200.
REGIME_ENABLED = True
REGIME_BTC_SYMBOL = "BTC-USDT-SWAP"   # benchmark (USDT perp = najdłuższa, stabilna historia)
REGIME_BAR = "1D"
REGIME_EMA_FAST = 50
REGIME_EMA_PERIOD = 200
REGIME_CANDLES_LIMIT = 250
REGIME_CACHE_TTL_SEC = 3600.0


logger.info(f"⚙️ [SYSTEM-INIT] Silnik Futures 3x {VERSION} PROD (M15 CHoCH Baseline) Online [QUOTE: {QUOTE_CCY} | PREFIKS: {REDIS_PREFIX} | SANDBOX: {IS_SANDBOX}]")
