"""
Test end-to-end silnika FUT (v18.0) — pełna ścieżka:
sygnał CHoCH M15 (LONG) → zlecenie OKX → ochrona OCO → zapis Redis PROTECTED → powiadomienie Telegram.

Bez połączenia sieciowego: zależności zewnętrzne (OKX REST, Telegram) są podmienione na fake'y,
a Redis używa natywnego trybu in-memory (UpstashRedisFuturesBridge z url=""), więc cały przepływ
sterowania oraz zapisy stanu są weryfikowane na PRAWDZIWYM kodzie produkcyjnym.

Uruchomienie (z katalogu repo, katalog nadrzędny musi zawierać pakiet `fut/`):
    python tests/test_end_to_end.py
"""
import os
import sys
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple

# 1) Tryb sandbox (bez EMERGENCY_SECRET, bez sys.exit) oraz ścieżka do pakietu fut/
os.environ["OKX_IS_SANDBOX"] = "true"
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from fut import state, config
from fut.config import FUTURES_INSTRUMENTS, QUOTE_CCY
from fut.redis_bridge import UpstashRedisFuturesBridge
from fut.engine import independent_4tf_sniper_worker


# ---------------------------------------------------------------------------
# Fake OKX client — metody wywoływane przez independent_4tf_sniper_worker
# ---------------------------------------------------------------------------
class FakeOKXClient:
    def __init__(self, candle_map: Dict[str, Dict[str, List[List[str]]]]):
        self.candle_map = candle_map          # (symbol -> {bar -> candles})
        self.instruments_cache: Dict[str, Dict[str, Any]] = {}
        self.placed_orders: List[Dict[str, Any]] = []
        self.protected: List[str] = []
        self.entry_live_price = 102.0
        self.wallet = {"total_equity": 1000.0, "available_cash": 500.0}

    async def check_spread_allowed(self, symbol: str, max_spread_pct: float = 0.0020) -> Tuple[bool, float]:
        return True, 0.0005

    async def get_macro_candles_cached(self, symbol: str, bar: str = "15m", limit: int = 100,
                                       cache_ttl_sec: float = 0.0) -> List[List[str]]:
        return self.candle_map.get(symbol, {}).get(bar, [])

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        return {"last": self.entry_live_price}

    async def get_wallet_balances(self, preferred_ccy: str = QUOTE_CCY) -> Optional[Dict[str, Any]]:
        return self.wallet

    async def load_instrument_specification(self, symbol: str) -> Optional[Dict[str, Any]]:
        return {"tickSz": 0.5, "ctVal": 10.0, "ctValCcy": "USD"}

    def calculate_contract_size(self, *args, **kwargs) -> Tuple[float, float]:
        return 5.0, 100.0

    async def execute_futures_order(self, *args, **kwargs) -> Optional[Dict[str, Any]]:
        self.placed_orders.append(kwargs)
        return {"code": "0", "data": [{"ordId": "1001", "sCode": "0", "clOrdId": kwargs.get("cl_ord_id", "")}]}

    async def get_parent_order_state(self, symbol: str, ord_id: str) -> Optional[Dict[str, Any]]:
        return {"state": "filled"}

    async def get_position_details(self, symbol: str, pos_side: str) -> Optional[Dict[str, Any]]:
        return {"size": 5.0, "avgPx": 102.0, "margin": 100.0}

    async def ensure_position_protection(self, *args, **kwargs) -> Tuple[str, str]:
        self.protected.append(kwargs.get("symbol", ""))
        return "PROTECTED", "algo-xyz-123"


# ---------------------------------------------------------------------------
# Fake Telegram — zapisuje powiadomienia zamiast wysyłać
# ---------------------------------------------------------------------------
class FakeTelegram:
    def __init__(self):
        self.messages: List[str] = []

    async def push(self, text: str):
        self.messages.append(text)


# ---------------------------------------------------------------------------
# Generatory świec (format OKX: [ts, open, high, low, close, volume] — stringi)
# ---------------------------------------------------------------------------
def _closed_bucket_ms(bar_sec: int) -> int:
    """Timestamp (ms) ostatniej ZAMKNIĘTEJ świecy dla danego interwału względem 'teraz'."""
    now_ms = int(time.time() * 1000)
    bar_ms = bar_sec * 1000
    current = (now_ms // bar_ms) * bar_ms
    return current - bar_ms


def make_uptrend(n: int, bar_sec: int, start: float, step: float) -> List[List[str]]:
    """Ściśle rosnący trend (zamknięcia rosną) — dla byczych EMA 4H/1H."""
    last_ts = _closed_bucket_ms(bar_sec)
    out: List[List[str]] = []
    price = start
    for i in range(n):
        ts = last_ts - (n - 1 - i) * bar_sec * 1000
        close = price
        open_ = close - step * 0.5
        high = close + step * 0.5
        low = open_ - step * 0.5
        out.append([str(ts), f"{open_:.4f}", f"{high:.4f}", f"{low:.4f}", f"{close:.4f}", "100"])
        price = close + step
    return out


def make_choch_breakout_15m(n: int = 60) -> List[List[str]]:
    """Struktura CHoCH LONG: swing high ~i=55, potem breakout ostatniej świecy powyżej niego."""
    step = 900
    last_ts = _closed_bucket_ms(step)
    # bazowa cena rośnie do ~100 do indeksu 53
    prices = [90.0 + (100.0 - 90.0) * (i / 53) for i in range(54)]
    # 54..59 — ręcznie: swing high na 55, breakout na 59
    extra = [
        (100.4, 100.5),  # 54
        (100.8, 101.0),  # 55  <- swing high (high=101.0)
        (100.3, 100.6),  # 56
        (100.5, 100.7),  # 57
        (100.9, 101.2),  # 58
        (102.0, 102.5),  # 59  <- breakout (close=102.0 > swing high 101.0, open=101.0)
    ]
    out: List[List[str]] = []
    for i in range(n):
        ts = last_ts - (n - 1 - i) * step * 1000
        if i < 54:
            close = prices[i]
            open_ = close - 0.05
            high = close + 0.3
            low = open_ - 0.05
        else:
            close, high = extra[i - 54]
            open_ = 101.0 if i == n - 1 else close - 0.2
            low = open_ - 0.1
        out.append([str(ts), f"{open_:.4f}", f"{high:.4f}", f"{low:.4f}", f"{close:.4f}", "100"])
    return out


# ---------------------------------------------------------------------------
# Konfiguracja fake'ów
# ---------------------------------------------------------------------------
def build_fixture():
    btc = FUTURES_INSTRUMENTS[0]["symbol"]  # sandbox: BTC-USD_UM_XPERP-310328
    candle_map = {
        btc: {
            "4H": make_uptrend(220, 14400, 90.0, 0.1),
            "1H": make_uptrend(80, 3600, 98.0, 0.1),
            "15m": make_choch_breakout_15m(60),
        }
    }
    okx = FakeOKXClient(candle_map)
    okx.instruments_cache = {btc: {"tickSz": 0.5, "ctVal": 10.0, "ctValCcy": "USD"}}
    redis = UpstashRedisFuturesBridge("", "", None)  # url="" -> tryb in-memory
    tg = FakeTelegram()
    return btc, okx, redis, tg


async def run_single_cycle(okx, redis, tg) -> Tuple[int, int, Optional[Dict[str, Any]]]:
    state.ctx.is_master = True
    state.ctx.shutdown_event = asyncio.Event()
    state.ctx.alpha_lock = asyncio.Lock()
    state.ctx.trading_paused = False

    worker = asyncio.create_task(independent_4tf_sniper_worker(None, redis, tg, okx, None))

    # czekaj aż poleci zlecenie (max ~15 s), potem łagodnie zamknij pętlę
    deadline = time.monotonic() + 15.0
    while not okx.placed_orders and time.monotonic() < deadline:
        await asyncio.sleep(0.2)

    state.ctx.shutdown_event.set()
    try:
        await asyncio.wait_for(worker, timeout=10.0)
    except asyncio.TimeoutError:
        worker.cancel()
        with suppress_cancelled():
            await worker

    positions = {k: v for k, v in redis._local_positions.items()}
    return len(okx.placed_orders), len(tg.messages), positions


class suppress_cancelled:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return exc_type is asyncio.CancelledError


# ---------------------------------------------------------------------------
# Główny test
# ---------------------------------------------------------------------------
def main() -> int:
    btc, okx, redis, tg = build_fixture()

    n_orders, n_msgs, positions = asyncio.run(run_single_cycle(okx, redis, tg))

    failures: List[str] = []

    print("=" * 60)
    print("TEST END-TO-END: sygnał CHoCH LONG → OKX → OCO → Redis → Telegram")
    print("=" * 60)

    # 1) Zlecenie zostało złożone na OKX
    if n_orders >= 1:
        order = okx.placed_orders[0]
        print(f"  PASS  zlecenie OKX złożone (side={order.get('side')}, posSide={order.get('pos_side')}, qty={order.get('quantity')})")
    else:
        failures.append("brak zlecenia OKX — sygnał nie przeszedł filtrów")

    # 2) Pozycja w Redis ma stan PROTECTED
    protected = [v for k, v in positions.items() if v.get("state") == "PROTECTED"]
    if protected:
        rec = protected[0]
        print(f"  PASS  Redis PROTECTED: instId={rec.get('instId')}, algoId={rec.get('algoId')}, slPx={rec.get('slPx')}, tpPx={rec.get('tpPx')}")
        if rec.get("instId") != btc:
            failures.append(f"zły instId: {rec.get('instId')} != {btc}")
    else:
        failures.append(f"brak pozycji PROTECTED w Redis (stan: {[v.get('state') for v in positions.values()]})")

    # 3) Pewne zlecenie ma podpięty TP/SL (attached_tp / attached_sl)
    if n_orders >= 1 and protected and okx.placed_orders[0].get("attached_tp") and okx.placed_orders[0].get("attached_sl"):
        print(f"  PASS  zlecenie z OCO TP/SL (tp={okx.placed_orders[0]['attached_tp']}, sl={okx.placed_orders[0]['attached_sl']})")
    else:
        failures.append("zlecenie bez podpiętego TP/SL (OCO)")

    # 4) Powiadomienie Telegram o wejściu
    entry_msgs = [m for m in tg.messages if "ENTRY" in m]
    if entry_msgs:
        print(f"  PASS  Telegram: powiadomienie o wejściu wysłane ({len(entry_msgs)} wiadomość(i))")
    else:
        failures.append("brak powiadomienia Telegram o wejściu")

    print("=" * 60)
    if failures:
        print(f"WYNIK: {len(failures)} BŁĄDÓW")
        for f in failures:
            print(f"   ✗ {f}")
        return 1
    print("WYNIK: WSZYSTKIE TESTY E2E ZALICZONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())