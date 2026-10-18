"""Funkcje czyste: generowanie ID, walidacja, zaokrąglenia, wskaźniki techniczne."""
import os
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
from typing import Dict, Any, List, Optional, Tuple


def generate_cl_ord_id(prefix: str, base_symbol: str) -> str:
    clean_base = "".join(filter(str.isalnum, base_symbol))[:4].upper()
    ts_compact = hex(int(time.time() * 1000))[2:].upper()
    salt = os.urandom(2).hex().upper()
    return f"{prefix}{clean_base}{ts_compact}{salt}"[:32]


def get_canonical_pos_key(prefix: str, inst_id: str, pos_side: str) -> str:
    return f"{prefix}POS:{inst_id}:{pos_side.lower()}"


def validate_okx_response(
    res_json: Optional[Dict[str, Any]],
    expected_id_field: Optional[str] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
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


def find_m15_recent_swing_high(candles_15m: List[List[str]], lookback: int = 16) -> Optional[float]:
    if len(candles_15m) < 5:
        return None
    highs = [float(c[2]) for c in candles_15m]
    n = len(highs)
    start_idx = n - 3
    end_idx = max(2, n - 1 - lookback)
    for i in range(start_idx, end_idx - 1, -1):
        c_h = highs[i]
        if (highs[i - 1] <= c_h and highs[i - 2] <= c_h and
            highs[i + 1] <= c_h and highs[i + 2] <= c_h):
            return c_h
    return None


def find_m15_recent_swing_low(candles_15m: List[List[str]], lookback: int = 16) -> Optional[float]:
    if len(candles_15m) < 5:
        return None
    lows = [float(c[3]) for c in candles_15m]
    n = len(lows)
    start_idx = n - 3
    end_idx = max(2, n - 1 - lookback)
    for i in range(start_idx, end_idx - 1, -1):
        c_l = lows[i]
        if (lows[i - 1] >= c_l and lows[i - 2] >= c_l and
            lows[i + 1] >= c_l and lows[i + 2] >= c_l):
            return c_l
    return None


def find_structural_h1_swing_sl(
    candles_1h: List[List[str]],
    entry_price: float,
    pos_side: str,
    tick_sz: float,
    lookback_bars: int = 40,
    pivot_radius: int = 2,
    cushion_pct: float = 0.0015,
    min_sl_pct: float = 0.012,
    max_sl_pct: float = 0.035,
    rr_ratio: float = 1.8
) -> Tuple[float, float, float]:
    lows = [float(c[3]) for c in candles_1h]
    highs = [float(c[2]) for c in candles_1h]
    curr_h1_idx = len(lows) - 1

    if curr_h1_idx < 3 or entry_price <= 0:
        def_sl = 0.018
        p_sl = entry_price * (1.0 - def_sl) if pos_side == "long" else entry_price * (1.0 + def_sl)
        p_tp = entry_price * (1.0 + def_sl * rr_ratio) if pos_side == "long" else entry_price * (1.0 - def_sl * rr_ratio)
        return round_price_to_tick(p_sl, tick_sz, "DOWN"), round_price_to_tick(p_tp, tick_sz, "UP"), def_sl

    found_level: Optional[float] = None
    start_idx = curr_h1_idx - 1
    end_idx = max(1, curr_h1_idx - lookback_bars)

    if pos_side == "long":
        for i in range(start_idx, end_idx - 1, -1):
            eff_radius = 1 if i == (curr_h1_idx - 1) else pivot_radius
            if (i - eff_radius) < 0 or (i + eff_radius) > curr_h1_idx:
                continue

            is_pivot = True
            c_low = lows[i]
            for offset in range(1, eff_radius + 1):
                if lows[i - offset] <= c_low or lows[i + offset] <= c_low:
                    is_pivot = False
                    break
            if is_pivot and c_low < entry_price:
                found_level = c_low
                break

        if found_level is None:
            valid_lows = [l for l in lows[max(0, curr_h1_idx - 14) : curr_h1_idx + 1] if l < entry_price]
            found_level = min(valid_lows) if valid_lows else entry_price * (1.0 - min_sl_pct)

        raw_sl_price = found_level * (1.0 - cushion_pct)
        clamped_sl_pct = max(min_sl_pct, min((entry_price - raw_sl_price) / entry_price, max_sl_pct))

        price_sl = round_price_to_tick(entry_price * (1.0 - clamped_sl_pct), tick_sz, "DOWN")
        eff_sl_pct = max(min_sl_pct, abs(entry_price - price_sl) / entry_price if entry_price > 0 else min_sl_pct)
        price_tp = round_price_to_tick(entry_price * (1.0 + eff_sl_pct * rr_ratio), tick_sz, "UP")

    else:
        for i in range(start_idx, end_idx - 1, -1):
            eff_radius = 1 if i == (curr_h1_idx - 1) else pivot_radius
            if (i - eff_radius) < 0 or (i + eff_radius) > curr_h1_idx:
                continue

            is_pivot = True
            c_high = highs[i]
            for offset in range(1, eff_radius + 1):
                if highs[i - offset] >= c_high or highs[i + offset] >= c_high:
                    is_pivot = False
                    break
            if is_pivot and c_high > entry_price:
                found_level = c_high
                break

        if found_level is None:
            valid_highs = [h for h in highs[max(0, curr_h1_idx - 14) : curr_h1_idx + 1] if h > entry_price]
            found_level = max(valid_highs) if valid_highs else entry_price * (1.0 + min_sl_pct)

        raw_sl_price = found_level * (1.0 + cushion_pct)
        clamped_sl_pct = max(min_sl_pct, min((raw_sl_price - entry_price) / entry_price, max_sl_pct))

        price_sl = round_price_to_tick(entry_price * (1.0 + clamped_sl_pct), tick_sz, "UP")
        eff_sl_pct = max(min_sl_pct, abs(price_sl - entry_price) / entry_price if entry_price > 0 else min_sl_pct)
        price_tp = round_price_to_tick(entry_price * (1.0 - eff_sl_pct * rr_ratio), tick_sz, "DOWN")

    return price_sl, price_tp, eff_sl_pct


def get_strictly_closed_candles_slice(
    candles: List[List[str]],
    bar_sec: int,
    now_sec: float
) -> Tuple[List[List[str]], Optional[List[str]]]:
    """Deterministyczny Timestamp Bucket Resolver eliminujący pułapkę pustego pierwszego ticka."""
    if not candles:
        return [], None

    now_ms = int(now_sec * 1000)
    bar_ms = bar_sec * 1000
    current_bucket = (now_ms // bar_ms) * bar_ms
    target_closed_bucket = current_bucket - bar_ms

    last_candle_ts = int(candles[-1][0])

    if last_candle_ts == current_bucket:
        closed_slice = candles[:-1]
    elif last_candle_ts == target_closed_bucket:
        closed_slice = candles[:]
    elif last_candle_ts < target_closed_bucket:
        return [], None
    else:
        closed_slice = [c for c in candles if int(c[0]) <= target_closed_bucket]

    last_closed = closed_slice[-1] if closed_slice and int(closed_slice[-1][0]) == target_closed_bucket else None
    return closed_slice, last_closed