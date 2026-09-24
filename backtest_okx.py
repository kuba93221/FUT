import asyncio
import aiohttp
import time
import math
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc

BASE_API_URL = "https://eea.okx.com"
FALLBACK_API_URL = "https://www.okx.com"

# Koszyk par do przetestowania na żywym rynku X-PERP
TARGET_SYMBOLS = [
    {"base": "BTC", "preferred": "BTC-USD_UM_XPERP-310404", "fallback": "BTC-USDC", "round": 2},
    {"base": "ETH", "preferred": "ETH-USD_UM_XPERP-310404", "fallback": "ETH-USDC", "round": 2},
    {"base": "SOL", "preferred": "SOL-USD_UM_XPERP-310404", "fallback": "SOL-USDC", "round": 2},
    {"base": "XRP", "preferred": "XRP-USD_UM_XPERP-310404", "fallback": "XRP-USDC", "round": 4}
]

TARGET_LEVERAGE = 3
TAKER_FEE_PCT = 0.0010  # 0.10% prowizji maklerskiej OKX za otwarcie + zamkniecie

def calculate_ema(prices: List[float], period: int) -> float:
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    k = 2.0 / (period + 1.0)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = (p * k) + (ema * (1.0 - k))
    return ema

def calculate_rsi(prices: List[float], period: int = 14) -> float:
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

def calculate_atr(candles: List[List[str]], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    tr_list = []
    for i in range(1, min(period + 1, len(candles))):
        h = float(candles[-i][2])
        l = float(candles[-i][3])
        prev_c = float(candles[-(i + 1)][4])
        tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    return sum(tr_list) / len(tr_list) if tr_list else 0.0

def calculate_adx_pure(candles: List[List[str]], period: int = 14) -> float:
    """Oblicza wskaźnik ADX metodą Wildera w czystym Pythonie."""
    if len(candles) < (period * 2 + 1):
        return 20.0

    highs = [float(c[2]) for c in candles]
    lows = [float(c[3]) for c in candles]
    closes = [float(c[4]) for c in candles]

    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, len(candles)):
        h, l = highs[i], lows[i]
        ph, pl, pc = highs[i-1], lows[i-1], closes[i-1]

        tr = max(h - l, abs(h - pc), abs(l - pc))
        tr_list.append(tr)

        up_m = h - ph
        down_m = pl - l

        plus_dm.append(up_m if up_m > down_m and up_m > 0 else 0.0)
        minus_dm.append(down_m if down_m > up_m and down_m > 0 else 0.0)

    tr_smooth = sum(tr_list[:period])
    plus_smooth = sum(plus_dm[:period])
    minus_smooth = sum(minus_dm[:period])

    dx_list = []
    for i in range(period, len(tr_list)):
        tr_smooth = tr_smooth - (tr_smooth / period) + tr_list[i]
        plus_smooth = plus_smooth - (plus_smooth / period) + plus_dm[i]
        minus_smooth = minus_smooth - (minus_smooth / period) + minus_dm[i]

        if tr_smooth == 0:
            continue

        p_di = 100.0 * (plus_smooth / tr_smooth)
        m_di = 100.0 * (minus_smooth / tr_smooth)
        di_sum = p_di + m_di
        dx = (abs(p_di - m_di) / di_sum) * 100.0 if di_sum > 0 else 0.0
        dx_list.append(dx)

    if len(dx_list) < period:
        return 20.0

    adx = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx = ((adx * (period - 1)) + dx) / period

    return round(adx, 2)

async def fetch_300_candles(session: aiohttp.ClientSession, symbol: str) -> List[List[str]]:
    """Pobiera 300 świec 15-minutowych z OKX za pomocą 3 zapytań paginowanych."""
    all_candles: List[List[str]] = []
    oldest_ts: Optional[str] = None

    for _ in range(3):
        url = f"{BASE_API_URL}/api/v5/market/candles?instId={symbol}&bar=15m&limit=100"
        if oldest_ts:
            url += f"&after={oldest_ts}"

        try:
            async with session.get(url, timeout=6) as resp:
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    chunk = data["data"]
                    all_candles.extend(chunk)
                    oldest_ts = chunk[-1][0]
                else:
                    break
        except Exception:
            break
        await asyncio.sleep(0.15)

    if not all_candles:
        # Próba fallback na ogólny URL
        for _ in range(3):
            url = f"{FALLBACK_API_URL}/api/v5/market/candles?instId={symbol}&bar=15m&limit=100"
            if oldest_ts:
                url += f"&after={oldest_ts}"
            try:
                async with session.get(url, timeout=6) as resp:
                    data = await resp.json()
                    if data.get("code") == "0" and data.get("data"):
                        chunk = data["data"]
                        all_candles.extend(chunk)
                        oldest_ts = chunk[-1][0]
                    else:
                        break
            except Exception:
                break
            await asyncio.sleep(0.15)

    # OKX zwraca od najnowszej do najstarszej -> sortujemy chronologicznie
    all_candles.reverse()
    return all_candles

async def resolve_active_instrument(session: aiohttp.ClientSession, base: str, preferred: str, fallback: str) -> str:
    url = f"{BASE_API_URL}/api/v5/public/instruments?instType=FUTURES"
    try:
        async with session.get(url, timeout=5) as resp:
            data = await resp.json()
            if data.get("code") == "0" and data.get("data"):
                for item in data["data"]:
                    inst_id = item.get("instId", "")
                    if item.get("state") == "live" and base.upper() in inst_id and "XPERP" in inst_id:
                        return inst_id
    except Exception:
        pass
    return preferred

def simulate_forward_outcome(
    candles: List[List[str]],
    entry_idx: int,
    pos_side: str,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    max_holding_bars: int = 16
) -> Dict[str, Any]:
    """
    Sprawdza na kolejnych świecach, co wydarzyło się z pozycją:
    Czy uderzyła w TP, SL czy została zamknięta przez Time-Stop.
    """
    total_bars = len(candles)
    for step in range(1, max_holding_bars + 1):
        idx = entry_idx + step
        if idx >= total_bars:
            break

        c = candles[idx]
        high = float(c[2])
        low = float(c[3])
        close = float(c[4])

        if pos_side == "long":
            # Zasada ostrożnościowa: jeśli w jednej świecy padł i SL i TP, liczymy SL!
            if low <= sl_price:
                gross_pct = (sl_price - entry_price) / entry_price
                net_roe = (gross_pct * TARGET_LEVERAGE - TAKER_FEE_PCT * TARGET_LEVERAGE) * 100.0
                return {"outcome": "SL", "exit_price": sl_price, "bars_held": step, "net_roe": round(net_roe, 2)}
            if high >= tp_price:
                gross_pct = (tp_price - entry_price) / entry_price
                net_roe = (gross_pct * TARGET_LEVERAGE - TAKER_FEE_PCT * TARGET_LEVERAGE) * 100.0
                return {"outcome": "TP", "exit_price": tp_price, "bars_held": step, "net_roe": round(net_roe, 2)}
        else:
            if high >= sl_price:
                gross_pct = (entry_price - sl_price) / entry_price
                net_roe = (gross_pct * TARGET_LEVERAGE - TAKER_FEE_PCT * TARGET_LEVERAGE) * 100.0
                return {"outcome": "SL", "exit_price": sl_price, "bars_held": step, "net_roe": round(net_roe, 2)}
            if low <= tp_price:
                gross_pct = (entry_price - tp_price) / entry_price
                net_roe = (gross_pct * TARGET_LEVERAGE - TAKER_FEE_PCT * TARGET_LEVERAGE) * 100.0
                return {"outcome": "TP", "exit_price": tp_price, "bars_held": step, "net_roe": round(net_roe, 2)}

    # Time-Stop (zamknięcie na ostatniej świecy holding_bars)
    exit_c = candles[min(entry_idx + max_holding_bars, total_bars - 1)]
    final_p = float(exit_c[4])
    if pos_side == "long":
        gross_pct = (final_p - entry_price) / entry_price
    else:
        gross_pct = (entry_price - final_p) / entry_price

    net_roe = (gross_pct * TARGET_LEVERAGE - TAKER_FEE_PCT * TARGET_LEVERAGE) * 100.0
    return {"outcome": "TIME_STOP", "exit_price": final_p, "bars_held": max_holding_bars, "net_roe": round(net_roe, 2)}

def evaluate_candle_slice(
    slice_candles: List[List[str]],
    price_round: int
) -> List[Dict[str, Any]]:
    """
    Ocenia dany punkt w czasie i generuje zlecenia dla:
    - Wersji v14.2 (bez filtrów nadrzędnych)
    - Wersji v14.3 (z filtrem EMA-200, ADX i świecą odrzucenia)
    """
    if len(slice_candles) < 55:
        return []

    closes = [float(c[4]) for c in slice_candles]
    highs = [float(c[2]) for c in slice_candles]
    lows = [float(c[3]) for c in slice_candles]
    opens = [float(c[1]) for c in slice_candles]
    volumes = [float(c[5]) for c in slice_candles]

    curr_p = closes[-1]
    prev_c = closes[-2]
    atr = calculate_atr(slice_candles, 14)
    if atr <= 0 or curr_p <= 0:
        return []

    # Obliczenie wskaźników nadrzędnych (v14.3)
    ema_20 = calculate_ema(closes, 20)
    ema_50 = calculate_ema(closes, 50)
    ema_200 = calculate_ema(closes, 200) if len(closes) >= 200 else ema_50
    adx_val = calculate_adx_pure(slice_candles, 14)
    rsi_val = calculate_rsi(closes, 14)
    vol_sma = sum(volumes[-21:-1]) / 20.0 if len(volumes) >= 21 else volumes[-1]

    # Reguła makro trendu:
    macro_bullish = curr_p > ema_200
    macro_bearish = curr_p < ema_200

    candidates = []

    # 1. STRATEGIA MOMENTUM (ROC-10)
    roc_10 = ((curr_p - closes[-11]) / closes[-11]) * 100.0 if len(closes) >= 12 else 0.0
    raw_mom_long = roc_10 > 1.5
    raw_mom_short = roc_10 < -1.5

    if raw_mom_long or raw_mom_short:
        side = "long" if raw_mom_long else "short"
        sl_pct = max(0.008, min((atr * 1.5) / curr_p, 0.020))
        tp_pct = sl_pct * 1.6
        sl_p = round(curr_p * (1.0 - sl_pct) if side == "long" else curr_p * (1.0 + sl_pct), price_round)
        tp_p = round(curr_p * (1.0 + tp_pct) if side == "long" else curr_p * (1.0 - tp_pct), price_round)

        # Filtr v14.3 dla Momentum:
        v143_allowed = True
        reject_reasons = []

        if adx_val < 20.0:
            v143_allowed = False
            reject_reasons.append(f"ADX={adx_val} < 20 (Chop Zone)")
        if side == "long" and not macro_bullish:
            v143_allowed = False
            reject_reasons.append("Cena < EMA-200 (Kontrtrend)")
        if side == "short" and not macro_bearish:
            v143_allowed = False
            reject_reasons.append("Cena > EMA-200 (Kontrtrend)")
        if side == "long" and rsi_val > 65.0:
            v143_allowed = False
            reject_reasons.append(f"RSI={rsi_val} (Przegrzanie rynku)")
        if side == "short" and rsi_val < 35.0:
            v143_allowed = False
            reject_reasons.append(f"RSI={rsi_val} (Skrajne wyprzedanie)")

        candidates.append({
            "strategy": "MOMENTUM",
            "side": side,
            "entry_price": curr_p,
            "sl_price": sl_p,
            "tp_price": tp_p,
            "v142_trigger": True,
            "v143_allowed": v143_allowed,
            "reject_reason": ", ".join(reject_reasons) if reject_reasons else "ZGODNY"
        })

    # 2. STRATEGIA BREAKOUT (Wstęgi Bollingera)
    if len(closes) >= 22:
        period_bb = 20
        prev_closes = closes[-(period_bb + 1):-1]
        sma_bb = sum(prev_closes) / period_bb
        var_bb = sum((x - sma_bb) ** 2 for x in prev_closes) / period_bb
        std_bb = math.sqrt(var_bb) if var_bb > 0 else 1e-6
        upper_bb = sma_bb + (2.0 * std_bb)
        lower_bb = sma_bb - (2.0 * std_bb)
        bandwidth_t1 = (upper_bb - lower_bb) / sma_bb if sma_bb > 0 else 0.0

        is_compression = bandwidth_t1 < 0.018
        raw_brk_long = is_compression and (curr_p > upper_bb)
        raw_brk_short = is_compression and (curr_p < lower_bb)

        if raw_brk_long or raw_brk_short:
            side = "long" if raw_brk_long else "short"
            sl_pct = max(0.008, min((atr * 1.5) / curr_p, 0.020))
            tp_pct = sl_pct * 2.0
            sl_p = round(curr_p * (1.0 - sl_pct) if side == "long" else curr_p * (1.0 + sl_pct), price_round)
            tp_p = round(curr_p * (1.0 + tp_pct) if side == "long" else curr_p * (1.0 - tp_pct), price_round)

            v143_allowed = True
            reject_reasons = []

            if adx_val < 20.0:
                v143_allowed = False
                reject_reasons.append(f"ADX={adx_val} < 20 (Brak dynamiki)")
            if side == "long" and not macro_bullish:
                v143_allowed = False
                reject_reasons.append("Cena < EMA-200")
            if side == "short" and not macro_bearish:
                v143_allowed = False
                reject_reasons.append("Cena > EMA-200")
            if volumes[-2] < (vol_sma * 1.25):
                v143_allowed = False
                reject_reasons.append("Brak wolumenu instytucji (<1.25x SMA)")

            candidates.append({
                "strategy": "BREAKOUT",
                "side": side,
                "entry_price": curr_p,
                "sl_price": sl_p,
                "tp_price": tp_p,
                "v142_trigger": True,
                "v143_allowed": v143_allowed,
                "reject_reason": ", ".join(reject_reasons) if reject_reasons else "ZGODNY"
            })

    # 3. STRATEGIA TREND PULLBACK
    dist_to_ema = abs(curr_p - ema_20) / curr_p
    near_ema = dist_to_ema <= 0.0045

    raw_pb_long = (ema_20 > ema_50) and near_ema and (curr_p >= ema_20 * 0.998)
    raw_pb_short = (ema_20 < ema_50) and near_ema and (curr_p <= ema_20 * 1.002)

    if raw_pb_long or raw_pb_short:
        side = "long" if raw_pb_long else "short"
        sl_pct = max(0.008, min((atr * 1.2) / curr_p, 0.020))
        tp_pct = sl_pct * 2.2
        sl_p = round(curr_p * (1.0 - sl_pct) if side == "long" else curr_p * (1.0 + sl_pct), price_round)
        tp_p = round(curr_p * (1.0 + tp_pct) if side == "long" else curr_p * (1.0 - tp_pct), price_round)

        # Weryfikacja świecy odrzucenia (Rejection Bounce):
        # Poprzednia świeca zamknęła się obroną średniej
        rejection_ok = False
        if side == "long":
            rejection_ok = (closes[-2] > opens[-2]) and (closes[-2] >= ema_20)
        else:
            rejection_ok = (closes[-2] < opens[-2]) and (closes[-2] <= ema_20)

        v143_allowed = True
        reject_reasons = []

        if not rejection_ok:
            v143_allowed = False
            reject_reasons.append("Brak potwierdzenia obrony świecą (Spadający nóż)")
        if side == "long" and not macro_bullish:
            v143_allowed = False
            reject_reasons.append("Cena < EMA-200")
        if side == "short" and not macro_bearish:
            v143_allowed = False
            reject_reasons.append("Cena > EMA-200")

        candidates.append({
            "strategy": "PULLBACK",
            "side": side,
            "entry_price": curr_p,
            "sl_price": sl_p,
            "tp_price": tp_p,
            "v142_trigger": True,
            "v143_allowed": v143_allowed,
            "reject_reason": ", ".join(reject_reasons) if reject_reasons else "ZGODNY"
        })

    return candidates

async def run_pair_backtest(session: aiohttp.ClientSession, inst_conf: Dict[str, Any]) -> Dict[str, Any]:
    base = inst_conf["base"]
    print(f"\n🔍 [POBIERANIE DANYCH] Łączenie z OKX dla {base}...")

    symbol = await resolve_active_instrument(session, base, inst_conf["preferred"], inst_conf["fallback"])
    candles = await fetch_300_candles(session, symbol)

    if not candles or len(candles) < 80:
        # Fallback na spot
        candles = await fetch_300_candles(session, inst_conf["fallback"])
        symbol = inst_conf["fallback"]

    total_candles = len(candles)
    print(f"📊 [DANE POBRANE] {symbol}: Zassano {total_candles} świec 15m (~{round(total_candles*15/60, 1)} godzin).")

    v142_trades = []
    v143_trades = []
    saved_sl_count = 0

    # Iteracja Walk-Forward od 55 świecy do (total_candles - 16)
    for i in range(55, total_candles - 16):
        slice_c = candles[:i+1]
        signals = evaluate_candle_slice(slice_c, inst_conf["round"])

        for sig in signals:
            outcome = simulate_forward_outcome(
                candles=candles,
                entry_idx=i,
                pos_side=sig["side"],
                entry_price=sig["entry_price"],
                sl_price=sig["sl_price"],
                tp_price=sig["tp_price"]
            )

            record = {
                "candle_idx": i,
                "time": datetime.fromtimestamp(int(candles[i][0])/1000, tz=timezone.utc).strftime('%m-%d %H:%M'),
                "strategy": sig["strategy"],
                "side": sig["side"].upper(),
                "entry": sig["entry_price"],
                "outcome": outcome["outcome"],
                "roe": outcome["net_roe"],
                "reject_reason": sig["reject_reason"]
            }

            v142_trades.append(record)

            if sig["v143_allowed"]:
                v143_trades.append(record)
            else:
                if outcome["outcome"] == "SL":
                    saved_sl_count += 1

    return {
        "base": base,
        "symbol": symbol,
        "total_candles": total_candles,
        "v142_trades": v142_trades,
        "v143_trades": v143_trades,
        "saved_sl_count": saved_sl_count
    }

def print_comparison_report(results: List[Dict[str, Any]]):
    print("\n" + "=" * 92)
    print(" 🚀 RAPORT PORÓWNAWCZY Z ŻYWEGO RYNKU OKX: SILNIK v14.2 vs v14.3 PRECISION")
    print("=" * 92)

    total_v142_count = sum(len(r["v142_trades"]) for r in results)
    total_v143_count = sum(len(r["v143_trades"]) for r in results)

    v142_wins = sum(sum(1 for t in r["v142_trades"] if t["outcome"] == "TP") for r in results)
    v143_wins = sum(sum(1 for t in r["v143_trades"] if t["outcome"] == "TP") for r in results)

    v142_losses = sum(sum(1 for t in r["v142_trades"] if t["outcome"] == "SL") for r in results)
    v143_losses = sum(sum(1 for t in r["v143_trades"] if t["outcome"] == "SL") for r in results)

    v142_pnl = sum(sum(t["roe"] for t in r["v142_trades"]) for r in results)
    v143_pnl = sum(sum(t["roe"] for t in r["v143_trades"]) for r in results)

    total_saved_sl = sum(r["saved_sl_count"] for r in results)

    wr_142 = round((v142_wins / total_v142_count) * 100, 1) if total_v142_count > 0 else 0.0
    wr_143 = round((v143_wins / total_v143_count) * 100, 1) if total_v143_count > 0 else 0.0

    print(f"\n{'PARAMETR':<32} | {'WERSJA v14.2 (STARA)':<22} | {'WERSJA v14.3 (NOWA)':<22}")
    print("-" * 84)
    print(f"{'Liczba wygenerowanych wejść':<32} | {total_v142_count:<22} | {total_v143_count:<22}")
    print(f"{'Zyskowne transakcje (TP)':<32} | {v142_wins:<22} | {v143_wins:<22}")
    print(f"{'Stratne transakcje (SL)':<32} | {v142_losses:<22} | {v143_losses:<22}")
    print(f"{'SKUTECZNOŚĆ (WIN RATE)':<32} | {f'{wr_142}%':<22} | {f'{wr_143}%':<22}")
    print(f"{'SKUMULOWANY WYNIK (ROE 3x)':<32} | {f'{round(v142_pnl, 2)}%':<22} | {f'{round(v143_pnl, 2)}%':<22}")
    print("-" * 84)
    print(f"🛡️  URATOWANE STOP LOSSY (v14.3 odrzuciło stratę):  {total_saved_sl} transakcji!")

    # Przegląd przykładowych zablokowanych strat
    print("\n📋 DOWÓD MATEMATYCZNY: PRZYKŁADY TRANSAKCJI, KTÓRE W v14.2 WPADŁY W SL, A v14.3 JE ZABLOKOWAŁ:")
    print("-" * 92)
    print(f"{'CZAS':<12} | {'PARA':<6} | {'STRATEGIA':<10} | {'TYP':<6} | {'WYNIK v14.2':<12} | {'POWÓD BLOKADY W v14.3'}")
    print("-" * 92)

    shown = 0
    for r in results:
        for t in r["v142_trades"]:
            if t["outcome"] == "SL" and "ZGODNY" not in t["reject_reason"]:
                print(f"{t['time']:<12} | {r['base']:<6} | {t['strategy']:<10} | {t['side']:<6} | {f'{t['roe']}% SL':<12} | 🛑 {t['reject_reason']}")
                shown += 1
                if shown >= 8:
                    break
        if shown >= 8:
            break

    if shown == 0:
        print("Brak uderzeń w SL w analizowanym oknie czasowym.")

    print("=" * 92)
    print("Wniosek: Nowe filtry eliminują wejścia w strefie Chopu (ADX < 20) i pod prąd EMA-200.")
    print("=" * 92 + "\n")

async def main():
    print("⚡ [START SYMULATORA] Uruchamianie silnika analitycznego OKX Backtest v14.2 vs v14.3...")
    async with aiohttp.ClientSession() as session:
        tasks = [run_pair_backtest(session, conf) for conf in TARGET_SYMBOLS]
        results = await asyncio.gather(*tasks)
        print_comparison_report(results)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika.")
