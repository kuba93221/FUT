"""Klienci OKX: Smart Money Oracle, WebSocket Price Feed i Futures Client."""
import os
import time
import json
import hmac
import base64
import hashlib
import asyncio
import threading
import aiohttp
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from .config import logger, CONFIG, QUOTE_CCY, TARGET_LEVERAGE, TARGET_MARGIN_MODE
from .utils import validate_okx_response, generate_cl_ord_id, floor_to_lot, format_sz, format_px
from .rate_limiter import TokenBucketRateLimiter
from . import state

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc


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
        """Prawidłowy schemat OKX API v5: [ts, sellVol, buyVol]."""
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
                    sell_v = float(latest[1])
                    buy_v = float(latest[2])
                elif isinstance(latest, dict):
                    sell_v = float(latest.get("sellVol", 0.0))
                    buy_v = float(latest.get("buyVol", 0.0))
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
            return True, f"ZGODNY Z PRZEPŁYWEM (Ratio: {flow.get('ratio')})", flow
        elif target_pos_side.lower() == "short":
            if buy_v > (sell_v * self.max_imbalance):
                return False, f"Aggressive Institutional Buy Absorption (Buy: {round(buy_v, 1)} > Sell: {round(sell_v, 1)})", flow
            return True, f"ZGODNY Z PRZEPŁYWEM (Ratio: {flow.get('ratio')})", flow

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
            while not ws.closed and self._running and not (state.ASYNC_SHUTDOWN_EVENT and state.ASYNC_SHUTDOWN_EVENT.is_set()):
                await asyncio.sleep(20)
                if not ws.closed:
                    await ws.send_str("ping")
        except Exception:
            pass

    async def start_listener(self, symbols: list):
        self._running = True
        sub_args = [{"channel": "tickers", "instId": sym} for sym in symbols]
        subscribe_msg = json.dumps({"op": "subscribe", "args": sub_args})

        while self._running and not (state.ASYNC_SHUTDOWN_EVENT and state.ASYNC_SHUTDOWN_EVENT.is_set()):
            ws_url = self.ws_endpoints[self.current_ep_index % len(self.ws_endpoints)]
            try:
                logger.info(f"🌐 [WS-CONNECT] Łączenie z OKX SWAP EEA: {ws_url}...")
                async with self.session.ws_connect(ws_url, heartbeat=None) as ws:
                    await ws.send_str(subscribe_msg)
                    logger.info(f"📡 [WS-SUBSCRIBED] Subskrypcja aktywna dla {symbols}")
                    self.last_msg_time = time.monotonic()
                    ping_task = asyncio.create_task(self._ping_worker(ws))

                    try:
                        while self._running and not (state.ASYNC_SHUTDOWN_EVENT and state.ASYNC_SHUTDOWN_EVENT.is_set()):
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
                                logger.warning("⚠️ [WS-DISCONNECTED] Gniazdo zamknięte. Następny serwer...")
                                self.current_ep_index += 1
                                break
                    finally:
                        ping_task.cancel()
            except Exception as e:
                logger.error(f"❌ [WS-ERROR] Błąd strumienia ({ws_url}): {e}. Wznawianie za 5s...")
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
        self._candles_cache: Dict[str, Dict[str, Any]] = {}
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
                    matching = []
                    for item in data["data"]:
                        inst_id = item.get("instId", "")
                        inst_state = item.get("state", "")
                        if inst_state == "live" and (inst_id.upper().startswith(f"{clean_target}-") or inst_id.upper().startswith(f"{clean_target}_")) and "XPERP" in inst_id.upper():
                            exp_time = int(item.get("expTime", 0) or 0)
                            matching.append((exp_time, inst_id))
                    if matching:
                        matching.sort(key=lambda x: x[0], reverse=True)
                        best_inst_id = matching[0][1]
                        logger.info(f"🎯 [AUTO-DISCOVERY] Dopasowano symbol Live (najdalszy expTime): {best_inst_id}")
                        return best_inst_id
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
                        logger.info(f"📋 [SPEC-LOADED] {symbol} | tickSz: {spec['tickSz']} | lotSz: {spec['lotSz']} | ctVal: {spec['ctVal']} {spec['ctValCcy']}")
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
        max_allowed_margin: float,
        is_usd_ct: bool = False
    ) -> Tuple[float, float]:
        spec = self.instruments_cache.get(symbol)
        if not spec or current_price <= 0:
            return 0.0, 0.0

        ct_val = float(spec.get("ctVal", 1.0))
        ct_val_ccy = str(spec.get("ctValCcy", "")).upper()
        min_sz = float(spec.get("minSz", 0.01))
        lot_sz = float(spec.get("lotSz", 0.01))

        base_ccy = symbol.split('-')[0].upper()
        if ct_val_ccy in ["USD", "USDC", "USDT"] or is_usd_ct:
            contract_nominal_quote = ct_val
        elif ct_val_ccy in [base_ccy, "BTC", "ETH", "SOL", "XRP"]:
            contract_nominal_quote = ct_val * current_price
        else:
            contract_nominal_quote = ct_val * current_price

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
        if state.GLOBAL_WS_FEED:
            ws_price = state.GLOBAL_WS_FEED.get_last_price(symbol)
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
        collected = []
        after_ts = None
        remaining = limit

        while remaining > 0:
            fetch_sz = min(remaining, 100)
            await self.limiter_public.consume()
            request_path = f"/api/v5/market/candles?instId={symbol}&bar={bar}&limit={fetch_sz}"
            if after_ts:
                request_path += f"&after={after_ts}"
            url = f"{self.base_url}{request_path}"
            headers = {"Content-Type": "application/json"}
            if self.is_sandbox:
                headers["x-simulated-trading"] = "1"
            try:
                async with self.session.get(url, headers=headers, timeout=5) as resp:
                    data = await resp.json()
                    if data.get("code") == "0" and data.get("data"):
                        batch = data["data"]
                        collected.extend(batch)
                        after_ts = batch[-1][0]
                        remaining -= len(batch)
                        if len(batch) < fetch_sz:
                            break
                    else:
                        break
            except Exception as e:
                logger.error(f"[OKX-CANDLES] {symbol} {bar}: {e}")
                break

        if not collected:
            return []

        dedup = {c[0]: c for c in collected}
        sorted_candles = [dedup[k] for k in sorted(dedup.keys(), key=lambda x: int(x))]
        return sorted_candles

    async def get_macro_candles_cached(
        self,
        symbol: str,
        bar: str = "15m",
        limit: int = 100,
        cache_ttl_sec: float = 0.0
    ) -> List[List[str]]:
        now = time.monotonic()
        cache_key = f"{symbol}_{bar}"

        if cache_ttl_sec > 0:
            cached = self._candles_cache.get(cache_key)
            if cached and (now - cached["ts"]) < cache_ttl_sec and len(cached["data"]) >= min(limit, 100):
                return cached["data"]

        data = await self.get_macro_candles_raw(symbol, bar=bar, limit=limit)
        if data and len(data) > 0:
            self._candles_cache[cache_key] = {"data": data, "ts": now}
            return data
        elif cache_key in self._candles_cache:
            logger.warning(f"⚠️ [CACHE-RETAINED] {symbol} {bar}: Użycie poprzedniego bufora z powodu chwilowego błędu sieci.")
            return self._candles_cache[cache_key]["data"]

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
                    algo_state = str(item.get("state", "live")).lower()
                    found_id = str(item.get("algoId", ""))
                    px_str = item.get("actualPx") or item.get("tpTriggerPx") or item.get("slTriggerPx") or "0"
                    actual_px = float(px_str) if px_str else 0.0
                    return algo_state, actual_px, found_id
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
        for delay in (0.25, 0.50, 1.00):
            await asyncio.sleep(delay)
            algo_state, actual_px, found_id = await self.get_algo_order_state(
                algo_cl_ord_id=attach_algo_cl_ord_id
            )
            if algo_state not in ["UNKNOWN", "NOT_FOUND"] and found_id:
                return "PROTECTED", found_id

        pending_algos = await self.get_pending_algo_orders(symbol, ord_type="oco")
        if pending_algos is None:
            logger.critical(f"🚨 [PROTECTION-UNKNOWN] {symbol}: Stan OCO nieznany (Timeout/Błąd API). Zakaz drugiego OCO!")
            return "PROTECTION_UNKNOWN", None

        matched = next((a for a in pending_algos if a.get("algoClOrdId") == attach_algo_cl_ord_id), None)
        if matched and matched.get("algoId"):
            return "PROTECTED", matched.get("algoId")

        standalone_cl_id = generate_cl_ord_id("S", symbol.split('-')[0])
        logger.warning(f"⚠️ [ATTACHED-NOT-FOUND] {symbol}: Brak attached OCO potwierdzony. Składanie Standalone OCO ({standalone_cl_id})...")
        standalone_res = await self.execute_futures_oco(
            symbol=symbol,
            pos_side=pos_side,
            quantity=real_size,
            price_tp=price_tp,
            price_sl=price_sl,
            tick_sz=tick_sz,
            algo_cl_ord_id=standalone_cl_id
        )
        ok, msg, item = validate_okx_response(standalone_res, expected_id_field="algoId")
        if ok and item:
            return "PROTECTED", item.get("algoId")

        return "RECOVERY_REQUIRED", None