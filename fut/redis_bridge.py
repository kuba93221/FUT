"""Most Redis przez REST Upstash: pozycje, cooldowny, daily loss, blokada lidera."""
import time
import msgpack
import aiohttp
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from .config import logger, REDIS_PREFIX, get_seconds_until_midnight_utc, IS_SANDBOX
from . import state

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc


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

            pattern_cd = f"{self.prefix}COOLDOWN:*"
            async with self.session.get(f"{self.url}/keys/{pattern_cd}", headers=self.headers, timeout=4) as r_cd:
                if r_cd.status == 200:
                    cd_keys = (await r_cd.json()).get("result", [])
                    for k in cd_keys:
                        clean_cd = k.replace(f"{self.prefix}COOLDOWN:", "")
                        async with self.session.get(f"{self.url}/ttl/{k}", headers=self.headers, timeout=4) as r_ttl:
                            if r_ttl.status == 200:
                                ttl_val = (await r_ttl.json()).get("result", 0)
                                if ttl_val and int(ttl_val) > 0:
                                    self._local_cooldowns[clean_cd] = time.time() + int(ttl_val)

            self._initialized = True
            logger.info(f"💾 [REDIS-CACHE-INIT] Zsynchronizowano: Strata={self._local_daily_loss} | Pozycje={len(self._local_positions)} | Cooldowny={len(self._local_cooldowns)}")
        except Exception as e:
            logger.error(f"⚠️ [REDIS-INIT-SYNC-ERROR] {e}")

    async def acquire_master_engine_lock(self, worker_id: str, ttl_seconds: int = 60) -> bool:
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

    async def renew_master_engine_lock(self, worker_id: str, ttl_seconds: int = 60) -> bool:
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

    async def release_master_engine_lock(self, worker_id: str) -> bool:
        if not self.url:
            return True
        safe_key = self._enforce_prefix("ENGINE_ACTIVE_LOCK")
        lua_script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('DEL', KEYS[1]) "
            "else return 0 end"
        )
        cmd = ["EVAL", lua_script, "1", safe_key, worker_id]
        try:
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=3) as resp:
                data = await resp.json()
                return data.get("result") == 1
        except Exception as e:
            logger.error(f"[REDIS-RELEASE-LOCK-ERR] {e}")
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
            return state.GLOBAL_TRADING_PAUSED
        safe_key = self._enforce_prefix("SYSTEM_STATE:TRADING_PAUSED")
        try:
            cmd = ["GET", safe_key]
            async with self.session.post(f"{self.url}", json=cmd, headers=self.headers, timeout=4) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    return str(res).upper() == "TRUE"
                if not IS_SANDBOX:
                    return True
        except Exception:
            if not IS_SANDBOX:
                return True
        return state.GLOBAL_TRADING_PAUSED

    async def set_circuit_breaker(self, active: bool) -> bool:
        if not self.url:
            return True
        safe_key = self._enforce_prefix("SYSTEM_STATE:CIRCUIT_BREAKER")
        val = "TRUE" if active else "FALSE"
        ttl_sec = get_seconds_until_midnight_utc() if active else 86400
        try:
            cmd = ["SET", safe_key, val, "EX", str(ttl_sec)]
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
                if not IS_SANDBOX:
                    return True
        except Exception:
            if not IS_SANDBOX:
                return True
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