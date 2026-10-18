"""Deterministyczny Token Bucket z atomową rezerwacją tokenów."""
import time
import asyncio
from typing import Optional


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