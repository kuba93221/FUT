"""Współdzielony globalny stan silnika.

Wszystkie zmienne globalne trzymamy w jednym module, a pozostałe moduły
odwołują się do nich przez ``import fut.state as state`` (referencja do
modułu, więc mutacje w ``main`` są widoczne wszędzie).
"""
import os
import time
import asyncio
import threading
from typing import Any, Optional, Set

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
GLOBAL_ALPHA_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
BACKGROUND_TASKS: Set[asyncio.Task] = set()

RATE_LIMITER_PUBLIC: Optional[Any] = None
RATE_LIMITER_TRADE: Optional[Any] = None
RATE_LIMITER_ACCOUNT: Optional[Any] = None

GLOBAL_WS_FEED: Optional[Any] = None
GLOBAL_OKX_CLIENT: Optional[Any] = None
GLOBAL_REDIS_BRIDGE: Optional[Any] = None
GLOBAL_TG: Optional[Any] = None
GLOBAL_SMART_MONEY_ORACLE: Optional[Any] = None

IS_MASTER_ENGINE_NODE: bool = False
ENGINE_WORKER_ID: str = f"WORKER_{os.getpid()}_{int(time.time())}"

SHUTDOWN_COMPLETE = threading.Event()
PROCESS_DRAINING = threading.Event()
GLOBAL_TRADING_PAUSED: bool = False