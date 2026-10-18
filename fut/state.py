"""Współdzielony globalny stan silnika.

Wszystkie zmienne globalne trzymamy w jednym module, a pozostałe moduły
odwołują się do nich przez ``import fut.state as state`` (referencja do
modułu, więc mutacje w ``main`` są widoczne wszędzie).
"""
import os
import time
import asyncio
import threading
from dataclasses import dataclass, field
from typing import Any, Optional, Set


@dataclass
class EngineContext:
    """Kontekst runtime'u silnika Futures 3x (zastępuje dawne globalne state.*)."""

    # ---- pętla i synchronizacja asyncio ----
    background_loop: Optional[asyncio.AbstractEventLoop] = None
    alpha_lock: Optional[asyncio.Lock] = None
    shutdown_event: Optional[asyncio.Event] = None
    background_tasks: Set[asyncio.Task] = field(default_factory=set)

    # ---- limitery (TokenBucketRateLimiter) ----
    rate_limiter_public: Optional[Any] = None
    rate_limiter_trade: Optional[Any] = None
    rate_limiter_account: Optional[Any] = None

    # ---- usługi (klienci / most / feed / orka) ----
    ws_feed: Optional[Any] = None
    okx_client: Optional[Any] = None
    redis_bridge: Optional[Any] = None
    tg: Optional[Any] = None
    smart_money_oracle: Optional[Any] = None

    # ---- flagi i identyfikatory ----
    is_master: bool = False
    worker_id: str = field(default_factory=lambda: f"WORKER_{os.getpid()}_{int(time.time())}")

    # ---- zdarzenia synchronizacyjne (wątki) ----
    shutdown_complete: threading.Event = field(default_factory=threading.Event)
    process_draining: threading.Event = field(default_factory=threading.Event)

    # ---- sterowanie handlem ----
    trading_paused: bool = False


ctx: EngineContext = EngineContext()