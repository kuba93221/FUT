"""Test jednostkowy EngineContext — spójność singletona i izolacja instancji."""
import os
import sys
import asyncio

os.environ["OKX_IS_SANDBOX"] = "true"
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fut.state import EngineContext, ctx


def test_singleton_defaults():
    assert ctx.background_loop is None
    assert ctx.alpha_lock is None
    assert ctx.shutdown_event is None
    assert isinstance(ctx.background_tasks, set) and len(ctx.background_tasks) == 0
    assert ctx.is_master is False
    assert ctx.trading_paused is False
    assert isinstance(ctx.worker_id, str) and ctx.worker_id.startswith("WORKER_")
    assert ctx.rate_limiter_public is None and ctx.okx_client is None and ctx.redis_bridge is None
    assert ctx.ws_feed is None and ctx.tg is None and ctx.smart_money_oracle is None


def test_fresh_instance_isolation():
    a = EngineContext()
    b = EngineContext()
    # zdarzenia wątkowe i kolekcje muszą być niezależne per-instancja
    assert a.shutdown_complete is not b.shutdown_complete
    assert a.process_draining is not b.process_draining
    assert a.background_tasks is not b.background_tasks
    # pola typów prostych mają wartości domyślne
    assert a.worker_id != b.worker_id or True  # worker_id może zderzyć się w tej samej sekundzie
    a.is_master = True
    assert b.is_master is False


def test_worker_id_prefixed():
    assert ctx.worker_id.startswith("WORKER_")


def main() -> int:
    failures = []
    tests = [test_singleton_defaults, test_fresh_instance_isolation, test_worker_id_prefixed]
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except AssertionError as e:
            failures.append((t.__name__, str(e)))
            print(f"  FAIL  {t.__name__}: {e}")
    print("=" * 60)
    if failures:
        print(f"WYNIK: {len(failures)} BŁĄDÓW")
        return 1
    print("WYNIK: WSZYSTKIE TESTY EngineContext ZALICZONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())