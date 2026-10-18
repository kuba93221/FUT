"""Silnik Futures 3x (OKX X-Perp) — refaktoryzacja modularna.

Moduły:
- config: konfiguracja, logger, instrumenty, CONFIG
- state: typowany kontekst runtime'u silnika (EngineContext, singleton state.ctx)
- utils: funkcje czyste (zaokrąglenia, wskaźniki, walidacja)
- rate_limiter: TokenBucketRateLimiter
- telegram: TelegramThrottledDispatcher
- redis_bridge: UpstashRedisFuturesBridge
- okx: OKXSmartMoneyOracle, OKXWebSocketPriceFeed, OKXFuturesClient
- engine: workery, bootstrap, supervisor, reconcile
- http_api: Flask + endpointy administracyjne
- main: pętla async, start, sygnały, __main__
"""

__version__ = "v18.0"