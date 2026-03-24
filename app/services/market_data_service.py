"""
Market data service factory.

Selects the active provider based on MARKET_DATA_PROVIDER env var:
  - "gbm"     (default) — simulated Geometric Brownian Motion prices
  - "alpaca"            — real US stock prices via Alpaca paper trading API
  - "polygon"           — real US stock prices via Polygon.io REST API

Usage everywhere in the codebase stays identical:
    from app.services.market_data_service import market_data_service
    price = await market_data_service.get_current_price("AAPL")
"""
from __future__ import annotations

from app.core.logger import get_logger
from app.services.providers.base import MarketDataProvider

log = get_logger(__name__)


def _create_provider() -> MarketDataProvider:
    from app.core.config import settings

    provider = settings.MARKET_DATA_PROVIDER.lower()

    if provider == "alpaca":
        if not settings.ALPACA_API_KEY or not settings.ALPACA_SECRET_KEY:
            raise RuntimeError(
                "MARKET_DATA_PROVIDER=alpaca requires ALPACA_API_KEY and "
                "ALPACA_SECRET_KEY to be set in .env"
            )
        from app.services.providers.alpaca import AlpacaMarketDataProvider
        instance = AlpacaMarketDataProvider(
            api_key=settings.ALPACA_API_KEY,
            secret_key=settings.ALPACA_SECRET_KEY,
        )
        log.info(
            "Market data provider selected",
            provider="alpaca",
            api_key_loaded=bool(settings.ALPACA_API_KEY),
        )
        return instance

    if provider == "polygon":
        from app.services.providers.polygon import PolygonMarketDataProvider
        instance = PolygonMarketDataProvider(api_key=settings.POLYGON_API_KEY)
        log.info(
            "Market data provider selected",
            provider="polygon",
            api_key_loaded=bool(settings.POLYGON_API_KEY),
        )
        return instance

    # Default: GBM simulation
    from app.services.providers.gbm import GBMMarketDataProvider
    instance = GBMMarketDataProvider()
    log.info("Market data provider selected", provider="gbm", api_key_loaded=False)
    return instance


# Singleton — imported by the rest of the codebase
market_data_service: MarketDataProvider = _create_provider()
