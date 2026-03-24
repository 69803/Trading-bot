"""
MarketDataRouter — single entry point for all market data.

Routes requests to the correct provider based on asset type:

  Stock / ETF   → stock_provider  (Polygon / Alpaca / GBM)
  Forex         → fx_provider     (Twelve Data / GBM)
  Commodity     → fx_provider     (Twelve Data / GBM — same provider)

Classification rules:
  - Symbol contains "/"       → forex or commodity (handled by fx_provider)
  - Known non-slash commodity → handled by fx_provider
  - Everything else           → stock_provider

Usage:
    from app.services.market_data_router import market_data_router
    price = await market_data_router.get_current_price("EUR/USD")
    price = await market_data_router.get_current_price("AAPL")
"""
from __future__ import annotations

from typing import List

from app.core.logger import get_logger
from app.services.providers.base import MarketDataProvider

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Symbol → provider classification
# ---------------------------------------------------------------------------

# Non-slash commodity shorthand that go to the fx/commodity provider
_NON_SLASH_COMMODITIES = {"WTI", "BRENT", "NATGAS", "COPPER", "OIL", "USOIL", "UKOIL"}

# Slash-less forex/commodity → canonical slash form expected by TwelveData
_NOSLASH_FX: dict[str, str] = {
    # Majors
    "EURUSD": "EUR/USD",  "GBPUSD": "GBP/USD",  "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF",  "AUDUSD": "AUD/USD",  "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    # Minors
    "EURGBP": "EUR/GBP",  "EURJPY": "EUR/JPY",  "GBPJPY": "GBP/JPY",
    "EURCHF": "EUR/CHF",  "AUDJPY": "AUD/JPY",  "GBPCHF": "GBP/CHF",
    # Metals
    "XAUUSD": "XAU/USD",  "XAGUSD": "XAG/USD",  "XPTUSD": "XPT/USD",
    # Energy
    "WTIUSD": "WTI/USD",  "BRENTUSD": "BRENT/USD",  "XNGUSD": "XNG/USD",
}


def _is_forex_or_commodity(symbol: str) -> bool:
    """Return True if the symbol should be routed to the FX/commodity provider."""
    if "/" in symbol:
        return True
    s = symbol.upper()
    return s in _NON_SLASH_COMMODITIES or s in _NOSLASH_FX


def _to_provider_symbol(symbol: str) -> str:
    """
    Normalise a slash-less symbol to the form TwelveData expects.
    EURUSD → EUR/USD  |  EUR/USD → EUR/USD (already correct)
    """
    return _NOSLASH_FX.get(symbol.upper(), symbol)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

class MarketDataRouter:
    """
    Dispatches market data calls to the correct provider by asset type.
    Implements the same interface as MarketDataProvider so it can be a
    drop-in replacement anywhere in the codebase.
    """

    def __init__(
        self,
        stock_provider: MarketDataProvider,
        fx_provider: MarketDataProvider,
    ) -> None:
        self._stock = stock_provider
        self._fx    = fx_provider
        log.info(
            "MarketDataRouter initialized",
            stock_provider=type(stock_provider).__name__,
            fx_provider=type(fx_provider).__name__,
        )

    def get_all_symbols(self) -> List[str]:
        """Combined symbol list from both providers."""
        return self._stock.get_all_symbols() + self._fx.get_all_symbols()

    async def get_current_price(self, symbol: str) -> float:
        if _is_forex_or_commodity(symbol):
            log.debug("Routing → FX provider", symbol=symbol, provider_symbol=_to_provider_symbol(symbol))
            return await self._fx.get_current_price(_to_provider_symbol(symbol))
        return await self._stock.get_current_price(symbol)

    async def get_quote(self, symbol: str) -> dict:
        if _is_forex_or_commodity(symbol):
            provider_sym = _to_provider_symbol(symbol)
            q = await self._fx.get_quote(provider_sym)
            q["symbol"] = symbol  # return original symbol so callers see EURUSD not EUR/USD
            return q
        return await self._stock.get_quote(symbol)

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 200):
        if _is_forex_or_commodity(symbol):
            return await self._fx.get_candles(_to_provider_symbol(symbol), timeframe, limit)
        return await self._stock.get_candles(symbol, timeframe, limit)

    async def get_historical_candles(self, symbol: str, timeframe: str, start_date, end_date):
        if _is_forex_or_commodity(symbol):
            return await self._fx.get_historical_candles(
                _to_provider_symbol(symbol), timeframe, start_date, end_date
            )
        return await self._stock.get_historical_candles(symbol, timeframe, start_date, end_date)

    async def update_price(self, symbol: str) -> float:
        if _is_forex_or_commodity(symbol):
            return await self._fx.update_price(_to_provider_symbol(symbol))
        return await self._stock.update_price(symbol)


# ---------------------------------------------------------------------------
# Singleton — the only import the rest of the codebase needs
# ---------------------------------------------------------------------------

def _create_router() -> MarketDataRouter:
    from app.core.config import settings
    from app.services.market_data_service import market_data_service as stock_provider
    from app.services.providers.twelvedata import TwelveDataProvider

    fx_provider = TwelveDataProvider(api_key=settings.TWELVE_DATA_API_KEY)
    has_key = bool(settings.TWELVE_DATA_API_KEY)
    if has_key:
        log.info(
            "FOREX/COMMODITY DATA SOURCE: TWELVEDATA (REAL)",
            api_key_loaded=True,
            TWELVEDATA_KEY_LOADED=True,
        )
    else:
        log.warning(
            "FOREX/COMMODITY DATA SOURCE: GBM_SIMULATION (no API key)",
            api_key_loaded=False,
            TWELVEDATA_KEY_LOADED=False,
        )
    return MarketDataRouter(stock_provider=stock_provider, fx_provider=fx_provider)


market_data_router: MarketDataRouter = _create_router()
