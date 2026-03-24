"""Abstract interface that all market data providers must implement."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List


class MarketDataProvider(ABC):
    """Common interface for GBM (simulated) and Alpaca (real) data providers."""

    @abstractmethod
    def get_all_symbols(self) -> List[str]:
        """Return the list of supported trading symbols."""
        ...

    @abstractmethod
    async def get_current_price(self, symbol: str) -> float:
        """Return the latest price for *symbol*."""
        ...

    @abstractmethod
    async def get_quote(self, symbol: str) -> dict:
        """Return {symbol, price, change, change_pct, bid, ask, timestamp}."""
        ...

    @abstractmethod
    async def get_candles(
        self, symbol: str, timeframe: str, limit: int = 200
    ) -> List[dict]:
        """
        Return the most recent *limit* OHLCV candles for *symbol*.
        Each candle: {timestamp, open, high, low, close, volume}
        """
        ...

    @abstractmethod
    async def get_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        start_date,
        end_date,
    ) -> List[dict]:
        """
        Return candles covering [start_date, end_date] for backtesting.
        Each candle includes 'symbol' and 'timeframe' keys in addition to OHLCV.
        """
        ...

    @abstractmethod
    async def update_price(self, symbol: str) -> float:
        """
        Called by the scheduler every 30 s.
        GBM: advance the random walk.
        Alpaca: no-op (prices fetched live on demand).
        """
        ...
