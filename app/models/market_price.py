"""SQLAlchemy model for historical market price data (OHLCV).

Stores normalized daily / intraday price bars downloaded from yfinance or
other providers. A unique constraint on (symbol, datetime_utc, interval)
prevents duplicate rows during incremental imports.
"""
from datetime import datetime

from sqlalchemy import DateTime, Float, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MarketPrice(Base):
    """One OHLCV bar for a symbol at a given datetime and interval."""

    __tablename__ = "market_prices"
    __table_args__ = (
        UniqueConstraint(
            "symbol", "datetime_utc", "interval",
            name="uq_market_price_symbol_dt_interval",
        ),
        Index("ix_market_price_symbol_dt", "symbol", "datetime_utc"),
    )

    symbol:       Mapped[str]   = mapped_column(String(20),  nullable=False, index=True)
    datetime_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    interval:     Mapped[str]   = mapped_column(String(10),  nullable=False, default="1d")
    open:         Mapped[float] = mapped_column(Float,        nullable=False)
    high:         Mapped[float] = mapped_column(Float,        nullable=False)
    low:          Mapped[float] = mapped_column(Float,        nullable=False)
    close:        Mapped[float] = mapped_column(Float,        nullable=False)
    volume:       Mapped[float] = mapped_column(Float,        nullable=False, default=0.0)
    source:       Mapped[str]   = mapped_column(String(50),   nullable=False, default="yfinance")

    def __repr__(self) -> str:
        return (
            f"<MarketPrice {self.symbol!r} {self.interval!r} "
            f"{self.datetime_utc:%Y-%m-%d %H:%M} close={self.close}>"
        )
