"""SQLAlchemy model for historical economic calendar events.

Stores macro events (CPI, NFP, rate decisions …) with their actual /
forecast / previous values. A composite unique constraint prevents
duplicates during incremental imports.
"""
from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class HistoricalEvent(Base):
    """One historical economic event with its metadata and release values."""

    __tablename__ = "historical_events"
    __table_args__ = (
        # Deduplicate by time + currency + name + source
        UniqueConstraint(
            "event_datetime_utc", "currency", "event_name", "source",
            name="uq_historical_event",
        ),
        Index("ix_historical_event_dt",              "event_datetime_utc"),
        Index("ix_historical_event_currency_dt",     "currency", "event_datetime_utc"),
    )

    event_datetime_utc: Mapped[datetime]        = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    country:    Mapped[Optional[str]] = mapped_column(String(10),  nullable=True)
    currency:   Mapped[str]           = mapped_column(String(10),  nullable=False, index=True)
    event_name: Mapped[str]           = mapped_column(String(255), nullable=False)
    impact:     Mapped[str]           = mapped_column(String(20),  nullable=False, default="low")
    # Release values stored as strings because formats vary by event type
    actual:     Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    forecast:   Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    previous:   Mapped[Optional[str]] = mapped_column(String(50),  nullable=True)
    source:     Mapped[str]           = mapped_column(String(50),  nullable=False, default="manual")

    def __repr__(self) -> str:
        return (
            f"<HistoricalEvent {self.event_datetime_utc:%Y-%m-%d} "
            f"{self.currency!r} {self.event_name!r} impact={self.impact!r}>"
        )
