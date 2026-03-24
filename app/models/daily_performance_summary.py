"""Daily performance summary snapshot for one portfolio trading day."""
from __future__ import annotations

import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import Date, ForeignKey, Index, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DailyPerformanceSummary(Base):
    """One row per (portfolio, UTC calendar day).

    Computed from closed ``Position`` records; persisted so the history
    is retained even after positions are archived.
    """

    __tablename__ = "daily_performance_summaries"
    __table_args__ = (
        UniqueConstraint(
            "portfolio_id", "date_utc",
            name="uq_daily_perf_portfolio_date",
        ),
        Index("ix_daily_perf_portfolio_date", "portfolio_id", "date_utc"),
    )

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    date_utc: Mapped[date] = mapped_column(Date, nullable=False)

    total_trades:   Mapped[int]   = mapped_column(default=0)
    winning_trades: Mapped[int]   = mapped_column(default=0)
    losing_trades:  Mapped[int]   = mapped_column(default=0)
    win_rate:       Mapped[float] = mapped_column(default=0.0)
    total_pnl:      Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"))
    avg_pnl:        Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"))

    best_symbol:  Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    worst_symbol: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    best_hour:    Mapped[Optional[int]] = mapped_column(nullable=True)
    worst_hour:   Mapped[Optional[int]] = mapped_column(nullable=True)

    def __repr__(self) -> str:
        return (
            f"<DailyPerformanceSummary {self.date_utc} "
            f"trades={self.total_trades} pnl={self.total_pnl}>"
        )
