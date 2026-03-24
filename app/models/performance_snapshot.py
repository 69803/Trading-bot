"""PerformanceSnapshot — point-in-time record of portfolio trading metrics.

One row is written at most once per hour per portfolio (rate-limited by
bot_service). Rows accumulate over time so callers can query performance
trends without recomputing everything from positions on every request.
"""
import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, Numeric
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class PerformanceSnapshot(Base):
    __tablename__ = "performance_snapshots"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # Trade counts
    total_trades: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    open_positions: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    winning_trades: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    losing_trades: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # PnL
    total_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"), nullable=False)
    daily_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"), nullable=False)
    avg_win: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"), nullable=False)
    avg_loss: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"), nullable=False)
    best_trade_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"), nullable=False)
    worst_trade_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 4), default=Decimal("0"), nullable=False)

    # Ratios
    win_rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    profit_factor: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    # Streaks
    consecutive_wins: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    consecutive_losses: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Drawdown
    max_drawdown_pct: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    # Activity
    trades_per_day: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)

    __table_args__ = (
        Index("ix_perf_snap_portfolio_captured", "portfolio_id", "captured_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<PerformanceSnapshot portfolio={self.portfolio_id} "
            f"at={self.captured_at} trades={self.total_trades} "
            f"win_rate={self.win_rate:.1%}>"
        )
