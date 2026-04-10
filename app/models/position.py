import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.portfolio import Portfolio


class Position(Base):
    __tablename__ = "positions"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)  # long / short
    # investment_amount: original dollar amount invested by the user
    investment_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    avg_entry_price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    current_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    stop_loss_price: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 8), nullable=True
    )
    take_profit_price: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 8), nullable=True
    )
    is_open: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, index=True
    )
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    closed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    closed_price: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 8), nullable=True
    )
    realized_pnl: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), default=Decimal("0.0"), nullable=False
    )

    # ── Bot identifier — which bot opened this position ─────────────────────
    bot_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)

    # ── Trade mode — set at open time by the caller ─────────────────────────
    # True  → paper trade (simulated, no real money)
    # False → live trade  (real broker execution)
    # None  → legacy row created before this column existed; treated as paper
    is_paper: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=None)

    # ── Event context — set at open time by bot_service ─────────────────────
    # "normal"                   – no event risk at open
    # "reduced_size_due_to_event" – position size halved due to medium-impact event
    event_context: Mapped[str | None] = mapped_column(String(40), nullable=True, default=None)

    # ── TP/SL cross-detection state ─────────────────────────────────────────
    # The live price recorded at the END of the last TP/SL evaluation cycle.
    # Used for cross-detection: if price moved from prev_evaluated_price to
    # live_price across a TP or SL level, the position closes even if the
    # current snapshot no longer shows the level as breached (bounce scenario).
    # NULL on newly-opened positions — cross detection activates from cycle 2.
    prev_evaluated_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)

    # ── Trailing stop / break-even state ────────────────────────────────────
    # Best price seen since open (high-water mark for longs, low for shorts)
    high_water_mark: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    # Current trailing stop level (updated each cycle when price moves favorably)
    trailing_stop_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    # True once SL has been moved to entry (break-even activated)
    break_even_activated: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Relationships
    portfolio: Mapped["Portfolio"] = relationship("Portfolio", back_populates="positions")

    def __repr__(self) -> str:
        return (
            f"<Position id={self.id} symbol={self.symbol!r} "
            f"side={self.side!r} qty={self.quantity} open={self.is_open}>"
        )
