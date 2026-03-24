import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.user import User


class StrategySignal(Base):
    __tablename__ = "strategy_signals"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    signal_type: Mapped[str] = mapped_column(
        String(10), nullable=False
    )  # buy / sell / hold
    ema_fast_value: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    ema_slow_value: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    rsi_value: Mapped[Decimal] = mapped_column(Numeric(8, 4), nullable=False)
    price_at_signal: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False)
    acted_on: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="strategy_signals")

    def __repr__(self) -> str:
        return (
            f"<StrategySignal id={self.id} symbol={self.symbol!r} "
            f"signal={self.signal_type!r}>"
        )
