import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Integer, Numeric
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.user import User


class RiskSettings(Base):
    __tablename__ = "risk_settings"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
    )
    max_position_size_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0.05"), nullable=False
    )
    max_daily_loss_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0.02"), nullable=False
    )
    max_open_positions: Mapped[int] = mapped_column(
        Integer, default=10, nullable=False
    )
    stop_loss_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0.03"), nullable=False
    )
    take_profit_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0.06"), nullable=False
    )
    max_drawdown_pct: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0.20"), nullable=False
    )

    # ── Advanced risk management (all default to 0 / False = disabled) ───────
    # Trailing stop: if > 0, trail the stop at this % below peak price
    trailing_stop_pct: Mapped[Decimal] = mapped_column(
        Numeric(6, 4), default=Decimal("0.00"), nullable=False
    )
    # Break-even: move SL to entry when unrealised gain reaches this %
    break_even_trigger_pct: Mapped[Decimal] = mapped_column(
        Numeric(6, 4), default=Decimal("0.00"), nullable=False
    )
    # Consecutive loss circuit breaker (0 = disabled)
    max_consecutive_losses: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    # Max new trades per 60-minute window (0 = disabled)
    max_trades_per_hour: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    # Volatility-adjusted sizing: reduce position size when ATR is high
    volatility_sizing_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="risk_settings")

    def __repr__(self) -> str:
        return f"<RiskSettings user_id={self.user_id}>"
