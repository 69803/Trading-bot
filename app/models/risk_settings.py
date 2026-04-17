import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, ForeignKey, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.user import User


class RiskSettings(Base):
    __tablename__ = "risk_settings"
    __table_args__ = (
        UniqueConstraint("user_id", "bot_id", "account_mode", name="uq_risk_settings_user_bot_mode"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    bot_id: Mapped[str] = mapped_column(String(50), nullable=False, default="trendmaster")
    account_mode: Mapped[str] = mapped_column(String(10), nullable=False, default="paper", index=True)
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
    trailing_stop_pct: Mapped[Decimal] = mapped_column(
        Numeric(6, 4), default=Decimal("0.00"), nullable=False
    )
    break_even_trigger_pct: Mapped[Decimal] = mapped_column(
        Numeric(6, 4), default=Decimal("0.00"), nullable=False
    )
    max_consecutive_losses: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    max_trades_per_hour: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    volatility_sizing_enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="risk_settings")

    def __repr__(self) -> str:
        return f"<RiskSettings user_id={self.user_id} bot_id={self.bot_id}>"
