import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, ForeignKey, Integer, JSON, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.user import User


class StrategyConfig(Base):
    __tablename__ = "strategy_configs"
    __table_args__ = (
        UniqueConstraint("user_id", "bot_id", name="uq_strategy_configs_user_bot"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    bot_id: Mapped[str] = mapped_column(String(50), nullable=False, default="trendmaster")
    ema_fast: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    ema_slow: Mapped[int] = mapped_column(Integer, default=10, nullable=False)
    rsi_period: Mapped[int] = mapped_column(Integer, default=14, nullable=False)
    rsi_overbought: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=Decimal("55.0"), nullable=False
    )
    rsi_oversold: Mapped[Decimal] = mapped_column(
        Numeric(5, 2), default=Decimal("45.0"), nullable=False
    )
    auto_trade: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    symbols: Mapped[Any] = mapped_column(JSON, default=lambda: [], nullable=False)
    asset_classes: Mapped[Any] = mapped_column(JSON, default=lambda: ["stocks"], nullable=False)
    investment_amount: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), default=Decimal("100"), nullable=False
    )
    run_interval_seconds: Mapped[int] = mapped_column(Integer, default=60, nullable=False)
    per_symbol_max_positions: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    allow_buy: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    allow_sell: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    cooldown_seconds: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="strategy_config")

    def __repr__(self) -> str:
        return f"<StrategyConfig user_id={self.user_id} bot_id={self.bot_id} auto_trade={self.auto_trade}>"
