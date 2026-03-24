from typing import TYPE_CHECKING, List

from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.portfolio import Portfolio
    from app.models.strategy_config import StrategyConfig
    from app.models.risk_settings import RiskSettings
    from app.models.backtest_run import BacktestRun
    from app.models.strategy_signal import StrategySignal
    from app.models.bot_state import BotState
    from app.models.refresh_token import RefreshToken


class User(Base):
    __tablename__ = "users"

    email: Mapped[str] = mapped_column(
        String(320), unique=True, index=True, nullable=False
    )
    hashed_password: Mapped[str] = mapped_column(String(128), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Relationships
    portfolio: Mapped["Portfolio"] = relationship(
        "Portfolio", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )
    strategy_config: Mapped["StrategyConfig"] = relationship(
        "StrategyConfig",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
    risk_settings: Mapped["RiskSettings"] = relationship(
        "RiskSettings",
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
    backtest_runs: Mapped[List["BacktestRun"]] = relationship(
        "BacktestRun", back_populates="user", cascade="all, delete-orphan"
    )
    strategy_signals: Mapped[List["StrategySignal"]] = relationship(
        "StrategySignal", back_populates="user", cascade="all, delete-orphan"
    )
    bot_state: Mapped["BotState"] = relationship(
        "BotState", back_populates="user", uselist=False, cascade="all, delete-orphan"
    )
    refresh_tokens: Mapped[List["RefreshToken"]] = relationship(
        "RefreshToken", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<User id={self.id} email={self.email!r}>"
