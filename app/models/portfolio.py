import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, List

from sqlalchemy import ForeignKey, Numeric, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.user import User
    from app.models.position import Position
    from app.models.order import Order
    from app.models.portfolio_snapshot import PortfolioSnapshot


class Portfolio(Base):
    __tablename__ = "portfolios"
    __table_args__ = (
        # Each user has at most one portfolio per account mode (paper / live).
        # Replaces the old single-column unique=True on user_id.
        UniqueConstraint("user_id", "account_mode", name="uq_portfolios_user_mode"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # 'paper' (default) or 'live'.  Never mix data between modes.
    account_mode: Mapped[str] = mapped_column(
        String(10), nullable=False, default="paper", index=True
    )
    initial_capital: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), default=Decimal("10000.0"), nullable=False
    )
    cash_balance: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    realized_pnl: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), default=Decimal("0.0"), nullable=False
    )

    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="portfolio")
    positions: Mapped[List["Position"]] = relationship(
        "Position", back_populates="portfolio", cascade="all, delete-orphan"
    )
    orders: Mapped[List["Order"]] = relationship(
        "Order", back_populates="portfolio", cascade="all, delete-orphan"
    )
    snapshots: Mapped[List["PortfolioSnapshot"]] = relationship(
        "PortfolioSnapshot", back_populates="portfolio", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Portfolio id={self.id} user_id={self.user_id} cash={self.cash_balance}>"
