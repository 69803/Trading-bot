import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, List

from sqlalchemy import ForeignKey, Numeric
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

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
        index=True,
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
