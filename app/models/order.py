import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import ForeignKey, Numeric, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.portfolio import Portfolio
    from app.models.trade import Trade


class Order(Base):
    __tablename__ = "orders"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)       # buy / sell
    order_type: Mapped[str] = mapped_column(String(10), nullable=False) # market / limit
    # investment_amount: the dollar amount the user chose to invest (IQ Option style).
    # quantity is derived: quantity = investment_amount / fill_price
    investment_amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    filled_quantity: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), default=Decimal("0.0"), nullable=False
    )
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    avg_fill_price: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 8), nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, index=True
    )  # pending / filled / cancelled / rejected
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    portfolio: Mapped["Portfolio"] = relationship("Portfolio", back_populates="orders")
    trades: Mapped[List["Trade"]] = relationship(
        "Trade", back_populates="order", cascade="all, delete-orphan"
    )

    @property
    def realized_pnl(self) -> Optional[Decimal]:
        """Sum of realized PnL from all trades on this order. None if not yet closed."""
        if not self.trades:
            return None
        pnls = [t.realized_pnl for t in self.trades if t.realized_pnl is not None]
        return sum(pnls, Decimal("0")) if pnls else None

    def __repr__(self) -> str:
        return (
            f"<Order id={self.id} symbol={self.symbol!r} "
            f"side={self.side!r} status={self.status!r}>"
        )
