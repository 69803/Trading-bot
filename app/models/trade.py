import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.order import Order
    from app.models.portfolio import Portfolio


class Trade(Base):
    __tablename__ = "trades"

    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("orders.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)  # buy / sell
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    commission: Mapped[Decimal] = mapped_column(
        Numeric(18, 8), default=Decimal("0.0"), nullable=False
    )
    realized_pnl: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 8), nullable=True
    )
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # Relationships
    order: Mapped["Order"] = relationship("Order", back_populates="trades")
    portfolio: Mapped["Portfolio"] = relationship("Portfolio")

    def __repr__(self) -> str:
        return (
            f"<Trade id={self.id} symbol={self.symbol!r} "
            f"side={self.side!r} qty={self.quantity} price={self.price}>"
        )
