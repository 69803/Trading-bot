import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, Numeric
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.portfolio import Portfolio


class PortfolioSnapshot(Base):
    __tablename__ = "portfolio_snapshots"

    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    total_value: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # Relationships
    portfolio: Mapped["Portfolio"] = relationship(
        "Portfolio", back_populates="snapshots"
    )

    def __repr__(self) -> str:
        return (
            f"<PortfolioSnapshot portfolio_id={self.portfolio_id} "
            f"total_value={self.total_value} ts={self.timestamp}>"
        )
