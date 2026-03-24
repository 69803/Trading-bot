import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, computed_field


class PortfolioOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    initial_capital: Decimal
    cash_balance: Decimal
    realized_pnl: Decimal
    created_at: datetime
    updated_at: datetime

    unrealized_pnl: Decimal = Decimal("0.0")

    @computed_field  # type: ignore[misc]
    @property
    def total_value(self) -> Decimal:
        return self.cash_balance + self.unrealized_pnl


class PortfolioSummary(BaseModel):
    balance: Decimal
    equity: Decimal
    pnl: Decimal
    daily_pnl: Decimal
    open_positions_count: int
    closed_positions_count: int
    win_rate: float
    bot_running: bool


class BalanceOut(BaseModel):
    """Simple real-time balance snapshot used by the trade page."""
    cash_balance: Decimal
    equity: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal


class PositionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    portfolio_id: uuid.UUID
    symbol: str
    side: str
    # investment_amount: original dollar amount the user chose to invest
    investment_amount: Optional[Decimal] = None
    quantity: Decimal
    avg_entry_price: Decimal
    current_price: Optional[Decimal]
    stop_loss_price: Optional[Decimal]
    take_profit_price: Optional[Decimal]
    is_open: bool
    opened_at: datetime
    closed_at: Optional[datetime]
    closed_price: Optional[Decimal]
    realized_pnl: Decimal
    created_at: datetime
    updated_at: datetime

    @computed_field  # type: ignore[misc]
    @property
    def unrealized_pnl(self) -> Decimal:
        if self.current_price is None:
            return Decimal("0.0")
        if self.side == "long":
            return (self.current_price - self.avg_entry_price) * self.quantity
        return (self.avg_entry_price - self.current_price) * self.quantity

    @computed_field  # type: ignore[misc]
    @property
    def pnl_percentage(self) -> Decimal:
        """P/L as a percentage of the original investment amount."""
        if self.current_price is None:
            return Decimal("0.0")
        # Use investment_amount if stored; fall back to cost basis
        invest = (
            self.investment_amount
            if self.investment_amount
            else self.avg_entry_price * self.quantity
        )
        if invest == 0:
            return Decimal("0.0")
        if self.side == "long":
            pnl = (self.current_price - self.avg_entry_price) * self.quantity
        else:
            pnl = (self.avg_entry_price - self.current_price) * self.quantity
        return (pnl / invest * 100).quantize(Decimal("0.01"))


class PortfolioHistoryPoint(BaseModel):
    timestamp: datetime
    total_value: Decimal
    cash: Decimal
