import uuid
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, model_validator


class OrderCreate(BaseModel):
    symbol: str
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"]

    # IQ Option style: user specifies how much money to invest
    investment_amount: Optional[float] = None  # e.g. 200.0  → invest $200

    # Legacy / advanced: specify share count directly (still supported)
    quantity: Optional[float] = None

    limit_price: Optional[float] = None

    @model_validator(mode="after")
    def check_amount_or_quantity(self) -> "OrderCreate":
        if self.investment_amount is None and self.quantity is None:
            raise ValueError("Provide either investment_amount or quantity")
        if self.investment_amount is not None and self.investment_amount <= 0:
            raise ValueError("investment_amount must be greater than 0")
        if self.quantity is not None and self.quantity <= 0:
            raise ValueError("quantity must be greater than 0")
        return self


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    portfolio_id: uuid.UUID
    symbol: str
    side: str
    order_type: str
    investment_amount: Optional[Decimal]
    quantity: Decimal
    filled_quantity: Decimal
    limit_price: Optional[Decimal]
    avg_fill_price: Optional[Decimal]
    status: str
    rejection_reason: Optional[str]
    created_at: datetime
    updated_at: datetime
    bot_id: Optional[str] = None
    # Derived from related Trade records (populated when trades are eager-loaded)
    realized_pnl: Optional[Decimal] = None


class OrderListResponse(BaseModel):
    items: list[OrderOut]
    total: int
