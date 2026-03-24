import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class TradeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    order_id: uuid.UUID
    portfolio_id: uuid.UUID
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    commission: Decimal
    realized_pnl: Optional[Decimal]
    executed_at: datetime
    created_at: datetime


class TradeListResponse(BaseModel):
    items: list[TradeOut]
    total: int
