import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class RiskSettingsOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    max_position_size_pct: Decimal
    max_daily_loss_pct: Decimal
    max_open_positions: int
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_drawdown_pct: Decimal
    created_at: datetime
    updated_at: datetime


class RiskSettingsUpdate(BaseModel):
    """All fields optional — used for PATCH requests."""

    max_position_size_pct: Optional[Decimal] = None
    max_daily_loss_pct: Optional[Decimal] = None
    max_open_positions: Optional[int] = None
    stop_loss_pct: Optional[Decimal] = None
    take_profit_pct: Optional[Decimal] = None
    max_drawdown_pct: Optional[Decimal] = None


class RiskStatusOut(BaseModel):
    current_drawdown_pct: Decimal
    daily_pnl: Decimal
    daily_pnl_pct: Decimal
    open_position_count: int
    trading_halted: bool
    halt_reason: Optional[str]
