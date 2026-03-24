import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict


class StrategyConfigOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    ema_fast: int
    ema_slow: int
    rsi_period: int
    rsi_overbought: Decimal
    rsi_oversold: Decimal
    auto_trade: bool
    symbols: Any
    asset_classes: Any
    investment_amount: Decimal
    run_interval_seconds: int
    per_symbol_max_positions: int
    allow_buy: bool
    allow_sell: bool
    cooldown_seconds: int
    created_at: datetime
    updated_at: datetime


class StrategyConfigUpdate(BaseModel):
    """All fields optional — used for PATCH requests."""

    ema_fast: Optional[int] = None
    ema_slow: Optional[int] = None
    rsi_period: Optional[int] = None
    rsi_overbought: Optional[Decimal] = None
    rsi_oversold: Optional[Decimal] = None
    auto_trade: Optional[bool] = None
    symbols: Optional[List[str]] = None
    asset_classes: Optional[List[str]] = None
    investment_amount: Optional[Decimal] = None
    run_interval_seconds: Optional[int] = None
    per_symbol_max_positions: Optional[int] = None
    allow_buy: Optional[bool] = None
    allow_sell: Optional[bool] = None
    cooldown_seconds: Optional[int] = None


class BotConfigOut(BaseModel):
    # Strategy
    ema_fast: int
    ema_slow: int
    rsi_period: int
    rsi_overbought: Decimal
    rsi_oversold: Decimal
    symbols: Any
    asset_classes: Any
    investment_amount: Decimal
    run_interval_seconds: int
    per_symbol_max_positions: int
    allow_buy: bool
    allow_sell: bool
    cooldown_seconds: int
    # Risk
    stop_loss_pct: Decimal
    take_profit_pct: Decimal
    max_open_positions: int
    max_daily_loss_pct: Decimal
    max_position_size_pct: Decimal


class BotConfigUpdate(BaseModel):
    # Strategy
    ema_fast: Optional[int] = None
    ema_slow: Optional[int] = None
    rsi_period: Optional[int] = None
    rsi_overbought: Optional[Decimal] = None
    rsi_oversold: Optional[Decimal] = None
    symbols: Optional[List[str]] = None
    asset_classes: Optional[List[str]] = None
    investment_amount: Optional[Decimal] = None
    run_interval_seconds: Optional[int] = None
    per_symbol_max_positions: Optional[int] = None
    allow_buy: Optional[bool] = None
    allow_sell: Optional[bool] = None
    cooldown_seconds: Optional[int] = None
    # Risk
    stop_loss_pct: Optional[Decimal] = None
    take_profit_pct: Optional[Decimal] = None
    max_open_positions: Optional[int] = None
    max_daily_loss_pct: Optional[Decimal] = None
    max_position_size_pct: Optional[Decimal] = None


class SignalOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    symbol: str
    signal_type: str
    ema_fast_value: Decimal
    ema_slow_value: Decimal
    rsi_value: Decimal
    price_at_signal: Decimal
    confidence: Decimal
    acted_on: bool
    triggered_at: datetime
    created_at: datetime


class SignalListResponse(BaseModel):
    signals: list[SignalOut]
