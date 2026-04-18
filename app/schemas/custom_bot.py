"""Pydantic schemas for CustomBot CRUD."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class CustomBotConfig(BaseModel):
    """Rich configuration stored inside CustomBot.config JSON column."""
    # General
    description: Optional[str] = None
    color: str = "#6366f1"
    direction: str = "both"  # "long_only" | "short_only" | "both"

    # Markets
    trade_stocks: bool = True
    trade_forex: bool = True
    trade_crypto: bool = False
    trade_commodities: bool = False
    allowed_symbols: List[str] = Field(default_factory=list)
    blocked_symbols: List[str] = Field(default_factory=list)

    # Strategy / Logic
    timeframe: str = "1h"
    confirmation_timeframe: str = "4h"
    min_confidence_score: int = 35
    min_rr_ratio: float = 1.5
    use_technical_indicators: bool = True
    allow_reversals: bool = False
    allow_averaging_down: bool = False
    allow_pyramiding: bool = False
    run_interval_seconds: int = 300
    cooldown_seconds: int = 900
    per_symbol_max_positions: int = 1

    # Technical indicators
    ema_fast: int = 9
    ema_slow: int = 21
    rsi_period: int = 14
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0

    # Risk
    risk_level: str = "moderate"  # "conservative" | "moderate" | "aggressive"
    risk_per_trade_pct: float = 1.0
    max_drawdown_pct: float = 15.0
    max_open_positions: int = 5
    max_position_size_pct: float = 10.0
    daily_loss_limit_pct: float = 3.0
    stop_after_consecutive_losses: int = 0
    daily_profit_target_pct: float = 0.0

    # Entry / Exit
    stop_loss_pct: float = 2.0
    take_profit_pct: float = 4.0
    trailing_stop: bool = False
    trailing_stop_pct: float = 0.5
    breakeven: bool = False
    breakeven_trigger_pct: float = 1.0
    partial_close: bool = False
    partial_close_pct: float = 50.0
    max_trade_duration_hours: int = 0
    close_end_of_day: bool = False

    # Filters
    only_market_hours: bool = True
    trade_premarket: bool = False
    trade_after_hours: bool = False
    session_filter: str = "all"  # "all" | "london" | "ny" | "asian"
    volatility_filter: bool = False
    volume_filter: bool = False
    trend_filter: bool = True
    news_filter: bool = False
    spread_filter: bool = False

    # Execution
    investment_amount: float = 100.0
    capital_allocation_pct: float = 20.0
    priority: int = 1
    enabled_on_save: bool = False


class CustomBotCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    config: CustomBotConfig = Field(default_factory=CustomBotConfig)


class CustomBotUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    config: Optional[CustomBotConfig] = None


class CustomBotOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    bot_id: str
    description: Optional[str]
    color: str
    config: Any
    is_enabled: bool
    created_at: datetime
    updated_at: datetime


class CustomBotListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    bot_id: str
    color: str
    is_enabled: bool
    is_running: bool = False
    cycles_run: int = 0
    last_log: Optional[str] = None
    created_at: datetime
    updated_at: datetime
