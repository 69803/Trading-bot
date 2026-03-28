import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, field_validator


class BacktestRunRequest(BaseModel):
    symbol: str
    timeframe: Literal["1h", "4h", "1d"]
    start_date: date
    end_date: date
    initial_capital: float
    ema_fast: int
    ema_slow: int
    rsi_period: int
    rsi_overbought: float
    rsi_oversold: float
    stop_loss_pct: float
    take_profit_pct: float
    commission_pct: float = 0.001
    use_sentiment: bool = True          # False → technical-only (ignores news layer)
    position_size_pct: float = 0.05    # fraction of equity per trade (0.01–0.20)
    force_pct_tp_sl: bool = False      # True → skip ATR, use stop_loss_pct/take_profit_pct directly
    use_signal_exit: bool = True       # False → only TP/SL closes positions (no signal-based exit)

    @field_validator("end_date")
    @classmethod
    def end_date_after_start(cls, v: date, info: Any) -> date:
        start = info.data.get("start_date")
        if start and v <= start:
            raise ValueError("end_date must be after start_date")
        return v

    @field_validator("ema_slow")
    @classmethod
    def ema_slow_gt_fast(cls, v: int, info: Any) -> int:
        fast = info.data.get("ema_fast")
        if fast and v <= fast:
            raise ValueError("ema_slow must be greater than ema_fast")
        return v


class BacktestRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    symbol: str
    timeframe: str
    start_date: date
    end_date: date
    status: str
    progress_pct: int
    results: Optional[Any]
    equity_curve: Optional[Any]
    trade_log: Optional[Any]
    created_at: datetime


class BacktestListResponse(BaseModel):
    items: list[BacktestRunOut]
    total: int
