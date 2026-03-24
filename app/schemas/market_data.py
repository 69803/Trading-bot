from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel


class CandleOut(BaseModel):
    symbol: str
    timeframe: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


class CandleListResponse(BaseModel):
    symbol: str
    timeframe: str
    candles: list[CandleOut]


class QuoteOut(BaseModel):
    symbol: str
    price: Decimal
    change: Decimal
    change_pct: Decimal
    bid: Decimal
    ask: Decimal
    timestamp: datetime


class QuoteListResponse(BaseModel):
    quotes: list[QuoteOut]
