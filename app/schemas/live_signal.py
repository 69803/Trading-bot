"""Schema for the on-demand live signal endpoint (Phase 9)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel


class LiveSignalIndicators(BaseModel):
    price: float
    rsi: float
    ema_fast: float
    ema_slow: float
    macd: float
    macd_histogram: float
    atr: float
    volume_ratio: float


class LiveSignal(BaseModel):
    """Complete pipeline snapshot for one symbol — returned by GET /signals/live."""

    symbol: str
    analyzed_at: datetime

    # ── Technical layer ───────────────────────────────────────────────────────
    tech_direction: Literal["BUY", "SELL", "HOLD"]
    tech_confidence: int
    tech_reasons: List[str]
    indicators: LiveSignalIndicators
    ema_crossover: Optional[str] = None
    macd_crossover: Optional[str] = None
    rsi_extreme: Optional[str] = None
    candles_used: int = 0

    # ── Sentiment layer ───────────────────────────────────────────────────────
    sentiment_label: Literal["positive", "negative", "neutral"]
    sentiment_score: float
    sentiment_impact: int
    sentiment_modifier: float
    sentiment_source: str
    news_count: int
    top_headlines: List[str]

    # ── Decision layer ────────────────────────────────────────────────────────
    direction: Literal["BUY", "SELL", "HOLD", "BLOCKED"]
    final_confidence: int
    override_reason: Optional[str] = None
    decision_reasons: List[str]

    # ── Status flags ──────────────────────────────────────────────────────────
    is_actionable: bool
    is_blocked: bool


class DecisionLogOut(BaseModel):
    """Serialised DecisionLog row for the frontend decisions table."""

    id: str
    symbol: str
    decided_at: datetime
    direction: str
    final_confidence: int
    tech_direction: str
    tech_confidence: int
    sentiment_label: str
    sentiment_score: float
    sentiment_impact: int
    executed: bool
    rejection_reason: Optional[str] = None
    override_reason: Optional[str] = None
    risk_stop_loss: Optional[float] = None
    risk_take_profit: Optional[float] = None
    risk_rr_ratio: Optional[float] = None
    reasons: Optional[str] = None


class LiveSignalsResponse(BaseModel):
    signals: List[LiveSignal]
    generated_at: datetime
    symbols_requested: int
    symbols_ok: int
    symbols_failed: int


class DecisionsResponse(BaseModel):
    items: List[DecisionLogOut]
    total: int
    symbol: Optional[str] = None
