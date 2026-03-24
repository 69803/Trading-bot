"""Pydantic schemas for the technical analysis engine output."""
from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class ScoreFactor(BaseModel):
    """
    One indicator's contribution to the composite score.

    Used to explain exactly *why* the engine scored a signal the way it did.
    Every factor that affected the composite score gets one entry here.
    """
    name: str        # e.g. "EMA_TREND", "RSI_LEVEL", "MACD_HIST"
    domain: str      # "trend" | "momentum" | "volatility"
    points: int      # signed contribution (e.g. +30 or -15)
    max_points: int  # maximum absolute contribution for this factor
    reason: str      # human-readable explanation


class IndicatorValues(BaseModel):
    """Raw indicator values captured at signal time."""

    price: float
    rsi: float
    ema_fast: float
    ema_slow: float
    macd: float
    macd_signal: float
    macd_histogram: float
    atr: float
    volume_ratio: float = Field(
        description="Current volume ÷ N-period average (1.0 = average)"
    )
    adx: float = Field(
        default=float("nan"),
        description="Average Directional Index — trend strength 0-100 (>25 = trending, <20 = sideways)",
    )


class TechnicalSignal(BaseModel):
    """
    Structured output of the technical analysis engine.

    Used as the first input to the decision engine (Phase 4).
    """

    symbol: str
    timeframe: str

    # Core signal
    direction: Literal["BUY", "SELL", "HOLD"]
    confidence: int = Field(ge=0, le=100, description="Signal strength 0–100")
    reasons: List[str] = Field(
        description="Human-readable list of indicators that drove this signal"
    )

    # Full indicator snapshot
    indicators: IndicatorValues

    # Metadata
    analyzed_at: datetime
    candles_used: int = Field(description="Number of candles consumed in the analysis")

    # Trend strength summary (set by technical_engine)
    trend_strength: Literal["strong", "moderate", "weak", "sideways"] = "weak"

    # Optional context for the decision engine
    ema_crossover: Optional[Literal["bullish", "bearish"]] = None
    macd_crossover: Optional[Literal["bullish", "bearish"]] = None
    rsi_extreme: Optional[Literal["oversold", "overbought"]] = None

    # Structured score breakdown — the core observability feature
    composite_score: int = Field(
        default=0,
        description="Raw composite score before direction assignment (range: −110 to +110)",
    )
    score_breakdown: List[ScoreFactor] = Field(
        default_factory=list,
        description="Per-indicator contributions to the composite score",
    )
    hold_reason: Optional[str] = Field(
        default=None,
        description="Specific reason the signal was HOLD (sideways gate, score too low, etc.)",
    )

    @property
    def is_actionable(self) -> bool:
        """Returns True if direction is BUY or SELL (not HOLD)."""
        return self.direction in ("BUY", "SELL")

    @property
    def confidence_label(self) -> str:
        if self.confidence >= 75:
            return "strong"
        if self.confidence >= 50:
            return "moderate"
        if self.confidence >= 30:
            return "weak"
        return "very_weak"
