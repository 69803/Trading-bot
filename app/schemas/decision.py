"""Pydantic schemas for the decision engine output."""
from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class FinalDecision(BaseModel):
    """
    Combined output of the decision engine (Phase 4).

    Merges a TechnicalSignal and a SentimentResult into a single,
    actionable trading decision with full reasoning.

    Possible directions
    ───────────────────
    BUY     — technical signal confirmed (or not contradicted) by sentiment.
    SELL    — technical signal confirmed (or not contradicted) by sentiment.
    HOLD    — technical signal is HOLD, or opposing sentiment overrides it.
    BLOCKED — actionable technical signal exists but high-impact opposing
              news makes execution too risky.  The bot skips the trade.
    """

    symbol: str

    # Final verdict
    direction: Literal["BUY", "SELL", "HOLD", "BLOCKED"]
    confidence: int = Field(ge=0, le=100, description="Final blended confidence 0–100")
    reasons: List[str] = Field(description="Ordered list of reasons that drove this decision")

    # Inputs snapshot (for audit / frontend display)
    technical_direction: Literal["BUY", "SELL", "HOLD"]
    technical_confidence: int = Field(ge=0, le=100)
    sentiment_label: Literal["positive", "negative", "neutral"]
    sentiment_score: float = Field(ge=-1.0, le=1.0)
    sentiment_impact: int = Field(ge=0, le=100)
    news_count: int = 0

    # Optional context
    override_reason: Optional[str] = None   # Set when direction differs from technical

    decided_at: datetime

    # One-liner summary for logs / dashboard (e.g. "BUY [+35]: EMA+RSI+MACD | neutral news")
    decision_summary: str = ""

    # Raw composite score from technical engine (before sentiment blending)
    tech_composite_score: int = 0

    # ── Derived helpers ──────────────────────────────────────────────────────

    @property
    def is_actionable(self) -> bool:
        """True only when the bot should open or close a position."""
        return self.direction in ("BUY", "SELL")

    @property
    def is_blocked(self) -> bool:
        return self.direction == "BLOCKED"

    @property
    def confidence_label(self) -> str:
        if self.confidence >= 75:
            return "strong"
        if self.confidence >= 50:
            return "moderate"
        if self.confidence >= 30:
            return "weak"
        return "very_weak"
