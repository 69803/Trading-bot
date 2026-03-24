"""Pydantic schemas for the news & sentiment engine output."""
from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class NewsItem(BaseModel):
    """A single news article with its individual sentiment score."""

    title: str
    description: Optional[str] = None
    source: str = "unknown"
    published_at: Optional[datetime] = None
    url: Optional[str] = None

    # Per-article sentiment (computed by sentiment_engine)
    score: float = Field(
        default=0.0,
        ge=-1.0,
        le=1.0,
        description="Sentiment score for this article: -1 (very negative) → +1 (very positive)",
    )
    label: Literal["positive", "negative", "neutral"] = "neutral"
    impact_keywords: List[str] = Field(
        default_factory=list,
        description="High-impact financial keywords found in this article",
    )


class SentimentResult(BaseModel):
    """
    Aggregated sentiment output for one symbol.

    Used as the second input to the decision engine (Phase 4).
    """

    symbol: str

    # Aggregate scores
    sentiment_score: float = Field(
        ge=-1.0,
        le=1.0,
        description="Weighted aggregate sentiment: -1 (very negative) → +1 (very positive)",
    )
    impact_score: int = Field(
        ge=0,
        le=100,
        description="How market-moving the news looks: 0 (none) → 100 (extreme)",
    )
    label: Literal["positive", "negative", "neutral"]

    # News detail
    news_count: int = 0
    headlines: List[str] = Field(default_factory=list, description="Article titles only")
    items: List[NewsItem] = Field(default_factory=list, description="Full article detail")

    # Metadata
    analyzed_at: datetime
    source: str = "simulated"

    # ── Derived helpers ──────────────────────────────────────────────────────

    @property
    def is_positive(self) -> bool:
        return self.sentiment_score > 0.15

    @property
    def is_negative(self) -> bool:
        return self.sentiment_score < -0.15

    @property
    def is_high_impact(self) -> bool:
        return self.impact_score >= 60

    @property
    def confidence_modifier(self) -> float:
        """
        Returns a multiplier the decision engine applies to technical confidence.

        Positive news boosts confidence; negative news reduces it.
        High impact amplifies the effect.

        Range: roughly 0.5 → 1.5
        """
        base = 1.0 + (self.sentiment_score * 0.4)          # ±0.4
        amplifier = 1.0 + (self.impact_score / 100) * 0.2  # up to ×1.2
        return round(base * amplifier, 4)
