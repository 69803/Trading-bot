"""
DecisionLog — Phase 7.

Persists every bot decision (with full technical + sentiment context)
for audit, debugging, and future backtesting.
"""
from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DecisionLog(Base):
    __tablename__ = "decision_logs"

    # ── Foreign keys ─────────────────────────────────────────────────────────
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # ── Symbol & timing ──────────────────────────────────────────────────────
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # ── Final decision ───────────────────────────────────────────────────────
    direction: Mapped[str] = mapped_column(
        String(10), nullable=False
    )  # BUY | SELL | HOLD | BLOCKED
    final_confidence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    override_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed: Mapped[bool] = mapped_column(
        nullable=False, default=False
    )  # True when an order was actually placed
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Technical signal snapshot ────────────────────────────────────────────
    tech_direction: Mapped[str] = mapped_column(String(10), nullable=False)
    tech_confidence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tech_rsi: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_ema_fast: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_ema_slow: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_macd: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_macd_hist: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_atr: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_volume_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    tech_ema_crossover: Mapped[str | None] = mapped_column(String(10), nullable=True)
    tech_macd_crossover: Mapped[str | None] = mapped_column(String(10), nullable=True)
    tech_rsi_extreme: Mapped[str | None] = mapped_column(String(12), nullable=True)
    candles_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # ── Sentiment snapshot ───────────────────────────────────────────────────
    sentiment_label: Mapped[str] = mapped_column(String(10), nullable=False)
    sentiment_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    sentiment_impact: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sentiment_source: Mapped[str] = mapped_column(String(20), nullable=False)
    sentiment_news_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sentiment_modifier: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)

    # ── Risk assessment snapshot ─────────────────────────────────────────────
    risk_approved: Mapped[bool | None] = mapped_column(nullable=True)
    risk_position_size: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk_stop_loss: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk_take_profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk_rr_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    risk_sizing_method: Mapped[str | None] = mapped_column(String(15), nullable=True)
    risk_rejection: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Full reasons (joined text) ───────────────────────────────────────────
    reasons: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── Extended observability fields ────────────────────────────────────────
    # Raw composite score from the technical engine (before direction assignment)
    tech_composite_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # ADX value at decision time (trend strength)
    tech_adx: Mapped[float | None] = mapped_column(Float, nullable=True)
    # JSON array of per-indicator score factors (ScoreFactor objects)
    tech_score_breakdown: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Why the signal was HOLD (sideways gate, dead zone, etc.)
    tech_hold_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # One-liner summary of the full decision chain
    decision_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<DecisionLog symbol={self.symbol!r} direction={self.direction!r} "
            f"confidence={self.final_confidence} executed={self.executed}>"
        )
