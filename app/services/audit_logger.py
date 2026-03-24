"""
Audit Logger — Phase 7.

Persists a DecisionLog row after every bot cycle evaluation, whether or
not a trade was executed.  Call `log_decision()` from bot_service after
the risk assessment step.
"""
from __future__ import annotations

import json
import math
import uuid
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.decision_log import DecisionLog
from app.schemas.decision import FinalDecision
from app.schemas.risk_assessment import RiskAssessment
from app.schemas.sentiment import SentimentResult
from app.schemas.technical import TechnicalSignal

log = get_logger(__name__)


async def log_decision(
    db: AsyncSession,
    *,
    user_id: uuid.UUID,
    portfolio_id: uuid.UUID,
    technical: TechnicalSignal,
    sentiment: SentimentResult,
    decision: FinalDecision,
    assessment: Optional[RiskAssessment] = None,
    executed: bool = False,
    execution_rejection: Optional[str] = None,
) -> None:
    """
    Persist a DecisionLog row.

    Args:
        db:                   Active async session (caller owns commit).
        user_id:              User who owns the bot.
        portfolio_id:         Portfolio the bot trades on.
        technical:            TechnicalSignal from technical_engine.
        sentiment:            SentimentResult from sentiment_engine.
        decision:             FinalDecision from decision_engine.
        assessment:           RiskAssessment from risk_manager (None if skipped).
        executed:             True if an order was placed.
        execution_rejection:  Reason order was not placed (if executed=False).
    """
    ind = technical.indicators

    entry = DecisionLog(
        user_id          = user_id,
        portfolio_id     = portfolio_id,
        symbol           = technical.symbol,
        decided_at       = decision.decided_at,

        # Final decision
        direction        = decision.direction,
        final_confidence = decision.confidence,
        override_reason  = decision.override_reason,
        executed         = executed,
        rejection_reason = execution_rejection,

        # Technical snapshot
        tech_direction    = technical.direction,
        tech_confidence   = technical.confidence,
        tech_rsi          = _safe(ind.rsi),
        tech_ema_fast     = _safe(ind.ema_fast),
        tech_ema_slow     = _safe(ind.ema_slow),
        tech_macd         = _safe(ind.macd),
        tech_macd_hist    = _safe(ind.macd_histogram),
        tech_atr          = _safe(ind.atr),
        tech_volume_ratio = _safe(ind.volume_ratio),
        tech_price        = _safe(ind.price),
        tech_ema_crossover  = technical.ema_crossover,
        tech_macd_crossover = technical.macd_crossover,
        tech_rsi_extreme    = technical.rsi_extreme,
        candles_used      = technical.candles_used,

        # Sentiment snapshot
        sentiment_label      = sentiment.label,
        sentiment_score      = sentiment.sentiment_score,
        sentiment_impact     = sentiment.impact_score,
        sentiment_source     = sentiment.source,
        sentiment_news_count = sentiment.news_count,
        sentiment_modifier   = sentiment.confidence_modifier,

        # Risk snapshot (optional)
        risk_approved      = assessment.approved          if assessment else None,
        risk_position_size = assessment.position_size_dollars if assessment else None,
        risk_stop_loss     = assessment.stop_loss_price   if assessment else None,
        risk_take_profit   = assessment.take_profit_price if assessment else None,
        risk_rr_ratio      = assessment.risk_reward_ratio if assessment else None,
        risk_sizing_method = assessment.sizing_method     if assessment else None,
        risk_rejection     = assessment.rejection_reason  if assessment else None,

        # Full reasons joined
        reasons = " | ".join(decision.reasons),

        # Extended observability
        tech_composite_score = technical.composite_score,
        tech_adx             = _safe(technical.indicators.adx),
        tech_score_breakdown = (
            json.dumps([f.model_dump() for f in technical.score_breakdown])
            if technical.score_breakdown else None
        ),
        tech_hold_reason  = technical.hold_reason,
        decision_summary  = decision.decision_summary or None,
    )

    db.add(entry)
    await db.flush()

    log.debug(
        "Decision logged",
        symbol    = technical.symbol,
        direction = decision.direction,
        executed  = executed,
        confidence= decision.confidence,
    )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _safe(value: float) -> Optional[float]:
    """Return None for NaN/inf so Postgres numeric columns don't choke."""
    import math
    if value is None:
        return None
    try:
        if math.isnan(value) or math.isinf(value):
            return None
    except (TypeError, ValueError):
        return None
    return value
