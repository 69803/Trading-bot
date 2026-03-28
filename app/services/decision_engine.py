"""
Decision Engine — Phase 4 (v2 — tighter news filtering).

Combines a TechnicalSignal (Phase 2) and a SentimentResult (Phase 3)
into a single FinalDecision with blended confidence and full audit trail.

Blending model
──────────────

Step 1 — Base confidence
    Start with technical_signal.confidence (0–100).

Step 2 — Sentiment confidence modifier
    Apply sentiment_result.confidence_modifier (≈ 0.5 → 1.5).
    adjusted = technical_confidence × confidence_modifier   (clamped 0–100)

Step 3 — Alignment check
    ALIGNED:  technical and sentiment point the same direction.
              → small additional boost (+5 pts).
    NEUTRAL:  sentiment is neutral — no override, keep adjusted score.
    OPPOSED:  technical and sentiment disagree.

Step 4 — Opposition override (only when OPPOSED)
    |sentiment_score| < 0.25  — mild: reduce confidence by 20 %, keep direction.
    |sentiment_score| 0.25–0.40 — moderate: downgrade direction to HOLD.
    |sentiment_score| > 0.40 + is_high_impact — strong + impactful:
                               direction → BLOCKED (trade skipped entirely).

    Real news examples:
      "Strong negative sentiment → block BUY"  — score < -0.40 + high-impact
      "Strong positive sentiment → block SELL" — score > +0.40 + high-impact

Step 5 — Minimum confidence gate
    After all adjustments, if direction is BUY/SELL but confidence < 30,
    downgrade to HOLD (signal too weak to act on).

Thresholds summary
──────────────────
    OPPOSE_MILD_MAX       = 0.25   (mild — 20% confidence cut)
    OPPOSE_MODERATE_MAX   = 0.40   (moderate → HOLD)
    OPPOSE_STRONG_MIN     = 0.40   (strong + high-impact → BLOCKED)
    MIN_ACTION_CONFIDENCE = 0      (disabled — threshold handled in technical_engine)
    ALIGNMENT_BONUS       = 5      (pts added when signals agree)
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from app.core.logger import get_logger
from app.schemas.decision import FinalDecision
from app.schemas.sentiment import SentimentResult
from app.schemas.technical import TechnicalSignal
from app.services.technical_engine import BUY_THRESHOLD

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Tuneable thresholds
# ---------------------------------------------------------------------------
OPPOSE_MILD_MAX       = 0.25   # below → mild opposition (confidence penalty only)
OPPOSE_MODERATE_MAX   = 0.40   # 0.25–0.40 → moderate opposition (downgrade to HOLD)
OPPOSE_STRONG_MIN     = 0.40   # above + high-impact → BLOCKED
MIN_ACTION_CONFIDENCE = 0      # disabled — EMA direction always passes
ALIGNMENT_BONUS       = 5      # pts added when technical & sentiment agree


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def decide(
    technical: TechnicalSignal,
    sentiment: SentimentResult,
) -> FinalDecision:
    """
    Merge *technical* and *sentiment* into a FinalDecision.

    Args:
        technical: Output of technical_engine.analyze().
        sentiment: Output of sentiment_engine.analyze().

    Returns:
        FinalDecision with direction, blended confidence, and reasons.
    """
    now = datetime.now(timezone.utc)
    reasons: List[str] = list(technical.reasons)
    override_reason: Optional[str] = None

    # ── Step 1: base from technical ─────────────────────────────────────────
    direction   = technical.direction
    confidence  = technical.confidence   # 0–100

    # ── Step 2: apply sentiment confidence modifier ──────────────────────────
    modifier   = sentiment.confidence_modifier          # ≈ 0.5–1.5
    adjusted   = int(round(confidence * modifier))
    adjusted   = max(0, min(100, adjusted))

    _add_sentiment_summary(reasons, sentiment)

    # ── Step 3 & 4: handle HOLD from technical separately ───────────────────
    if direction == "HOLD":
        # Sentiment can inform context but cannot promote a HOLD to actionable
        log.debug(
            "Decision: HOLD (technical)",
            symbol=technical.symbol,
            adjusted_confidence=adjusted,
        )
        return _build(
            technical, sentiment,
            direction="HOLD",
            confidence=0,
            reasons=reasons,
            override_reason=None,
            now=now,
        )

    # ── Step 3: alignment check ──────────────────────────────────────────────
    tech_is_bullish = direction == "BUY"
    tech_is_bearish = direction == "SELL"

    sentiment_aligns  = (
        (tech_is_bullish and sentiment.is_positive) or
        (tech_is_bearish and sentiment.is_negative)
    )
    sentiment_neutral = sentiment.label == "neutral"
    sentiment_opposes = (
        (tech_is_bullish and sentiment.is_negative) or
        (tech_is_bearish and sentiment.is_positive)
    )

    if sentiment_aligns:
        adjusted = min(100, adjusted + ALIGNMENT_BONUS)
        reasons.append(
            f"Sentiment CONFIRMS {direction} signal "
            f"({sentiment.label}, score={sentiment.sentiment_score:+.2f}) "
            f"— confidence boosted to {adjusted}"
        )

    elif sentiment_neutral:
        reasons.append(
            f"Sentiment neutral (score={sentiment.sentiment_score:+.2f}) "
            f"— no directional override"
        )

    # ── Step 4: opposition override ──────────────────────────────────────────
    elif sentiment_opposes:
        opp_magnitude = abs(sentiment.sentiment_score)

        if opp_magnitude >= OPPOSE_STRONG_MIN and sentiment.is_high_impact:
            # BLOCKED: strong opposing news with high market impact
            direction = "BLOCKED"
            override_reason = (
                f"BLOCKED — high-impact {sentiment.label} news strongly opposes "
                f"{technical.direction} signal "
                f"(sentiment={sentiment.sentiment_score:+.2f}, "
                f"impact={sentiment.impact_score}/100, "
                f"articles={sentiment.news_count})"
            )
            reasons.append(override_reason)
            adjusted = 0   # no confidence for blocked trades

        elif opp_magnitude >= OPPOSE_MILD_MAX:
            # HOLD: moderate opposition — too risky to act
            direction = "HOLD"
            override_reason = (
                f"Downgraded {technical.direction} → HOLD — "
                f"{sentiment.label} sentiment opposes technical signal "
                f"(score={sentiment.sentiment_score:+.2f})"
            )
            reasons.append(override_reason)
            adjusted = 0

        else:
            # Mild opposition — keep direction, reduce confidence
            penalty  = int(adjusted * 0.20)
            adjusted = max(0, adjusted - penalty)
            msg = (
                f"Confidence reduced by 20% ({penalty} pts) — mild opposing "
                f"sentiment ({sentiment.label}, score={sentiment.sentiment_score:+.2f})"
            )
            reasons.append(msg)

    # ── Step 5: minimum confidence gate ─────────────────────────────────────
    if direction in ("BUY", "SELL") and adjusted < MIN_ACTION_CONFIDENCE:
        override_reason = (
            f"Downgraded {direction} → HOLD — blended confidence {adjusted} "
            f"below minimum action threshold ({MIN_ACTION_CONFIDENCE})"
        )
        reasons.append(override_reason)
        direction = "HOLD"
        adjusted  = 0

    decision_summary = _build_decision_summary(
        technical=technical,
        direction=direction,
        confidence=adjusted,
        sentiment=sentiment,
        override_reason=override_reason,
    )

    log.info(
        "FINAL DECISION",
        symbol=technical.symbol,
        summary=decision_summary,
    )

    return _build(
        technical, sentiment,
        direction=direction,
        confidence=adjusted,
        reasons=reasons,
        override_reason=override_reason,
        decision_summary=decision_summary,
        now=now,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_decision_summary(
    technical: TechnicalSignal,
    direction: str,
    confidence: int,
    sentiment: SentimentResult,
    override_reason: Optional[str],
) -> str:
    """
    Build a compact one-liner that explains the full decision chain.

    Examples:
      "BUY [score=+35, conf=35]: EMA_CROSSOVER(+40) | RSI_LEVEL(+10) | ADX_STRENGTH(+8) | sentiment=neutral"
      "HOLD [score=+18]: score below threshold ±25 | ADX=17.2"
      "BLOCKED: high-impact negative news opposes BUY (sentiment=-0.52)"
    """
    score = technical.composite_score

    if direction == "BLOCKED":
        return f"BLOCKED: {override_reason or 'high-impact news override'}"

    if direction == "HOLD":
        hold_detail = technical.hold_reason or f"score {score:+d} below threshold ±{BUY_THRESHOLD}"
        return f"HOLD [score={score:+d}]: {hold_detail}"

    # BUY or SELL — list top 3 factors by absolute contribution
    top_factors = sorted(
        [f for f in technical.score_breakdown if f.points != 0],
        key=lambda f: abs(f.points),
        reverse=True,
    )[:4]
    factor_str = "  |  ".join(f"{f.name}({f.points:+d})" for f in top_factors)

    sent_str = f"sentiment={sentiment.label}({sentiment.sentiment_score:+.2f})"
    if override_reason:
        sent_str += f"  →  {override_reason}"

    return (
        f"{direction} [score={score:+d}, conf={confidence}]: "
        f"{factor_str}  ||  {sent_str}"
    )


def _add_sentiment_summary(reasons: List[str], sentiment: SentimentResult) -> None:
    """Append a compact sentiment summary line to the reasons list."""
    src   = sentiment.source
    count = sentiment.news_count
    score = sentiment.sentiment_score
    imp   = sentiment.impact_score
    label = sentiment.label.upper()
    mod   = sentiment.confidence_modifier

    reasons.append(
        f"Sentiment [{src}]: {label} "
        f"score={score:+.2f}  impact={imp}/100  "
        f"articles={count}  modifier=×{mod:.2f}"
    )


def _build(
    technical: TechnicalSignal,
    sentiment: SentimentResult,
    *,
    direction: str,
    confidence: int,
    reasons: List[str],
    override_reason: Optional[str],
    decision_summary: str = "",
    now: datetime,
) -> FinalDecision:
    """Construct the FinalDecision dataclass."""
    return FinalDecision(
        symbol               = technical.symbol,
        direction            = direction,           # type: ignore[arg-type]
        confidence           = confidence,
        reasons              = reasons,
        technical_direction  = technical.direction,
        technical_confidence = technical.confidence,
        sentiment_label      = sentiment.label,
        sentiment_score      = sentiment.sentiment_score,
        sentiment_impact     = sentiment.impact_score,
        news_count           = sentiment.news_count,
        override_reason      = override_reason,
        decided_at           = now,
        decision_summary     = decision_summary,
        tech_composite_score = technical.composite_score,
    )
