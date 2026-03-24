"""
Historical Performance Guardrail Service.

Uses recent trade history (from contextual_analytics_service) to block or
reduce trades when evidence from closed positions shows consistently bad
outcomes in a given symbol, UTC hour, or event context.

Three independent rules — evaluated in priority order (highest severity wins):

  Rule 1 — SYMBOL BLOCK
    Fires when a symbol has enough closed trades AND both:
      • win_rate < GUARDRAIL_SYMBOL_MIN_WIN_RATE
      • total_pnl < GUARDRAIL_SYMBOL_MAX_NEGATIVE_PNL
    Using both guards against blocking a valid low-win-rate / high-reward
    strategy that is still profitable overall.
    Action: BLOCK

  Rule 2 — HOUR REDUCE
    Fires when the current UTC open hour has enough closed trades AND:
      • win_rate < GUARDRAIL_HOUR_MIN_WIN_RATE
    Action: REDUCE (not a full block — hourly patterns are noisier)

  Rule 3 — EVENT CONTEXT ESCALATION
    Fires only when this trade is already in a "reduced_size_due_to_event"
    context (i.e. a medium-impact event was detected) AND the historical
    performance of that context has enough trades AND:
      • win_rate < GUARDRAIL_EVENT_CTX_MIN_WIN_RATE
    Escalates: the REDUCE from the event risk layer becomes a BLOCK.
    Action: BLOCK

All rules require a minimum sample size before they can fire (configured
via GUARDRAIL_MIN_TRADES_* settings).  Below the minimum the rule is
skipped — not enough evidence to act.

Priority: BLOCK > REDUCE > ALLOW
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.services.contextual_analytics_service import (
    CTX_REDUCED,
    EventContextStats,
    HourStats,
    SymbolStats,
    get_performance_by_event_context,
    get_performance_by_open_hour,
    get_performance_by_symbol,
)

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Output type
# ---------------------------------------------------------------------------

@dataclass
class GuardrailResult:
    """
    Decision returned by :func:`check_historical_guardrail`.

    Attributes
    ----------
    action:
        ``"BLOCK"``  — do not place the trade.
        ``"REDUCE"`` — allow the trade but halve position size.
        ``"ALLOW"``  — no restriction from this layer.
    rule:
        Which rule triggered, or ``"none"`` when ALLOW.
    reason:
        Human-readable explanation, written to bot logs.
    """
    action: str   # "BLOCK" | "REDUCE" | "ALLOW"
    rule:   str   # "symbol_performance" | "hour_performance" | "event_context" | "none"
    reason: str


_ALLOW = GuardrailResult(action="ALLOW", rule="none", reason="historical performance within thresholds")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def check_historical_guardrail(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    symbol: str,
    current_hour_utc: int,
    is_event_reduced_trade: bool = False,
) -> GuardrailResult:
    """
    Evaluate all historical performance rules for the pending trade.

    Parameters
    ----------
    db:
        Open async DB session.
    portfolio_id:
        Portfolio whose closed-position history is used.
    symbol:
        Trading symbol about to be traded (e.g. ``"EURUSD"``).
    current_hour_utc:
        UTC hour (0–23) at which the trade would open.
    is_event_reduced_trade:
        True when the event-risk layer already set ``cal_reduce_size = True``
        (i.e. a medium-impact event was detected).  Used by Rule 3 to decide
        whether to escalate REDUCE → BLOCK.

    Returns
    -------
    GuardrailResult
        Highest-severity result across all rules.
        BLOCK > REDUCE > ALLOW.
    """
    if not settings.HISTORICAL_GUARDRAIL_ENABLED:
        return _ALLOW

    # Fetch all context data in parallel (three independent queries)
    symbol_stats: List[SymbolStats]       = await get_performance_by_symbol(db, portfolio_id)
    hour_stats:   List[HourStats]         = await get_performance_by_open_hour(db, portfolio_id)
    ctx_stats:    List[EventContextStats] = await get_performance_by_event_context(db, portfolio_id)

    results: List[GuardrailResult] = []

    # ── Rule 1: Symbol performance ────────────────────────────────────────────
    sym = next((s for s in symbol_stats if s.symbol == symbol), None)
    if sym is not None:
        r = _check_symbol_rule(sym, symbol)
        if r is not None:
            results.append(r)

    # ── Rule 2: Hour performance ──────────────────────────────────────────────
    hour = next((h for h in hour_stats if h.hour_utc == current_hour_utc), None)
    if hour is not None:
        r = _check_hour_rule(hour, current_hour_utc)
        if r is not None:
            results.append(r)

    # ── Rule 3: Event context escalation ─────────────────────────────────────
    if is_event_reduced_trade:
        ctx = next((c for c in ctx_stats if c.context == CTX_REDUCED), None)
        if ctx is not None:
            r = _check_event_ctx_rule(ctx)
            if r is not None:
                results.append(r)

    if not results:
        log.debug(
            "Historical guardrail: ALLOW",
            symbol=symbol,
            hour_utc=current_hour_utc,
            is_event_reduced=is_event_reduced_trade,
        )
        return _ALLOW

    # Return highest-severity result (BLOCK > REDUCE > ALLOW)
    _severity = {"BLOCK": 2, "REDUCE": 1, "ALLOW": 0}
    best = max(results, key=lambda r: _severity[r.action])
    return best


# ---------------------------------------------------------------------------
# Rule evaluators
# ---------------------------------------------------------------------------

def _check_symbol_rule(sym: SymbolStats, symbol: str) -> GuardrailResult | None:
    """
    Rule 1: Block symbol when BOTH win_rate is poor AND total_pnl is net-negative.
    Requiring both prevents blocking a low-win-rate / high-reward strategy.
    """
    if sym.total_trades < settings.GUARDRAIL_MIN_TRADES_SYMBOL:
        log.debug(
            "Symbol rule: skipped — insufficient sample",
            symbol=symbol,
            trades=sym.total_trades,
            required=settings.GUARDRAIL_MIN_TRADES_SYMBOL,
        )
        return None

    win_rate_bad = sym.win_rate < settings.GUARDRAIL_SYMBOL_MIN_WIN_RATE
    pnl_bad      = sym.total_pnl < settings.GUARDRAIL_SYMBOL_MAX_NEGATIVE_PNL

    if win_rate_bad and pnl_bad:
        reason = (
            f"symbol {symbol} has poor historical performance: "
            f"win_rate={sym.win_rate:.1%} (threshold {settings.GUARDRAIL_SYMBOL_MIN_WIN_RATE:.0%}), "
            f"total_pnl={sym.total_pnl:.2f} (threshold {settings.GUARDRAIL_SYMBOL_MAX_NEGATIVE_PNL:.2f}) "
            f"over {sym.total_trades} trades"
        )
        log.warning(
            "Symbol rule TRIGGERED",
            symbol=symbol,
            win_rate=f"{sym.win_rate:.1%}",
            total_pnl=sym.total_pnl,
            trades=sym.total_trades,
        )
        return GuardrailResult(action="BLOCK", rule="symbol_performance", reason=reason)

    log.debug(
        "Symbol rule: ALLOW",
        symbol=symbol,
        win_rate=f"{sym.win_rate:.1%}",
        total_pnl=sym.total_pnl,
        trades=sym.total_trades,
    )
    return None


def _check_hour_rule(hour: HourStats, hour_utc: int) -> GuardrailResult | None:
    """
    Rule 2: Reduce size when the current UTC hour has historically poor win rate.
    REDUCE not BLOCK — hourly performance is noisy and recovers over time.
    """
    if hour.total_trades < settings.GUARDRAIL_MIN_TRADES_HOUR:
        log.debug(
            "Hour rule: skipped — insufficient sample",
            hour_utc=hour_utc,
            trades=hour.total_trades,
            required=settings.GUARDRAIL_MIN_TRADES_HOUR,
        )
        return None

    if hour.win_rate < settings.GUARDRAIL_HOUR_MIN_WIN_RATE:
        reason = (
            f"UTC hour {hour_utc:02d}:xx has poor historical performance: "
            f"win_rate={hour.win_rate:.1%} (threshold {settings.GUARDRAIL_HOUR_MIN_WIN_RATE:.0%}) "
            f"over {hour.total_trades} trades"
        )
        log.warning(
            "Hour rule TRIGGERED",
            hour_utc=hour_utc,
            win_rate=f"{hour.win_rate:.1%}",
            total_pnl=hour.total_pnl,
            trades=hour.total_trades,
        )
        return GuardrailResult(action="REDUCE", rule="hour_performance", reason=reason)

    log.debug(
        "Hour rule: ALLOW",
        hour_utc=hour_utc,
        win_rate=f"{hour.win_rate:.1%}",
        trades=hour.total_trades,
    )
    return None


def _check_event_ctx_rule(ctx: EventContextStats) -> GuardrailResult | None:
    """
    Rule 3: Escalate REDUCE → BLOCK when the "reduced_size_due_to_event"
    context has historically bad win rate.
    Only evaluated when the current trade is already event-reduced.
    """
    if ctx.total_trades < settings.GUARDRAIL_MIN_TRADES_EVENT_CTX:
        log.debug(
            "Event context rule: skipped — insufficient sample",
            context=CTX_REDUCED,
            trades=ctx.total_trades,
            required=settings.GUARDRAIL_MIN_TRADES_EVENT_CTX,
        )
        return None

    if ctx.win_rate < settings.GUARDRAIL_EVENT_CTX_MIN_WIN_RATE:
        reason = (
            f"event-reduced trades have poor historical performance: "
            f"win_rate={ctx.win_rate:.1%} (threshold {settings.GUARDRAIL_EVENT_CTX_MIN_WIN_RATE:.0%}) "
            f"over {ctx.total_trades} trades — escalating REDUCE to BLOCK"
        )
        log.warning(
            "Event context rule TRIGGERED — escalating REDUCE to BLOCK",
            context=CTX_REDUCED,
            win_rate=f"{ctx.win_rate:.1%}",
            total_pnl=ctx.total_pnl,
            trades=ctx.total_trades,
        )
        return GuardrailResult(action="BLOCK", rule="event_context", reason=reason)

    log.debug(
        "Event context rule: ALLOW",
        context=CTX_REDUCED,
        win_rate=f"{ctx.win_rate:.1%}",
        trades=ctx.total_trades,
    )
    return None
