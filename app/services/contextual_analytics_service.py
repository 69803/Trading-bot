"""
Contextual Analytics Service.

Provides performance breakdowns beyond the global stats already in
analytics_service.py:

  get_performance_by_symbol(db, portfolio_id)
      → metrics per trading symbol (win rate, PnL, profit factor, drawdown)

  get_performance_by_open_hour(db, portfolio_id)
      → metrics grouped by UTC hour of position open (0–23)

  get_performance_by_event_context(db, portfolio_id, window_minutes)
      → metrics grouped by event-risk context at trade open time:

        "reduced_size_due_to_event"    – position.event_context field set by bot
        "trade_near_high_impact_event" – retroactive: high-impact event in DB
                                         within ±window_minutes of opened_at
        "trade_near_medium_impact_event" – same for medium-impact
        "trade_without_near_event"     – no event detected in either source

All functions operate on closed Position rows (is_open=False, realized_pnl set).
No side effects — pure query functions.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.historical_event import HistoricalEvent
from app.models.position import Position
from app.services.event_risk_service import symbol_to_currencies
from app.services.historical_economic_events_service import HIGH_IMPACT_KEYWORDS

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

class SymbolStats(BaseModel):
    symbol: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float          # 0.0–1.0
    avg_win: float
    avg_loss: float          # absolute value (positive)
    total_pnl: float
    profit_factor: float
    max_drawdown_pct: float
    open_positions: int


class HourStats(BaseModel):
    hour_utc: int            # 0–23
    total_trades: int
    winning_trades: int
    win_rate: float
    total_pnl: float
    avg_pnl: float


class EventContextStats(BaseModel):
    context: str             # one of the four categories below
    total_trades: int
    winning_trades: int
    win_rate: float
    total_pnl: float
    avg_pnl: float


# Canonical event context labels
CTX_REDUCED   = "reduced_size_due_to_event"
CTX_NEAR_HIGH = "trade_near_high_impact_event"
CTX_NEAR_MED  = "trade_near_medium_impact_event"
CTX_NO_EVENT  = "trade_without_near_event"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_performance_by_symbol(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> List[SymbolStats]:
    """
    Return per-symbol performance metrics for all closed positions.
    Symbols with no closed trades are omitted.
    Open-position counts are included for active symbols.
    """
    closed = await _fetch_closed(db, portfolio_id)
    if not closed:
        return []

    open_by_symbol = await _count_open_by_symbol(db, portfolio_id)

    # Group PnL values by symbol
    pnl_by_symbol: Dict[str, List[float]] = defaultdict(list)
    for pos in closed:
        pnl_by_symbol[pos.symbol].append(float(pos.realized_pnl))

    results: List[SymbolStats] = []
    for symbol, pnls in sorted(pnl_by_symbol.items()):
        wins   = [v for v in pnls if v > 0]
        losses = [v for v in pnls if v <= 0]

        gross_profit = sum(wins)
        gross_loss   = abs(sum(losses))

        results.append(SymbolStats(
            symbol          = symbol,
            total_trades    = len(pnls),
            winning_trades  = len(wins),
            losing_trades   = len(losses),
            win_rate        = round(len(wins) / len(pnls), 4),
            avg_win         = round(gross_profit / len(wins), 4) if wins else 0.0,
            avg_loss        = round(gross_loss / len(losses), 4) if losses else 0.0,
            total_pnl       = round(sum(pnls), 4),
            profit_factor   = round(gross_profit / gross_loss, 3) if gross_loss > 0 else 0.0,
            max_drawdown_pct= round(_max_drawdown(pnls), 4),
            open_positions  = open_by_symbol.get(symbol, 0),
        ))

    return results


async def get_performance_by_open_hour(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> List[HourStats]:
    """
    Return performance metrics grouped by UTC hour of position open (0–23).
    Hours with no trades are omitted.
    """
    closed = await _fetch_closed(db, portfolio_id)
    if not closed:
        return []

    pnl_by_hour: Dict[int, List[float]] = defaultdict(list)
    for pos in closed:
        if pos.opened_at is None:
            continue
        hour = _aware(pos.opened_at).hour
        pnl_by_hour[hour].append(float(pos.realized_pnl))

    results: List[HourStats] = []
    for hour in sorted(pnl_by_hour.keys()):
        pnls = pnl_by_hour[hour]
        wins = [v for v in pnls if v > 0]
        results.append(HourStats(
            hour_utc       = hour,
            total_trades   = len(pnls),
            winning_trades = len(wins),
            win_rate       = round(len(wins) / len(pnls), 4),
            total_pnl      = round(sum(pnls), 4),
            avg_pnl        = round(sum(pnls) / len(pnls), 4),
        ))

    return results


async def get_performance_by_event_context(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    window_minutes: int = 60,
) -> List[EventContextStats]:
    """
    Return performance metrics grouped by event-risk context.

    Classification priority (first match wins):
      1. position.event_context == "reduced_size_due_to_event"
         → CTX_REDUCED
      2. High-impact event in historical_events within ±window_minutes of opened_at
         for the position's currencies
         → CTX_NEAR_HIGH
      3. Medium-impact event in the same window
         → CTX_NEAR_MED
      4. No event found
         → CTX_NO_EVENT

    Uses a single bulk DB query for historical events — O(1) DB queries
    regardless of number of positions.
    """
    closed = await _fetch_closed(db, portfolio_id)
    if not closed:
        return _empty_context_stats()

    # Bulk-fetch all historical events in the date range of the positions
    events_lookup = await _build_events_lookup(db, closed, window_minutes)

    pnl_by_ctx: Dict[str, List[float]] = defaultdict(list)

    for pos in closed:
        ctx = _classify_position(pos, events_lookup, window_minutes)
        pnl_by_ctx[ctx].append(float(pos.realized_pnl))

    results: List[EventContextStats] = []
    for ctx in [CTX_REDUCED, CTX_NEAR_HIGH, CTX_NEAR_MED, CTX_NO_EVENT]:
        pnls = pnl_by_ctx.get(ctx, [])
        if not pnls:
            results.append(EventContextStats(
                context        = ctx,
                total_trades   = 0,
                winning_trades = 0,
                win_rate       = 0.0,
                total_pnl      = 0.0,
                avg_pnl        = 0.0,
            ))
            continue
        wins = [v for v in pnls if v > 0]
        results.append(EventContextStats(
            context        = ctx,
            total_trades   = len(pnls),
            winning_trades = len(wins),
            win_rate       = round(len(wins) / len(pnls), 4),
            total_pnl      = round(sum(pnls), 4),
            avg_pnl        = round(sum(pnls) / len(pnls), 4),
        ))

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _fetch_closed(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> List[Position]:
    result = await db.execute(
        select(Position)
        .where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == False,          # noqa: E712
        )
        .order_by(Position.opened_at.asc())
    )
    return list(result.scalars().all())


async def _count_open_by_symbol(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> Dict[str, int]:
    result = await db.execute(
        select(Position.symbol, Position.id)
        .where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == True,           # noqa: E712
        )
    )
    counts: Dict[str, int] = defaultdict(int)
    for symbol, _ in result.all():
        counts[symbol] += 1
    return dict(counts)


async def _build_events_lookup(
    db: AsyncSession,
    positions: List[Position],
    window_minutes: int,
) -> List[HistoricalEvent]:
    """
    Fetch all HistoricalEvent rows that fall within the combined date span of
    all positions (±window_minutes).  Returns the raw list; callers filter
    per-position in Python.
    """
    open_times = [
        _aware(p.opened_at) for p in positions if p.opened_at is not None
    ]
    if not open_times:
        return []

    span_start = min(open_times) - timedelta(minutes=window_minutes)
    span_end   = max(open_times) + timedelta(minutes=window_minutes)

    result = await db.execute(
        select(HistoricalEvent).where(
            and_(
                HistoricalEvent.event_datetime_utc >= span_start,
                HistoricalEvent.event_datetime_utc <= span_end,
            )
        )
    )
    return list(result.scalars().all())


def _classify_position(
    pos: Position,
    all_events: List[HistoricalEvent],
    window_minutes: int,
) -> str:
    """Classify one closed position into an event-context bucket."""
    # Priority 1: stored context flag from bot
    if pos.event_context == CTX_REDUCED:
        return CTX_REDUCED

    if pos.opened_at is None:
        return CTX_NO_EVENT

    opened_utc = _aware(pos.opened_at)
    window     = timedelta(minutes=window_minutes)
    currencies = symbol_to_currencies(pos.symbol)

    # Filter events within the position's time window and matching currencies
    nearby = [
        ev for ev in all_events
        if ev.currency in currencies
        and abs((_aware_ev(ev.event_datetime_utc) - opened_utc).total_seconds()) <= window.total_seconds()
    ]

    if not nearby:
        return CTX_NO_EVENT

    # Priority 2: any high-impact event nearby
    if any(_is_high(ev) for ev in nearby):
        return CTX_NEAR_HIGH

    # Priority 3: any medium-impact event nearby
    if any(_is_medium(ev) for ev in nearby):
        return CTX_NEAR_MED

    return CTX_NO_EVENT


def _is_high(ev: HistoricalEvent) -> bool:
    if ev.impact and ev.impact.lower() == "high":
        return True
    return any(kw in ev.event_name.upper() for kw in HIGH_IMPACT_KEYWORDS)


def _is_medium(ev: HistoricalEvent) -> bool:
    return bool(ev.impact and ev.impact.lower() == "medium")


def _max_drawdown(pnl_series: List[float]) -> float:
    if not pnl_series:
        return 0.0
    cumulative = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnl_series:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        if peak > 0:
            dd = (peak - cumulative) / peak
            if dd > max_dd:
                max_dd = dd
    return max_dd


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _aware_ev(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _empty_context_stats() -> List[EventContextStats]:
    return [
        EventContextStats(
            context=ctx, total_trades=0, winning_trades=0,
            win_rate=0.0, total_pnl=0.0, avg_pnl=0.0,
        )
        for ctx in [CTX_REDUCED, CTX_NEAR_HIGH, CTX_NEAR_MED, CTX_NO_EVENT]
    ]
