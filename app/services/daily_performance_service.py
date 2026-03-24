"""
Daily Performance Service (PASO 8 — Paper Trading Evaluation Mode).

Provides two layers of access to trading performance data:

  Layer 1 — Trade log:
    ``get_trade_log`` returns every closed position as a structured
    ``TradeLogEntry``, surfacing fields useful for paper-trading analysis:
    symbol, direction, pnl, win/loss, open_hour_utc, event_context,
    was_reduced_size.

  Layer 2 — Daily aggregates:
    ``compute_daily_performance`` groups the trade log by UTC calendar day
    and computes: total trades, win rate, total/avg PnL, best/worst symbol,
    best/worst hour.

    ``save_daily_performance_snapshot`` persists a computed summary to the
    ``daily_performance_summaries`` table (upsert on portfolio + date).

    ``get_daily_performance_snapshots`` retrieves stored snapshots (fast
    read path — does not re-scan positions).

No trading logic is modified by this module — observation only.
"""
from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional

from pydantic import BaseModel
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.daily_performance_summary import DailyPerformanceSummary
from app.models.position import Position

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Output schemas (Pydantic — API / service boundary)
# ---------------------------------------------------------------------------

class TradeLogEntry(BaseModel):
    """One closed position formatted for paper-trading analysis."""

    position_id:    uuid.UUID
    symbol:         str
    direction:      str           # "BUY" (long) | "SELL" (short)
    pnl:            float
    is_win:         bool
    open_hour_utc:  int           # UTC hour the position was opened (0–23)
    event_context:  Optional[str] # raw value from Position.event_context
    was_reduced_size: bool        # True when position size was halved due to event
    is_paper:       Optional[bool] # True=paper, False=live, None=legacy (treated as paper)
    opened_at:      datetime
    closed_at:      Optional[datetime]

    model_config = {"from_attributes": True}


class DailyPerformanceOut(BaseModel):
    """Aggregated performance metrics for one UTC calendar day."""

    date:           str           # ISO format: "2026-03-20"
    total_trades:   int
    winning_trades: int
    losing_trades:  int
    win_rate:       float         # 0.0 – 1.0
    total_pnl:      float
    avg_pnl:        float
    best_symbol:    Optional[str] # symbol with highest total PnL that day
    worst_symbol:   Optional[str] # symbol with lowest total PnL that day
    best_hour:      Optional[int] # UTC open hour with highest total PnL
    worst_hour:     Optional[int] # UTC open hour with lowest total PnL


# ---------------------------------------------------------------------------
# Mode filtering
# ---------------------------------------------------------------------------

#: Allowed values for the ``mode`` parameter across all public functions.
ALLOWED_MODES = ("all", "paper", "live")


def _mode_clause(mode: str):
    """Return a SQLAlchemy WHERE clause (or None) for the given trade mode.

    Null-handling rule (backward compatibility):
      ``is_paper IS NULL`` means the row was created before this column was
      added.  Since the system has only ever run paper trades, NULL is treated
      as paper in every filter mode except ``live``.

    Mapping:
      ``mode="paper"`` → ``is_paper IS TRUE  OR  is_paper IS NULL``
      ``mode="live"``  → ``is_paper IS FALSE``
      ``mode="all"``   → no filter (returns everything including NULL rows)
    """
    if mode == "paper":
        return or_(Position.is_paper == True, Position.is_paper == None)  # noqa: E711,E712
    if mode == "live":
        return Position.is_paper == False   # noqa: E712
    return None   # "all" — no additional filter


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_entry(pos: Position) -> TradeLogEntry:
    """Convert a closed Position row to a TradeLogEntry."""
    pnl = float(pos.realized_pnl or 0)
    opened_at = pos.opened_at
    if opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=timezone.utc)
    closed_at = pos.closed_at
    if closed_at is not None and closed_at.tzinfo is None:
        closed_at = closed_at.replace(tzinfo=timezone.utc)
    return TradeLogEntry(
        position_id=pos.id,
        symbol=pos.symbol,
        direction="BUY" if pos.side == "long" else "SELL",
        pnl=pnl,
        is_win=pnl > 0,
        open_hour_utc=opened_at.hour,
        event_context=pos.event_context,
        was_reduced_size=(pos.event_context == "reduced_size_due_to_event"),
        is_paper=pos.is_paper,
        opened_at=opened_at,
        closed_at=closed_at,
    )


def _best_worst(mapping: Dict) -> tuple:
    """Return (key_with_max_value, key_with_min_value) from a dict."""
    if not mapping:
        return None, None
    best  = max(mapping, key=mapping.__getitem__)
    worst = min(mapping, key=mapping.__getitem__)
    return best, worst


def _aggregate_day(entries: List[TradeLogEntry]) -> DailyPerformanceOut:
    """Compute one DailyPerformanceOut from a list of same-day entries."""
    total   = len(entries)
    wins    = sum(1 for e in entries if e.is_win)
    total_pnl = sum(e.pnl for e in entries)
    avg_pnl   = total_pnl / total if total else 0.0

    sym_pnl:  Dict[str, float] = defaultdict(float)
    hour_pnl: Dict[int, float] = defaultdict(float)
    for e in entries:
        sym_pnl[e.symbol]          += e.pnl
        hour_pnl[e.open_hour_utc]  += e.pnl

    best_sym,  worst_sym  = _best_worst(sym_pnl)
    best_hour, worst_hour = _best_worst(hour_pnl)

    # Closed_at drives the day grouping; fall back to opened_at
    day_str = (entries[0].closed_at or entries[0].opened_at).strftime("%Y-%m-%d")

    return DailyPerformanceOut(
        date=day_str,
        total_trades=total,
        winning_trades=wins,
        losing_trades=total - wins,
        win_rate=wins / total if total else 0.0,
        total_pnl=round(total_pnl, 4),
        avg_pnl=round(avg_pnl, 4),
        best_symbol=best_sym,
        worst_symbol=worst_sym,
        best_hour=best_hour,
        worst_hour=worst_hour,
    )


# ---------------------------------------------------------------------------
# Public API — Layer 1: trade log
# ---------------------------------------------------------------------------

async def get_trade_log(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    days: int = 30,
    mode: str = "all",
) -> List[TradeLogEntry]:
    """Return all closed positions for this portfolio in the last ``days`` days.

    Parameters
    ----------
    mode:
        ``"all"``   — return every closed position (default, backward-compatible).
        ``"paper"`` — only positions where ``is_paper IS TRUE OR is_paper IS NULL``.
        ``"live"``  — only positions where ``is_paper IS FALSE``.

    Results are ordered most-recent-first.
    """
    if mode not in ALLOWED_MODES:
        raise ValueError(f"mode must be one of {ALLOWED_MODES}, got {mode!r}")

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    conditions = [
        Position.portfolio_id == portfolio_id,
        Position.is_open == False,  # noqa: E712
        Position.closed_at >= cutoff,
    ]
    clause = _mode_clause(mode)
    if clause is not None:
        conditions.append(clause)

    result = await db.execute(
        select(Position).where(*conditions).order_by(Position.closed_at.desc())
    )
    positions = list(result.scalars().all())
    log.debug(
        "Trade log fetched",
        portfolio_id=str(portfolio_id),
        days=days,
        mode=mode,
        count=len(positions),
    )
    return [_to_entry(p) for p in positions]


# ---------------------------------------------------------------------------
# Public API — Layer 2: daily aggregates
# ---------------------------------------------------------------------------

async def compute_daily_performance(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    days: int = 30,
    mode: str = "all",
) -> List[DailyPerformanceOut]:
    """Compute daily performance summaries from closed positions.

    Returns one ``DailyPerformanceOut`` per calendar day (UTC) that had at
    least one closed trade, sorted newest-first.  Uses live Position data —
    does not read from the snapshot table.

    ``mode`` accepts ``"all"`` (default), ``"paper"``, or ``"live"``.
    """
    entries = await get_trade_log(db, portfolio_id, days=days, mode=mode)
    if not entries:
        return []

    by_day: Dict[date, List[TradeLogEntry]] = defaultdict(list)
    for entry in entries:
        ref_dt = entry.closed_at or entry.opened_at
        by_day[ref_dt.date()].append(entry)

    return [
        _aggregate_day(by_day[d])
        for d in sorted(by_day.keys(), reverse=True)
    ]


async def save_daily_performance_snapshot(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    target_date: Optional[date] = None,
    mode: str = "all",
) -> DailyPerformanceSummary:
    """Persist the daily performance summary for ``target_date`` (default: today UTC).

    Uses an upsert pattern: deletes any existing row for the same
    (portfolio_id, date_utc) before inserting the freshly computed one.
    Returns the persisted model instance.

    ``mode`` controls which trades contribute to the snapshot
    (``"all"``, ``"paper"``, or ``"live"``).
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    # Compute for the target day only
    all_entries = await get_trade_log(db, portfolio_id, days=1, mode=mode)
    day_entries = [
        e for e in all_entries
        if (e.closed_at or e.opened_at).date() == target_date
    ]

    # Build aggregated values
    if day_entries:
        agg = _aggregate_day(day_entries)
        row = DailyPerformanceSummary(
            portfolio_id=portfolio_id,
            date_utc=target_date,
            total_trades=agg.total_trades,
            winning_trades=agg.winning_trades,
            losing_trades=agg.losing_trades,
            win_rate=agg.win_rate,
            total_pnl=Decimal(str(agg.total_pnl)),
            avg_pnl=Decimal(str(agg.avg_pnl)),
            best_symbol=agg.best_symbol,
            worst_symbol=agg.worst_symbol,
            best_hour=agg.best_hour,
            worst_hour=agg.worst_hour,
        )
    else:
        row = DailyPerformanceSummary(
            portfolio_id=portfolio_id,
            date_utc=target_date,
        )

    # Upsert: remove stale snapshot first, then add fresh one
    await db.execute(
        delete(DailyPerformanceSummary).where(
            DailyPerformanceSummary.portfolio_id == portfolio_id,
            DailyPerformanceSummary.date_utc == target_date,
        )
    )
    db.add(row)
    await db.flush()

    log.info(
        "Daily performance snapshot saved",
        portfolio_id=str(portfolio_id),
        date=str(target_date),
        trades=row.total_trades,
        total_pnl=float(row.total_pnl),
    )
    return row


async def get_daily_performance_snapshots(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    days: int = 30,
    mode: str = "all",
) -> List[DailyPerformanceSummary]:
    """Retrieve persisted daily performance snapshots, newest-first.

    Note: snapshots store pre-computed aggregates and do not carry a
    ``mode`` tag themselves.  Filtering by ``mode`` here applies to the
    *live recomputation path* (``compute_daily_performance``), not to
    stored rows.  For mode-specific snapshots, call
    ``save_daily_performance_snapshot(mode=...)`` to persist separate
    snapshots and retrieve them directly.  When ``mode`` is anything
    other than ``"all"``, this function falls back to a live computation
    to ensure correct results.
    """
    if mode != "all":
        # Snapshots don't carry a mode tag; recompute live so the filter is exact.
        live = await compute_daily_performance(db, portfolio_id, days=days, mode=mode)
        # Wrap live results in lightweight objects the caller can treat uniformly.
        return live  # type: ignore[return-value]

    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days)
    result = await db.execute(
        select(DailyPerformanceSummary)
        .where(
            DailyPerformanceSummary.portfolio_id == portfolio_id,
            DailyPerformanceSummary.date_utc >= cutoff,
        )
        .order_by(DailyPerformanceSummary.date_utc.desc())
    )
    return list(result.scalars().all())
