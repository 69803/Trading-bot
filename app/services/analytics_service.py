"""
Analytics Service.

Computes real-time performance statistics from the Trade, Position, and
DecisionLog tables.  All functions accept an AsyncSession and a user_id;
they are pure query functions with no side effects.

Metrics returned
────────────────
  win_rate              – % of closed positions with positive PnL
  total_pnl             – sum of all realized PnL (closed positions)
  daily_pnl             – realized PnL for the current UTC day
  max_drawdown_pct      – largest peak-to-trough equity decline (%)
  consecutive_wins      – current streak of consecutive winning trades
  consecutive_losses    – current streak of consecutive losing trades
  trades_per_day        – average closed positions per calendar day
  avg_win               – average PnL of winning trades
  avg_loss              – average PnL of losing trades (absolute value)
  profit_factor         – total gross profit / total gross loss
  total_trades          – total number of closed positions
  open_positions        – number of currently open positions
  best_trade_pnl        – single best realized PnL
  worst_trade_pnl       – single worst realized PnL
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from pydantic import BaseModel, Field
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.performance_snapshot import PerformanceSnapshot
from app.models.position import Position

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

class PerformanceStats(BaseModel):
    """Aggregated trading performance metrics for one portfolio."""

    # Counts
    total_trades: int = 0
    open_positions: int = 0
    winning_trades: int = 0
    losing_trades: int = 0

    # PnL
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0          # absolute value (positive number)
    best_trade_pnl: float = 0.0
    worst_trade_pnl: float = 0.0

    # Ratios
    win_rate: float = Field(default=0.0, description="0.0–1.0")
    profit_factor: float = 0.0    # gross_profit / gross_loss (0 if no losses)
    avg_rr_ratio: float = 0.0     # placeholder (requires SL/TP data)

    # Streaks
    consecutive_wins: int = 0
    consecutive_losses: int = 0

    # Drawdown
    max_drawdown_pct: float = 0.0

    # Activity
    trades_per_day: float = 0.0
    first_trade_at: Optional[datetime] = None
    last_trade_at: Optional[datetime] = None

    computed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class DailyPnL(BaseModel):
    """PnL summary for a single UTC day."""
    date: date
    realized_pnl: float
    trades_closed: int


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_performance_stats(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> PerformanceStats:
    """
    Compute full performance statistics for a portfolio.

    Uses closed Position rows (is_open=False, realized_pnl set).
    """
    # ── Fetch all closed positions ────────────────────────────────────────────
    result = await db.execute(
        select(Position)
        .where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == False,         # noqa: E712
        )
        .order_by(Position.closed_at.asc())
    )
    closed: list[Position] = list(result.scalars().all())

    # ── Count open positions ──────────────────────────────────────────────────
    open_count_result = await db.execute(
        select(func.count(Position.id)).where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == True,          # noqa: E712
        )
    )
    open_count = open_count_result.scalar_one_or_none() or 0

    if not closed:
        return PerformanceStats(open_positions=open_count)

    # ── Core PnL calculations ─────────────────────────────────────────────────
    pnl_values = [float(p.realized_pnl) for p in closed]

    wins  = [v for v in pnl_values if v > 0]
    losses = [v for v in pnl_values if v <= 0]

    total_pnl    = sum(pnl_values)
    win_rate     = len(wins) / len(pnl_values) if pnl_values else 0.0
    avg_win      = sum(wins) / len(wins) if wins else 0.0
    avg_loss     = abs(sum(losses) / len(losses)) if losses else 0.0
    gross_profit = sum(wins)
    gross_loss   = abs(sum(losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0
    best_trade   = max(pnl_values)
    worst_trade  = min(pnl_values)

    # ── Daily PnL (today UTC) ─────────────────────────────────────────────────
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    daily_pnl = sum(
        float(p.realized_pnl)
        for p in closed
        if p.closed_at and _aware(p.closed_at) >= today_start
    )

    # ── Streak calculation (walk backwards) ──────────────────────────────────
    consecutive_wins   = 0
    consecutive_losses = 0
    for pnl in reversed(pnl_values):
        if pnl > 0:
            if consecutive_losses > 0:
                break
            consecutive_wins += 1
        else:
            if consecutive_wins > 0:
                break
            consecutive_losses += 1

    # ── Max drawdown (from equity curve using cumulative PnL) ─────────────────
    max_drawdown_pct = _compute_max_drawdown(pnl_values)

    # ── Trades per day ────────────────────────────────────────────────────────
    first_trade = closed[0]
    last_trade  = closed[-1]
    first_at    = _aware(first_trade.closed_at) if first_trade.closed_at else None
    last_at     = _aware(last_trade.closed_at)  if last_trade.closed_at  else None

    if first_at and last_at:
        days_active = max(1, (last_at - first_at).days + 1)
        trades_per_day = len(closed) / days_active
    else:
        trades_per_day = float(len(closed))

    return PerformanceStats(
        total_trades       = len(closed),
        open_positions     = open_count,
        winning_trades     = len(wins),
        losing_trades      = len(losses),
        total_pnl          = round(total_pnl, 4),
        daily_pnl          = round(daily_pnl, 4),
        avg_win            = round(avg_win, 4),
        avg_loss           = round(avg_loss, 4),
        best_trade_pnl     = round(best_trade, 4),
        worst_trade_pnl    = round(worst_trade, 4),
        win_rate           = round(win_rate, 4),
        profit_factor      = round(profit_factor, 3),
        consecutive_wins   = consecutive_wins,
        consecutive_losses = consecutive_losses,
        max_drawdown_pct   = round(max_drawdown_pct, 4),
        trades_per_day     = round(trades_per_day, 2),
        first_trade_at     = first_at,
        last_trade_at      = last_at,
    )


async def get_daily_pnl_series(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    days: int = 30,
) -> list[DailyPnL]:
    """
    Return per-day PnL for the last *days* calendar days.
    Days with no closed trades are omitted.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    result = await db.execute(
        select(Position)
        .where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == False,         # noqa: E712
            Position.closed_at >= cutoff,
        )
        .order_by(Position.closed_at.asc())
    )
    positions: list[Position] = list(result.scalars().all())

    # Group by UTC date
    daily: dict[date, list[float]] = {}
    for p in positions:
        if not p.closed_at:
            continue
        d = _aware(p.closed_at).date()
        daily.setdefault(d, []).append(float(p.realized_pnl))

    return [
        DailyPnL(
            date=d,
            realized_pnl=round(sum(pnls), 4),
            trades_closed=len(pnls),
        )
        for d, pnls in sorted(daily.items())
    ]


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

async def save_performance_snapshot(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    min_interval_seconds: int = 3600,
) -> PerformanceSnapshot | None:
    """
    Compute current performance stats and persist a PerformanceSnapshot row.

    Rate-limited: skips the write if a snapshot already exists for this
    portfolio within the last *min_interval_seconds* (default: 1 hour).
    Returns the new snapshot, or None if the write was skipped.
    """
    from decimal import Decimal

    # Rate-limit check — avoid flooding the table on every bot cycle
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=min_interval_seconds)
    recent = await db.execute(
        select(PerformanceSnapshot.id)
        .where(
            PerformanceSnapshot.portfolio_id == portfolio_id,
            PerformanceSnapshot.captured_at >= cutoff,
        )
        .limit(1)
    )
    if recent.scalar_one_or_none() is not None:
        return None  # already have a recent snapshot

    stats = await get_performance_stats(db, portfolio_id)

    snap = PerformanceSnapshot(
        portfolio_id     = portfolio_id,
        captured_at      = stats.computed_at,
        total_trades     = stats.total_trades,
        open_positions   = stats.open_positions,
        winning_trades   = stats.winning_trades,
        losing_trades    = stats.losing_trades,
        total_pnl        = Decimal(str(stats.total_pnl)),
        daily_pnl        = Decimal(str(stats.daily_pnl)),
        avg_win          = Decimal(str(stats.avg_win)),
        avg_loss         = Decimal(str(stats.avg_loss)),
        best_trade_pnl   = Decimal(str(stats.best_trade_pnl)),
        worst_trade_pnl  = Decimal(str(stats.worst_trade_pnl)),
        win_rate         = stats.win_rate,
        profit_factor    = stats.profit_factor,
        consecutive_wins = stats.consecutive_wins,
        consecutive_losses = stats.consecutive_losses,
        max_drawdown_pct = stats.max_drawdown_pct,
        trades_per_day   = stats.trades_per_day,
    )
    db.add(snap)
    await db.flush()
    log.info(
        "Performance snapshot saved",
        portfolio_id=str(portfolio_id),
        total_trades=stats.total_trades,
        win_rate=f"{stats.win_rate:.1%}",
        total_pnl=stats.total_pnl,
        max_drawdown=f"{stats.max_drawdown_pct:.1%}",
    )
    return snap


async def get_performance_snapshots(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    days: int = 30,
    limit: int = 500,
) -> list[PerformanceSnapshot]:
    """
    Return PerformanceSnapshot rows for *portfolio_id* within the last
    *days* calendar days, newest first, capped at *limit* rows.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = await db.execute(
        select(PerformanceSnapshot)
        .where(
            PerformanceSnapshot.portfolio_id == portfolio_id,
            PerformanceSnapshot.captured_at >= cutoff,
        )
        .order_by(PerformanceSnapshot.captured_at.desc())
        .limit(limit)
    )
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Risk helpers used by bot_service
# ---------------------------------------------------------------------------

async def count_consecutive_losses(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> int:
    """
    Return the number of consecutive losing trades at the end of trade history.
    Returns 0 if the last trade was a win or there is no history.
    """
    result = await db.execute(
        select(Position.realized_pnl)
        .where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == False,         # noqa: E712
        )
        .order_by(Position.closed_at.desc())
        .limit(20)  # only need recent history
    )
    recent_pnls = [float(row[0]) for row in result.fetchall()]

    count = 0
    for pnl in recent_pnls:
        if pnl <= 0:
            count += 1
        else:
            break
    return count


async def count_trades_last_hour(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
) -> int:
    """Return number of positions opened in the last 60 minutes."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    result = await db.execute(
        select(func.count(Position.id)).where(
            Position.portfolio_id == portfolio_id,
            Position.opened_at >= cutoff,
        )
    )
    return result.scalar_one_or_none() or 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_max_drawdown(pnl_series: list[float]) -> float:
    """
    Compute max drawdown as a fraction of peak equity from a PnL series.

    Returns a positive number (e.g. 0.15 = 15% drawdown).
    Returns 0.0 if the series never has a drawdown.
    """
    if not pnl_series:
        return 0.0

    # Build cumulative equity curve (starting at 0)
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
    """Ensure datetime is UTC-aware (SQLite may return naive datetimes)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
