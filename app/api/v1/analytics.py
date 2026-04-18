"""
Analytics API — GET /api/v1/analytics

Returns real-time performance statistics, PnL history, and stored
performance snapshots for the authenticated user's portfolio.
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_account_mode, get_current_user, get_db
from app.models.performance_snapshot import PerformanceSnapshot
from app.models.portfolio import Portfolio
from app.models.user import User
from app.services.daily_performance_service import (
    DailyPerformanceOut,
    TradeLogEntry,
    compute_daily_performance,
    get_daily_performance_snapshots,
    get_trade_log,
    save_daily_performance_snapshot,
)
from app.services.contextual_analytics_service import (
    EventContextStats,
    HourStats,
    SymbolStats,
    get_performance_by_event_context,
    get_performance_by_open_hour,
    get_performance_by_symbol,
)
from app.services.analytics_service import (
    DailyPnL,
    PerformanceStats,
    get_daily_pnl_series,
    get_performance_snapshots,
    get_performance_stats,
    save_performance_snapshot,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_portfolio(db: AsyncSession, user: User, account_mode: str = "paper") -> Portfolio:
    result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == user.id, Portfolio.account_mode == account_mode)
    )
    portfolio = result.scalars().first()
    if portfolio is None:
        raise HTTPException(status_code=404, detail=f"Portfolio not found for mode '{account_mode}'")
    return portfolio


# ---------------------------------------------------------------------------
# Output schema for stored snapshots
# ---------------------------------------------------------------------------

class PerformanceSnapshotOut(BaseModel):
    """Serialised view of a PerformanceSnapshot DB row."""
    id: str
    portfolio_id: str
    captured_at: datetime

    total_trades: int
    open_positions: int
    winning_trades: int
    losing_trades: int

    total_pnl: float
    daily_pnl: float
    avg_win: float
    avg_loss: float
    best_trade_pnl: float
    worst_trade_pnl: float

    win_rate: float
    profit_factor: float

    consecutive_wins: int
    consecutive_losses: int
    max_drawdown_pct: float
    trades_per_day: float

    class Config:
        from_attributes = True

    @classmethod
    def from_orm(cls, snap: PerformanceSnapshot) -> "PerformanceSnapshotOut":
        return cls(
            id=str(snap.id),
            portfolio_id=str(snap.portfolio_id),
            captured_at=snap.captured_at,
            total_trades=snap.total_trades,
            open_positions=snap.open_positions,
            winning_trades=snap.winning_trades,
            losing_trades=snap.losing_trades,
            total_pnl=float(snap.total_pnl),
            daily_pnl=float(snap.daily_pnl),
            avg_win=float(snap.avg_win),
            avg_loss=float(snap.avg_loss),
            best_trade_pnl=float(snap.best_trade_pnl),
            worst_trade_pnl=float(snap.worst_trade_pnl),
            win_rate=snap.win_rate,
            profit_factor=snap.profit_factor,
            consecutive_wins=snap.consecutive_wins,
            consecutive_losses=snap.consecutive_losses,
            max_drawdown_pct=snap.max_drawdown_pct,
            trades_per_day=snap.trades_per_day,
        )


class SnapshotListResponse(BaseModel):
    items: list[PerformanceSnapshotOut]
    total: int


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/stats",
    response_model=PerformanceStats,
    summary="Live performance statistics",
    description=(
        "Computes win rate, PnL, drawdown, streaks, and activity metrics "
        "live from all closed positions in this portfolio."
    ),
)
async def get_stats(
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> PerformanceStats:
    portfolio = await _get_portfolio(db, user, account_mode)
    return await get_performance_stats(db, portfolio.id)


@router.get(
    "/daily-pnl",
    response_model=list[DailyPnL],
    summary="Daily PnL series (last N days)",
    description=(
        "Returns per-day realized PnL for the last *days* calendar days. "
        "Days with no closed trades are omitted."
    ),
)
async def get_daily_pnl(
    days: int = Query(default=30, ge=1, le=365),
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[DailyPnL]:
    portfolio = await _get_portfolio(db, user, account_mode)
    return await get_daily_pnl_series(db, portfolio.id, days=days)


@router.get(
    "/snapshots",
    response_model=SnapshotListResponse,
    summary="Performance snapshots over time",
    description=(
        "Returns stored performance snapshots captured by the bot (at most "
        "one per hour). Use *days* to control the lookback window."
    ),
)
async def list_snapshots(
    days:  int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=500, ge=1, le=2000),
    db:    AsyncSession = Depends(get_db),
    user:  User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> SnapshotListResponse:
    portfolio = await _get_portfolio(db, user, account_mode)
    snaps = await get_performance_snapshots(db, portfolio.id, days=days, limit=limit)
    items = [PerformanceSnapshotOut.from_orm(s) for s in snaps]
    return SnapshotListResponse(items=items, total=len(items))


@router.get(
    "/snapshots/latest",
    response_model=PerformanceSnapshotOut,
    summary="Most recent performance snapshot",
    description="Returns the single most recent stored performance snapshot.",
)
async def get_latest_snapshot(
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> PerformanceSnapshotOut:
    portfolio = await _get_portfolio(db, user, account_mode)
    snaps = await get_performance_snapshots(db, portfolio.id, days=365, limit=1)
    if not snaps:
        raise HTTPException(
            status_code=404,
            detail="No performance snapshots yet — run the bot to generate one.",
        )
    return PerformanceSnapshotOut.from_orm(snaps[0])


@router.post(
    "/snapshots",
    response_model=PerformanceSnapshotOut,
    status_code=201,
    summary="Force a performance snapshot now",
    description=(
        "Immediately computes and stores a performance snapshot, bypassing "
        "the hourly rate limit. Useful for testing or manual triggers."
    ),
)
async def force_snapshot(
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> PerformanceSnapshotOut:
    portfolio = await _get_portfolio(db, user, account_mode)
    snap = await save_performance_snapshot(
        db, portfolio.id, min_interval_seconds=0
    )
    if snap is None:
        raise HTTPException(status_code=500, detail="Snapshot creation failed")
    await db.commit()
    return PerformanceSnapshotOut.from_orm(snap)


# ---------------------------------------------------------------------------
# Contextual analytics endpoints (PASO 4)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Daily performance endpoints (PASO 8)
# ---------------------------------------------------------------------------

class DailyPerformanceSnapshotOut(BaseModel):
    """Serialised DailyPerformanceSummary DB row."""
    id:             str
    portfolio_id:   str
    date_utc:       str
    total_trades:   int
    winning_trades: int
    losing_trades:  int
    win_rate:       float
    total_pnl:      float
    avg_pnl:        float
    best_symbol:    str | None
    worst_symbol:   str | None
    best_hour:      int | None
    worst_hour:     int | None

    class Config:
        from_attributes = True


@router.get(
    "/daily-performance",
    response_model=list[DailyPerformanceOut],
    summary="Daily performance summary (live)",
    description=(
        "Aggregates closed positions by UTC calendar day and returns one "
        "summary per active trading day within the lookback window.\n\n"
        "Each summary includes: total trades, win rate, total/avg PnL, "
        "best and worst symbol (by total day PnL), best and worst UTC open hour. "
        "Results are sorted newest-first. Days with no closed trades are omitted."
    ),
)
async def get_daily_performance(
    days: int = Query(default=30, ge=1, le=365,
                      description="Number of calendar days to look back"),
    mode: str = Query(default="all",
                      description="Trade mode filter: 'all' (default), 'paper', or 'live'. "
                                  "Legacy rows with null is_paper are treated as paper."),
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[DailyPerformanceOut]:
    if mode not in ("all", "paper", "live"):
        raise HTTPException(status_code=422, detail="mode must be 'all', 'paper', or 'live'")
    portfolio = await _get_portfolio(db, user, account_mode)
    return await compute_daily_performance(db, portfolio.id, days=days, mode=mode)


@router.get(
    "/daily-performance/trade-log",
    response_model=list[TradeLogEntry],
    summary="Individual closed trade log",
    description=(
        "Returns every closed position as a structured trade log entry, "
        "enriched with: direction (BUY/SELL), win/loss flag, UTC open hour, "
        "event context, and whether position size was reduced. "
        "Results are sorted most-recent-first."
    ),
)
async def get_trade_log_endpoint(
    days: int = Query(default=30, ge=1, le=365),
    mode: str = Query(default="all",
                      description="Trade mode filter: 'all' (default), 'paper', or 'live'. "
                                  "Legacy rows with null is_paper are treated as paper."),
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[TradeLogEntry]:
    if mode not in ("all", "paper", "live"):
        raise HTTPException(status_code=422, detail="mode must be 'all', 'paper', or 'live'")
    portfolio = await _get_portfolio(db, user, account_mode)
    return await get_trade_log(db, portfolio.id, days=days, mode=mode)


@router.post(
    "/daily-performance/snapshot",
    response_model=DailyPerformanceSnapshotOut,
    status_code=201,
    summary="Persist today's daily performance summary",
    description=(
        "Computes today's aggregated metrics and stores them in the "
        "``daily_performance_summaries`` table (upsert). "
        "Call this at end-of-day to build a historical record."
    ),
)
async def save_daily_snapshot(
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> DailyPerformanceSnapshotOut:
    portfolio = await _get_portfolio(db, user, account_mode)
    row = await save_daily_performance_snapshot(db, portfolio.id)
    await db.commit()
    return DailyPerformanceSnapshotOut(
        id=str(row.id),
        portfolio_id=str(row.portfolio_id),
        date_utc=str(row.date_utc),
        total_trades=row.total_trades,
        winning_trades=row.winning_trades,
        losing_trades=row.losing_trades,
        win_rate=row.win_rate,
        total_pnl=float(row.total_pnl),
        avg_pnl=float(row.avg_pnl),
        best_symbol=row.best_symbol,
        worst_symbol=row.worst_symbol,
        best_hour=row.best_hour,
        worst_hour=row.worst_hour,
    )


@router.get(
    "/daily-performance/snapshots",
    response_model=list[DailyPerformanceSnapshotOut],
    summary="Stored daily performance snapshots",
    description=(
        "Returns previously persisted daily performance snapshots, newest-first. "
        "Snapshots are created via the POST endpoint or automatically "
        "by calling ``save_daily_performance_snapshot`` from bot logic."
    ),
)
async def list_daily_snapshots(
    days: int = Query(default=30, ge=1, le=365),
    mode: str = Query(default="all",
                      description="Trade mode filter: 'all' (default), 'paper', or 'live'. "
                                  "Legacy rows with null is_paper are treated as paper."),
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[DailyPerformanceSnapshotOut]:
    if mode not in ("all", "paper", "live"):
        raise HTTPException(status_code=422, detail="mode must be 'all', 'paper', or 'live'")
    portfolio = await _get_portfolio(db, user, account_mode)
    rows = await get_daily_performance_snapshots(db, portfolio.id, days=days, mode=mode)

    def _serialize(r) -> DailyPerformanceSnapshotOut:
        # Stored DailyPerformanceSummary rows have .id / .date_utc;
        # live DailyPerformanceOut objects (returned when mode != "all") have .date.
        if hasattr(r, "id"):
            return DailyPerformanceSnapshotOut(
                id=str(r.id),
                portfolio_id=str(r.portfolio_id),
                date_utc=str(r.date_utc),
                total_trades=r.total_trades,
                winning_trades=r.winning_trades,
                losing_trades=r.losing_trades,
                win_rate=r.win_rate,
                total_pnl=float(r.total_pnl),
                avg_pnl=float(r.avg_pnl),
                best_symbol=r.best_symbol,
                worst_symbol=r.worst_symbol,
                best_hour=r.best_hour,
                worst_hour=r.worst_hour,
            )
        # Live recomputed DailyPerformanceOut — no persisted id
        return DailyPerformanceSnapshotOut(
            id="",
            portfolio_id=str(portfolio.id),
            date_utc=r.date,
            total_trades=r.total_trades,
            winning_trades=r.winning_trades,
            losing_trades=r.losing_trades,
            win_rate=r.win_rate,
            total_pnl=r.total_pnl,
            avg_pnl=r.avg_pnl,
            best_symbol=r.best_symbol,
            worst_symbol=r.worst_symbol,
            best_hour=r.best_hour,
            worst_hour=r.worst_hour,
        )

    return [_serialize(r) for r in rows]


@router.get(
    "/by-symbol",
    response_model=list[SymbolStats],
    summary="Performance breakdown by trading symbol",
    description=(
        "Returns win rate, PnL, profit factor, and drawdown for each symbol "
        "that has at least one closed trade in this portfolio."
    ),
)
async def get_by_symbol(
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[SymbolStats]:
    portfolio = await _get_portfolio(db, user, account_mode)
    return await get_performance_by_symbol(db, portfolio.id)


@router.get(
    "/by-hour",
    response_model=list[HourStats],
    summary="Performance breakdown by UTC open hour",
    description=(
        "Groups closed trades by the UTC hour they were opened (0–23). "
        "Useful for identifying which session hours produce the best results."
    ),
)
async def get_by_hour(
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[HourStats]:
    portfolio = await _get_portfolio(db, user, account_mode)
    return await get_performance_by_open_hour(db, portfolio.id)


@router.get(
    "/by-event-context",
    response_model=list[EventContextStats],
    summary="Performance breakdown by event-risk context",
    description=(
        "Classifies each closed trade into one of four event-context buckets "
        "and computes win rate and PnL per bucket:\n\n"
        "- **reduced_size_due_to_event** — bot halved position size due to a "
        "medium-impact event (stored on the position at open time)\n"
        "- **trade_near_high_impact_event** — a high-impact event was in the "
        "DB within ±window_minutes of the trade open (retroactive check)\n"
        "- **trade_near_medium_impact_event** — same for medium-impact\n"
        "- **trade_without_near_event** — no event detected in either source\n\n"
        "All four buckets are always returned (total_trades=0 when empty)."
    ),
)
async def get_by_event_context(
    window_minutes: int = Query(default=60, ge=1, le=480,
                                description="Half-width of event search window in minutes"),
    db:   AsyncSession = Depends(get_db),
    user: User         = Depends(get_current_user),
    account_mode: str   = Depends(get_account_mode),
) -> list[EventContextStats]:
    portfolio = await _get_portfolio(db, user, account_mode)
    return await get_performance_by_event_context(db, portfolio.id, window_minutes=window_minutes)
