"""
Tests for daily_performance_service.py and the
GET /api/v1/analytics/daily-performance family of endpoints (PASO 8).

Covers:
  Trade log:
    - returns only closed positions (open ones excluded)
    - direction mapped from side: long→BUY, short→SELL
    - was_reduced_size=True when event_context=="reduced_size_due_to_event"
    - was_reduced_size=False for normal trades
    - open_hour_utc taken from opened_at

  Daily aggregates (compute_daily_performance):
    - groups by UTC calendar day (closed_at date)
    - counts wins, losses, win_rate
    - total_pnl and avg_pnl are correct
    - best_symbol = symbol with highest sum PnL that day
    - worst_symbol = symbol with lowest sum PnL that day
    - best_hour / worst_hour by sum PnL that day
    - empty portfolio returns []
    - multi-day data returns one row per day, newest first

  Snapshots:
    - save_daily_performance_snapshot persists a row
    - second call on same date upserts (no duplicate)
    - get_daily_performance_snapshots retrieves stored rows

  API endpoints:
    - GET /api/v1/analytics/daily-performance returns 200
    - GET /api/v1/analytics/daily-performance/trade-log returns 200
    - POST /api/v1/analytics/daily-performance/snapshot returns 201
    - GET /api/v1/analytics/daily-performance/snapshots returns 200

All DB tests use the in-memory SQLite fixture from conftest.py.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.daily_performance_summary import DailyPerformanceSummary
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.services.daily_performance_service import (
    compute_daily_performance,
    get_daily_performance_snapshots,
    get_trade_log,
    save_daily_performance_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _portfolio(db: AsyncSession) -> Portfolio:
    p = Portfolio(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        cash_balance=Decimal("10000"),
        initial_capital=Decimal("10000"),
        realized_pnl=Decimal("0"),
    )
    db.add(p)
    return p


def _closed(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    symbol: str = "EURUSD",
    side: str = "long",
    pnl: float = 10.0,
    days_ago: int = 0,
    hour_utc: int = 10,
    event_context: str | None = None,
) -> Position:
    """Insert a closed position closed ``days_ago`` UTC calendar days ago."""
    now = datetime.now(timezone.utc)
    opened_at = (now - timedelta(days=days_ago)).replace(
        hour=hour_utc, minute=0, second=0, microsecond=0
    )
    closed_at = opened_at + timedelta(hours=1)
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side=side,
        quantity=Decimal("100"),
        avg_entry_price=Decimal("1.1"),
        current_price=Decimal("1.1"),
        is_open=False,
        opened_at=opened_at,
        closed_at=closed_at,
        closed_price=Decimal("1.1"),
        realized_pnl=Decimal(str(pnl)),
        event_context=event_context,
    )
    db.add(pos)
    return pos


def _open(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    symbol: str = "EURUSD",
) -> Position:
    """Insert an open position."""
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side="long",
        quantity=Decimal("100"),
        avg_entry_price=Decimal("1.1"),
        current_price=Decimal("1.1"),
        is_open=True,
        opened_at=datetime.now(timezone.utc),
        realized_pnl=Decimal("0"),
    )
    db.add(pos)
    return pos


# ---------------------------------------------------------------------------
# Trade log — get_trade_log
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trade_log_returns_closed_positions(db: AsyncSession):
    """Closed positions appear in the trade log."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=20.0)
    _closed(db, port.id, pnl=-5.0)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=7)
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_trade_log_excludes_open_positions(db: AsyncSession):
    """Open positions must NOT appear in the trade log."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=10.0)
    _open(db, port.id)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=7)
    assert len(entries) == 1
    assert entries[0].is_win


@pytest.mark.asyncio
async def test_trade_log_direction_long_is_buy(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, side="long")
    await db.flush()

    entries = await get_trade_log(db, port.id, days=1)
    assert entries[0].direction == "BUY"


@pytest.mark.asyncio
async def test_trade_log_direction_short_is_sell(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, side="short")
    await db.flush()

    entries = await get_trade_log(db, port.id, days=1)
    assert entries[0].direction == "SELL"


@pytest.mark.asyncio
async def test_trade_log_was_reduced_size_event_context(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, event_context="reduced_size_due_to_event")
    _closed(db, port.id, event_context="normal")
    _closed(db, port.id, event_context=None)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=1)
    by_ctx = {e.event_context: e for e in entries}
    assert by_ctx["reduced_size_due_to_event"].was_reduced_size is True
    assert by_ctx["normal"].was_reduced_size is False
    assert by_ctx[None].was_reduced_size is False


@pytest.mark.asyncio
async def test_trade_log_open_hour_utc(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, hour_utc=9)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=1)
    assert entries[0].open_hour_utc == 9


@pytest.mark.asyncio
async def test_trade_log_win_loss_classification(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, pnl=5.0)
    _closed(db, port.id, pnl=-3.0)
    _closed(db, port.id, pnl=0.0)   # break-even → not a win
    await db.flush()

    entries = await get_trade_log(db, port.id, days=1)
    wins = [e for e in entries if e.is_win]
    assert len(wins) == 1
    assert wins[0].pnl == 5.0


@pytest.mark.asyncio
async def test_trade_log_respects_days_window(db: AsyncSession):
    """Positions closed outside the window are excluded."""
    port = _portfolio(db)
    _closed(db, port.id, days_ago=0)   # today — inside
    _closed(db, port.id, days_ago=60)  # 60 days ago — outside
    await db.flush()

    entries = await get_trade_log(db, port.id, days=30)
    assert len(entries) == 1


# ---------------------------------------------------------------------------
# Daily aggregates — compute_daily_performance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_daily_performance_empty_portfolio(db: AsyncSession):
    port = _portfolio(db)
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=30)
    assert result == []


@pytest.mark.asyncio
async def test_daily_performance_single_day(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, pnl=10.0, days_ago=0)
    _closed(db, port.id, pnl=-4.0, days_ago=0)
    _closed(db, port.id, pnl=6.0,  days_ago=0)
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=1)
    assert len(result) == 1
    day = result[0]
    assert day.total_trades == 3
    assert day.winning_trades == 2
    assert day.losing_trades == 1
    assert abs(day.win_rate - 2/3) < 0.01
    assert abs(day.total_pnl - 12.0) < 0.01
    assert abs(day.avg_pnl - 4.0) < 0.01


@pytest.mark.asyncio
async def test_daily_performance_multi_day_newest_first(db: AsyncSession):
    """Each day produces one row; results are sorted newest-first."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=5.0,  days_ago=0)
    _closed(db, port.id, pnl=3.0,  days_ago=1)
    _closed(db, port.id, pnl=-2.0, days_ago=2)
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=7)
    assert len(result) == 3
    dates = [r.date for r in result]
    assert dates == sorted(dates, reverse=True)


@pytest.mark.asyncio
async def test_daily_performance_best_worst_symbol(db: AsyncSession):
    """best_symbol has the highest total PnL, worst_symbol the lowest."""
    port = _portfolio(db)
    _closed(db, port.id, symbol="EURUSD", pnl=20.0,  days_ago=0)
    _closed(db, port.id, symbol="EURUSD", pnl=10.0,  days_ago=0)  # EURUSD total = +30
    _closed(db, port.id, symbol="GBPUSD", pnl=-15.0, days_ago=0)  # GBPUSD total = -15
    _closed(db, port.id, symbol="USDJPY", pnl=5.0,   days_ago=0)  # USDJPY total = +5
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=1)
    assert len(result) == 1
    day = result[0]
    assert day.best_symbol == "EURUSD"
    assert day.worst_symbol == "GBPUSD"


@pytest.mark.asyncio
async def test_daily_performance_best_worst_hour(db: AsyncSession):
    """best_hour has the highest total PnL for that open hour, worst the lowest."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=15.0,  days_ago=0, hour_utc=9)   # hour 9 total = +15
    _closed(db, port.id, pnl=5.0,   days_ago=0, hour_utc=9)   # hour 9 total = +15 (continued)
    _closed(db, port.id, pnl=-10.0, days_ago=0, hour_utc=14)  # hour 14 total = -10
    _closed(db, port.id, pnl=3.0,   days_ago=0, hour_utc=16)  # hour 16 total = +3
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=1)
    day = result[0]
    assert day.best_hour == 9
    assert day.worst_hour == 14


# ---------------------------------------------------------------------------
# Snapshots — save and retrieve
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_daily_snapshot_persists_row(db: AsyncSession):
    """save_daily_performance_snapshot creates a DB row."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=10.0, days_ago=0)
    _closed(db, port.id, pnl=-3.0, days_ago=0)
    await db.flush()

    row = await save_daily_performance_snapshot(db, port.id)
    await db.flush()

    assert row.portfolio_id == port.id
    assert row.total_trades == 2
    assert row.winning_trades == 1
    assert row.losing_trades == 1
    assert abs(float(row.total_pnl) - 7.0) < 0.01
    assert abs(row.win_rate - 0.5) < 0.01


@pytest.mark.asyncio
async def test_save_daily_snapshot_upserts_on_same_date(db: AsyncSession):
    """Calling save twice for the same date replaces the old row — no duplicate."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=10.0, days_ago=0)
    await db.flush()

    await save_daily_performance_snapshot(db, port.id)
    await db.flush()
    await save_daily_performance_snapshot(db, port.id)  # second call
    await db.flush()

    result = await db.execute(
        select(DailyPerformanceSummary).where(
            DailyPerformanceSummary.portfolio_id == port.id
        )
    )
    rows = result.scalars().all()
    assert len(rows) == 1  # upserted, not duplicated


@pytest.mark.asyncio
async def test_get_daily_snapshots_retrieves_stored(db: AsyncSession):
    """get_daily_performance_snapshots returns previously saved rows."""
    port = _portfolio(db)
    _closed(db, port.id, pnl=8.0, days_ago=0)
    await db.flush()

    await save_daily_performance_snapshot(db, port.id)
    await db.flush()

    snapshots = await get_daily_performance_snapshots(db, port.id, days=7)
    assert len(snapshots) == 1
    assert snapshots[0].total_trades == 1


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_daily_performance_returns_200(
    client: AsyncClient, auth_headers: dict
):
    """GET /api/v1/analytics/daily-performance returns 200 (even with no data)."""
    resp = await client.get(
        "/api/v1/analytics/daily-performance",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_api_trade_log_returns_200(
    client: AsyncClient, auth_headers: dict
):
    resp = await client.get(
        "/api/v1/analytics/daily-performance/trade-log",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_api_save_snapshot_returns_201(
    client: AsyncClient, auth_headers: dict
):
    resp = await client.post(
        "/api/v1/analytics/daily-performance/snapshot",
        headers=auth_headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert "total_trades" in body
    assert "win_rate" in body
    assert "date_utc" in body


@pytest.mark.asyncio
async def test_api_list_daily_snapshots_returns_200(
    client: AsyncClient, auth_headers: dict
):
    resp = await client.get(
        "/api/v1/analytics/daily-performance/snapshots",
        headers=auth_headers,
    )
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


# ---------------------------------------------------------------------------
# is_paper mode filtering tests (PASO 8 refinement)
# ---------------------------------------------------------------------------

def _closed_with_mode(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    *,
    pnl: float = 10.0,
    is_paper: bool | None = True,
    symbol: str = "EURUSD",
    days_ago: int = 0,
) -> Position:
    """Insert a closed position with explicit is_paper value."""
    now = datetime.now(timezone.utc)
    opened_at = (now - timedelta(days=days_ago)).replace(
        hour=10, minute=0, second=0, microsecond=0
    )
    closed_at = opened_at + timedelta(hours=1)
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side="long",
        quantity=Decimal("100"),
        avg_entry_price=Decimal("1.1"),
        current_price=Decimal("1.1"),
        is_open=False,
        opened_at=opened_at,
        closed_at=closed_at,
        closed_price=Decimal("1.1"),
        realized_pnl=Decimal(str(pnl)),
        is_paper=is_paper,
    )
    db.add(pos)
    return pos


@pytest.mark.asyncio
async def test_mode_paper_returns_only_paper_trades(db: AsyncSession):
    """mode='paper' returns only is_paper=True trades."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=10.0, is_paper=True)
    _closed_with_mode(db, port.id, pnl=20.0, is_paper=False)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=7, mode="paper")
    assert len(entries) == 1
    assert entries[0].pnl == 10.0
    assert entries[0].is_paper is True


@pytest.mark.asyncio
async def test_mode_live_returns_only_live_trades(db: AsyncSession):
    """mode='live' returns only is_paper=False trades."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=10.0, is_paper=True)
    _closed_with_mode(db, port.id, pnl=20.0, is_paper=False)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=7, mode="live")
    assert len(entries) == 1
    assert entries[0].pnl == 20.0
    assert entries[0].is_paper is False


@pytest.mark.asyncio
async def test_mode_all_returns_all_trades(db: AsyncSession):
    """mode='all' returns both paper and live trades."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=10.0, is_paper=True)
    _closed_with_mode(db, port.id, pnl=20.0, is_paper=False)
    await db.flush()

    entries = await get_trade_log(db, port.id, days=7, mode="all")
    assert len(entries) == 2


@pytest.mark.asyncio
async def test_mode_mixed_dataset(db: AsyncSession):
    """Mixed dataset: paper, live, null rows — each mode returns correct subset."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=10.0, is_paper=True)
    _closed_with_mode(db, port.id, pnl=20.0, is_paper=False)
    _closed_with_mode(db, port.id, pnl=30.0, is_paper=None)  # legacy null
    await db.flush()

    all_entries  = await get_trade_log(db, port.id, days=7, mode="all")
    paper_entries = await get_trade_log(db, port.id, days=7, mode="paper")
    live_entries  = await get_trade_log(db, port.id, days=7, mode="live")

    assert len(all_entries) == 3
    assert len(paper_entries) == 2   # is_paper=True + null
    assert len(live_entries) == 1    # is_paper=False only

    paper_pnls = {e.pnl for e in paper_entries}
    assert paper_pnls == {10.0, 30.0}  # True and null both appear

    assert live_entries[0].pnl == 20.0


@pytest.mark.asyncio
async def test_backward_compat_null_treated_as_paper(db: AsyncSession):
    """Legacy rows with is_paper=NULL appear under mode='paper' and 'all', NOT 'live'."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=99.0, is_paper=None)
    await db.flush()

    paper_entries = await get_trade_log(db, port.id, days=7, mode="paper")
    live_entries  = await get_trade_log(db, port.id, days=7, mode="live")
    all_entries   = await get_trade_log(db, port.id, days=7, mode="all")

    assert len(paper_entries) == 1
    assert len(live_entries) == 0
    assert len(all_entries) == 1


@pytest.mark.asyncio
async def test_daily_performance_mode_paper(db: AsyncSession):
    """compute_daily_performance with mode='paper' excludes live trades."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=10.0, is_paper=True)
    _closed_with_mode(db, port.id, pnl=50.0, is_paper=False)
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=7, mode="paper")
    assert len(result) == 1
    assert abs(result[0].total_pnl - 10.0) < 0.01
    assert result[0].total_trades == 1


@pytest.mark.asyncio
async def test_daily_performance_mode_live(db: AsyncSession):
    """compute_daily_performance with mode='live' excludes paper trades."""
    port = _portfolio(db)
    _closed_with_mode(db, port.id, pnl=10.0, is_paper=True)
    _closed_with_mode(db, port.id, pnl=50.0, is_paper=False)
    await db.flush()

    result = await compute_daily_performance(db, port.id, days=7, mode="live")
    assert len(result) == 1
    assert abs(result[0].total_pnl - 50.0) < 0.01
    assert result[0].total_trades == 1


@pytest.mark.asyncio
async def test_api_trade_log_mode_invalid_returns_422(
    client: AsyncClient, auth_headers: dict
):
    """?mode=bad returns 422 Unprocessable Entity."""
    resp = await client.get(
        "/api/v1/analytics/daily-performance/trade-log?mode=bad",
        headers=auth_headers,
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_api_daily_performance_mode_param(
    client: AsyncClient, auth_headers: dict
):
    """?mode=paper / ?mode=live / ?mode=all all return 200."""
    for mode in ("paper", "live", "all"):
        resp = await client.get(
            f"/api/v1/analytics/daily-performance?mode={mode}",
            headers=auth_headers,
        )
        assert resp.status_code == 200, f"mode={mode} returned {resp.status_code}"


@pytest.mark.asyncio
async def test_api_snapshots_mode_param(
    client: AsyncClient, auth_headers: dict
):
    """?mode=paper / ?mode=live / ?mode=all all return 200 on snapshots endpoint."""
    for mode in ("paper", "live", "all"):
        resp = await client.get(
            f"/api/v1/analytics/daily-performance/snapshots?mode={mode}",
            headers=auth_headers,
        )
        assert resp.status_code == 200, f"mode={mode} returned {resp.status_code}"


@pytest.mark.asyncio
async def test_api_snapshots_mode_invalid_returns_422(
    client: AsyncClient, auth_headers: dict
):
    """?mode=garbage returns 422 on snapshots endpoint."""
    resp = await client.get(
        "/api/v1/analytics/daily-performance/snapshots?mode=garbage",
        headers=auth_headers,
    )
    assert resp.status_code == 422
