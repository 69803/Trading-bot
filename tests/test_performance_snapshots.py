"""
Tests for performance snapshot persistence and retrieval.

Covers:
  - save_performance_snapshot() stores correct metric values
  - hourly rate-limit: second call within interval returns None
  - min_interval_seconds=0 bypasses the rate limit
  - get_performance_snapshots() filters by days window
  - GET /analytics/snapshots returns paginated list
  - GET /analytics/snapshots/latest returns most recent
  - POST /analytics/snapshots (force) creates a row immediately
  - GET /analytics/snapshots/latest returns 404 when no snapshots exist
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.performance_snapshot import PerformanceSnapshot
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.services.analytics_service import (
    get_performance_snapshots,
    save_performance_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_portfolio(db: AsyncSession) -> Portfolio:
    p = Portfolio(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        cash_balance=Decimal("10000"),
        initial_capital=Decimal("10000"),
        realized_pnl=Decimal("0"),
    )
    db.add(p)
    return p


def _make_closed_position(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    pnl: float,
    closed_at: datetime | None = None,
) -> Position:
    now = closed_at or datetime.now(timezone.utc)
    p = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("1000"),
        avg_entry_price=Decimal("1.1000"),
        current_price=Decimal("1.1050"),
        is_open=False,
        opened_at=now - timedelta(hours=1),
        closed_at=now,
        closed_price=Decimal("1.1050"),
        realized_pnl=Decimal(str(pnl)),
    )
    db.add(p)
    return p


# ---------------------------------------------------------------------------
# Unit: save_performance_snapshot
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_snapshot_stores_correct_metrics(db: AsyncSession):
    portfolio = _make_portfolio(db)
    _make_closed_position(db, portfolio.id, pnl=50.0)
    _make_closed_position(db, portfolio.id, pnl=30.0)
    _make_closed_position(db, portfolio.id, pnl=-10.0)
    await db.flush()

    snap = await save_performance_snapshot(db, portfolio.id, min_interval_seconds=0)

    assert snap is not None
    assert snap.portfolio_id == portfolio.id
    assert snap.total_trades == 3
    assert snap.winning_trades == 2
    assert snap.losing_trades == 1
    assert float(snap.total_pnl) == pytest.approx(70.0, abs=0.01)
    assert snap.win_rate == pytest.approx(2 / 3, abs=0.01)
    assert snap.profit_factor == pytest.approx(8.0, abs=0.01)  # 80/10
    assert float(snap.avg_win) == pytest.approx(40.0, abs=0.01)
    assert float(snap.avg_loss) == pytest.approx(10.0, abs=0.01)
    assert snap.max_drawdown_pct >= 0.0
    assert snap.captured_at is not None


@pytest.mark.asyncio
async def test_snapshot_empty_portfolio(db: AsyncSession):
    portfolio = _make_portfolio(db)
    await db.flush()

    snap = await save_performance_snapshot(db, portfolio.id, min_interval_seconds=0)

    assert snap is not None
    assert snap.total_trades == 0
    assert snap.win_rate == 0.0
    assert float(snap.total_pnl) == 0.0


@pytest.mark.asyncio
async def test_rate_limit_blocks_second_call(db: AsyncSession):
    """Second call within the rate-limit window returns None (no duplicate row)."""
    portfolio = _make_portfolio(db)
    await db.flush()

    first = await save_performance_snapshot(db, portfolio.id, min_interval_seconds=3600)
    assert first is not None

    second = await save_performance_snapshot(db, portfolio.id, min_interval_seconds=3600)
    assert second is None


@pytest.mark.asyncio
async def test_rate_limit_zero_always_saves(db: AsyncSession):
    """min_interval_seconds=0 bypasses the rate limit."""
    portfolio = _make_portfolio(db)
    await db.flush()

    first  = await save_performance_snapshot(db, portfolio.id, min_interval_seconds=0)
    second = await save_performance_snapshot(db, portfolio.id, min_interval_seconds=0)

    assert first is not None
    assert second is not None
    assert first.id != second.id


# ---------------------------------------------------------------------------
# Unit: get_performance_snapshots
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_snapshots_filters_by_days(db: AsyncSession):
    portfolio = _make_portfolio(db)
    await db.flush()

    # Manually insert two snapshots: one recent, one old
    now = datetime.now(timezone.utc)
    for days_ago, trades in [(1, 5), (60, 10)]:
        snap = PerformanceSnapshot(
            id=uuid.uuid4(),
            portfolio_id=portfolio.id,
            captured_at=now - timedelta(days=days_ago),
            total_trades=trades,
            open_positions=0, winning_trades=trades, losing_trades=0,
            win_rate=1.0, profit_factor=0.0, consecutive_wins=trades,
            consecutive_losses=0, max_drawdown_pct=0.0, trades_per_day=1.0,
        )
        db.add(snap)
    await db.flush()

    recent = await get_performance_snapshots(db, portfolio.id, days=30)
    assert len(recent) == 1
    assert recent[0].total_trades == 5

    all_snaps = await get_performance_snapshots(db, portfolio.id, days=90)
    assert len(all_snaps) == 2


@pytest.mark.asyncio
async def test_get_snapshots_returned_newest_first(db: AsyncSession):
    portfolio = _make_portfolio(db)
    await db.flush()

    now = datetime.now(timezone.utc)
    for hours_ago in [5, 3, 1]:
        snap = PerformanceSnapshot(
            id=uuid.uuid4(),
            portfolio_id=portfolio.id,
            captured_at=now - timedelta(hours=hours_ago),
            total_trades=hours_ago,  # use hours_ago as a distinguishing value
            open_positions=0, winning_trades=0, losing_trades=0,
            win_rate=0.0, profit_factor=0.0, consecutive_wins=0,
            consecutive_losses=0, max_drawdown_pct=0.0, trades_per_day=0.0,
        )
        db.add(snap)
    await db.flush()

    snaps = await get_performance_snapshots(db, portfolio.id, days=1)
    assert snaps[0].total_trades == 1   # most recent (1 hour ago)
    assert snaps[-1].total_trades == 5  # oldest (5 hours ago)


# ---------------------------------------------------------------------------
# API: GET /analytics/snapshots
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_list_snapshots_empty(client: AsyncClient, auth_headers: dict):
    resp = await client.get("/api/v1/analytics/snapshots", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["items"] == []


@pytest.mark.asyncio
async def test_api_force_snapshot_then_list(client: AsyncClient, auth_headers: dict):
    # Force a snapshot
    post_resp = await client.post("/api/v1/analytics/snapshots", headers=auth_headers)
    assert post_resp.status_code == 201
    snap = post_resp.json()
    assert "id" in snap
    assert "captured_at" in snap
    assert snap["total_trades"] >= 0

    # Now list should have 1
    list_resp = await client.get("/api/v1/analytics/snapshots", headers=auth_headers)
    assert list_resp.status_code == 200
    data = list_resp.json()
    assert data["total"] == 1
    assert data["items"][0]["id"] == snap["id"]


@pytest.mark.asyncio
async def test_api_latest_snapshot_404_when_none(client: AsyncClient, auth_headers: dict):
    resp = await client.get("/api/v1/analytics/snapshots/latest", headers=auth_headers)
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_api_latest_snapshot_returns_most_recent(
    client: AsyncClient, auth_headers: dict
):
    # Create two snapshots
    await client.post("/api/v1/analytics/snapshots", headers=auth_headers)
    second = await client.post("/api/v1/analytics/snapshots", headers=auth_headers)

    latest = await client.get("/api/v1/analytics/snapshots/latest", headers=auth_headers)
    assert latest.status_code == 200
    assert latest.json()["id"] == second.json()["id"]


@pytest.mark.asyncio
async def test_api_snapshots_days_query_param(client: AsyncClient, auth_headers: dict):
    await client.post("/api/v1/analytics/snapshots", headers=auth_headers)

    resp_30 = await client.get(
        "/api/v1/analytics/snapshots?days=30", headers=auth_headers
    )
    resp_0 = await client.get(
        "/api/v1/analytics/snapshots?days=1", headers=auth_headers
    )
    assert resp_30.status_code == 200
    assert resp_0.status_code == 200
    # Both should include our just-created snapshot (it's within 1 day)
    assert resp_30.json()["total"] >= 1
    assert resp_0.json()["total"] >= 1
