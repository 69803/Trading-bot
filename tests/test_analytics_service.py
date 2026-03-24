"""
Unit tests for the analytics service.

Uses the in-memory SQLite fixture from conftest.py.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from app.models.portfolio import Portfolio
from app.models.position import Position
from app.services.analytics_service import (
    count_consecutive_losses,
    count_trades_last_hour,
    get_daily_pnl_series,
    get_performance_stats,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_portfolio(db) -> Portfolio:
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
    db,
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


def _make_open_position(db, portfolio_id: uuid.UUID) -> Position:
    p = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("1000"),
        avg_entry_price=Decimal("1.1000"),
        current_price=Decimal("1.1050"),
        is_open=True,
        opened_at=datetime.now(timezone.utc),
    )
    db.add(p)
    return p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_portfolio_returns_zero_stats(db):
    portfolio = _make_portfolio(db)
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.total_trades == 0
    assert stats.total_pnl == 0.0
    assert stats.win_rate == 0.0


@pytest.mark.asyncio
async def test_win_rate_calculation(db):
    portfolio = _make_portfolio(db)
    _make_closed_position(db, portfolio.id, pnl=10.0)
    _make_closed_position(db, portfolio.id, pnl=5.0)
    _make_closed_position(db, portfolio.id, pnl=-3.0)
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.total_trades == 3
    assert stats.winning_trades == 2
    assert stats.losing_trades == 1
    assert stats.win_rate == pytest.approx(2 / 3, abs=0.01)


@pytest.mark.asyncio
async def test_total_pnl(db):
    portfolio = _make_portfolio(db)
    _make_closed_position(db, portfolio.id, pnl=100.0)
    _make_closed_position(db, portfolio.id, pnl=-30.0)
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.total_pnl == pytest.approx(70.0, abs=0.01)


@pytest.mark.asyncio
async def test_profit_factor(db):
    portfolio = _make_portfolio(db)
    _make_closed_position(db, portfolio.id, pnl=60.0)
    _make_closed_position(db, portfolio.id, pnl=40.0)
    _make_closed_position(db, portfolio.id, pnl=-20.0)
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    # gross profit = 100, gross loss = 20 → profit factor = 5.0
    assert stats.profit_factor == pytest.approx(5.0, abs=0.01)


@pytest.mark.asyncio
async def test_consecutive_wins(db):
    portfolio = _make_portfolio(db)
    base = datetime.now(timezone.utc)
    _make_closed_position(db, portfolio.id, pnl=-5.0,  closed_at=base - timedelta(hours=3))
    _make_closed_position(db, portfolio.id, pnl=10.0,  closed_at=base - timedelta(hours=2))
    _make_closed_position(db, portfolio.id, pnl=20.0,  closed_at=base - timedelta(hours=1))
    _make_closed_position(db, portfolio.id, pnl=15.0,  closed_at=base)
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.consecutive_wins == 3
    assert stats.consecutive_losses == 0


@pytest.mark.asyncio
async def test_consecutive_losses(db):
    portfolio = _make_portfolio(db)
    base = datetime.now(timezone.utc)
    _make_closed_position(db, portfolio.id, pnl=20.0,  closed_at=base - timedelta(hours=3))
    _make_closed_position(db, portfolio.id, pnl=-5.0,  closed_at=base - timedelta(hours=2))
    _make_closed_position(db, portfolio.id, pnl=-8.0,  closed_at=base - timedelta(hours=1))
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.consecutive_losses == 2
    assert stats.consecutive_wins == 0


@pytest.mark.asyncio
async def test_open_positions_counted(db):
    portfolio = _make_portfolio(db)
    _make_open_position(db, portfolio.id)
    _make_open_position(db, portfolio.id)
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.open_positions == 2


@pytest.mark.asyncio
async def test_daily_pnl_series(db):
    portfolio = _make_portfolio(db)
    today = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    _make_closed_position(db, portfolio.id, pnl=10.0, closed_at=today)
    _make_closed_position(db, portfolio.id, pnl=5.0,  closed_at=today)
    _make_closed_position(db, portfolio.id, pnl=-3.0, closed_at=yesterday)
    await db.flush()

    series = await get_daily_pnl_series(db, portfolio.id, days=7)
    assert len(series) == 2
    today_entry = next((d for d in series if d.date == today.date()), None)
    assert today_entry is not None
    assert today_entry.realized_pnl == pytest.approx(15.0, abs=0.01)
    assert today_entry.trades_closed == 2


@pytest.mark.asyncio
async def test_count_consecutive_losses(db):
    portfolio = _make_portfolio(db)
    base = datetime.now(timezone.utc)
    _make_closed_position(db, portfolio.id, pnl=10.0, closed_at=base - timedelta(hours=3))
    _make_closed_position(db, portfolio.id, pnl=-5.0, closed_at=base - timedelta(hours=2))
    _make_closed_position(db, portfolio.id, pnl=-3.0, closed_at=base - timedelta(hours=1))
    await db.flush()

    count = await count_consecutive_losses(db, portfolio.id)
    assert count == 2


@pytest.mark.asyncio
async def test_count_trades_last_hour(db):
    portfolio = _make_portfolio(db)
    now = datetime.now(timezone.utc)
    # 2 positions opened in the last 30 minutes
    for _ in range(2):
        p = Position(
            id=uuid.uuid4(),
            portfolio_id=portfolio.id,
            symbol="EURUSD",
            side="long",
            quantity=Decimal("1000"),
            avg_entry_price=Decimal("1.1"),
            current_price=Decimal("1.1"),
            is_open=True,
            opened_at=now - timedelta(minutes=30),
        )
        db.add(p)
    # 1 position opened 90 minutes ago (outside window)
    p_old = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio.id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("1000"),
        avg_entry_price=Decimal("1.1"),
        current_price=Decimal("1.1"),
        is_open=True,
        opened_at=now - timedelta(minutes=90),
    )
    db.add(p_old)
    await db.flush()

    count = await count_trades_last_hour(db, portfolio.id)
    assert count == 2


@pytest.mark.asyncio
async def test_max_drawdown_non_negative(db):
    portfolio = _make_portfolio(db)
    base = datetime.now(timezone.utc)
    # Win then big loss sequence
    _make_closed_position(db, portfolio.id, pnl=100.0, closed_at=base - timedelta(hours=3))
    _make_closed_position(db, portfolio.id, pnl=-60.0, closed_at=base - timedelta(hours=2))
    _make_closed_position(db, portfolio.id, pnl=20.0,  closed_at=base - timedelta(hours=1))
    await db.flush()

    stats = await get_performance_stats(db, portfolio.id)
    assert stats.max_drawdown_pct >= 0
    assert stats.max_drawdown_pct < 1.0  # should be less than 100%
