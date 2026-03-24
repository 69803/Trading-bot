"""
Tests for contextual_analytics_service.py

Covers:
  get_performance_by_symbol:
    - empty portfolio → empty list
    - single symbol aggregation (win rate, avg_win, avg_loss, profit_factor, drawdown)
    - multiple symbols split correctly
    - open_positions counted per symbol

  get_performance_by_open_hour:
    - empty portfolio → empty list
    - trades grouped by UTC open hour
    - hour with only losses returns correct win_rate=0
    - multiple hours split correctly

  get_performance_by_event_context:
    - empty portfolio → all four buckets with total_trades=0
    - "reduced_size_due_to_event" stored context classified correctly
    - trade near high-impact event (retroactive via historical_events) → CTX_NEAR_HIGH
    - trade near medium-impact event → CTX_NEAR_MED
    - trade with no event in window → CTX_NO_EVENT
    - mixed portfolio: each trade lands in the right bucket
    - high-impact keyword (not just flag) triggers CTX_NEAR_HIGH
    - event from unrelated currency does not affect classification
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.historical_event import HistoricalEvent
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.services.contextual_analytics_service import (
    CTX_NEAR_HIGH,
    CTX_NEAR_MED,
    CTX_NO_EVENT,
    CTX_REDUCED,
    get_performance_by_event_context,
    get_performance_by_open_hour,
    get_performance_by_symbol,
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
    symbol: str,
    pnl: float,
    opened_at: datetime | None = None,
    event_context: str | None = None,
) -> Position:
    now = opened_at or datetime.now(timezone.utc)
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side="long",
        quantity=Decimal("100"),
        avg_entry_price=Decimal("1.1000"),
        current_price=Decimal("1.1050"),
        is_open=False,
        opened_at=now,
        closed_at=now + timedelta(hours=1),
        closed_price=Decimal("1.1050"),
        realized_pnl=Decimal(str(pnl)),
        event_context=event_context,
    )
    db.add(pos)
    return pos


def _open(db: AsyncSession, portfolio_id: uuid.UUID, symbol: str) -> Position:
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side="long",
        quantity=Decimal("100"),
        avg_entry_price=Decimal("1.1000"),
        current_price=Decimal("1.1050"),
        is_open=True,
        opened_at=datetime.now(timezone.utc),
        realized_pnl=Decimal("0"),
    )
    db.add(pos)
    return pos


def _event(
    db: AsyncSession,
    currency: str,
    impact: str,
    event_datetime_utc: datetime,
    event_name: str = "CPI m/m",
) -> HistoricalEvent:
    ev = HistoricalEvent(
        id=uuid.uuid4(),
        event_datetime_utc=event_datetime_utc,
        country="US",
        currency=currency,
        event_name=event_name,
        impact=impact,
        actual=None,
        forecast="0.2%",
        previous="0.1%",
        source="test",
    )
    db.add(ev)
    return ev


def _utc(delta_minutes: float = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=delta_minutes)


# ===========================================================================
# get_performance_by_symbol
# ===========================================================================

@pytest.mark.asyncio
async def test_by_symbol_empty_portfolio(db: AsyncSession):
    port = _portfolio(db)
    await db.flush()
    result = await get_performance_by_symbol(db, port.id)
    assert result == []


@pytest.mark.asyncio
async def test_by_symbol_single_symbol_aggregation(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, "EURUSD", pnl=20.0)
    _closed(db, port.id, "EURUSD", pnl=30.0)
    _closed(db, port.id, "EURUSD", pnl=-10.0)
    await db.flush()

    result = await get_performance_by_symbol(db, port.id)
    assert len(result) == 1
    s = result[0]
    assert s.symbol == "EURUSD"
    assert s.total_trades == 3
    assert s.winning_trades == 2
    assert s.losing_trades == 1
    assert s.win_rate == pytest.approx(2 / 3, abs=0.01)
    assert s.total_pnl == pytest.approx(40.0, abs=0.01)
    assert s.avg_win == pytest.approx(25.0, abs=0.01)   # (20+30)/2
    assert s.avg_loss == pytest.approx(10.0, abs=0.01)  # abs(-10)/1
    assert s.profit_factor == pytest.approx(5.0, abs=0.01)  # 50/10


@pytest.mark.asyncio
async def test_by_symbol_multiple_symbols_split(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, "EURUSD", pnl=10.0)
    _closed(db, port.id, "EURUSD", pnl=-5.0)
    _closed(db, port.id, "GBPUSD", pnl=20.0)
    _closed(db, port.id, "GBPUSD", pnl=15.0)
    _closed(db, port.id, "GBPUSD", pnl=-8.0)
    await db.flush()

    result = await get_performance_by_symbol(db, port.id)
    symbols = {r.symbol: r for r in result}
    assert set(symbols.keys()) == {"EURUSD", "GBPUSD"}
    assert symbols["EURUSD"].total_trades == 2
    assert symbols["GBPUSD"].total_trades == 3
    assert symbols["GBPUSD"].winning_trades == 2


@pytest.mark.asyncio
async def test_by_symbol_open_positions_counted(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, "EURUSD", pnl=5.0)
    _open(db, port.id, "EURUSD")
    _open(db, port.id, "EURUSD")
    _open(db, port.id, "GBPUSD")
    await db.flush()

    result = await get_performance_by_symbol(db, port.id)
    sym = {r.symbol: r for r in result}
    assert sym["EURUSD"].open_positions == 2
    # GBPUSD has no closed trades → not in result, but open_positions not checked here
    # EURUSD open count is what matters
    assert sym["EURUSD"].open_positions == 2


@pytest.mark.asyncio
async def test_by_symbol_all_losses_profit_factor_zero(db: AsyncSession):
    port = _portfolio(db)
    _closed(db, port.id, "EURUSD", pnl=-10.0)
    _closed(db, port.id, "EURUSD", pnl=-5.0)
    await db.flush()

    result = await get_performance_by_symbol(db, port.id)
    s = result[0]
    assert s.win_rate == 0.0
    assert s.profit_factor == 0.0    # no gross profit


# ===========================================================================
# get_performance_by_open_hour
# ===========================================================================

@pytest.mark.asyncio
async def test_by_hour_empty_portfolio(db: AsyncSession):
    port = _portfolio(db)
    await db.flush()
    result = await get_performance_by_open_hour(db, port.id)
    assert result == []


@pytest.mark.asyncio
async def test_by_hour_groups_correctly(db: AsyncSession):
    port = _portfolio(db)
    base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hour9  = base.replace(hour=9)
    hour14 = base.replace(hour=14)

    _closed(db, port.id, "EURUSD", pnl=10.0, opened_at=hour9)
    _closed(db, port.id, "EURUSD", pnl=-3.0, opened_at=hour9)
    _closed(db, port.id, "EURUSD", pnl=20.0, opened_at=hour14)
    await db.flush()

    result = await get_performance_by_open_hour(db, port.id)
    by_hour = {r.hour_utc: r for r in result}

    assert 9 in by_hour
    assert 14 in by_hour
    assert by_hour[9].total_trades == 2
    assert by_hour[9].winning_trades == 1
    assert by_hour[9].win_rate == pytest.approx(0.5, abs=0.01)
    assert by_hour[9].total_pnl == pytest.approx(7.0, abs=0.01)
    assert by_hour[14].total_trades == 1
    assert by_hour[14].win_rate == pytest.approx(1.0, abs=0.01)


@pytest.mark.asyncio
async def test_by_hour_all_losses(db: AsyncSession):
    port = _portfolio(db)
    hour10 = datetime.now(timezone.utc).replace(hour=10, minute=0, second=0, microsecond=0)
    _closed(db, port.id, "EURUSD", pnl=-5.0, opened_at=hour10)
    _closed(db, port.id, "EURUSD", pnl=-8.0, opened_at=hour10)
    await db.flush()

    result = await get_performance_by_open_hour(db, port.id)
    assert result[0].win_rate == 0.0
    assert result[0].avg_pnl == pytest.approx(-6.5, abs=0.01)


# ===========================================================================
# get_performance_by_event_context
# ===========================================================================

@pytest.mark.asyncio
async def test_by_event_context_empty_portfolio(db: AsyncSession):
    port = _portfolio(db)
    await db.flush()
    result = await get_performance_by_event_context(db, port.id)
    # All four buckets returned, all with total_trades=0
    assert len(result) == 4
    contexts = {r.context for r in result}
    assert contexts == {CTX_REDUCED, CTX_NEAR_HIGH, CTX_NEAR_MED, CTX_NO_EVENT}
    assert all(r.total_trades == 0 for r in result)


@pytest.mark.asyncio
async def test_by_event_context_reduced_from_stored_field(db: AsyncSession):
    """A position with event_context='reduced_size_due_to_event' → CTX_REDUCED."""
    port = _portfolio(db)
    _closed(db, port.id, "EURUSD", pnl=15.0, event_context=CTX_REDUCED)
    _closed(db, port.id, "EURUSD", pnl=-5.0, event_context=CTX_REDUCED)
    await db.flush()

    result = await get_performance_by_event_context(db, port.id)
    by_ctx = {r.context: r for r in result}

    reduced = by_ctx[CTX_REDUCED]
    assert reduced.total_trades == 2
    assert reduced.winning_trades == 1
    assert reduced.win_rate == pytest.approx(0.5, abs=0.01)
    assert reduced.total_pnl == pytest.approx(10.0, abs=0.01)

    # Other buckets should be empty
    assert by_ctx[CTX_NO_EVENT].total_trades == 0


@pytest.mark.asyncio
async def test_by_event_context_near_high_impact_retroactive(db: AsyncSession):
    """Trade opened near a high-impact event in historical_events → CTX_NEAR_HIGH."""
    port = _portfolio(db)
    opened = _utc(0)
    _closed(db, port.id, "EURUSD", pnl=10.0, opened_at=opened, event_context="normal")
    # USD high-impact event 20 min after open — within default 60 min window
    _event(db, currency="USD", impact="high",
           event_datetime_utc=opened + timedelta(minutes=20))
    await db.flush()

    result = await get_performance_by_event_context(db, port.id, window_minutes=60)
    by_ctx = {r.context: r for r in result}

    assert by_ctx[CTX_NEAR_HIGH].total_trades == 1
    assert by_ctx[CTX_NO_EVENT].total_trades == 0


@pytest.mark.asyncio
async def test_by_event_context_near_medium_impact_retroactive(db: AsyncSession):
    """Trade opened near a medium-impact event → CTX_NEAR_MED."""
    port = _portfolio(db)
    opened = _utc(0)
    _closed(db, port.id, "EURUSD", pnl=-8.0, opened_at=opened)
    # "Retail Sales" is not in HIGH_IMPACT_KEYWORDS → stays medium
    _event(db, currency="EUR", impact="medium", event_name="Retail Sales",
           event_datetime_utc=opened + timedelta(minutes=30))
    await db.flush()

    result = await get_performance_by_event_context(db, port.id, window_minutes=60)
    by_ctx = {r.context: r for r in result}

    assert by_ctx[CTX_NEAR_MED].total_trades == 1
    assert by_ctx[CTX_NEAR_HIGH].total_trades == 0


@pytest.mark.asyncio
async def test_by_event_context_no_event(db: AsyncSession):
    """Trade with no event nearby and no stored context → CTX_NO_EVENT."""
    port = _portfolio(db)
    opened = _utc(0)
    _closed(db, port.id, "EURUSD", pnl=5.0, opened_at=opened)
    # Event is 120 min away, outside 60 min window
    _event(db, currency="USD", impact="high",
           event_datetime_utc=opened + timedelta(minutes=120))
    await db.flush()

    result = await get_performance_by_event_context(db, port.id, window_minutes=60)
    by_ctx = {r.context: r for r in result}

    assert by_ctx[CTX_NO_EVENT].total_trades == 1
    assert by_ctx[CTX_NEAR_HIGH].total_trades == 0


@pytest.mark.asyncio
async def test_by_event_context_mixed_portfolio(db: AsyncSession):
    """Multiple trades land in correct buckets."""
    port = _portfolio(db)
    base = _utc(0)

    # 1. Reduced-size trade (stored context)
    _closed(db, port.id, "EURUSD", pnl=10.0, opened_at=base,
            event_context=CTX_REDUCED)

    # 2. Normal trade near high-impact event (retroactive)
    t2 = base + timedelta(hours=2)
    _closed(db, port.id, "EURUSD", pnl=-5.0, opened_at=t2, event_context="normal")
    _event(db, currency="USD", impact="high",
           event_datetime_utc=t2 + timedelta(minutes=15))

    # 3. Trade near medium-impact event (retroactive) — use non-keyword name
    t3 = base + timedelta(hours=4)
    _closed(db, port.id, "GBPUSD", pnl=8.0, opened_at=t3)
    _event(db, currency="GBP", impact="medium", event_name="Retail Sales",
           event_datetime_utc=t3 - timedelta(minutes=20))

    # 4. No-event trade
    _closed(db, port.id, "USDJPY", pnl=-3.0, opened_at=base + timedelta(hours=6))

    await db.flush()

    result = await get_performance_by_event_context(db, port.id, window_minutes=60)
    by_ctx = {r.context: r for r in result}

    assert by_ctx[CTX_REDUCED].total_trades == 1
    assert by_ctx[CTX_NEAR_HIGH].total_trades == 1
    assert by_ctx[CTX_NEAR_MED].total_trades == 1
    assert by_ctx[CTX_NO_EVENT].total_trades == 1


@pytest.mark.asyncio
async def test_by_event_context_keyword_triggers_high_impact(db: AsyncSession):
    """Event tagged 'medium' but name contains 'Non-Farm' → classified as high-impact."""
    port = _portfolio(db)
    opened = _utc(0)
    _closed(db, port.id, "EURUSD", pnl=7.0, opened_at=opened)
    _event(db, currency="USD", impact="medium",
           event_name="Non-Farm Payrolls",
           event_datetime_utc=opened + timedelta(minutes=10))
    await db.flush()

    result = await get_performance_by_event_context(db, port.id, window_minutes=60)
    by_ctx = {r.context: r for r in result}

    assert by_ctx[CTX_NEAR_HIGH].total_trades == 1
    assert by_ctx[CTX_NEAR_MED].total_trades == 0


@pytest.mark.asyncio
async def test_by_event_context_unrelated_currency_ignored(db: AsyncSession):
    """A JPY event does not affect an EURUSD trade classification."""
    port = _portfolio(db)
    opened = _utc(0)
    _closed(db, port.id, "EURUSD", pnl=5.0, opened_at=opened)
    # JPY event, but EURUSD only cares about EUR and USD
    _event(db, currency="JPY", impact="high",
           event_datetime_utc=opened + timedelta(minutes=5))
    await db.flush()

    result = await get_performance_by_event_context(db, port.id, window_minutes=60)
    by_ctx = {r.context: r for r in result}

    assert by_ctx[CTX_NO_EVENT].total_trades == 1
    assert by_ctx[CTX_NEAR_HIGH].total_trades == 0
