"""
Tests for historical_guardrail_service.py

Covers:
  Rule 1 — Symbol performance:
    - enough samples + poor win_rate + net-negative pnl → BLOCK
    - enough samples + poor win_rate but pnl still positive → ALLOW
      (requires BOTH conditions to fire)
    - not enough samples → ALLOW (insufficient evidence)
    - good performance → ALLOW

  Rule 2 — Hour performance:
    - enough samples + poor win_rate at current hour → REDUCE
    - not enough samples → ALLOW
    - different hour (not current) is bad, current is fine → ALLOW

  Rule 3 — Event context escalation:
    - is_event_reduced=True + enough samples + poor ctx win_rate → BLOCK
    - is_event_reduced=False → rule not evaluated (ALLOW)
    - not enough samples → ALLOW

  Combined:
    - multiple rules fire → highest severity wins (BLOCK over REDUCE)

  Feature flag:
    - HISTORICAL_GUARDRAIL_ENABLED=False → always ALLOW regardless of data

All tests use the in-memory SQLite fixture from conftest.py.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio import Portfolio
from app.models.position import Position
from app.services.contextual_analytics_service import CTX_REDUCED
from app.services.historical_guardrail_service import check_historical_guardrail


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
    hour_utc: int = 10,
    event_context: str | None = None,
) -> Position:
    """Create a closed position opened at the given UTC hour today."""
    opened = datetime.now(timezone.utc).replace(
        hour=hour_utc, minute=0, second=0, microsecond=0
    )
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side="long",
        quantity=Decimal("100"),
        avg_entry_price=Decimal("1.1"),
        current_price=Decimal("1.1"),
        is_open=False,
        opened_at=opened,
        closed_at=opened + timedelta(hours=1),
        closed_price=Decimal("1.1"),
        realized_pnl=Decimal(str(pnl)),
        event_context=event_context,
    )
    db.add(pos)
    return pos


def _make_bad_symbol(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    symbol: str,
    n: int = 12,
    win_frac: float = 0.25,   # 25 % win rate
    win_pnl: float = 5.0,
    loss_pnl: float = -20.0,
    hour_utc: int = 10,
) -> None:
    """Insert n closed positions with the given win fraction and PnL."""
    wins   = int(n * win_frac)
    losses = n - wins
    for _ in range(wins):
        _closed(db, portfolio_id, symbol, pnl=win_pnl,  hour_utc=hour_utc)
    for _ in range(losses):
        _closed(db, portfolio_id, symbol, pnl=loss_pnl, hour_utc=hour_utc)


def _make_bad_hour(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    symbol: str,
    hour_utc: int,
    n: int = 6,
    win_frac: float = 0.20,
) -> None:
    wins   = int(n * win_frac)
    losses = n - wins
    for _ in range(wins):
        _closed(db, portfolio_id, symbol, pnl=5.0,   hour_utc=hour_utc)
    for _ in range(losses):
        _closed(db, portfolio_id, symbol, pnl=-15.0, hour_utc=hour_utc)


# ---------------------------------------------------------------------------
# Rule 1 — Symbol performance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_symbol_blocked_by_poor_performance(db: AsyncSession):
    """Symbol with low win_rate AND net-negative PnL → BLOCK."""
    port = _portfolio(db)
    # 12 trades, 25% win rate, large net loss
    _make_bad_symbol(db, port.id, "EURUSD", n=12, win_frac=0.25,
                     win_pnl=5.0, loss_pnl=-20.0)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=10,
    )
    assert result.action == "BLOCK"
    assert result.rule == "symbol_performance"
    assert "EURUSD" in result.reason
    assert "win_rate" in result.reason


@pytest.mark.asyncio
async def test_symbol_not_blocked_when_pnl_positive(db: AsyncSession):
    """Low win rate but positive total PnL (high reward) → ALLOW.
    Use hour_utc=10 for positions but query hour 3 (no data) so the
    hour rule cannot fire and only the symbol rule is tested.
    """
    port = _portfolio(db)
    # 12 trades, 25% win rate, but big wins outweigh small losses (net +255)
    _make_bad_symbol(db, port.id, "EURUSD", n=12, win_frac=0.25,
                     win_pnl=100.0, loss_pnl=-5.0, hour_utc=10)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=3,  # hour 3 has no data → hour rule skips
    )
    assert result.action == "ALLOW"


@pytest.mark.asyncio
async def test_symbol_not_blocked_when_insufficient_sample(db: AsyncSession):
    """Fewer trades than min_trades_symbol → symbol rule skipped → ALLOW."""
    port = _portfolio(db)
    # Only 5 trades (default min is 10); use hour 3 so hour rule skips too
    _make_bad_symbol(db, port.id, "EURUSD", n=5, win_frac=0.0,
                     win_pnl=0.0, loss_pnl=-50.0, hour_utc=10)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=3,  # hour 3 has no data → hour rule skips
    )
    assert result.action == "ALLOW"


@pytest.mark.asyncio
async def test_symbol_allowed_with_good_performance(db: AsyncSession):
    """Symbol with 65% win rate and positive PnL → ALLOW."""
    port = _portfolio(db)
    _make_bad_symbol(db, port.id, "EURUSD", n=12, win_frac=0.65,
                     win_pnl=15.0, loss_pnl=-10.0)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=10,
    )
    assert result.action == "ALLOW"


@pytest.mark.asyncio
async def test_symbol_not_blocked_when_no_history(db: AsyncSession):
    """No closed trades at all → ALLOW (nothing to evaluate)."""
    port = _portfolio(db)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=10,
    )
    assert result.action == "ALLOW"


# ---------------------------------------------------------------------------
# Rule 2 — Hour performance
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hour_reduces_on_poor_performance(db: AsyncSession):
    """Current UTC hour has poor win_rate with enough samples → REDUCE."""
    port = _portfolio(db)
    _make_bad_hour(db, port.id, "EURUSD", hour_utc=14, n=6, win_frac=0.17)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=14,
    )
    assert result.action == "REDUCE"
    assert result.rule == "hour_performance"
    assert "14" in result.reason


@pytest.mark.asyncio
async def test_hour_not_reduced_when_insufficient_sample(db: AsyncSession):
    """Fewer trades in this hour than min_trades_hour → rule skipped → ALLOW."""
    port = _portfolio(db)
    # Only 3 trades in hour 14 (default min is 5)
    _make_bad_hour(db, port.id, "EURUSD", hour_utc=14, n=3, win_frac=0.0)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=14,
    )
    assert result.action == "ALLOW"


@pytest.mark.asyncio
async def test_different_bad_hour_does_not_affect_current_hour(db: AsyncSession):
    """Hour 9 is bad but we're trading at hour 14 → ALLOW."""
    port = _portfolio(db)
    _make_bad_hour(db, port.id, "EURUSD", hour_utc=9, n=6, win_frac=0.17)
    # Hour 14 has good performance
    for _ in range(5):
        _closed(db, port.id, "EURUSD", pnl=10.0, hour_utc=14)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=14,
    )
    assert result.action == "ALLOW"


# ---------------------------------------------------------------------------
# Rule 3 — Event context escalation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_event_ctx_escalates_reduce_to_block(db: AsyncSession):
    """is_event_reduced + bad reduced context history → BLOCK."""
    port = _portfolio(db)
    # 10 event-reduced trades with 30% win rate (below threshold 40%)
    for _ in range(3):
        _closed(db, port.id, "EURUSD", pnl=5.0,   event_context=CTX_REDUCED)
    for _ in range(7):
        _closed(db, port.id, "EURUSD", pnl=-15.0, event_context=CTX_REDUCED)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=10,
        is_event_reduced_trade=True,
    )
    assert result.action == "BLOCK"
    assert result.rule == "event_context"
    assert "REDUCE to BLOCK" in result.reason


@pytest.mark.asyncio
async def test_event_ctx_rule_not_evaluated_when_not_reduced(db: AsyncSession):
    """is_event_reduced=False → Rule 3 never evaluates.
    All positions at hour 10, query at hour 3 → hour rule also skips.
    Symbol pnl = 3*5 + 7*(-15) = -90 > -100 threshold → symbol rule also skips.
    """
    port = _portfolio(db)
    for _ in range(3):
        _closed(db, port.id, "EURUSD", pnl=5.0,   event_context=CTX_REDUCED, hour_utc=10)
    for _ in range(7):
        _closed(db, port.id, "EURUSD", pnl=-15.0, event_context=CTX_REDUCED, hour_utc=10)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=3,   # no data at hour 3
        is_event_reduced_trade=False,
    )
    assert result.action == "ALLOW"


@pytest.mark.asyncio
async def test_event_ctx_not_escalated_when_insufficient_sample(db: AsyncSession):
    """Fewer event-reduced trades than min_trades_event_ctx → Rule 3 skipped.
    Positions at hour 10, query at hour 3 to isolate Rule 3 from hour rule.
    """
    port = _portfolio(db)
    # Only 5 reduced trades (default min is 8)
    for _ in range(5):
        _closed(db, port.id, "EURUSD", pnl=-20.0, event_context=CTX_REDUCED, hour_utc=10)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=3,   # no data at hour 3
        is_event_reduced_trade=True,
    )
    assert result.action == "ALLOW"


# ---------------------------------------------------------------------------
# Combined rules — highest severity wins
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_block_wins_over_reduce(db: AsyncSession):
    """When both symbol block and hour reduce fire, BLOCK is returned."""
    port = _portfolio(db)
    # Bad symbol (triggers BLOCK)
    _make_bad_symbol(db, port.id, "EURUSD", n=12, win_frac=0.25,
                     win_pnl=5.0, loss_pnl=-20.0, hour_utc=10)
    # Bad hour 10 (triggers REDUCE)
    _make_bad_hour(db, port.id, "EURUSD", hour_utc=10, n=6, win_frac=0.17)
    await db.flush()

    result = await check_historical_guardrail(
        db=db, portfolio_id=port.id,
        symbol="EURUSD", current_hour_utc=10,
    )
    assert result.action == "BLOCK"


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_feature_disabled_always_allows(db: AsyncSession):
    """HISTORICAL_GUARDRAIL_ENABLED=False → ALLOW regardless of data."""
    port = _portfolio(db)
    # Insert very bad data
    _make_bad_symbol(db, port.id, "EURUSD", n=20, win_frac=0.10,
                     win_pnl=1.0, loss_pnl=-50.0)
    await db.flush()

    with patch("app.services.historical_guardrail_service.settings") as mock_s:
        mock_s.HISTORICAL_GUARDRAIL_ENABLED = False
        result = await check_historical_guardrail(
            db=db, portfolio_id=port.id,
            symbol="EURUSD", current_hour_utc=10,
        )

    assert result.action == "ALLOW"
    assert result.rule == "none"
