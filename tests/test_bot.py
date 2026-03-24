"""
Tests for the auto-trading bot:
- Bot cycle executes and opens trade when signal fires
- No duplicate positions per symbol
- TP/SL closes position
- Limit order fill when price crosses
- Portfolio snapshot creation
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bot_state import BotState
from app.models.portfolio import Portfolio
from app.models.portfolio_snapshot import PortfolioSnapshot
from app.models.position import Position
from app.models.strategy_config import StrategyConfig
from app.services import bot_service
from app.services.expert_filters_service import ExpertFilterResult
from app.services.portfolio_service import take_portfolio_snapshot

_EXPERT_ALLOW = ExpertFilterResult(action="ALLOW", filter_name="none", reason="test bypass")

# Patches for both expert filter stages so bot tests are session/time independent
def _expert_patches():
    return (
        patch("app.services.bot_service.check_pre_analysis_filters",
              new_callable=AsyncMock, return_value=_EXPERT_ALLOW),
        patch("app.services.bot_service.check_post_analysis_filters",
              return_value=_EXPERT_ALLOW),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_candles(closes: list[float]) -> list[dict]:
    """Build candles with recent timestamps so the staleness check passes."""
    now = datetime.now(timezone.utc)
    n = len(closes)
    return [
        {
            "timestamp": (now - timedelta(hours=n - i)).isoformat(),
            "open": c,
            "high": c * 1.001,
            "low": c * 0.999,
            "close": c,
            "volume": 1000.0,
        }
        for i, c in enumerate(closes)
    ]


async def _get_portfolio(db: AsyncSession, user_id) -> Portfolio:
    result = await db.execute(select(Portfolio).where(Portfolio.user_id == user_id))
    return result.scalar_one_or_none()


async def _get_bot_state(db: AsyncSession, user_id) -> BotState:
    result = await db.execute(select(BotState).where(BotState.user_id == user_id))
    return result.scalar_one_or_none()


async def _open_positions(db: AsyncSession, portfolio_id) -> list[Position]:
    result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == True,  # noqa: E712
        )
    )
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# 1. Start / Stop via API
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bot_starts_and_stops(client: AsyncClient, auth_headers: dict):
    """Start and stop via API; state persists in DB."""
    start = await client.post("/api/v1/bot/start", headers=auth_headers)
    assert start.status_code == 200
    assert start.json()["is_running"] is True

    stop = await client.post("/api/v1/bot/stop", headers=auth_headers)
    assert stop.status_code == 200
    assert stop.json()["is_running"] is False


# ---------------------------------------------------------------------------
# 2. Bot cycle opens a trade when buy signal fires
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bot_cycle_opens_trade_on_buy_signal(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    When the bot cycle runs with a guaranteed buy signal (EMA fast > slow, RSI oversold),
    it should open a long position.
    """
    # Start bot via API
    await client.post("/api/v1/bot/start", headers=auth_headers)

    # Retrieve user_id from /auth/me
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])

    portfolio = await _get_portfolio(db, user_id)
    balance_before = float(portfolio.cash_balance)

    # Build a candle series that guarantees a buy signal:
    # - EMA50 > EMA200 (uptrend): prices strictly increasing for 250 bars
    # - RSI oversold: last 15 bars drop sharply
    n = 250
    closes = [1.0 + i * 0.005 for i in range(n)]
    for i in range(n - 15, n):
        closes[i] = closes[i - 1] * 0.985

    candles = _make_candles(closes)
    current_px = closes[-1]

    def _candles_side_effect(symbol, timeframe, limit=100):
        # _evaluate_open_positions calls with limit=3 — return stable near-price candles
        # so no TP/SL is falsely triggered by historical price extremes
        if limit <= 5:
            return _make_candles([current_px] * limit)
        return candles

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            side_effect=_candles_side_effect,
        ),
        patch(
            "app.services.market_data_router.market_data_router.get_current_price",
            new_callable=AsyncMock,
            return_value=current_px,
        ),
        patch("app.services.bot_service.get_news", new_callable=AsyncMock, return_value=[]),
        patch("app.services.bot_service._is_active_trading_session", return_value=True),
        patch("app.services.bot_service.check_pre_analysis_filters", new_callable=AsyncMock, return_value=_EXPERT_ALLOW),
        patch("app.services.bot_service.check_post_analysis_filters", return_value=_EXPERT_ALLOW),
    ):
        state = await _get_bot_state(db, user_id)
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    # Reload portfolio
    await db.refresh(portfolio)
    positions = await _open_positions(db, portfolio.id)

    eurusd = [p for p in positions if p.symbol == "EURUSD"]
    assert len(eurusd) == 1, "Expected one EURUSD long position after buy signal"
    assert eurusd[0].side == "long"
    assert float(portfolio.cash_balance) < balance_before


# ---------------------------------------------------------------------------
# 3. No duplicate positions per symbol
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bot_cycle_no_duplicate_positions(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """Running the bot cycle twice should not open a second position."""
    await client.post("/api/v1/bot/start", headers=auth_headers)

    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])

    n = 250
    closes = [1.0 + i * 0.005 for i in range(n)]
    for i in range(n - 15, n):
        closes[i] = closes[i - 1] * 0.985

    candles = _make_candles(closes)
    current_px = closes[-1]

    def _candles_side_effect(symbol, timeframe, limit=100):
        if limit <= 5:
            return _make_candles([current_px] * limit)
        return candles

    with (
        patch("app.services.bot_service._market_data_router_for_candles.get_candles", new_callable=AsyncMock, side_effect=_candles_side_effect),
        patch("app.services.market_data_router.market_data_router.get_current_price", new_callable=AsyncMock, return_value=current_px),
        patch("app.services.bot_service.get_news", new_callable=AsyncMock, return_value=[]),
        patch("app.services.bot_service._is_active_trading_session", return_value=True),
        patch("app.services.bot_service.check_pre_analysis_filters", new_callable=AsyncMock, return_value=_EXPERT_ALLOW),
        patch("app.services.bot_service.check_post_analysis_filters", return_value=_EXPERT_ALLOW),
    ):
        state = await _get_bot_state(db, user_id)
        # First cycle — should open
        await bot_service._run_user_cycle(db, state)
        await db.commit()
        # Second cycle — should NOT open a duplicate
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    portfolio = await _get_portfolio(db, user_id)
    positions = await _open_positions(db, portfolio.id)

    eurusd_positions = [p for p in positions if p.symbol == "EURUSD"]
    assert len(eurusd_positions) == 1, "Should have exactly one open EURUSD position"


# ---------------------------------------------------------------------------
# 4. TP/SL closes an existing position
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bot_cycle_closes_position_on_take_profit(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    If a position has take_profit_price and current price >= TP, the cycle should
    close the position.
    """
    await client.post("/api/v1/bot/start", headers=auth_headers)

    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])

    portfolio = await _get_portfolio(db, user_id)

    # Manually create an open long position with a TP set
    entry_price = Decimal("1.08500")
    tp_price = Decimal("1.09000")
    position = Position(
        id=uuid4(),
        portfolio_id=portfolio.id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("10"),
        avg_entry_price=entry_price,
        take_profit_price=tp_price,
        stop_loss_price=Decimal("1.07000"),
        is_open=True,
        opened_at=datetime.now(timezone.utc),
    )
    db.add(position)
    await db.commit()

    # Current price above TP — bot should close
    price_above_tp = 1.09500

    # Need enough candles for indicators; last candle close drives current_price
    n = 250
    closes = [1.085 + i * 0.0001 for i in range(n - 1)] + [price_above_tp]
    candles = _make_candles(closes)

    with (
        patch("app.services.bot_service._market_data_router_for_candles.get_candles", new_callable=AsyncMock, return_value=candles),
        patch("app.services.market_data_router.market_data_router.get_current_price", new_callable=AsyncMock, return_value=price_above_tp),
        patch("app.services.bot_service._is_active_trading_session", return_value=True),
        patch("app.services.bot_service.check_pre_analysis_filters", new_callable=AsyncMock, return_value=_EXPERT_ALLOW),
        patch("app.services.bot_service.check_post_analysis_filters", return_value=_EXPERT_ALLOW),
    ):
        state = await _get_bot_state(db, user_id)
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    await db.refresh(position)
    assert not position.is_open, "Position should be closed after TP hit"


# ---------------------------------------------------------------------------
# 5. Limit order fill via fill_pending_limit_orders
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fill_pending_limit_order_when_price_crosses(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """A pending limit buy order fills when current price drops to/below limit."""
    # Place a limit buy order with a very high limit price (will cross immediately)
    resp = await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={
            "symbol": "EURUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": 10,
            "limit_price": 1.20,   # above EURUSD market (~1.085): buy fills when price <= 1.20
        },
    )
    assert resp.status_code == 201
    order_data = resp.json()
    assert order_data["status"] == "pending"
    order_id = order_data["id"]

    # Commit and expire the test session so it sees the committed order
    await db.commit()
    db.expire_all()

    # Now run the limit-order filler; current price is ~1.085 which is <= 9999
    await bot_service.fill_pending_limit_orders(db)

    # Check the order is now filled
    resp2 = await client.get(f"/api/v1/orders/{order_id}", headers=auth_headers)
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "filled"


# ---------------------------------------------------------------------------
# 6. Portfolio snapshot creation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_portfolio_snapshot_is_created(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """take_portfolio_snapshot should insert a PortfolioSnapshot row."""
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])

    portfolio = await _get_portfolio(db, user_id)

    # Count snapshots before
    result_before = await db.execute(
        select(PortfolioSnapshot).where(PortfolioSnapshot.portfolio_id == portfolio.id)
    )
    count_before = len(list(result_before.scalars().all()))

    await take_portfolio_snapshot(db, portfolio.id)
    await db.commit()

    result_after = await db.execute(
        select(PortfolioSnapshot).where(PortfolioSnapshot.portfolio_id == portfolio.id)
    )
    snapshots = list(result_after.scalars().all())
    assert len(snapshots) == count_before + 1

    snap = snapshots[-1]
    assert float(snap.total_value) > 0
    assert float(snap.cash) > 0


# ---------------------------------------------------------------------------
# 7. Cycle log is updated
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bot_cycle_updates_last_log(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """After a cycle, BotState.cycles_run increments and last_log is set."""
    await client.post("/api/v1/bot/start", headers=auth_headers)

    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])

    state = await _get_bot_state(db, user_id)
    cycles_before = state.cycles_run or 0

    n = 250
    closes = [1.085] * n
    candles = _make_candles(closes)

    with (
        patch("app.services.bot_service._market_data_router_for_candles.get_candles", new_callable=AsyncMock, return_value=candles),
        patch("app.services.market_data_router.market_data_router.get_current_price", new_callable=AsyncMock, return_value=1.085),
    ):
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    await db.refresh(state)
    assert (state.cycles_run or 0) == cycles_before + 1
    assert state.last_log is not None
    assert state.last_cycle_at is not None


# ---------------------------------------------------------------------------
# 8. Regression tests — TP/SL position close logic
# ---------------------------------------------------------------------------

def _make_position(
    portfolio_id,
    *,
    symbol: str = "EURUSD",
    side: str = "long",
    entry: str = "1.08000",
    tp: str | None = None,
    sl: str | None = None,
    qty: str = "10",
    invest: str = "100",
) -> Position:
    pos = Position(
        id=uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side=side,
        quantity=Decimal(qty),
        avg_entry_price=Decimal(entry),
        take_profit_price=Decimal(tp) if tp else None,
        stop_loss_price=Decimal(sl) if sl else None,
        investment_amount=Decimal(invest),
        is_open=True,
        opened_at=datetime.now(timezone.utc),
        realized_pnl=Decimal("0"),
    )
    return pos


@pytest.mark.asyncio
async def test_evaluate_long_position_closes_on_take_profit(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    _evaluate_open_positions closes a LONG when live price >= TP.
    PnL must be positive (profitable trade).
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08000", tp="1.09000", sl="1.07000",
    )
    db.add(pos)
    await db.flush()

    price_above_tp = 1.09500  # clearly above TP=1.09000

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=price_above_tp,
        ),
        # Empty candles so candle wicks = live_price; no SL-wick interference
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, "Expected one close message for LONG TP hit"
    assert "take_profit" in close_msgs[0]
    assert remaining == []

    await db.refresh(pos)
    assert not pos.is_open, "LONG position must be closed after TP hit"
    assert float(pos.realized_pnl) > 0, "Profitable LONG must yield positive PnL"


@pytest.mark.asyncio
async def test_evaluate_short_position_closes_on_take_profit(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    _evaluate_open_positions closes a SHORT when live price <= TP.
    For a short: entry=1.08, TP=1.07 (price drops = profit).
    Regression: SHORT TP direction was historically the most likely to be stuck.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="short",
        entry="1.08000",
        tp="1.07000",   # SHORT TP is BELOW entry
        sl="1.09000",   # SHORT SL is ABOVE entry
    )
    db.add(pos)
    await db.flush()

    price_below_tp = 1.06500  # below TP=1.07000 → close profitably

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=price_below_tp,
        ),
        # Empty candles so candle wicks = live_price; prevents GBM candle highs
        # from accidentally triggering SL (1.09000) instead of TP (1.07000)
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, "Expected one close message for SHORT TP hit"
    assert "take_profit" in close_msgs[0]
    assert remaining == []

    await db.refresh(pos)
    assert not pos.is_open, "SHORT position must be closed after TP hit"
    assert float(pos.realized_pnl) > 0, "Profitable SHORT must yield positive PnL"


@pytest.mark.asyncio
async def test_tp_closes_even_outside_trading_session(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    BUG REGRESSION: TP/SL must fire even when the session filter blocks new trades.

    Before the fix, _is_active_trading_session() returning False caused an early
    return at the top of _process_symbol — BEFORE the TP/SL check.  Positions
    could never close during the 16 hours/day outside London+NY windows.

    After the fix, _evaluate_open_positions runs FIRST, unconditionally.
    """
    await client.post("/api/v1/bot/start", headers=auth_headers)

    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08500", tp="1.09000", sl="1.07000",
    )
    db.add(pos)
    await db.commit()

    price_above_tp = 1.09500

    with (
        # Simulate outside-session hours — no new trades should open
        patch("app.services.bot_service._is_active_trading_session", return_value=False),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=price_above_tp,
        ),
    ):
        state = await _get_bot_state(db, user_id)
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    await db.refresh(pos)
    assert not pos.is_open, (
        "Position must close even outside trading session — "
        "TP/SL must not be blocked by the session filter (regression)"
    )


@pytest.mark.asyncio
async def test_multiple_open_positions_all_evaluated(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    _evaluate_open_positions must evaluate ALL open positions, not just the first.
    Two positions on the same symbol both hitting TP should both close.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos1 = _make_position(
        portfolio.id, side="long",
        entry="1.08000", tp="1.09000", sl="1.07000", qty="5", invest="50",
    )
    pos2 = _make_position(
        portfolio.id, side="long",
        entry="1.08000", tp="1.09000", sl="1.07000", qty="5", invest="50",
    )
    db.add(pos1)
    db.add(pos2)
    await db.flush()

    price_above_tp = 1.09500

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=price_above_tp,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 2, "Both positions must close when both hit TP"
    assert remaining == [], "No positions should remain open"

    await db.refresh(pos1)
    await db.refresh(pos2)
    assert not pos1.is_open, "First position must be closed"
    assert not pos2.is_open, "Second position must be closed"


# ---------------------------------------------------------------------------
# 9. Intrabar TP/SL detection via candle high/low
# ---------------------------------------------------------------------------

def _make_ohlcv_candle(close: float, high: float, low: float) -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "open": close,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1000.0,
    }


@pytest.mark.asyncio
async def test_long_tp_triggered_by_candle_wick_high(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    ROOT CAUSE REGRESSION: A LONG position must close when the candle HIGH
    crosses TP even if the current live price is still below TP.

    Scenario:
      - LONG entry=1.08000, TP=1.09000
      - live_price=1.08800  (below TP — live-only check would NOT close)
      - candle.high=1.09200 (above TP — intrabar wick touched TP)
      → position MUST close at TP=1.09000
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08000", tp="1.09000", sl="1.07000",
    )
    db.add(pos)
    await db.flush()

    live_price_below_tp = 1.08800   # does NOT cross TP by itself
    candle_with_high_wick = _make_ohlcv_candle(close=1.08800, high=1.09200, low=1.08500)

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=live_price_below_tp,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[candle_with_high_wick],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, (
        "LONG position should close when candle wick high crosses TP, "
        "even if live price is below TP"
    )
    assert "take_profit" in close_msgs[0]
    assert remaining == []

    await db.refresh(pos)
    assert not pos.is_open, "LONG must be closed after intrabar wick touched TP"
    # Close price should be exactly at TP (1.09000), not at live price (1.08800)
    assert float(pos.closed_price) == pytest.approx(1.09000, abs=1e-5), (
        "Should close at exact TP level, not at live price snapshot"
    )
    assert float(pos.realized_pnl) > 0, "Profitable LONG: PnL must be positive"


@pytest.mark.asyncio
async def test_short_tp_triggered_by_candle_wick_low(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    ROOT CAUSE REGRESSION: A SHORT position must close when the candle LOW
    crosses TP even if the current live price is still above TP.

    Scenario:
      - SHORT entry=1.08000, TP=1.07000 (SHORT TP is below entry)
      - live_price=1.07200  (above TP for SHORT — live-only check would NOT close)
      - candle.low=1.06800  (below TP — intrabar wick touched TP)
      → position MUST close at TP=1.07000
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="short",
        entry="1.08000",
        tp="1.07000",   # SHORT TP below entry
        sl="1.09000",   # SHORT SL above entry
    )
    db.add(pos)
    await db.flush()

    live_price_above_tp = 1.07200   # above SHORT TP — would NOT close live-only
    candle_with_low_wick = _make_ohlcv_candle(close=1.07200, high=1.07400, low=1.06800)

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=live_price_above_tp,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[candle_with_low_wick],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, (
        "SHORT position should close when candle wick low crosses TP, "
        "even if live price is above SHORT TP"
    )
    assert "take_profit" in close_msgs[0]
    assert remaining == []

    await db.refresh(pos)
    assert not pos.is_open, "SHORT must be closed after intrabar wick touched TP"
    # Close price should be exactly at TP (1.07000), not at live price (1.07200)
    assert float(pos.closed_price) == pytest.approx(1.07000, abs=1e-5), (
        "Should close at exact TP level, not at live price snapshot"
    )
    assert float(pos.realized_pnl) > 0, "Profitable SHORT: PnL must be positive"


@pytest.mark.asyncio
async def test_short_below_tp_closes_via_full_bot_cycle(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    REGRESSION — UI/backend price consistency for SHORT positions.

    Observed in production (paper): SHORT EURUSD entry=1.16, TP=1.15,
    current price shown in UI = 1.08.  Position remained open despite
    current price being well below TP (TP should trigger when price <= TP).

    Root cause: Bug #1 — session filter blocked the TP/SL check.
    After the fix, _evaluate_open_positions runs unconditionally BEFORE
    the session guard.

    This test runs the full bot cycle (not just _evaluate_open_positions
    directly) to confirm the complete path works end-to-end.

    Price source consistency note:
      UI "Current" = GET /market/quote  → same TwelveData/GBM provider
      Backend close = get_current_price → same provider, same walk
      Both prices are from the same continuous simulation; no scaling
      mismatch exists between the two.
    """
    await client.post("/api/v1/bot/start", headers=auth_headers)

    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    # Reproduce the exact observed scenario:
    # SHORT entry=1.16, TP=1.15 (price fell 0.86% from entry → TP)
    # Current market price 1.08 — well past TP, position should be closed
    pos = _make_position(
        portfolio.id, side="short",
        entry="1.16000",
        tp="1.15000",   # SHORT TP is below entry (profit when price drops)
        sl="1.17000",   # SHORT SL is above entry
    )
    db.add(pos)
    await db.commit()

    # Price that is clearly below SHORT TP (1.08 < 1.15 = TP → must close)
    price_below_tp = 1.08

    # Need candles for technical analysis; also need near-price candles for
    # _evaluate_open_positions so no false wick triggers on other levels.
    n = 250
    analysis_closes = [1.08] * n   # flat series — no artificial signals
    analysis_candles = _make_candles(analysis_closes)

    def _candles_side_effect(symbol, timeframe, limit=100):
        if limit <= 5:
            return _make_candles([price_below_tp] * limit)
        return analysis_candles

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=price_below_tp,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            side_effect=_candles_side_effect,
        ),
        patch("app.services.bot_service._is_active_trading_session", return_value=True),
        patch("app.services.bot_service.get_news", new_callable=AsyncMock, return_value=[]),
        patch("app.services.bot_service.check_pre_analysis_filters", new_callable=AsyncMock, return_value=_EXPERT_ALLOW),
        patch("app.services.bot_service.check_post_analysis_filters", return_value=_EXPERT_ALLOW),
    ):
        state = await _get_bot_state(db, user_id)
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    await db.refresh(pos)
    assert not pos.is_open, (
        "SHORT position must close when current price (1.08) < TP (1.15). "
        "Regression: session filter must not block TP/SL evaluation."
    )
    assert float(pos.closed_price) == pytest.approx(1.15000, abs=1e-4), (
        "SHORT must close at exact TP level (1.15000), not at live price snapshot"
    )
    assert float(pos.realized_pnl) > 0, "Profitable SHORT (price fell from entry): PnL must be positive"


@pytest.mark.asyncio
async def test_tp_fires_even_when_consecutive_loss_guard_is_active(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    ROOT CAUSE REGRESSION — pre-guard TP/SL pass.

    Scenario that reproduced missed-TP in production:
      1. SHORT opened, price drops through TP.
      2. Consecutive-loss circuit breaker fires in _run_user_cycle.
      3. _run_user_cycle returns BEFORE calling _process_symbol.
      4. _evaluate_open_positions is NEVER called.
      5. Position stays open.
      6. Price recovers above TP → TP opportunity permanently missed.

    Fix: unconditional pre-guard TP/SL pass runs at the TOP of
    _run_user_cycle, BEFORE any risk guard can return early.  Risk guards
    now only prevent NEW positions from being opened — they never block
    TP/SL evaluation on existing positions.
    """
    await client.post("/api/v1/bot/start", headers=auth_headers)

    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    # SHORT with TP clearly below current price
    pos = _make_position(
        portfolio.id, side="short",
        entry="1.10000",
        tp="1.09000",   # SHORT TP below entry
        sl="1.11000",   # SHORT SL above entry
    )
    db.add(pos)
    await db.commit()

    price_below_tp = 1.08500   # below TP=1.09000 — must trigger

    def _candles_side_effect(symbol, timeframe, limit=100):
        return _make_candles([price_below_tp] * min(limit, 10))

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=price_below_tp,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            side_effect=_candles_side_effect,
        ),
        # Simulate: consecutive-loss circuit breaker fires — max=1, losses=1
        # This used to cause _run_user_cycle to return early and skip ALL
        # symbol processing, including TP/SL evaluation.
        patch(
            "app.services.bot_service.count_consecutive_losses",
            new_callable=AsyncMock,
            return_value=999,   # far above any threshold → guard will fire
        ),
        patch("app.services.bot_service._is_active_trading_session", return_value=True),
        patch("app.services.bot_service.get_news", new_callable=AsyncMock, return_value=[]),
        patch("app.services.bot_service.check_pre_analysis_filters", new_callable=AsyncMock, return_value=_EXPERT_ALLOW),
        patch("app.services.bot_service.check_post_analysis_filters", return_value=_EXPERT_ALLOW),
    ):
        state = await _get_bot_state(db, user_id)
        await bot_service._run_user_cycle(db, state)
        await db.commit()

    await db.refresh(pos)
    assert not pos.is_open, (
        "SHORT must close even when consecutive-loss circuit breaker fires. "
        "Regression: pre-guard TP/SL pass must run before all risk guards."
    )
    assert float(pos.closed_price) == pytest.approx(1.09000, abs=1e-4), (
        "Must close at exact TP level (1.09000)"
    )
    assert float(pos.realized_pnl) > 0, "Profitable SHORT: PnL must be positive"


# ---------------------------------------------------------------------------
# 10. Price-cross detection (bounce-back scenario)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_short_tp_cross_then_bounce_still_closes(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    CROSS DETECTION: SHORT crosses TP then bounces back — must still close.

    Scenario:
      Cycle N  : live_price = 1.08200  (above SHORT TP=1.08000, position open)
                 → prev_evaluated_price stored as 1.08200
      Cycle N+1: live_price = 1.08300  (bounced ABOVE TP again)
                 → snapshot: effective_low=1.08300 > TP=1.08000 → no hit
                 → cross: prev=1.08200 > TP=1.08000, current=1.08300 > TP → no cross
      ... (price briefly dips below 1.08000 between cycles, then bounces)
      Cycle N+2: prev=1.08200 (still above TP), live_price=1.07800 (below TP)
                 → cross: prev=1.08200 > tp=1.08000 AND current=1.07800 <= tp → CLOSE

    This test simulates the exact "crossed-then-bounced" scenario by directly
    setting prev_evaluated_price to a value above TP, then evaluating with a
    current price below TP.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="short",
        entry="1.09000",
        tp="1.08000",   # SHORT TP below entry
        sl="1.10000",   # SHORT SL above entry
    )
    # Simulate: previous cycle saw price = 1.08200 (above SHORT TP=1.08000)
    pos.prev_evaluated_price = Decimal("1.08200")
    db.add(pos)
    await db.flush()

    # Current price = 1.07800 — BELOW TP (1.08000)
    # Cross: prev=1.08200 > tp=1.08000 AND current=1.07800 <= tp → CLOSE
    # Even though candle high is 1.08050 (above TP — snapshot alone would miss)
    current_price = 1.07800
    candle_near_tp = _make_ohlcv_candle(close=1.07800, high=1.08050, low=1.07750)

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=current_price,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[candle_near_tp],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, "SHORT must close: price crossed TP (prev above, now below)"
    assert "take_profit" in close_msgs[0]
    assert remaining == []

    await db.refresh(pos)
    assert not pos.is_open, "SHORT position must be closed after TP cross"
    assert float(pos.realized_pnl) > 0, "Profitable SHORT: PnL must be positive"


@pytest.mark.asyncio
async def test_long_tp_cross_then_bounce_still_closes(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    CROSS DETECTION: LONG crosses TP then bounces back — must still close.

    prev=1.08800 (below LONG TP=1.09000), current=1.09200 (above TP) → cross fires.
    Candle high=1.08950 so snapshot effective_high=1.09200 also fires — but
    the key is the cross detection path is exercised correctly.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08000",
        tp="1.09000",
        sl="1.07000",
    )
    pos.prev_evaluated_price = Decimal("1.08800")  # was below TP last cycle
    db.add(pos)
    await db.flush()

    current_price = 1.09200  # now above TP → cross: prev=1.08800 < tp=1.09000 ≤ current

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=current_price,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, "LONG must close: price crossed TP (prev below, now above)"
    assert "take_profit" in close_msgs[0]
    await db.refresh(pos)
    assert not pos.is_open


@pytest.mark.asyncio
async def test_short_sl_cross_then_bounce_still_closes(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    CROSS DETECTION: SHORT crosses SL then bounces back — must still close.

    SHORT SL is ABOVE entry.  prev=1.09800 (below SL=1.10000), price spikes
    to 1.10200 (above SL) then bounces.  current=1.10200 still above SL
    so snapshot also fires — but the cross logic is the defensive layer.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="short",
        entry="1.09000",
        tp="1.08000",
        sl="1.10000",   # SHORT SL above entry
    )
    pos.prev_evaluated_price = Decimal("1.09800")  # below SL last cycle
    db.add(pos)
    await db.flush()

    current_price = 1.10200  # above SL → SL cross fires; loss trade

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=current_price,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, "SHORT must close at SL: price crossed SL (prev below, now above)"
    assert "stop_loss" in close_msgs[0]
    await db.refresh(pos)
    assert not pos.is_open


@pytest.mark.asyncio
async def test_short_tp_float_precision_at_exact_boundary(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    FLOAT PRECISION REGRESSION — the exact scenario from the production screenshot.

    Observed: SHORT EURUSD entry=1.15, TP=1.15, Current shown as 1.15 in UI,
    PnL=-$0.03.  Position did NOT close.

    Root cause: TP stored as Decimal("1.15") → float = 1.15 exactly.
    get_current_price() returned 1.15001 (one GBM tick above TP, rounding
    artifact).  1.15001 <= 1.15 → False → no close, no log entry.

    Fix: _TP_SL_EPSILON = 0.0001 widens the trigger boundary so that a price
    within 1 pip of TP still fires.  1.15001 <= 1.15 + 0.0001 = 1.1501 → True.
    Close price is still set to exact TP (1.15), not tp+epsilon.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="short",
        entry="1.15000",
        tp="1.15000",   # TP = entry (degenerate/immediate-close case)
        sl="1.16000",
    )
    db.add(pos)
    await db.flush()

    # live_price is 0.00005 above TP (half a pip — typical GBM rounding artifact)
    # Without epsilon: 1.15005 <= 1.15000 → False → MISSED
    # With epsilon:    1.15005 <= 1.15000 + 0.0001 → True → CLOSES
    live_price_above_tp_by_float_error = 1.15005

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=live_price_above_tp_by_float_error,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, (
        "SHORT must close when live price is within epsilon of TP. "
        "Regression: float 1.15005 must trigger TP=1.15000 (within 1 pip)."
    )
    assert "take_profit" in close_msgs[0]
    assert remaining == []

    await db.refresh(pos)
    assert not pos.is_open, "Position must be closed"
    # Fill at exact TP level, not at the slightly-higher live price
    assert float(pos.closed_price) == pytest.approx(1.15000, abs=1e-4)


@pytest.mark.asyncio
async def test_long_sl_float_precision_at_exact_boundary(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    FLOAT PRECISION: LONG SL — price is within epsilon below SL, must close.

    sl = 1.07000, live_price = 1.06995 (0.5 pip below SL).
    Without epsilon: 1.06995 <= 1.07000 → True (this case already works).
    Additional case: live_price = 1.07003 (0.3 pip ABOVE SL).
    Without epsilon: 1.07003 <= 1.07000 → False → miss.
    With epsilon:    1.07003 <= 1.07000 + 0.0001 = 1.07010 → True → closes.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08000",
        tp="1.09000",
        sl="1.07000",
    )
    db.add(pos)
    await db.flush()

    # Price just barely above SL: 1.07003 > 1.07000 → without epsilon, no close
    live_price_barely_above_sl = 1.07003

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=live_price_barely_above_sl,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, (
        "LONG must close at SL when price is within epsilon. "
        "Regression: 1.07003 must trigger SL=1.07000 (within 1 pip)."
    )
    assert "stop_loss" in close_msgs[0]
    await db.refresh(pos)
    assert not pos.is_open


@pytest.mark.asyncio
async def test_cross_not_triggered_when_no_prev_price(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    Cross detection must NOT fire on the first evaluation (prev_evaluated_price=NULL).
    Without a previous price baseline there is no meaningful cross to detect.
    Position must stay open if snapshot+candle also don't trigger.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="short",
        entry="1.09000",
        tp="1.08000",
        sl="1.10000",
    )
    # prev_evaluated_price is None (default) — first evaluation cycle
    db.add(pos)
    await db.flush()

    # Price is safely between TP and SL — no triggers should fire
    safe_price = 1.08500

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=safe_price,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert close_msgs == [], "No close when prev_price is None and snapshot doesn't trigger"
    assert len(remaining) == 1

    # prev_evaluated_price must now be set for the next cycle
    await db.refresh(pos)
    assert pos.is_open
    assert pos.prev_evaluated_price is not None, (
        "prev_evaluated_price must be persisted after first evaluation"
    )
    assert float(pos.prev_evaluated_price) == pytest.approx(safe_price, abs=1e-5)


@pytest.mark.asyncio
async def test_prev_evaluated_price_updated_on_hold(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    When a position is NOT closed (hold), prev_evaluated_price must be updated
    to the current live price so the next cycle has a fresh baseline.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08000", tp="1.09000", sl="1.07000",
    )
    pos.prev_evaluated_price = Decimal("1.08300")
    db.add(pos)
    await db.flush()

    # Price moves slightly — no TP/SL trigger
    new_price = 1.08450

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=new_price,
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert close_msgs == []
    assert len(remaining) == 1

    await db.refresh(pos)
    assert pos.is_open
    assert float(pos.prev_evaluated_price) == pytest.approx(new_price, abs=1e-5), (
        "prev_evaluated_price must update to current live price after hold"
    )


@pytest.mark.asyncio
async def test_live_price_only_does_not_close_when_wick_misses(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    Sanity check: position stays open when NEITHER live price NOR candle wick
    reaches TP.  Ensures the intrabar fix doesn't produce false positives.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    pos = _make_position(
        portfolio.id, side="long",
        entry="1.08000", tp="1.09000", sl="1.07000",
    )
    db.add(pos)
    await db.flush()

    # Live price and candle high are both well below TP
    candle_no_wick = _make_ohlcv_candle(close=1.08500, high=1.08700, low=1.08300)

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=1.08500,   # below TP=1.09000
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=[candle_no_wick],
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert close_msgs == [], "No close when neither live price nor wick reaches TP"
    assert len(remaining) == 1, "Position should remain open"

    await db.refresh(pos)
    assert pos.is_open, "Position must stay open when TP not reached"


# ---------------------------------------------------------------------------
# Pre-open candle wick regression tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pre_open_wick_does_not_close_position(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    REGRESSION: A candle wick that occurred BEFORE the position was opened must
    not close the position.

    Scenario:
      - LONG position, entry=1.08, TP=1.09, SL=1.07
      - Position opened NOW
      - Candles returned by get_candles all have timestamps 2-4 hours in the past
        (i.e. before pos.opened_at)
      - Candle high=1.10 (above TP) and candle low=1.06 (below SL)
      - Live price=1.08 (inside TP/SL band)

    Before the fix: effective_high was derived from the pre-open candle high
    (1.10 >= TP 1.09) → position falsely closed.
    After the fix: pre-open candles are filtered out → effective_high=live_price=1.08
    → no close.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    now = datetime.now(timezone.utc)

    pos = Position(
        id=uuid4(),
        portfolio_id=portfolio.id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("10"),
        avg_entry_price=Decimal("1.08000"),
        take_profit_price=Decimal("1.09000"),
        stop_loss_price=Decimal("1.07000"),
        investment_amount=Decimal("100"),
        is_open=True,
        opened_at=now,          # position opened right now
        realized_pnl=Decimal("0"),
    )
    db.add(pos)
    await db.flush()

    # Candles whose timestamps are all well before pos.opened_at
    pre_open_candles = [
        {
            "timestamp": (now - timedelta(hours=4)).isoformat(),
            "open": 1.08, "high": 1.10, "low": 1.06, "close": 1.08, "volume": 1000,
        },
        {
            "timestamp": (now - timedelta(hours=3)).isoformat(),
            "open": 1.08, "high": 1.10, "low": 1.06, "close": 1.08, "volume": 1000,
        },
        {
            "timestamp": (now - timedelta(hours=2)).isoformat(),
            "open": 1.08, "high": 1.10, "low": 1.06, "close": 1.08, "volume": 1000,
        },
    ]

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=1.08000,   # live price inside TP/SL band
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=pre_open_candles,
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert close_msgs == [], (
        "Position must NOT close: candle wick (high=1.10 > TP=1.09) predates position open"
    )
    assert len(remaining) == 1, "Position must remain open"

    await db.refresh(pos)
    assert pos.is_open, "is_open must still be True — pre-open wick must not trigger TP"


@pytest.mark.asyncio
async def test_post_open_wick_does_close_position(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """
    Complementary positive case: a candle that opened AFTER the position was opened
    must still contribute its wick to effective_high/low and close the position.

    Scenario:
      - LONG position, entry=1.08, TP=1.09
      - Position opened 90 minutes ago
      - One qualifying candle: started 30 minutes ago (after pos.opened_at), high=1.10
      - Live price=1.08 (below TP — wick is the only trigger)

    Expected: position closes via candle wick.
    """
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    from uuid import UUID
    user_id = UUID(me["id"])
    portfolio = await _get_portfolio(db, user_id)

    now = datetime.now(timezone.utc)
    opened_at = now - timedelta(minutes=90)

    pos = Position(
        id=uuid4(),
        portfolio_id=portfolio.id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("10"),
        avg_entry_price=Decimal("1.08000"),
        take_profit_price=Decimal("1.09000"),
        stop_loss_price=Decimal("1.07000"),
        investment_amount=Decimal("100"),
        is_open=True,
        opened_at=opened_at,
        realized_pnl=Decimal("0"),
    )
    db.add(pos)
    await db.flush()

    candles = [
        # Pre-open: starts 2h ago — must be ignored
        {
            "timestamp": (now - timedelta(hours=2)).isoformat(),
            "open": 1.08, "high": 1.085, "low": 1.075, "close": 1.08, "volume": 1000,
        },
        # Post-open: starts 30 min ago — must contribute; high=1.10 crosses TP=1.09
        {
            "timestamp": (now - timedelta(minutes=30)).isoformat(),
            "open": 1.08, "high": 1.10, "low": 1.075, "close": 1.08, "volume": 1000,
        },
    ]

    with (
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_current_price",
            new_callable=AsyncMock,
            return_value=1.08000,   # live price below TP — only wick triggers
        ),
        patch(
            "app.services.bot_service._market_data_router_for_candles.get_candles",
            new_callable=AsyncMock,
            return_value=candles,
        ),
    ):
        close_msgs, remaining = await bot_service._evaluate_open_positions(
            db=db, portfolio=portfolio, symbol="EURUSD", risk=None,
        )

    assert len(close_msgs) == 1, "Position must close: post-open candle wick (1.10) crosses TP (1.09)"
    assert remaining == [], "No remaining open positions"
    assert "take_profit" in close_msgs[0]

    await db.refresh(pos)
    assert not pos.is_open, "Position must be closed by the qualifying wick"
