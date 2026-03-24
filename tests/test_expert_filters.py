"""
Tests for expert_filters_service.py (PASO 7 — Expert Forex Mode).

Covers all 6 filters plus the feature-flag bypass:
  A — Session filter:        outside hours → SKIP; inside London/NY → ALLOW
  B — Volatility filter:     low ATR → SKIP; adequate ATR → ALLOW
  C — Trend filter:          BUY below EMA200 → SKIP; BUY above EMA200 → ALLOW
                             SELL above EMA200 → SKIP; insufficient candles → ALLOW
  D — Signal quality filter: 0/3 conditions → SKIP; 2/3 conditions → ALLOW
  E — Overtrading filter:    daily limit hit → SKIP; cooldown not elapsed → SKIP
  F — Post-event delay:      recent high event → SKIP; old event → ALLOW

  Feature flag: EXPERT_FILTERS_ENABLED=False → ALLOW regardless of data.

All DB-dependent tests use the in-memory SQLite fixture from conftest.py.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.historical_event import HistoricalEvent
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.schemas.technical import IndicatorValues, TechnicalSignal
from app.services.expert_filters_service import (
    ExpertFilterResult,
    _check_overtrading,
    _check_post_event_delay,
    _check_session,
    _check_signal_quality,
    _check_trend,
    _check_volatility,
    check_post_analysis_filters,
    check_pre_analysis_filters,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signal(
    direction: str = "BUY",
    price: float = 1.1050,
    atr: float = 0.0010,
    ema_fast: float = 1.1020,
    ema_slow: float = 1.0990,
    rsi: float = 52.0,
    macd_histogram: float = 0.0003,
) -> TechnicalSignal:
    return TechnicalSignal(
        symbol="EURUSD",
        timeframe="1h",
        direction=direction,  # type: ignore[arg-type]
        confidence=70,
        reasons=[],
        indicators=IndicatorValues(
            price=price,
            rsi=rsi,
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            macd=0.0002,
            macd_signal=0.0,
            macd_histogram=macd_histogram,
            atr=atr,
            volume_ratio=1.0,
            adx=28.0,
        ),
        analyzed_at=datetime.now(timezone.utc),
        candles_used=200,
    )


def _candles(n: int = 250, start_price: float = 1.0, step: float = 0.0001) -> list:
    """Return n simple candle dicts with linearly increasing closes."""
    candles = []
    price = start_price
    for _ in range(n):
        candles.append({"open": price, "high": price + 0.0005,
                        "low": price - 0.0005, "close": price})
        price += step
    return candles


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


def _open_pos(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    opened_at: datetime,
) -> Position:
    pos = Position(
        id=uuid.uuid4(),
        portfolio_id=portfolio_id,
        symbol="EURUSD",
        side="long",
        quantity=Decimal("1000"),
        avg_entry_price=Decimal("1.10"),
        current_price=Decimal("1.10"),
        is_open=True,
        opened_at=opened_at,
    )
    db.add(pos)
    return pos


def _event(
    db: AsyncSession,
    currency: str,
    impact: str,
    dt: datetime,
    name: str = "CPI m/m",
) -> HistoricalEvent:
    ev = HistoricalEvent(
        event_datetime_utc=dt,
        currency=currency,
        event_name=name,
        impact=impact,
        source="test",
    )
    db.add(ev)
    return ev


# ---------------------------------------------------------------------------
# Filter A — Session
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("hour,expected", [
    (7,  "ALLOW"),   # London start
    (9,  "ALLOW"),   # London mid
    (10, "ALLOW"),   # London end-1
    (13, "ALLOW"),   # NY start
    (15, "ALLOW"),   # NY mid
    (16, "ALLOW"),   # NY end-1
    (6,  "SKIP"),    # before London
    (11, "SKIP"),    # after London, before NY
    (12, "SKIP"),    # gap
    (17, "SKIP"),    # after NY
    (23, "SKIP"),    # Asian
])
def test_session_filter(hour: int, expected: str):
    now = datetime(2026, 3, 20, hour, 30, 0, tzinfo=timezone.utc)
    result = _check_session(now)
    if expected == "ALLOW":
        assert result is None
    else:
        assert result is not None
        assert result.action == "SKIP"
        assert result.filter_name == "session"


# ---------------------------------------------------------------------------
# Filter B — Volatility
# ---------------------------------------------------------------------------

def test_volatility_passes_above_threshold():
    result = _check_volatility(atr=0.0010, symbol="EURUSD")
    assert result is None


def test_volatility_blocks_below_threshold():
    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_ATR_MIN = 0.0005
        result = _check_volatility(atr=0.0002, symbol="EURUSD")
    assert result is not None
    assert result.action == "SKIP"
    assert result.filter_name == "volatility"
    assert "ATR" in result.reason


def test_volatility_passes_nan_atr():
    """NaN ATR → can't evaluate → pass (benefit of the doubt)."""
    import math
    result = _check_volatility(atr=float("nan"), symbol="EURUSD")
    assert result is None


# ---------------------------------------------------------------------------
# Filter C — Trend (EMA200)
# ---------------------------------------------------------------------------

def test_trend_buy_above_ema200_passes():
    """BUY signal with price above EMA200 → ALLOW."""
    # Upward-trending candles: final price ≈ 1.025, EMA200 seed ≈ 1.0
    candles = _candles(250, start_price=1.0, step=0.0001)
    result = _check_trend(
        price=1.025, candles=candles, direction="BUY", symbol="EURUSD"
    )
    assert result is None


def test_trend_buy_below_ema200_blocked():
    """BUY signal with price below EMA200 → SKIP."""
    # Downward-trending candles: prices fall over 250 steps
    candles = _candles(250, start_price=1.10, step=-0.0001)
    # Price near the end is below EMA200 (which tracks declining average)
    # Use a very low price to ensure it's below any reasonable EMA200
    result = _check_trend(
        price=0.90, candles=candles, direction="BUY", symbol="EURUSD"
    )
    assert result is not None
    assert result.action == "SKIP"
    assert result.filter_name == "trend"
    assert "BUY" in result.reason


def test_trend_sell_below_ema200_passes():
    """SELL signal with price below EMA200 → ALLOW."""
    candles = _candles(250, start_price=1.10, step=-0.0001)
    result = _check_trend(
        price=0.90, candles=candles, direction="SELL", symbol="EURUSD"
    )
    assert result is None


def test_trend_sell_above_ema200_blocked():
    """SELL signal with price above EMA200 → SKIP."""
    candles = _candles(250, start_price=1.0, step=0.0001)
    result = _check_trend(
        price=2.0, candles=candles, direction="SELL", symbol="EURUSD"
    )
    assert result is not None
    assert result.action == "SKIP"
    assert result.filter_name == "trend"


def test_trend_hold_direction_skips_check():
    """HOLD direction → filter not evaluated → ALLOW."""
    candles = _candles(250, start_price=1.0, step=-0.0001)
    result = _check_trend(price=0.90, candles=candles, direction="HOLD", symbol="EURUSD")
    assert result is None


def test_trend_insufficient_candles_passes():
    """Fewer than 200 candles → can't compute EMA200 → pass."""
    candles = _candles(50, start_price=1.0, step=-0.0001)
    result = _check_trend(price=0.90, candles=candles, direction="BUY", symbol="EURUSD")
    assert result is None


# ---------------------------------------------------------------------------
# Filter D — Signal Quality
# ---------------------------------------------------------------------------

def test_signal_quality_all_conditions_met_passes():
    """All 3 conditions confirming BUY → ALLOW."""
    # price > ema_fast > ema_slow → EMA_OK
    # RSI=52 < 65 → RSI_OK
    # macd_histogram=+0.001 → MACD_OK
    sig = _signal(direction="BUY", price=1.10, ema_fast=1.09,
                  ema_slow=1.08, rsi=52.0, macd_histogram=0.001)
    result = _check_signal_quality(sig, "BUY", "EURUSD")
    assert result is None


def test_signal_quality_zero_conditions_blocked():
    """0/3 conditions met → SKIP (need 2)."""
    # price < ema_fast → EMA_FAIL for BUY
    # RSI=70 >= 65 → RSI_FAIL for BUY
    # macd_histogram=-0.001 < 0 → MACD_FAIL for BUY
    sig = _signal(direction="BUY", price=1.07, ema_fast=1.09,
                  ema_slow=1.08, rsi=70.0, macd_histogram=-0.001)
    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_MIN_SIGNAL_CONDITIONS = 2
        result = _check_signal_quality(sig, "BUY", "EURUSD")
    assert result is not None
    assert result.action == "SKIP"
    assert result.filter_name == "signal_quality"
    assert "0/3" in result.reason


def test_signal_quality_two_conditions_passes():
    """2/3 conditions met → ALLOW (threshold=2)."""
    # EMA_OK, RSI_OK, MACD_FAIL (negative histogram for BUY)
    sig = _signal(direction="BUY", price=1.10, ema_fast=1.09,
                  ema_slow=1.08, rsi=52.0, macd_histogram=-0.001)
    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_MIN_SIGNAL_CONDITIONS = 2
        result = _check_signal_quality(sig, "BUY", "EURUSD")
    assert result is None


def test_signal_quality_sell_direction():
    """SELL: price < ema_fast < ema_slow (EMA_OK), RSI=38>35 (RSI_OK),
    macd_hist<0 (MACD_OK) → ALLOW."""
    sig = _signal(direction="SELL", price=1.07, ema_fast=1.09,
                  ema_slow=1.10, rsi=38.0, macd_histogram=-0.001)
    result = _check_signal_quality(sig, "SELL", "EURUSD")
    assert result is None


def test_signal_quality_hold_skips_check():
    sig = _signal(direction="HOLD")
    result = _check_signal_quality(sig, "HOLD", "EURUSD")
    assert result is None


# ---------------------------------------------------------------------------
# Filter E — Overtrading
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_overtrading_daily_limit_blocks(db: AsyncSession):
    """5 positions opened today hits the daily limit → SKIP."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(5):
        _open_pos(db, port.id, today + timedelta(hours=i + 1))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_MAX_TRADES_PER_DAY = 5
        mock_s.EXPERT_COOLDOWN_MINUTES = 0
        result = await _check_overtrading(db, port.id, "EURUSD", now)

    assert result is not None
    assert result.action == "SKIP"
    assert result.filter_name == "overtrading"
    assert "daily" in result.reason


@pytest.mark.asyncio
async def test_overtrading_cooldown_blocks(db: AsyncSession):
    """A position opened 10 minutes ago blocks when cooldown=30min."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
    _open_pos(db, port.id, now - timedelta(minutes=10))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_MAX_TRADES_PER_DAY = 0
        mock_s.EXPERT_COOLDOWN_MINUTES = 30
        result = await _check_overtrading(db, port.id, "EURUSD", now)

    assert result is not None
    assert result.action == "SKIP"
    assert result.filter_name == "overtrading"
    assert "cooldown" in result.reason


@pytest.mark.asyncio
async def test_overtrading_passes_when_cooldown_elapsed(db: AsyncSession):
    """A position opened 60 minutes ago with cooldown=30min → ALLOW."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
    _open_pos(db, port.id, now - timedelta(minutes=60))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_FILTERS_ENABLED = True
        mock_s.EXPERT_SESSION_LONDON_START = 7
        mock_s.EXPERT_SESSION_LONDON_END = 11
        mock_s.EXPERT_SESSION_NY_START = 13
        mock_s.EXPERT_SESSION_NY_END = 17
        mock_s.EXPERT_MAX_TRADES_PER_DAY = 0
        mock_s.EXPERT_COOLDOWN_MINUTES = 30
        mock_s.EXPERT_POST_EVENT_DELAY_MINUTES = 0
        result = await check_pre_analysis_filters(
            symbol="EURUSD", db=db, portfolio_id=port.id, now=now,
        )

    assert result.action == "ALLOW"


# ---------------------------------------------------------------------------
# Filter F — Post-Event Delay
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_post_event_delay_blocks_recent_event(db: AsyncSession):
    """High-impact EUR event 5 minutes ago → SKIP when delay=15min."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
    _event(db, currency="EUR", impact="high", dt=now - timedelta(minutes=5))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_FILTERS_ENABLED = True
        mock_s.EXPERT_SESSION_LONDON_START = 7
        mock_s.EXPERT_SESSION_LONDON_END = 11
        mock_s.EXPERT_SESSION_NY_START = 13
        mock_s.EXPERT_SESSION_NY_END = 17
        mock_s.EXPERT_MAX_TRADES_PER_DAY = 0
        mock_s.EXPERT_COOLDOWN_MINUTES = 0
        mock_s.EXPERT_POST_EVENT_DELAY_MINUTES = 15
        result = await check_pre_analysis_filters(
            symbol="EURUSD", db=db, portfolio_id=port.id, now=now,
        )

    assert result.action == "SKIP"
    assert result.filter_name == "post_event_delay"
    assert "delay" in result.reason.lower()


@pytest.mark.asyncio
async def test_post_event_delay_passes_old_event(db: AsyncSession):
    """High-impact EUR event 30 minutes ago with delay=15min → ALLOW."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
    _event(db, currency="EUR", impact="high", dt=now - timedelta(minutes=30))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_FILTERS_ENABLED = True
        mock_s.EXPERT_SESSION_LONDON_START = 7
        mock_s.EXPERT_SESSION_LONDON_END = 11
        mock_s.EXPERT_SESSION_NY_START = 13
        mock_s.EXPERT_SESSION_NY_END = 17
        mock_s.EXPERT_MAX_TRADES_PER_DAY = 0
        mock_s.EXPERT_COOLDOWN_MINUTES = 0
        mock_s.EXPERT_POST_EVENT_DELAY_MINUTES = 15
        result = await check_pre_analysis_filters(
            symbol="EURUSD", db=db, portfolio_id=port.id, now=now,
        )

    assert result.action == "ALLOW"


@pytest.mark.asyncio
async def test_post_event_delay_medium_impact_ignored(db: AsyncSession):
    """Medium-impact event just released → NOT blocked (only high-impact triggers)."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 10, 0, 0, tzinfo=timezone.utc)
    _event(db, currency="EUR", impact="medium", dt=now - timedelta(minutes=2))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_FILTERS_ENABLED = True
        mock_s.EXPERT_SESSION_LONDON_START = 7
        mock_s.EXPERT_SESSION_LONDON_END = 11
        mock_s.EXPERT_SESSION_NY_START = 13
        mock_s.EXPERT_SESSION_NY_END = 17
        mock_s.EXPERT_MAX_TRADES_PER_DAY = 0
        mock_s.EXPERT_COOLDOWN_MINUTES = 0
        mock_s.EXPERT_POST_EVENT_DELAY_MINUTES = 15
        result = await check_pre_analysis_filters(
            symbol="EURUSD", db=db, portfolio_id=port.id, now=now,
        )

    assert result.action == "ALLOW"


# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_feature_flag_disabled_allows_pre_analysis(db: AsyncSession):
    """EXPERT_FILTERS_ENABLED=False → pre-analysis returns ALLOW regardless."""
    port = _portfolio(db)
    now = datetime(2026, 3, 20, 3, 0, 0, tzinfo=timezone.utc)  # Asian session
    _event(db, currency="EUR", impact="high", dt=now - timedelta(minutes=2))
    for i in range(10):
        _open_pos(db, port.id, now - timedelta(minutes=i))
    await db.flush()

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_FILTERS_ENABLED = False
        result = await check_pre_analysis_filters(
            symbol="EURUSD", db=db, portfolio_id=port.id, now=now,
        )

    assert result.action == "ALLOW"
    assert result.filter_name == "none"


def test_feature_flag_disabled_allows_post_analysis():
    """EXPERT_FILTERS_ENABLED=False → post-analysis returns ALLOW regardless."""
    sig = _signal(atr=0.00001)   # below volatility threshold
    candles = _candles(10)       # insufficient for EMA200

    with patch("app.services.expert_filters_service.settings") as mock_s:
        mock_s.EXPERT_FILTERS_ENABLED = False
        result = check_post_analysis_filters(
            technical=sig, candles=candles, direction="BUY", symbol="EURUSD",
        )

    assert result.action == "ALLOW"
    assert result.filter_name == "none"
