"""
Unit tests for the risk manager.

Tests position sizing, SL/TP calculation, volatility adjustment,
trailing stop, and break-even logic.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.schemas.decision import FinalDecision
from app.schemas.risk_assessment import RiskAssessment
from app.schemas.technical import IndicatorValues, TechnicalSignal
from app.services.risk_manager import (
    assess,
    check_break_even,
    update_trailing_stop,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_risk_settings(**kwargs) -> MagicMock:
    """Build a mock RiskSettings with sensible defaults."""
    m = MagicMock()
    m.max_open_positions = kwargs.get("max_open_positions", 10)
    m.max_position_size_pct = Decimal(str(kwargs.get("max_position_size_pct", "0.05")))
    m.stop_loss_pct = Decimal(str(kwargs.get("stop_loss_pct", "0.03")))
    m.take_profit_pct = Decimal(str(kwargs.get("take_profit_pct", "0.06")))
    m.max_daily_loss_pct = Decimal("0.02")
    m.max_drawdown_pct = Decimal("0.20")
    m.trailing_stop_pct = Decimal(str(kwargs.get("trailing_stop_pct", "0.00")))
    m.break_even_trigger_pct = Decimal(str(kwargs.get("break_even_trigger_pct", "0.00")))
    m.max_consecutive_losses = kwargs.get("max_consecutive_losses", 0)
    m.max_trades_per_hour = kwargs.get("max_trades_per_hour", 0)
    m.volatility_sizing_enabled = kwargs.get("volatility_sizing_enabled", False)
    return m


def _make_decision(direction: str = "BUY", symbol: str = "EURUSD") -> FinalDecision:
    return FinalDecision(
        symbol=symbol,
        direction=direction,
        confidence=60,
        reasons=["test"],
        technical_direction=direction if direction in ("BUY", "SELL") else "HOLD",
        technical_confidence=60,
        sentiment_label="neutral",
        sentiment_score=0.0,
        sentiment_impact=0,
        decided_at=datetime.now(timezone.utc),
    )


def _make_technical(
    price: float = 1.1000,
    atr: float = 0.005,
    direction: str = "BUY",
) -> TechnicalSignal:
    return TechnicalSignal(
        symbol="EURUSD",
        timeframe="1h",
        direction=direction,
        confidence=60,
        reasons=[],
        indicators=IndicatorValues(
            price=price,
            rsi=50.0,
            ema_fast=price + 0.001,
            ema_slow=price - 0.001,
            macd=0.001,
            macd_signal=0.0,
            macd_histogram=0.001,
            atr=atr,
            volume_ratio=1.2,
            adx=25.0,
        ),
        analyzed_at=datetime.now(timezone.utc),
        candles_used=100,
        composite_score=35,
    )


def _make_position(
    side: str = "long",
    entry: float = 1.1000,
    sl: float = 1.0900,
    tp: float = 1.1200,
) -> MagicMock:
    p = MagicMock()
    p.side = side
    p.is_open = True
    p.avg_entry_price = Decimal(str(entry))
    p.stop_loss_price = Decimal(str(sl))
    p.take_profit_price = Decimal(str(tp))
    p.high_water_mark = None
    p.trailing_stop_price = None
    p.break_even_activated = False
    p.symbol = "EURUSD"
    return p


# ---------------------------------------------------------------------------
# 1. Basic approval / rejection
# ---------------------------------------------------------------------------

class TestAssessBasic:

    def test_buy_approved(self):
        ra = assess(
            _make_decision("BUY"),
            _make_technical(),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        assert ra.approved is True
        assert ra.position_size_dollars == pytest.approx(500.0, abs=0.01)

    def test_sell_approved(self):
        ra = assess(
            _make_decision("SELL"),
            _make_technical(direction="SELL"),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        assert ra.approved is True

    def test_hold_rejected(self):
        ra = assess(
            _make_decision("HOLD"),
            _make_technical(),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        assert ra.approved is False

    def test_max_positions_rejected(self):
        ra = assess(
            _make_decision("BUY"),
            _make_technical(),
            equity=10_000.0,
            open_positions_count=10,
            risk_settings=_make_risk_settings(max_open_positions=10),
            invest_amount=500.0,
        )
        assert ra.approved is False
        assert "max open positions" in ra.rejection_reason.lower()

    def test_zero_equity_rejected(self):
        ra = assess(
            _make_decision("BUY"),
            _make_technical(),
            equity=0.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        assert ra.approved is False


# ---------------------------------------------------------------------------
# 2. SL/TP calculation
# ---------------------------------------------------------------------------

class TestSLTP:

    def test_atr_based_buy_sl_below_entry(self):
        ra = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.005),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        assert ra.approved
        assert ra.stop_loss_price < 1.1000
        assert ra.take_profit_price > 1.1000

    def test_atr_based_sell_sl_above_entry(self):
        ra = assess(
            _make_decision("SELL"),
            _make_technical(price=1.1000, atr=0.005, direction="SELL"),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        assert ra.approved
        assert ra.stop_loss_price > 1.1000
        assert ra.take_profit_price < 1.1000

    def test_pct_based_fallback_when_atr_zero(self):
        ra = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.0),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(stop_loss_pct="0.02", take_profit_pct="0.04"),
            invest_amount=500.0,
        )
        assert ra.approved
        assert ra.sizing_method == "pct_based"
        assert ra.stop_loss_price == pytest.approx(1.1000 * (1 - 0.02), abs=1e-5)
        assert ra.take_profit_price == pytest.approx(1.1000 * (1 + 0.04), abs=1e-5)

    def test_rr_ratio_at_least_1(self):
        ra = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.005),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(),
            invest_amount=500.0,
        )
        if ra.stop_loss_price:
            assert ra.risk_reward_ratio >= 1.0


# ---------------------------------------------------------------------------
# 3. Volatility-adjusted sizing
# ---------------------------------------------------------------------------

class TestVolatilitySizing:

    def test_high_atr_reduces_size(self):
        """When ATR/price >> reference, position should be scaled down."""
        base = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.001),  # normal ATR
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(volatility_sizing_enabled=True),
            invest_amount=1000.0,
        )
        high_vol = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.020),  # 4× high ATR
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(volatility_sizing_enabled=True),
            invest_amount=1000.0,
        )
        assert base.approved and high_vol.approved
        assert high_vol.position_size_dollars < base.position_size_dollars

    def test_normal_atr_does_not_reduce_size(self):
        """When ATR is at or below reference, size is unchanged."""
        base = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.001),
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(volatility_sizing_enabled=False),
            invest_amount=1000.0,
        )
        vol_on = assess(
            _make_decision("BUY"),
            _make_technical(price=1.1000, atr=0.001),  # ATR/price = 0.09% < 0.5% ref
            equity=10_000.0,
            open_positions_count=0,
            risk_settings=_make_risk_settings(volatility_sizing_enabled=True),
            invest_amount=1000.0,
        )
        # Both should be ~equal since ATR is low
        assert abs(base.position_size_dollars - vol_on.position_size_dollars) < 0.01


# ---------------------------------------------------------------------------
# 4. Trailing stop
# ---------------------------------------------------------------------------

class TestTrailingStop:

    def test_long_trailing_stop_not_triggered_initially(self):
        pos = _make_position("long", entry=1.1000)
        triggered = update_trailing_stop(pos, current_price=1.1010, trailing_stop_pct=0.01)
        assert triggered is False
        assert pos.trailing_stop_price is not None

    def test_long_trailing_stop_triggered_on_price_drop(self):
        pos = _make_position("long", entry=1.1000)
        # Price rises first to set high-water mark
        update_trailing_stop(pos, current_price=1.1100, trailing_stop_pct=0.01)
        # Then drops below trail level
        triggered = update_trailing_stop(pos, current_price=1.0980, trailing_stop_pct=0.01)
        assert triggered is True

    def test_short_trailing_stop_not_triggered_initially(self):
        pos = _make_position("short", entry=1.1000)
        triggered = update_trailing_stop(pos, current_price=1.0990, trailing_stop_pct=0.01)
        assert triggered is False

    def test_short_trailing_stop_triggered_on_price_rise(self):
        pos = _make_position("short", entry=1.1000)
        update_trailing_stop(pos, current_price=1.0900, trailing_stop_pct=0.01)
        triggered = update_trailing_stop(pos, current_price=1.1020, trailing_stop_pct=0.01)
        assert triggered is True

    def test_disabled_when_pct_zero(self):
        pos = _make_position("long", entry=1.1000)
        triggered = update_trailing_stop(pos, current_price=0.5000, trailing_stop_pct=0.0)
        assert triggered is False


# ---------------------------------------------------------------------------
# 5. Break-even
# ---------------------------------------------------------------------------

class TestBreakEven:

    def test_long_break_even_activates_when_gain_reached(self):
        pos = _make_position("long", entry=1.1000, sl=1.0950)
        # Gain of 1% triggers break-even at 0.5% threshold
        activated = check_break_even(pos, current_price=1.1110, break_even_trigger_pct=0.005)
        assert activated is True
        assert float(pos.stop_loss_price) == pytest.approx(1.1000, abs=1e-5)
        assert pos.break_even_activated is True

    def test_long_break_even_not_triggered_below_threshold(self):
        pos = _make_position("long", entry=1.1000, sl=1.0950)
        activated = check_break_even(pos, current_price=1.1002, break_even_trigger_pct=0.005)
        assert activated is False

    def test_short_break_even_activates(self):
        pos = _make_position("short", entry=1.1000, sl=1.1050)
        activated = check_break_even(pos, current_price=1.0940, break_even_trigger_pct=0.005)
        assert activated is True
        assert float(pos.stop_loss_price) == pytest.approx(1.1000, abs=1e-5)

    def test_break_even_not_reactivated(self):
        pos = _make_position("long", entry=1.1000, sl=1.0950)
        pos.break_even_activated = True  # already activated
        activated = check_break_even(pos, current_price=1.1200, break_even_trigger_pct=0.005)
        assert activated is False

    def test_disabled_when_pct_zero(self):
        pos = _make_position("long", entry=1.1000, sl=1.0950)
        activated = check_break_even(pos, current_price=1.2000, break_even_trigger_pct=0.0)
        assert activated is False
