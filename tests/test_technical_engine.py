"""
Unit tests for the technical analysis engine.

Tests the scoring logic, domain breakdown, hold_reason, and
score_breakdown observability fields added in the professional upgrade.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List

import pytest

from app.services.technical_engine import (
    ADX_MODERATE,
    ADX_SIDEWAYS,
    ADX_STRONG,
    BUY_THRESHOLD,
    SELL_THRESHOLD,
    analyze,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_candles(
    n: int = 120,
    close_start: float = 1.1000,
    trend: float = 0.0001,      # per-bar price drift
    volume: float = 1000.0,
    high_low_spread: float = 0.001,
) -> List[dict]:
    """Build a minimal synthetic OHLCV candle list (oldest → newest)."""
    candles = []
    ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    price = close_start
    for i in range(n):
        close  = price + trend * i
        candles.append({
            "timestamp": ts + i * 3600,
            "open":   close - 0.0002,
            "high":   close + high_low_spread,
            "low":    close - high_low_spread,
            "close":  close,
            "volume": volume,
        })
    return candles


def _bullish_candles(n: int = 120) -> List[dict]:
    """Strong uptrend: consistent positive drift to force BUY."""
    return _make_candles(n=n, trend=0.0005)   # 0.05% rise per bar


def _bearish_candles(n: int = 120) -> List[dict]:
    """Strong downtrend: consistent negative drift to force SELL."""
    return _make_candles(n=n, trend=-0.0005)


def _flat_candles(n: int = 120) -> List[dict]:
    """No trend (ADX will be very low), should produce HOLD."""
    import random
    random.seed(42)
    candles = []
    ts = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp())
    price = 1.1000
    for i in range(n):
        # Small random walk, no persistent trend
        price += random.uniform(-0.0001, 0.0001)
        candles.append({
            "timestamp": ts + i * 3600,
            "open":   price - 0.0001,
            "high":   price + 0.0005,
            "low":    price - 0.0005,
            "close":  price,
            "volume": 500.0,
        })
    return candles


# ---------------------------------------------------------------------------
# 1. Basic signal directions
# ---------------------------------------------------------------------------

class TestSignalDirection:

    def test_insufficient_candles_returns_hold(self):
        candles = _make_candles(n=10)
        sig = analyze("EURUSD", candles)
        assert sig.direction == "HOLD"
        assert sig.hold_reason is not None

    def test_bullish_returns_buy(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        # With a strong trend, should be BUY
        assert sig.direction == "BUY"
        assert sig.confidence > 0

    def test_bearish_returns_sell(self):
        candles = _bearish_candles(n=150)
        sig = analyze("EURUSD", candles)
        assert sig.direction == "SELL"
        assert sig.confidence > 0


# ---------------------------------------------------------------------------
# 2. Score breakdown (observability)
# ---------------------------------------------------------------------------

class TestScoreBreakdown:

    def test_score_breakdown_populated_for_actionable_signal(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        if sig.direction in ("BUY", "SELL"):
            assert len(sig.score_breakdown) > 0

    def test_score_breakdown_has_correct_domains(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        if sig.direction in ("BUY", "SELL"):
            domains = {f.domain for f in sig.score_breakdown}
            assert domains.issubset({"trend", "momentum", "volatility"})

    def test_composite_score_matches_direction(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        if sig.direction == "BUY":
            assert sig.composite_score >= BUY_THRESHOLD
        elif sig.direction == "SELL":
            assert sig.composite_score <= SELL_THRESHOLD
        else:
            assert SELL_THRESHOLD < sig.composite_score < BUY_THRESHOLD

    def test_score_breakdown_points_sum_approx_composite(self):
        """Sum of factor points should roughly match composite_score."""
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        if sig.direction in ("BUY", "SELL") and sig.score_breakdown:
            factor_sum = sum(f.points for f in sig.score_breakdown)
            # Allow for clamping (score is clamped to ±110)
            assert abs(factor_sum - sig.composite_score) <= 15

    def test_each_factor_has_reason(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        for factor in sig.score_breakdown:
            assert factor.name
            assert factor.reason
            assert factor.max_points > 0

    def test_factor_names_known(self):
        known = {"EMA_TREND", "EMA_CROSSOVER", "EMA_CHOPPY", "EMA_GAP",
                 "RSI_LEVEL", "RSI_MOMENTUM", "MACD_HIST",
                 "VOLUME", "ADX_STRENGTH"}
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        for f in sig.score_breakdown:
            assert f.name in known, f"Unknown factor name: {f.name}"


# ---------------------------------------------------------------------------
# 3. HOLD detection and hold_reason
# ---------------------------------------------------------------------------

class TestHoldReason:

    def test_hold_has_hold_reason(self):
        candles = _make_candles(n=10)   # insufficient
        sig = analyze("EURUSD", candles)
        assert sig.direction == "HOLD"
        assert sig.hold_reason is not None
        assert len(sig.hold_reason) > 0

    def test_hold_reason_mentions_threshold_or_adx(self):
        candles = _make_candles(n=10)
        sig = analyze("EURUSD", candles)
        reason_lower = sig.hold_reason.lower()
        has_expected = any(
            kw in reason_lower
            for kw in ("threshold", "adx", "candle", "insufficient", "nan")
        )
        assert has_expected, f"Unexpected hold_reason: {sig.hold_reason!r}"


# ---------------------------------------------------------------------------
# 4. Indicator values
# ---------------------------------------------------------------------------

class TestIndicatorValues:

    def test_price_is_last_close(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        expected_price = round(float(candles[-1]["close"]), 5)
        assert sig.indicators.price == pytest.approx(expected_price, abs=1e-3)

    def test_rsi_in_valid_range(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        if not math.isnan(sig.indicators.rsi):
            assert 0 <= sig.indicators.rsi <= 100

    def test_volume_ratio_positive(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        assert sig.indicators.volume_ratio > 0

    def test_confidence_in_range(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        assert 0 <= sig.confidence <= 100


# ---------------------------------------------------------------------------
# 5. Trend strength labels
# ---------------------------------------------------------------------------

class TestTrendStrength:

    def test_direction_buy_has_valid_trend_strength(self):
        candles = _bullish_candles(n=150)
        sig = analyze("EURUSD", candles)
        if sig.direction == "BUY":
            assert sig.trend_strength in ("strong", "moderate", "weak")

    def test_hold_has_sideways_or_weak(self):
        candles = _make_candles(n=10)
        sig = analyze("EURUSD", candles)
        assert sig.direction == "HOLD"
        assert sig.trend_strength in ("sideways", "weak")
