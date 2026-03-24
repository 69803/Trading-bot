"""Tests for strategy indicators and signal detection."""
import math
import pytest

from app.utils.indicators import calculate_ema, calculate_rsi


def _last_valid(series: list) -> float | None:
    """Return the last non-NaN value in the series."""
    for v in reversed(series):
        if v is not None and not math.isnan(v):
            return v
    return None


def _first_valid(series: list) -> float | None:
    for v in series:
        if v is not None and not math.isnan(v):
            return v
    return None


def _trending_up(n: int = 250) -> list[float]:
    return [100.0 + i * 0.1 for i in range(n)]


def _trending_down(n: int = 250) -> list[float]:
    return [200.0 - i * 0.1 for i in range(n)]


def _flat(n: int = 250, price: float = 1.085) -> list[float]:
    return [price] * n


class TestCalculateEMA:
    def test_returns_list_same_length(self):
        prices = list(range(1, 21))  # 1..20
        result = calculate_ema(prices, 5)
        assert isinstance(result, list)
        assert len(result) == len(prices)

    def test_insufficient_data_returns_all_nan(self):
        result = calculate_ema([1.0, 2.0], 10)
        # Returns list of NaN (not None) when data < period
        assert isinstance(result, list)
        assert all(math.isnan(v) for v in result)

    def test_exact_minimum_data_seeds_ema(self):
        prices = [1.0] * 10
        result = calculate_ema(prices, 10)
        last = _last_valid(result)
        assert last is not None
        assert abs(last - 1.0) < 0.01

    def test_rising_ema_last_above_first_valid(self):
        result = calculate_ema(_trending_up(), 50)
        first = _first_valid(result)
        last = _last_valid(result)
        assert first is not None and last is not None
        assert last > first

    def test_fast_ema_above_slow_in_uptrend(self):
        prices = _trending_up(250)
        fast = calculate_ema(prices, 9)
        slow = calculate_ema(prices, 21)
        assert _last_valid(fast) > _last_valid(slow)

    def test_nan_prefix_has_correct_length(self):
        result = calculate_ema(_trending_up(50), 10)
        nan_count = sum(1 for v in result if math.isnan(v))
        # First period-1 values are NaN
        assert nan_count == 9


class TestCalculateRSI:
    def test_returns_list_same_length(self):
        prices = _trending_up(50)
        result = calculate_rsi(prices, 14)
        assert isinstance(result, list)
        assert len(result) == 50

    def test_overbought_uptrend(self):
        prices = _trending_up(100)
        result = calculate_rsi(prices, 14)
        last = _last_valid(result)
        assert last is not None
        assert last > 70

    def test_oversold_downtrend(self):
        prices = _trending_down(100)
        result = calculate_rsi(prices, 14)
        last = _last_valid(result)
        assert last is not None
        assert last < 30

    def test_above_50_in_uptrend(self):
        prices = _trending_up(50)
        result = calculate_rsi(prices, 14)
        last = _last_valid(result)
        assert last is not None
        assert last > 50

    def test_insufficient_data_all_nan(self):
        result = calculate_rsi([1.0, 2.0], 14)
        assert isinstance(result, list)
        assert all(math.isnan(v) for v in result)


class TestStrategyEndpoints:
    @pytest.mark.asyncio
    async def test_get_strategy_config(self, client, auth_headers):
        resp = await client.get("/api/v1/strategy/config", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "ema_fast" in data
        assert "ema_slow" in data
        assert "rsi_period" in data

    @pytest.mark.asyncio
    async def test_update_strategy_config(self, client, auth_headers):
        resp = await client.put(
            "/api/v1/strategy/config",
            headers=auth_headers,
            json={"ema_fast": 9, "ema_slow": 21, "auto_trade": True},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ema_fast"] == 9
        assert data["ema_slow"] == 21

    @pytest.mark.asyncio
    async def test_run_signals(self, client, auth_headers):
        resp = await client.post("/api/v1/strategy/run", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "count" in data
        assert isinstance(data["count"], int)

    @pytest.mark.asyncio
    async def test_get_signals_list(self, client, auth_headers):
        resp = await client.get("/api/v1/strategy/signals", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "signals" in data
        assert isinstance(data["signals"], list)
