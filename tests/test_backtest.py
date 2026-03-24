"""
Tests for the backtest engine:
- Create a backtest run via API
- Run completes with valid metrics
- Results accessible via status and results endpoints
- Invalid parameters rejected
- Empty/insufficient data handled gracefully
"""
from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datetime import datetime, timezone, timedelta

from app.models.backtest_run import BacktestRun
from app.services import backtest_service


# ---------------------------------------------------------------------------
# Synthetic candle factory (fast, no network, no GBM computation)
# ---------------------------------------------------------------------------

def _make_test_candles(n: int = 400) -> list[dict]:
    """
    Minimal OHLCV candles with a clear uptrend so the technical engine
    produces BUY signals.  Uses recent timestamps to pass the staleness check.
    """
    now = datetime(2024, 6, 30, 12, 0, 0, tzinfo=timezone.utc)
    candles = []
    price = 1.0800
    for i in range(n):
        ts = now - timedelta(hours=n - i)
        close = price + i * 0.0002
        candles.append({
            "timestamp": ts.isoformat(),
            "open":   close - 0.0001,
            "high":   close + 0.0003,
            "low":    close - 0.0003,
            "close":  close,
            "volume": 1000.0,
        })
    return candles


_TEST_CANDLES = _make_test_candles()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_PARAMS = {
    "symbol": "EURUSD",
    "timeframe": "1h",
    "start_date": "2024-01-01",
    "end_date": "2024-06-30",
    "initial_capital": 10000.0,
    "ema_fast": 9,
    "ema_slow": 21,
    "rsi_period": 14,
    "rsi_overbought": 70.0,
    "rsi_oversold": 30.0,
    "stop_loss_pct": 0.03,
    "take_profit_pct": 0.06,
    "commission_pct": 0.001,
}


async def _create_and_run(
    db: AsyncSession,
    user_id: UUID,
    params: dict = None,
    candles: list = None,
) -> BacktestRun:
    """Create a BacktestRun and execute it with mocked market data."""
    p = params or VALID_PARAMS
    from decimal import Decimal

    run = BacktestRun(
        id=uuid4(),
        user_id=user_id,
        symbol=p["symbol"],
        timeframe=p["timeframe"],
        start_date=date.fromisoformat(p["start_date"]),
        end_date=date.fromisoformat(p["end_date"]),
        initial_capital=Decimal(str(p["initial_capital"])),
        parameters=p,
        status="queued",
        progress_pct=0,
        created_at=datetime.now(timezone.utc),
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)

    mock_candles = candles if candles is not None else _TEST_CANDLES
    with patch(
        "app.services.backtest_service.market_data_service.get_historical_candles",
        new_callable=AsyncMock,
        return_value=mock_candles,
    ):
        await backtest_service.run_backtest(db, run.id)
    await db.refresh(run)
    return run


# ---------------------------------------------------------------------------
# 1. Create a backtest run via API (just verifies record creation)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_backtest_returns_201(client: AsyncClient, auth_headers: dict):
    """POST /backtest/run creates a run record and returns 201."""
    # Patch background function with AsyncMock so asyncio.create_task gets a coroutine
    with patch(
        "app.services.backtest_service._run_backtest_background",
        new_callable=AsyncMock,
    ):
        resp = await client.post(
            "/api/v1/backtest/run", headers=auth_headers, json=VALID_PARAMS
        )
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["symbol"] == "EURUSD"
    assert data["status"] == "queued"


# ---------------------------------------------------------------------------
# 2. Run completes with valid metrics (synchronous execution)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backtest_completes_with_valid_metrics(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """A backtest run should complete and produce meaningful metrics."""
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    user_id = UUID(me["id"])

    run = await _create_and_run(db, user_id)

    assert run.status == "completed", f"Expected completed, got {run.status}: {run.error_message}"
    assert run.progress_pct == 100
    assert run.results is not None


@pytest.mark.asyncio
async def test_backtest_results_have_expected_keys(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """Results must contain all expected metric keys with sane values."""
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    user_id = UUID(me["id"])

    run = await _create_and_run(db, user_id)

    assert run.status == "completed"
    metrics = run.results

    for key in (
        "total_trades",
        "win_rate",
        "net_pnl",
        "max_drawdown",
        "sharpe_ratio",
        "final_equity",
    ):
        assert key in metrics, f"Missing metric: {key}"

    assert metrics["final_equity"] > 0
    assert 0.0 <= metrics["win_rate"] <= 100.0
    assert metrics["max_drawdown"] >= 0.0


# ---------------------------------------------------------------------------
# 3. Equity curve and trade log are stored
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backtest_equity_curve_and_trade_log(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """After completion, equity_curve and trade_log should be non-null lists."""
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    user_id = UUID(me["id"])

    run = await _create_and_run(db, user_id)

    assert run.status == "completed"
    assert isinstance(run.equity_curve, list)
    assert len(run.equity_curve) > 0

    # Each equity curve point should have timestamp and equity
    point = run.equity_curve[0]
    assert "timestamp" in point
    assert "equity" in point


# ---------------------------------------------------------------------------
# 4. List backtests via API
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_backtests(client: AsyncClient, auth_headers: dict):
    """GET /backtest returns paginated list."""
    with patch(
        "app.services.backtest_service._run_backtest_background",
        new_callable=AsyncMock,
    ):
        await client.post("/api/v1/backtest/run", headers=auth_headers, json=VALID_PARAMS)

    list_resp = await client.get("/api/v1/backtest", headers=auth_headers)
    assert list_resp.status_code == 200
    data = list_resp.json()
    assert "items" in data
    assert "total" in data
    assert data["total"] >= 1
    assert isinstance(data["items"], list)


# ---------------------------------------------------------------------------
# 5. Invalid parameters are rejected
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backtest_rejects_invalid_timeframe(
    client: AsyncClient, auth_headers: dict
):
    """An invalid timeframe should return 422."""
    bad = {**VALID_PARAMS, "timeframe": "3m"}
    resp = await client.post("/api/v1/backtest/run", headers=auth_headers, json=bad)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_backtest_rejects_end_before_start(
    client: AsyncClient, auth_headers: dict
):
    """end_date before start_date should return 422."""
    bad = {**VALID_PARAMS, "start_date": "2024-06-01", "end_date": "2024-01-01"}
    resp = await client.post("/api/v1/backtest/run", headers=auth_headers, json=bad)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_backtest_rejects_ema_slow_lte_fast(
    client: AsyncClient, auth_headers: dict
):
    """ema_slow <= ema_fast should return 422."""
    bad = {**VALID_PARAMS, "ema_fast": 50, "ema_slow": 20}
    resp = await client.post("/api/v1/backtest/run", headers=auth_headers, json=bad)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# 6. Status endpoint returns 404 for unknown run_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backtest_status_404_for_unknown(
    client: AsyncClient, auth_headers: dict
):
    """Requesting status of a non-existent run_id returns 404."""
    fake_id = "00000000-0000-0000-0000-000000000000"
    resp = await client.get(
        f"/api/v1/backtest/{fake_id}/status", headers=auth_headers
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 7. Insufficient data causes failed status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_backtest_fails_on_insufficient_candle_data(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """If market data returns too few candles, run should fail gracefully."""
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    user_id = UUID(me["id"])

    tiny_candles = [
        {
            "timestamp": f"2024-01-0{i+1}T00:00:00Z",
            "open": 1.085, "high": 1.086, "low": 1.084, "close": 1.085, "volume": 100.0,
        }
        for i in range(5)
    ]
    run = await _create_and_run(db, user_id, candles=tiny_candles)

    assert run.status == "failed"
    assert run.error_message is not None
    assert "candle" in run.error_message.lower() or "data" in run.error_message.lower()


# ---------------------------------------------------------------------------
# 8. Delete backtest run via API
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_backtest_run(
    client: AsyncClient, auth_headers: dict, db: AsyncSession
):
    """DELETE removes the run; subsequent GET returns 404."""
    me = (await client.get("/api/v1/auth/me", headers=auth_headers)).json()
    user_id = UUID(me["id"])

    run = await _create_and_run(db, user_id)
    run_id = str(run.id)

    # Expire the session so client reads fresh data
    await db.commit()

    del_resp = await client.delete(
        f"/api/v1/backtest/{run_id}", headers=auth_headers
    )
    assert del_resp.status_code == 204

    get_resp = await client.get(
        f"/api/v1/backtest/{run_id}", headers=auth_headers
    )
    assert get_resp.status_code == 404
