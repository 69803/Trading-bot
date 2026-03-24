"""Tests for risk management and bot control endpoints."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_get_risk_settings(client: AsyncClient, auth_headers: dict):
    resp = await client.get("/api/v1/risk/settings", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "max_position_size_pct" in data
    assert "max_daily_loss_pct" in data
    assert "max_open_positions" in data


@pytest.mark.asyncio
async def test_update_risk_settings(client: AsyncClient, auth_headers: dict):
    resp = await client.put(
        "/api/v1/risk/settings",
        headers=auth_headers,
        json={"max_open_positions": 5, "stop_loss_pct": "0.02"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["max_open_positions"] == 5


@pytest.mark.asyncio
async def test_get_risk_status(client: AsyncClient, auth_headers: dict):
    resp = await client.get("/api/v1/risk/status", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "trading_halted" in data
    assert "daily_pnl_pct" in data


@pytest.mark.asyncio
async def test_bot_status_initially_stopped(client: AsyncClient, auth_headers: dict):
    resp = await client.get("/api/v1/bot/status", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["is_running"] is False


@pytest.mark.asyncio
async def test_bot_start_and_stop(client: AsyncClient, auth_headers: dict):
    start_resp = await client.post("/api/v1/bot/start", headers=auth_headers)
    assert start_resp.status_code == 200
    assert start_resp.json()["is_running"] is True

    status_resp = await client.get("/api/v1/bot/status", headers=auth_headers)
    assert status_resp.json()["is_running"] is True

    stop_resp = await client.post("/api/v1/bot/stop", headers=auth_headers)
    assert stop_resp.status_code == 200
    assert stop_resp.json()["is_running"] is False


@pytest.mark.asyncio
async def test_portfolio_summary(client: AsyncClient, auth_headers: dict):
    resp = await client.get("/api/v1/portfolio/summary", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "balance" in data
    assert "equity" in data
    assert "bot_running" in data
    assert float(data["balance"]) > 0
