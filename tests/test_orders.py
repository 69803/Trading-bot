"""Tests for order placement, cancellation, and paper trading engine."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_place_market_buy_order(client: AsyncClient, auth_headers: dict):
    resp = await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={"symbol": "EURUSD", "side": "buy", "order_type": "market", "quantity": 100},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "filled"
    assert data["symbol"] == "EURUSD"
    assert data["side"] == "buy"
    assert float(data["filled_quantity"]) == 100.0


@pytest.mark.asyncio
async def test_place_market_sell_order(client: AsyncClient, auth_headers: dict):
    # Buy first to have a position to sell
    await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={"symbol": "EURUSD", "side": "buy", "order_type": "market", "quantity": 100},
    )
    resp = await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={"symbol": "EURUSD", "side": "sell", "order_type": "market", "quantity": 100},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "filled"


@pytest.mark.asyncio
async def test_place_limit_order_stays_pending(client: AsyncClient, auth_headers: dict):
    resp = await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={
            "symbol": "EURUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": 100,
            "limit_price": 0.0001,  # Far below market – won't fill
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "pending"


@pytest.mark.asyncio
async def test_cancel_pending_order(client: AsyncClient, auth_headers: dict):
    create_resp = await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={
            "symbol": "EURUSD",
            "side": "buy",
            "order_type": "limit",
            "quantity": 100,
            "limit_price": 0.0001,
        },
    )
    order_id = create_resp.json()["id"]

    cancel_resp = await client.delete(
        f"/api/v1/orders/{order_id}", headers=auth_headers
    )
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_list_orders(client: AsyncClient, auth_headers: dict):
    await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={"symbol": "EURUSD", "side": "buy", "order_type": "market", "quantity": 100},
    )
    resp = await client.get("/api/v1/orders", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert data["total"] >= 1


@pytest.mark.asyncio
async def test_order_reduces_cash_balance(client: AsyncClient, auth_headers: dict):
    summary_before = (
        await client.get("/api/v1/portfolio/summary", headers=auth_headers)
    ).json()
    balance_before = float(summary_before["balance"])

    await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={"symbol": "EURUSD", "side": "buy", "order_type": "market", "quantity": 100},
    )

    summary_after = (
        await client.get("/api/v1/portfolio/summary", headers=auth_headers)
    ).json()
    balance_after = float(summary_after["balance"])
    assert balance_after < balance_before


@pytest.mark.asyncio
async def test_limit_order_requires_limit_price(client: AsyncClient, auth_headers: dict):
    resp = await client.post(
        "/api/v1/orders",
        headers=auth_headers,
        json={"symbol": "EURUSD", "side": "buy", "order_type": "limit", "quantity": 100},
    )
    assert resp.status_code == 422
