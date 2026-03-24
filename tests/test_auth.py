"""Tests for authentication endpoints: register, login, refresh, logout, /me."""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_register_creates_user(client: AsyncClient):
    resp = await client.post(
        "/api/v1/auth/register",
        json={"email": "new@example.com", "password": "password123"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_register_duplicate_email_fails(client: AsyncClient):
    payload = {"email": "dup@example.com", "password": "password123"}
    await client.post("/api/v1/auth/register", json=payload)
    resp = await client.post("/api/v1/auth/register", json=payload)
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_register_weak_password_fails(client: AsyncClient):
    resp = await client.post(
        "/api/v1/auth/register",
        json={"email": "weak@example.com", "password": "short"},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_login_json_returns_tokens(client: AsyncClient):
    await client.post(
        "/api/v1/auth/register",
        json={"email": "login@example.com", "password": "password123"},
    )
    resp = await client.post(
        "/api/v1/auth/login/json",
        json={"email": "login@example.com", "password": "password123"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert "refresh_token" in data


@pytest.mark.asyncio
async def test_login_wrong_password_fails(client: AsyncClient):
    await client.post(
        "/api/v1/auth/register",
        json={"email": "wrong@example.com", "password": "password123"},
    )
    resp = await client.post(
        "/api/v1/auth/login/json",
        json={"email": "wrong@example.com", "password": "badpassword"},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_refresh_token_rotates(client: AsyncClient):
    reg = await client.post(
        "/api/v1/auth/register",
        json={"email": "refresh@example.com", "password": "password123"},
    )
    old_refresh = reg.json()["refresh_token"]

    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": old_refresh},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    # Old token should now be revoked
    resp2 = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": old_refresh},
    )
    assert resp2.status_code == 401


@pytest.mark.asyncio
async def test_me_returns_user(client: AsyncClient, registered_user: dict):
    headers = {"Authorization": f"Bearer {registered_user['access_token']}"}
    resp = await client.get("/api/v1/auth/me", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "@example.com" in data["email"]
    assert data["is_active"] is True


@pytest.mark.asyncio
async def test_me_without_token_fails(client: AsyncClient):
    resp = await client.get("/api/v1/auth/me")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_logout_revokes_token(client: AsyncClient):
    reg = await client.post(
        "/api/v1/auth/register",
        json={"email": "logout@example.com", "password": "password123"},
    )
    refresh_token = reg.json()["refresh_token"]

    logout_resp = await client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": refresh_token},
    )
    assert logout_resp.status_code == 200

    # Refresh should now fail
    resp = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": refresh_token},
    )
    assert resp.status_code == 401
