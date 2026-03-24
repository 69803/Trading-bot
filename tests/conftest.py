"""
Shared pytest fixtures.

Uses SQLite in-memory database (via aiosqlite) so tests run without Postgres.
Install: pip install aiosqlite
"""
import os
# Force GBM simulation for ALL providers — no live API calls during tests.
# These must be set BEFORE any app modules are imported so that module-level
# singletons (market_data_router, news_service, etc.) see empty keys and
# fall back to simulation / no-op behaviour.
os.environ["MARKET_DATA_PROVIDER"] = "gbm"
os.environ["TWELVE_DATA_API_KEY"]  = ""
os.environ["NEWS_API_KEY"]         = ""
os.environ["ALPHA_VANTAGE_KEY"]    = ""
os.environ["ALPACA_API_KEY"]       = ""
os.environ["ALPACA_SECRET_KEY"]    = ""
os.environ["POLYGON_API_KEY"]      = ""

import asyncio
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.db.base import Base
from app.db.session import AsyncSessionFactory
from app.main import create_app

# ---------------------------------------------------------------------------
# In-memory SQLite engine for tests
# StaticPool ensures all sessions share one connection (same in-memory DB).
# ---------------------------------------------------------------------------
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=False,
)
TestSessionFactory = async_sessionmaker(
    bind=test_engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
)


@pytest_asyncio.fixture(scope="session")
async def setup_db():
    """Create all tables once per test session."""
    # Import all models so Base.metadata is populated
    import app.models.user  # noqa: F401
    import app.models.portfolio  # noqa: F401
    import app.models.position  # noqa: F401
    import app.models.order  # noqa: F401
    import app.models.trade  # noqa: F401
    import app.models.strategy_config  # noqa: F401
    import app.models.strategy_signal  # noqa: F401
    import app.models.risk_settings  # noqa: F401
    import app.models.backtest_run  # noqa: F401
    import app.models.portfolio_snapshot  # noqa: F401
    import app.models.market_candle  # noqa: F401
    import app.models.bot_state  # noqa: F401
    import app.models.refresh_token  # noqa: F401
    import app.models.market_price  # noqa: F401
    import app.models.historical_event  # noqa: F401
    import app.models.performance_snapshot  # noqa: F401
    import app.models.daily_performance_summary  # noqa: F401

    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture()
async def db(setup_db) -> AsyncGenerator[AsyncSession, None]:
    """Yield a test session that rolls back after each test."""
    async with TestSessionFactory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture()
async def client(setup_db) -> AsyncGenerator[AsyncClient, None]:
    """HTTP test client wired to the FastAPI app with test DB."""
    app = create_app()

    # Override the DB dependency
    async def _override_get_db():
        async with TestSessionFactory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    from app.api.deps import get_db
    app.dependency_overrides[get_db] = _override_get_db

    # Disable scheduler during tests
    import app.scheduler as sched_module
    sched_module.start_scheduler = lambda: None
    sched_module.stop_scheduler = lambda: None

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


import uuid as _uuid


@pytest_asyncio.fixture()
async def registered_user(client: AsyncClient) -> dict:
    """Register a unique test user and return the token response."""
    unique_email = f"test_{_uuid.uuid4().hex[:8]}@example.com"
    resp = await client.post(
        "/api/v1/auth/register",
        json={"email": unique_email, "password": "testpass123"},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


@pytest_asyncio.fixture()
async def auth_headers(registered_user: dict) -> dict:
    return {"Authorization": f"Bearer {registered_user['access_token']}"}
