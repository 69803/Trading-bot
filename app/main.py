"""FastAPI application factory and lifespan handler."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.logger import get_logger
from app.db.init_db import init_db
from app.db.session import AsyncSessionFactory, engine
from app.scheduler import start_scheduler, stop_scheduler

log = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: seed DB on startup, dispose engine on shutdown."""
    log.info("Starting up", environment=settings.ENVIRONMENT)
    async with AsyncSessionFactory() as session:
        try:
            await init_db(session)
            await session.commit()
            log.info("Database seeding complete")
        except Exception:
            await session.rollback()
            log.exception("Database seeding failed")
            raise

    start_scheduler()
    log.info("Scheduler started")

    # Startup probes — log active providers and verify reachability
    from app.services.market_data_router import market_data_router

    stock_provider = settings.MARKET_DATA_PROVIDER.lower()
    twelvedata_loaded = bool(settings.TWELVE_DATA_API_KEY)
    log.info(f"STOCK_PROVIDER={stock_provider}")
    log.info(f"TWELVEDATA_KEY_LOADED={twelvedata_loaded}")
    log.info(f"TOTAL_SYMBOLS={len(market_data_router.get_all_symbols())}")

    # Probe stock provider (AAPL)
    try:
        price = await market_data_router.get_current_price("AAPL")
        log.info("Stock provider probe OK", symbol="AAPL", price=price)
    except Exception as exc:
        log.warning("Stock provider probe failed", error=str(exc))

    # Probe forex provider (EUR/USD)
    try:
        price = await market_data_router.get_current_price("EUR/USD")
        log.info("Forex provider probe OK", symbol="EUR/USD", price=price)
    except Exception as exc:
        log.warning("Forex provider probe failed", error=str(exc))

    # Probe commodity provider (XAU/USD)
    try:
        price = await market_data_router.get_current_price("XAU/USD")
        log.info("Commodity provider probe OK", symbol="XAU/USD", price=price)
    except Exception as exc:
        log.warning("Commodity provider probe failed", error=str(exc))

    yield  # Application is running

    stop_scheduler()
    log.info("Shutting down — disposing database engine")
    await engine.dispose()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application instance."""
    app = FastAPI(
        title="Paper Trading Platform API",
        description=(
            "REST API for the paper trading platform. "
            "Provides portfolio management, order execution simulation, "
            "strategy signals, risk controls and backtesting."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # ---------------------------------------------------------------------------
    # CORS — adjust origins for production deployments
    # ---------------------------------------------------------------------------
    # NOTE: allow_origins=["*"] cannot be combined with allow_credentials=True
    # per the CORS spec — browsers reject it.  Always list origins explicitly.
    if settings.ENVIRONMENT == "development":
        allow_origins = [
            "http://localhost:3000",
            "http://localhost:3001",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:3001",
        ]
    else:
        # Set FRONTEND_URL in Render environment variables to your frontend domain.
        # Multiple origins can be comma-separated: "https://a.com,https://b.com"
        frontend_url = settings.FRONTEND_URL
        allow_origins = [u.strip() for u in frontend_url.split(",") if u.strip()] if frontend_url else []

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---------------------------------------------------------------------------
    # Mount API router
    # ---------------------------------------------------------------------------
    app.include_router(api_router, prefix="/api/v1")

    @app.get("/health", tags=["health"], summary="Health check")
    async def health_check() -> dict:
        return {"status": "ok", "environment": settings.ENVIRONMENT}

    return app


app = create_app()
