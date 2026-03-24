"""
APScheduler integration for FastAPI.

Tasks:
  - Every 30s:  update market prices (keeps GBM walk alive)
  - Every 60s:  run bot cycle (auto-trade for all running bots)
  - Every 60s:  fill pending limit orders
  - Every 5min: take portfolio snapshots (equity history)
  - Every 1h:   purge expired refresh tokens

Architecture note:
  AsyncIOScheduler runs in the same event loop as FastAPI.
  Each job creates its own DB session (does NOT use FastAPI request-scoped deps).
  Ready to migrate to Celery+Redis: replace job bodies with task.delay() calls.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.core.logger import get_logger
from app.db.session import AsyncSessionFactory
from app.services.market_data_service import market_data_service

log = get_logger(__name__)

scheduler = AsyncIOScheduler(timezone="UTC")


# ---------------------------------------------------------------------------
# Job implementations
# ---------------------------------------------------------------------------

async def _job_update_prices() -> None:
    """
    Advance price state for all symbols every 30 s.

    Stocks/ETFs  — advanced via market_data_service (GBM stock provider).
    Forex/Commodities — advanced via market_data_router → TwelveDataProvider
                        → _GBMFallback.advance_price().

    BUG HISTORY: FX symbols were never advanced here because
    market_data_service.get_all_symbols() only returns stock symbols.
    The FX fallback GBM was therefore only advanced on demand (UI loads,
    bot evaluations), causing the simulated price to teleport by many pips
    between calls and making TP/SL evaluation unreliable.
    """
    from app.services.market_data_router import market_data_router
    from app.services.providers.twelvedata import SYMBOLS as _FX_SYMBOLS

    # Stocks
    for symbol in market_data_service.get_all_symbols():
        await market_data_service.update_price(symbol)

    # Forex and commodities — use the router so the symbol routing layer
    # handles slash-normalisation ("EUR/USD" etc.) transparently.
    for symbol in _FX_SYMBOLS:
        try:
            await market_data_router.update_price(symbol)
        except Exception as _exc:
            log.warning("FX price update failed", symbol=symbol, error=str(_exc))


async def _job_bot_cycle() -> None:
    """Run one auto-trade cycle for all users with bot running."""
    from app.services.bot_service import run_bot_cycle
    async with AsyncSessionFactory() as db:
        try:
            await run_bot_cycle(db)
        except Exception as exc:
            log.exception("Scheduler bot cycle failed", error=str(exc))
            await db.rollback()


async def _job_fill_limit_orders() -> None:
    """Check and fill any pending limit orders whose price was reached."""
    from app.services.bot_service import fill_pending_limit_orders
    async with AsyncSessionFactory() as db:
        try:
            await fill_pending_limit_orders(db)
        except Exception as exc:
            log.exception("Scheduler limit fill failed", error=str(exc))
            await db.rollback()


async def _job_portfolio_snapshots() -> None:
    """Take portfolio equity snapshots for all portfolios."""
    from sqlalchemy import select
    from app.models.portfolio import Portfolio
    from app.services.portfolio_service import take_portfolio_snapshot

    async with AsyncSessionFactory() as db:
        try:
            result = await db.execute(select(Portfolio))
            portfolios = result.scalars().all()
            for portfolio in portfolios:
                await take_portfolio_snapshot(db, portfolio.id)
            log.info("Portfolio snapshots taken", count=len(list(portfolios)))
        except Exception as exc:
            log.exception("Scheduler snapshot failed", error=str(exc))
            await db.rollback()


async def _job_purge_refresh_tokens() -> None:
    """Delete expired or revoked refresh tokens to keep the table clean."""
    from sqlalchemy import delete
    from app.models.refresh_token import RefreshToken

    now = datetime.now(timezone.utc)
    async with AsyncSessionFactory() as db:
        try:
            await db.execute(
                delete(RefreshToken).where(
                    (RefreshToken.expires_at < now) | (RefreshToken.revoked == True)  # noqa: E712
                )
            )
            await db.commit()
        except Exception as exc:
            log.exception("Scheduler token purge failed", error=str(exc))
            await db.rollback()


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler() -> None:
    """Register all jobs and start the scheduler."""
    scheduler.add_job(
        _job_update_prices,
        trigger=IntervalTrigger(seconds=30),
        id="update_prices",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        _job_bot_cycle,
        trigger=IntervalTrigger(seconds=60),
        id="bot_cycle",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        _job_fill_limit_orders,
        trigger=IntervalTrigger(seconds=60),
        id="fill_limit_orders",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        _job_portfolio_snapshots,
        trigger=IntervalTrigger(minutes=5),
        id="portfolio_snapshots",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.add_job(
        _job_purge_refresh_tokens,
        trigger=IntervalTrigger(hours=1),
        id="purge_refresh_tokens",
        replace_existing=True,
        max_instances=1,
    )
    scheduler.start()
    log.info("Scheduler started", jobs=len(scheduler.get_jobs()))


def stop_scheduler() -> None:
    """Gracefully stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        log.info("Scheduler stopped")
