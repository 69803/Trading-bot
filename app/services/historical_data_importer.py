"""Historical Data Importer.

Downloads 10 years of market prices and economic events, then persists
them to the database while skipping duplicates.

Can be run as a standalone script:

    python -m app.services.historical_data_importer

Or called programmatically from tests / admin endpoints:

    import asyncio
    from app.services.historical_data_importer import run_full_historical_import
    from app.db.session import AsyncSessionFactory

    async def main():
        async with AsyncSessionFactory() as db:
            result = await run_full_historical_import(db)
            print(result)

    asyncio.run(main())
"""
from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.models.historical_event import HistoricalEvent
from app.models.market_price import MarketPrice
from app.services.historical_economic_events_service import (
    EconomicEventRecord,
    get_historical_economic_events,
)
from app.services.historical_market_data_service import (
    DEFAULT_SYMBOLS,
    MarketDataPoint,
    get_historical_market_data,
)

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Tuneable constants
# ---------------------------------------------------------------------------

BATCH_SIZE     = 500    # rows per DB insert batch
DEFAULT_YEARS  = 10
DEFAULT_INTERVAL = "1d"

# Currencies to fetch economic events for (linked to our default symbols)
DEFAULT_CURRENCIES = ["USD", "EUR", "GBP", "JPY", "XAU", "XAG"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def import_historical_market_data(
    db:         AsyncSession,
    symbol:     str,
    start_date: date | str,
    end_date:   date | str,
    interval:   str = DEFAULT_INTERVAL,
) -> int:
    """
    Download and persist historical OHLCV data for one symbol.

    Returns the number of new rows inserted (duplicates are silently skipped).
    """
    start = _to_date(start_date)
    end   = _to_date(end_date)

    log.info(
        "Importing historical market data",
        symbol=symbol, start=str(start), end=str(end), interval=interval,
    )

    points = get_historical_market_data(
        symbol=symbol,
        start_date=start,
        end_date=end,
        interval=interval,
    )

    if not points:
        log.warning("No data returned from provider", symbol=symbol)
        return 0

    inserted = await _bulk_insert_market_prices(db, points)
    log.info(
        "Market data import complete",
        symbol=symbol, downloaded=len(points), inserted=inserted,
        skipped=len(points) - inserted,
    )
    return inserted


async def import_historical_economic_events(
    db:         AsyncSession,
    start_date: date | str,
    end_date:   date | str,
    currencies: Optional[List[str]] = None,
    csv_path:   Optional[str]       = None,
) -> int:
    """
    Download and persist historical economic events.

    Returns the number of new rows inserted (duplicates are silently skipped).
    """
    start = _to_date(start_date)
    end   = _to_date(end_date)
    curs  = currencies or DEFAULT_CURRENCIES

    log.info(
        "Importing historical economic events",
        start=str(start), end=str(end), currencies=curs,
    )

    records = get_historical_economic_events(
        start_date=start,
        end_date=end,
        currencies=curs,
        csv_path=csv_path,
    )

    if not records:
        log.info(
            "No economic events returned — skipping DB insert",
            hint="Configure TRADING_ECONOMICS_API_KEY or pass csv_path",
        )
        return 0

    inserted = await _bulk_insert_events(db, records)
    log.info(
        "Economic events import complete",
        downloaded=len(records), inserted=inserted,
        skipped=len(records) - inserted,
    )
    return inserted


async def run_full_historical_import(
    db:         AsyncSession,
    years:      int                  = DEFAULT_YEARS,
    symbols:    Optional[List[str]]  = None,
    interval:   str                  = DEFAULT_INTERVAL,
    currencies: Optional[List[str]]  = None,
    csv_path:   Optional[str]        = None,
    start_date: Optional[date | str] = None,
    end_date:   Optional[date | str] = None,
) -> Dict[str, int]:
    """
    Run the complete historical data import pipeline.

    Downloads market data for each symbol in *symbols* and economic events
    for *currencies*, then persists everything to the DB.

    Date range is determined by *start_date* / *end_date* when provided,
    otherwise falls back to today minus *years*.

    Returns a summary dict: ``{"symbol": rows_inserted, ..., "events": n}``.
    """
    _end   = _to_date(end_date)   if end_date   else date.today()
    _start = _to_date(start_date) if start_date else date(_end.year - years, _end.month, _end.day)
    end_date   = _end
    start_date = _start
    syms       = symbols or DEFAULT_SYMBOLS
    results:   Dict[str, int] = {}

    log.info(
        "Full historical import started",
        years=years, start=str(start_date), end=str(end_date),
        symbols=syms, interval=interval,
    )

    # ── Market data (one symbol at a time to be polite to Yahoo Finance) ──────
    for symbol in syms:
        try:
            n = await import_historical_market_data(
                db=db,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            )
            results[symbol] = n
            await db.commit()          # commit after each symbol
            await asyncio.sleep(0.5)   # small pause between downloads
        except Exception as exc:
            log.error(
                "Market data import failed for symbol",
                symbol=symbol, error=str(exc),
            )
            await db.rollback()
            results[symbol] = -1       # -1 = error

    # ── Economic events ───────────────────────────────────────────────────────
    try:
        n = await import_historical_economic_events(
            db=db,
            start_date=start_date,
            end_date=end_date,
            currencies=currencies,
            csv_path=csv_path,
        )
        results["events"] = n
        await db.commit()
    except Exception as exc:
        log.error("Economic events import failed", error=str(exc))
        await db.rollback()
        results["events"] = -1

    log.info("Full historical import complete", summary=results)
    return results


# ---------------------------------------------------------------------------
# Database insert helpers
# ---------------------------------------------------------------------------

async def _bulk_insert_market_prices(
    db:     AsyncSession,
    points: List[MarketDataPoint],
) -> int:
    """Insert market price rows in batches, skipping existing ones."""
    if not points:
        return 0

    inserted = 0
    for batch_start in range(0, len(points), BATCH_SIZE):
        batch = points[batch_start: batch_start + BATCH_SIZE]

        # Find existing (symbol, datetime_utc, interval) keys in this batch
        batch_keys = [
            (p.symbol, p.datetime_utc, p.interval) for p in batch
        ]
        symbols_in_batch = list({p.symbol for p in batch})

        existing_result = await db.execute(
            select(MarketPrice.symbol, MarketPrice.datetime_utc, MarketPrice.interval)
            .where(MarketPrice.symbol.in_(symbols_in_batch))
        )
        existing_keys = {
            (r.symbol, r.datetime_utc.replace(tzinfo=timezone.utc)
             if r.datetime_utc.tzinfo is None else r.datetime_utc, r.interval)
            for r in existing_result
        }

        new_rows = [
            p for p in batch
            if (p.symbol, p.datetime_utc, p.interval) not in existing_keys
        ]

        if new_rows:
            db.add_all([
                MarketPrice(
                    id           = uuid.uuid4(),
                    symbol       = p.symbol,
                    datetime_utc = p.datetime_utc,
                    interval     = p.interval,
                    open         = p.open,
                    high         = p.high,
                    low          = p.low,
                    close        = p.close,
                    volume       = p.volume,
                    source       = p.source,
                )
                for p in new_rows
            ])
            await db.flush()
            inserted += len(new_rows)

        log.debug(
            "Market price batch processed",
            batch_size=len(batch),
            new=len(new_rows),
            skipped=len(batch) - len(new_rows),
        )

    return inserted


async def _bulk_insert_events(
    db:      AsyncSession,
    records: List[EconomicEventRecord],
) -> int:
    """Insert economic event rows in batches, skipping existing ones."""
    if not records:
        return 0

    inserted = 0
    for batch_start in range(0, len(records), BATCH_SIZE):
        batch = records[batch_start: batch_start + BATCH_SIZE]

        # Dedup key: (event_datetime_utc, currency, event_name, source)
        existing_result = await db.execute(
            select(
                HistoricalEvent.event_datetime_utc,
                HistoricalEvent.currency,
                HistoricalEvent.event_name,
                HistoricalEvent.source,
            ).where(
                HistoricalEvent.currency.in_(list({r.currency for r in batch}))
            )
        )
        existing_keys = {
            (
                r.event_datetime_utc.replace(tzinfo=timezone.utc)
                if r.event_datetime_utc.tzinfo is None
                else r.event_datetime_utc,
                r.currency,
                r.event_name,
                r.source,
            )
            for r in existing_result
        }

        new_rows = [
            r for r in batch
            if (r.event_datetime_utc, r.currency, r.event_name, r.source)
            not in existing_keys
        ]

        if new_rows:
            db.add_all([
                HistoricalEvent(
                    id                 = uuid.uuid4(),
                    event_datetime_utc = r.event_datetime_utc,
                    country            = r.country,
                    currency           = r.currency,
                    event_name         = r.event_name,
                    impact             = r.impact,
                    actual             = r.actual,
                    forecast           = r.forecast,
                    previous           = r.previous,
                    source             = r.source,
                )
                for r in new_rows
            ])
            await db.flush()
            inserted += len(new_rows)

    return inserted


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------

def _to_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


# ---------------------------------------------------------------------------
# Script entry-point
# ---------------------------------------------------------------------------

async def _main() -> None:
    """Run the full import when executed as a script."""
    # Register every model with SQLAlchemy's mapper registry before opening a
    # session.  Without these imports the User mapper fails to resolve the
    # "RefreshToken" relationship string (it is only imported under
    # TYPE_CHECKING in user.py and therefore never registered at runtime).
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

    from app.db.session import AsyncSessionFactory

    log.info("Historical data importer — starting")

    start = settings.HISTORICAL_DEFAULT_START_DATE or "2015-01-01"
    end   = settings.HISTORICAL_DEFAULT_END_DATE   or str(date.today())

    async with AsyncSessionFactory() as db:
        summary = await run_full_historical_import(
            db=db,
            start_date=start,
            end_date=end,
        )

    log.info("Historical data importer — finished", summary=summary)
    print("\n=== Import Summary ===")
    for key, n in summary.items():
        status = "ERROR" if n == -1 else f"{n} rows inserted"
        print(f"  {key:12s}: {status}")


if __name__ == "__main__":
    asyncio.run(_main())
