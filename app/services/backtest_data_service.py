"""Backtest Data Service.

Provides async query helpers that let backtesting code retrieve historical
market prices and economic events relative to a given timestamp.

All functions accept an AsyncSession and return plain Python objects —
no HTTP calls, no side effects.

Typical usage inside a backtest loop
──────────────────────────────────────
    from app.services.backtest_data_service import (
        get_prices_near_timestamp,
        get_events_near_timestamp,
        trade_is_near_high_impact_event,
    )

    # Load 50 bars before and 10 bars after a trade timestamp
    bars = await get_prices_near_timestamp(
        db, symbol="EURUSD", timestamp_utc=trade_dt,
        before_bars=50, after_bars=10,
    )

    # Check whether the trade sits inside an event window
    risky = await trade_is_near_high_impact_event(
        db, symbol="EURUSD", timestamp_utc=trade_dt, window_minutes=60,
    )
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.historical_event import HistoricalEvent
from app.models.market_price import MarketPrice
from app.services.historical_economic_events_service import HIGH_IMPACT_KEYWORDS

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Currency extraction helper (same logic as bot_service._symbol_currencies)
# ---------------------------------------------------------------------------

def _symbol_currencies(symbol: str) -> set[str]:
    """Extract currency codes from a forex symbol such as "EURUSD"."""
    clean = symbol.upper().replace("/", "").replace("_", "").replace("-", "")
    if len(clean) >= 6:
        return {clean[:3], clean[3:6]}
    return {clean}


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------

async def get_events_near_timestamp(
    db:             AsyncSession,
    timestamp_utc:  datetime,
    minutes_before: int = 30,
    minutes_after:  int = 30,
    currencies:     Optional[List[str]] = None,
    impact_filter:  Optional[str]       = None,
) -> List[HistoricalEvent]:
    """
    Return historical events within a time window around *timestamp_utc*.

    Parameters
    ----------
    db:
        Active async DB session.
    timestamp_utc:
        Reference moment (should be UTC-aware).
    minutes_before / minutes_after:
        Half-width of the window in minutes.
    currencies:
        Optional list of currency codes to filter on.
    impact_filter:
        "high", "medium", "low" — return only events of this impact level.

    Returns
    -------
    List[HistoricalEvent] sorted by event_datetime_utc ascending.
    """
    ts = _ensure_utc(timestamp_utc)
    window_start = ts - timedelta(minutes=minutes_before)
    window_end   = ts + timedelta(minutes=minutes_after)

    conditions = [
        HistoricalEvent.event_datetime_utc >= window_start,
        HistoricalEvent.event_datetime_utc <= window_end,
    ]
    if currencies:
        conditions.append(HistoricalEvent.currency.in_(currencies))
    if impact_filter:
        conditions.append(HistoricalEvent.impact == impact_filter.lower())

    result = await db.execute(
        select(HistoricalEvent)
        .where(and_(*conditions))
        .order_by(HistoricalEvent.event_datetime_utc)
    )
    return list(result.scalars().all())


async def get_prices_near_timestamp(
    db:            AsyncSession,
    symbol:        str,
    timestamp_utc: datetime,
    before_bars:   int = 50,
    after_bars:    int = 50,
    interval:      str = "1d",
) -> List[MarketPrice]:
    """
    Return up to *before_bars* bars before and *after_bars* bars after
    *timestamp_utc* for *symbol*.

    Returns
    -------
    List[MarketPrice] sorted oldest → newest.
    """
    ts = _ensure_utc(timestamp_utc)

    # Bars before (include the bar that contains ts)
    before_result = await db.execute(
        select(MarketPrice)
        .where(
            MarketPrice.symbol       == symbol,
            MarketPrice.interval     == interval,
            MarketPrice.datetime_utc <= ts,
        )
        .order_by(MarketPrice.datetime_utc.desc())
        .limit(before_bars)
    )
    before_rows = list(reversed(before_result.scalars().all()))

    # Bars after
    after_result = await db.execute(
        select(MarketPrice)
        .where(
            MarketPrice.symbol       == symbol,
            MarketPrice.interval     == interval,
            MarketPrice.datetime_utc >  ts,
        )
        .order_by(MarketPrice.datetime_utc.asc())
        .limit(after_bars)
    )
    after_rows = list(after_result.scalars().all())

    return before_rows + after_rows


async def get_price_window(
    db:        AsyncSession,
    symbol:    str,
    start_utc: datetime,
    end_utc:   datetime,
    interval:  str = "1d",
) -> List[MarketPrice]:
    """
    Return all price bars for *symbol* between *start_utc* and *end_utc*.

    Returns
    -------
    List[MarketPrice] sorted oldest → newest.
    """
    result = await db.execute(
        select(MarketPrice)
        .where(
            MarketPrice.symbol       == symbol,
            MarketPrice.interval     == interval,
            MarketPrice.datetime_utc >= _ensure_utc(start_utc),
            MarketPrice.datetime_utc <= _ensure_utc(end_utc),
        )
        .order_by(MarketPrice.datetime_utc.asc())
    )
    return list(result.scalars().all())


async def trade_is_near_high_impact_event(
    db:             AsyncSession,
    symbol:         str,
    timestamp_utc:  datetime,
    window_minutes: int = 60,
) -> bool:
    """
    Return True if a high-impact event for the symbol's currencies falls
    within *window_minutes* of *timestamp_utc*.

    This is the primary integration hook for the trading bot: call this
    before opening a position to decide whether to reduce size or skip.
    """
    currencies  = list(_symbol_currencies(symbol))
    ts          = _ensure_utc(timestamp_utc)
    window_half = window_minutes // 2

    events = await get_events_near_timestamp(
        db=db,
        timestamp_utc=ts,
        minutes_before=window_half,
        minutes_after=window_half,
        currencies=currencies,
    )

    for ev in events:
        if ev.impact == "high":
            return True
        # Also catch events with high-impact names regardless of tagged impact
        name_upper = ev.event_name.upper()
        if any(kw in name_upper for kw in HIGH_IMPACT_KEYWORDS):
            return True

    return False


async def get_price_change_around_event(
    db:             AsyncSession,
    symbol:         str,
    event:          HistoricalEvent,
    bars_before:    int = 10,
    bars_after:     int = 10,
    interval:       str = "1d",
) -> dict:
    """
    Measure price behaviour before and after a historical event.

    Returns a dict with:
      - ``pre_close``   — close price of the bar immediately before the event
      - ``post_close``  — close price of the bar immediately after the event
      - ``pct_change``  — percentage change pre → post
      - ``bars_before`` / ``bars_after`` — list of close prices
    """
    ts    = _ensure_utc(event.event_datetime_utc)
    bars  = await get_prices_near_timestamp(
        db=db,
        symbol=symbol,
        timestamp_utc=ts,
        before_bars=bars_before,
        after_bars=bars_after,
        interval=interval,
    )

    pre_bars  = [b for b in bars if b.datetime_utc <= ts]
    post_bars = [b for b in bars if b.datetime_utc >  ts]

    pre_close  = float(pre_bars[-1].close)  if pre_bars  else None
    post_close = float(post_bars[0].close) if post_bars else None
    pct_change = (
        round((post_close - pre_close) / pre_close * 100, 4)
        if pre_close and post_close and pre_close != 0
        else None
    )

    return {
        "event":        event.event_name,
        "event_time":   str(ts),
        "symbol":       symbol,
        "pre_close":    pre_close,
        "post_close":   post_close,
        "pct_change":   pct_change,
        "bars_before":  [float(b.close) for b in pre_bars],
        "bars_after":   [float(b.close) for b in post_bars],
    }


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _ensure_utc(dt: datetime) -> datetime:
    """Return a UTC-aware datetime regardless of whether input has tzinfo."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
