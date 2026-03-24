"""Historical Market Data Service — yfinance downloader.

Downloads up to 10 years of OHLCV data for forex pairs and commodities,
normalises all timestamps to UTC, and returns clean dataclasses ready
for storage or analysis.

Supported symbols (internal → yfinance ticker)
───────────────────────────────────────────────
  EURUSD  → EURUSD=X
  GBPUSD  → GBPUSD=X
  USDJPY  → USDJPY=X
  XAUUSD  → GC=F   (Gold front-month futures)
  XAGUSD  → SI=F   (Silver front-month futures)
  OIL     → CL=F   (WTI Crude front-month futures)

yfinance limitations
────────────────────
  Daily ("1d") / weekly ("1wk") / monthly ("1mo") — unlimited history.
  Hourly ("1h") — capped at 730 calendar days by Yahoo Finance.
  Any interval shorter than 1 h — capped at 60 days.

  If a requested range exceeds the provider cap the service automatically
  chunks the download and logs a warning.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from app.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Symbol mapping  (internal name → yfinance ticker)
# ---------------------------------------------------------------------------

_YFINANCE_SYMBOLS: Dict[str, str] = {
    # Forex
    "EURUSD":  "EURUSD=X",
    "GBPUSD":  "GBPUSD=X",
    "USDJPY":  "USDJPY=X",
    "AUDUSD":  "AUDUSD=X",
    "USDCAD":  "USDCAD=X",
    "USDCHF":  "USDCHF=X",
    # Commodities
    "XAUUSD":  "GC=F",    # Gold futures
    "XAGUSD":  "SI=F",    # Silver futures
    "OIL":     "CL=F",    # WTI Crude Oil futures
    "CL=F":    "CL=F",    # Accept raw yfinance ticker too
    # Indices (optional)
    "SPX":     "^GSPC",
    "NDX":     "^IXIC",
    "DXY":     "DX-Y.NYB",
}

# yfinance interval caps (max calendar days Yahoo Finance allows per request)
_INTERVAL_MAX_DAYS: Dict[str, int] = {
    "1m":   7,
    "2m":   60,
    "5m":   60,
    "15m":  60,
    "30m":  60,
    "60m":  730,
    "1h":   730,
    "90m":  60,
    "1d":   36500,   # effectively unlimited
    "5d":   36500,
    "1wk":  36500,
    "1mo":  36500,
    "3mo":  36500,
}

# Default supported symbols for a full historical import
DEFAULT_SYMBOLS: List[str] = [
    "EURUSD", "GBPUSD", "USDJPY",
    "XAUUSD", "XAGUSD", "OIL",
]


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class MarketDataPoint:
    """One normalised OHLCV bar."""

    symbol:       str
    datetime_utc: datetime   # always UTC-aware
    interval:     str        # "1d", "1h", etc.
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float
    source:       str = "yfinance"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_historical_market_data(
    symbol: str,
    start_date: date | datetime | str,
    end_date:   date | datetime | str,
    interval:   str = "1d",
    retry_attempts: int = 3,
    retry_delay_sec: float = 2.0,
) -> List[MarketDataPoint]:
    """
    Download OHLCV data for *symbol* from Yahoo Finance via yfinance.

    Parameters
    ----------
    symbol:
        Internal symbol name (e.g. "EURUSD") or a raw yfinance ticker.
    start_date / end_date:
        Inclusive date range.  Accepts ``date``, ``datetime`` or ISO string.
    interval:
        Bar size: "1d" (default), "1h", "1wk", "1mo".
        For intervals < 1 d Yahoo Finance caps history — the function
        automatically chunks and logs a warning.
    retry_attempts:
        Number of download attempts before giving up on a chunk.
    retry_delay_sec:
        Seconds to wait between retries.

    Returns
    -------
    List[MarketDataPoint] sorted oldest → newest.
    Returns an empty list if the download fails entirely.
    """
    try:
        import yfinance as yf  # imported here to keep the module loadable without yfinance
    except ImportError:
        log.error("yfinance not installed — run: pip install yfinance")
        return []

    ticker   = _resolve_ticker(symbol)
    start_dt = _to_date(start_date)
    end_dt   = _to_date(end_date)
    max_days = _INTERVAL_MAX_DAYS.get(interval, 36500)

    log.info(
        "Historical market data download started",
        symbol=symbol, ticker=ticker,
        start=str(start_dt), end=str(end_dt),
        interval=interval,
    )

    # Split into chunks if the interval has a history cap
    chunks = _date_chunks(start_dt, end_dt, max_days)
    if len(chunks) > 1:
        log.warning(
            "Interval cap — splitting download into chunks",
            symbol=symbol, interval=interval,
            max_days=max_days, chunks=len(chunks),
        )

    all_points: List[MarketDataPoint] = []
    for chunk_start, chunk_end in chunks:
        points = _download_chunk(
            yf=yf,
            ticker=ticker,
            symbol=symbol,
            start=chunk_start,
            end=chunk_end,
            interval=interval,
            attempts=retry_attempts,
            delay=retry_delay_sec,
        )
        all_points.extend(points)
        if len(chunks) > 1:
            time.sleep(0.5)   # be polite to Yahoo Finance

    # Deduplicate (chunks may overlap by one day at boundaries)
    seen: set = set()
    unique: List[MarketDataPoint] = []
    for p in all_points:
        key = (p.symbol, p.datetime_utc, p.interval)
        if key not in seen:
            seen.add(key)
            unique.append(p)

    unique.sort(key=lambda p: p.datetime_utc)

    log.info(
        "Historical market data download complete",
        symbol=symbol, interval=interval,
        rows=len(unique),
        from_date=str(unique[0].datetime_utc.date()) if unique else "—",
        to_date=str(unique[-1].datetime_utc.date())  if unique else "—",
    )
    return unique


def get_available_symbols() -> List[str]:
    """Return the list of symbols supported by this service."""
    return DEFAULT_SYMBOLS.copy()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_ticker(symbol: str) -> str:
    """Map an internal symbol name to its yfinance ticker string."""
    return _YFINANCE_SYMBOLS.get(symbol.upper(), symbol)


def _to_date(value: date | datetime | str) -> date:
    """Coerce various date-like types to a ``date`` object."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    # ISO string: "2015-01-01" or "2015-01-01T00:00:00"
    return date.fromisoformat(str(value)[:10])


def _date_chunks(
    start: date, end: date, max_days: int,
) -> List[tuple[date, date]]:
    """
    Split [start, end] into sub-ranges no larger than *max_days*.
    Returns a list of (chunk_start, chunk_end) pairs.
    """
    from datetime import timedelta

    chunks: List[tuple[date, date]] = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=max_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks if chunks else [(start, end)]


def _download_chunk(
    yf,
    ticker: str,
    symbol: str,
    start: date,
    end: date,
    interval: str,
    attempts: int,
    delay: float,
) -> List[MarketDataPoint]:
    """Download one date chunk from Yahoo Finance, retrying on failure."""
    import pandas as pd
    from datetime import timedelta

    # yfinance end date is exclusive → add one day
    yf_end = end + timedelta(days=1)

    for attempt in range(1, attempts + 1):
        try:
            df = yf.download(
                tickers=ticker,
                start=str(start),
                end=str(yf_end),
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=False,
            )

            if df is None or df.empty:
                log.warning(
                    "yfinance returned empty DataFrame",
                    ticker=ticker, start=str(start), end=str(end),
                    attempt=attempt,
                )
                if attempt < attempts:
                    time.sleep(delay)
                    continue
                return []

            return _normalise_dataframe(df, symbol=symbol, interval=interval)

        except Exception as exc:
            log.warning(
                "yfinance download failed",
                ticker=ticker, attempt=attempt, error=str(exc),
            )
            if attempt < attempts:
                time.sleep(delay * attempt)   # exponential back-off
            else:
                log.error(
                    "yfinance download gave up after all attempts",
                    ticker=ticker, start=str(start), end=str(end),
                )
                return []

    return []


def _normalise_dataframe(df, symbol: str, interval: str) -> List[MarketDataPoint]:
    """
    Convert a yfinance DataFrame into a list of MarketDataPoint objects.

    yfinance returns a MultiIndex column DataFrame for single tickers in
    some versions; this function handles both the flat and multi-level cases.
    All timestamps are converted to UTC.
    """
    import pandas as pd
    import numpy as np

    # Flatten MultiIndex columns if present (yfinance >= 0.2.x)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    # Normalise column names to lowercase
    df.columns = [str(c).lower() for c in df.columns]

    required = {"open", "high", "low", "close"}
    missing  = required - set(df.columns)
    if missing:
        log.warning(
            "yfinance DataFrame missing expected columns",
            symbol=symbol, missing=list(missing), got=list(df.columns),
        )
        return []

    if "volume" not in df.columns:
        df["volume"] = 0.0

    points: List[MarketDataPoint] = []
    for ts, row in df.iterrows():
        # Convert index timestamp to UTC-aware datetime
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            dt_utc = ts.to_pydatetime().astimezone(timezone.utc)
        else:
            # Naive → assume UTC (Yahoo Finance daily bars are date-only)
            dt_utc = pd.Timestamp(ts).to_pydatetime().replace(tzinfo=timezone.utc)

        # Skip rows with NaN OHLC (happens at market open/close boundaries)
        try:
            o = float(row["open"])
            h = float(row["high"])
            lo = float(row["low"])
            c = float(row["close"])
            v = float(row.get("volume", 0) or 0)
        except (ValueError, TypeError):
            continue

        if any(np.isnan(x) for x in (o, h, lo, c)):
            continue

        points.append(MarketDataPoint(
            symbol=symbol,
            datetime_utc=dt_utc,
            interval=interval,
            open=round(o, 8),
            high=round(h, 8),
            low=round(lo, 8),
            close=round(c, 8),
            volume=round(v, 2),
            source="yfinance",
        ))

    return points
