"""
Unit tests for the historical data pipeline.

Uses the existing SQLite in-memory fixture from conftest.py — no Postgres
or real network calls required.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import List
from unittest.mock import patch, MagicMock

import pytest
import pytest_asyncio

from app.models.historical_event import HistoricalEvent
from app.models.market_price import MarketPrice
from app.services.backtest_data_service import (
    get_events_near_timestamp,
    get_price_change_around_event,
    get_prices_near_timestamp,
    trade_is_near_high_impact_event,
)
from app.services.historical_economic_events_service import (
    EconomicEventRecord,
    _normalise_impact,
    _str_or_none,
    is_high_impact_event,
)
from app.services.historical_market_data_service import (
    MarketDataPoint,
    _normalise_dataframe,
    _resolve_ticker,
    _to_date,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)


def _make_price(
    db_session,
    symbol:   str,
    dt:       datetime,
    close:    float = 1.1000,
    interval: str   = "1d",
) -> MarketPrice:
    mp = MarketPrice(
        id           = uuid.uuid4(),
        symbol       = symbol,
        datetime_utc = dt,
        interval     = interval,
        open         = close - 0.001,
        high         = close + 0.002,
        low          = close - 0.002,
        close        = close,
        volume       = 1000.0,
        source       = "test",
    )
    db_session.add(mp)
    return mp


def _make_event(
    db_session,
    currency:   str,
    dt:         datetime,
    name:       str   = "CPI m/m",
    impact:     str   = "high",
) -> HistoricalEvent:
    ev = HistoricalEvent(
        id                 = uuid.uuid4(),
        event_datetime_utc = dt,
        country            = "US",
        currency           = currency,
        event_name         = name,
        impact             = impact,
        actual             = "0.3%",
        forecast           = "0.2%",
        previous           = "0.1%",
        source             = "test",
    )
    db_session.add(ev)
    return ev


# ===========================================================================
# 1. Market data normalisation
# ===========================================================================

class TestMarketDataNormalisation:

    def test_resolve_ticker_known_symbol(self):
        assert _resolve_ticker("EURUSD") == "EURUSD=X"
        assert _resolve_ticker("XAUUSD") == "GC=F"
        assert _resolve_ticker("OIL")    == "CL=F"

    def test_resolve_ticker_unknown_passthrough(self):
        assert _resolve_ticker("UNKNOWN") == "UNKNOWN"

    def test_to_date_from_string(self):
        d = _to_date("2020-06-15")
        assert d == date(2020, 6, 15)

    def test_to_date_from_datetime(self):
        dt = datetime(2020, 6, 15, 12, 0, tzinfo=timezone.utc)
        assert _to_date(dt) == date(2020, 6, 15)

    def test_to_date_from_date(self):
        d = date(2020, 6, 15)
        assert _to_date(d) == d

    def test_normalise_dataframe(self):
        """_normalise_dataframe should convert a DataFrame into MarketDataPoints."""
        import pandas as pd
        import numpy as np

        index = pd.DatetimeIndex(
            [pd.Timestamp("2020-01-02", tz="UTC"),
             pd.Timestamp("2020-01-03", tz="UTC")],
        )
        df = pd.DataFrame({
            "open":   [1.10, 1.11],
            "high":   [1.12, 1.13],
            "low":    [1.09, 1.10],
            "close":  [1.11, 1.12],
            "volume": [1000, 2000],
        }, index=index)

        points = _normalise_dataframe(df, symbol="EURUSD", interval="1d")

        assert len(points) == 2
        assert points[0].symbol       == "EURUSD"
        assert points[0].close        == pytest.approx(1.11, abs=1e-6)
        assert points[0].datetime_utc.tzinfo == timezone.utc
        assert points[1].datetime_utc  > points[0].datetime_utc

    def test_normalise_dataframe_skips_nan(self):
        """Rows with NaN OHLC values should be dropped silently."""
        import pandas as pd
        import numpy as np

        index = pd.DatetimeIndex([
            pd.Timestamp("2020-01-02", tz="UTC"),
            pd.Timestamp("2020-01-03", tz="UTC"),
        ])
        df = pd.DataFrame({
            "open":   [np.nan, 1.11],
            "high":   [np.nan, 1.13],
            "low":    [np.nan, 1.10],
            "close":  [np.nan, 1.12],
            "volume": [0,      2000],
        }, index=index)

        points = _normalise_dataframe(df, symbol="EURUSD", interval="1d")
        assert len(points) == 1
        assert points[0].close == pytest.approx(1.12, abs=1e-6)


# ===========================================================================
# 2. Economic event normalisation
# ===========================================================================

class TestEconomicEventNormalisation:

    def test_normalise_impact_high(self):
        assert _normalise_impact("high")        == "high"
        assert _normalise_impact("3")           == "high"
        assert _normalise_impact("HIGH IMPACT") == "high"

    def test_normalise_impact_medium(self):
        assert _normalise_impact("medium")       == "medium"
        assert _normalise_impact("Moderate")     == "medium"

    def test_normalise_impact_low(self):
        assert _normalise_impact("low")     == "low"
        assert _normalise_impact("unknown") == "low"
        assert _normalise_impact("")        == "low"

    def test_str_or_none_empty(self):
        assert _str_or_none("") is None
        assert _str_or_none("  ") is None
        assert _str_or_none(None) is None
        assert _str_or_none("nan") is None

    def test_str_or_none_value(self):
        assert _str_or_none("0.3%") == "0.3%"
        assert _str_or_none(" 1.5 ") == "1.5"


# ===========================================================================
# 3. High-impact event detection
# ===========================================================================

class TestHighImpactDetection:

    def _event(self, name: str, impact: str = "low") -> EconomicEventRecord:
        return EconomicEventRecord(
            event_datetime_utc = _utc(2020, 1, 10),
            country   = "US",
            currency  = "USD",
            event_name = name,
            impact    = impact,
            actual    = None,
            forecast  = None,
            previous  = None,
            source    = "test",
        )

    def test_high_by_impact_flag(self):
        assert is_high_impact_event(self._event("Random Event", impact="high"))

    def test_high_by_keyword_cpi(self):
        assert is_high_impact_event(self._event("CPI m/m", impact="medium"))

    def test_high_by_keyword_nfp(self):
        assert is_high_impact_event(self._event("Non-Farm Payrolls", impact="low"))

    def test_high_by_keyword_fomc(self):
        assert is_high_impact_event(self._event("FOMC Statement", impact="low"))

    def test_high_by_keyword_fed(self):
        assert is_high_impact_event(self._event("Fed Interest Rate Decision"))

    def test_not_high(self):
        assert not is_high_impact_event(self._event("Retail Sales", impact="medium"))

    def test_not_high_low_unknown_event(self):
        assert not is_high_impact_event(self._event("Housing Starts", impact="low"))


# ===========================================================================
# 4. DB: insert without duplicates
# ===========================================================================

@pytest.mark.asyncio
async def test_market_price_no_duplicate(db):
    """Inserting the same (symbol, datetime_utc, interval) twice raises IntegrityError."""
    from sqlalchemy.exc import IntegrityError

    dt = _utc(2020, 3, 10)
    _make_price(db, "EURUSD", dt)
    await db.flush()

    _make_price(db, "EURUSD", dt)    # exact duplicate
    with pytest.raises(IntegrityError):
        await db.flush()


@pytest.mark.asyncio
async def test_historical_event_no_duplicate(db):
    """Same (datetime, currency, event_name, source) must raise IntegrityError."""
    from sqlalchemy.exc import IntegrityError

    dt = _utc(2020, 3, 10, 14)
    _make_event(db, "USD", dt, name="CPI m/m")
    await db.flush()

    _make_event(db, "USD", dt, name="CPI m/m")   # duplicate
    with pytest.raises(IntegrityError):
        await db.flush()


@pytest.mark.asyncio
async def test_market_price_different_interval_ok(db):
    """Same symbol + datetime but different interval should NOT conflict."""
    dt = _utc(2020, 3, 10)
    p1 = MarketPrice(
        id=uuid.uuid4(), symbol="EURUSD", datetime_utc=dt, interval="1d",
        open=1.10, high=1.12, low=1.09, close=1.11, volume=0, source="test",
    )
    p2 = MarketPrice(
        id=uuid.uuid4(), symbol="EURUSD", datetime_utc=dt, interval="1h",
        open=1.10, high=1.12, low=1.09, close=1.11, volume=0, source="test",
    )
    db.add(p1)
    db.add(p2)
    await db.flush()   # should not raise


# ===========================================================================
# 5. Backtest queries: events near timestamp
# ===========================================================================

@pytest.mark.asyncio
async def test_get_events_near_timestamp(db):
    """Only events within the window are returned."""
    ref   = _utc(2020, 5, 1, 12)
    # Inside window
    _make_event(db, "USD", _utc(2020, 5, 1, 11, 40), name="CPI m/m",  impact="high")
    _make_event(db, "USD", _utc(2020, 5, 1, 12, 20), name="FOMC",     impact="high")
    # Outside window (> 30 min)
    _make_event(db, "USD", _utc(2020, 5, 1, 11, 0),  name="PMI",      impact="low")
    _make_event(db, "USD", _utc(2020, 5, 1, 13, 30), name="Speeches", impact="low")
    await db.flush()

    events = await get_events_near_timestamp(db, ref, minutes_before=30, minutes_after=30)
    names = {e.event_name for e in events}
    assert "CPI m/m" in names
    assert "FOMC"    in names
    assert "PMI"     not in names
    assert "Speeches" not in names


@pytest.mark.asyncio
async def test_get_events_near_timestamp_currency_filter(db):
    """Currency filter should only return matching events."""
    ref = _utc(2020, 6, 1, 10)
    _make_event(db, "USD", _utc(2020, 6, 1, 10, 10), name="NFP",     impact="high")
    _make_event(db, "EUR", _utc(2020, 6, 1, 10, 10), name="ECB Rate",impact="high")
    await db.flush()

    eur_events = await get_events_near_timestamp(
        db, ref, minutes_before=30, minutes_after=30, currencies=["EUR"]
    )
    assert all(e.currency == "EUR" for e in eur_events)
    assert any(e.event_name == "ECB Rate" for e in eur_events)


# ===========================================================================
# 6. Backtest queries: prices near timestamp
# ===========================================================================

@pytest.mark.asyncio
async def test_get_prices_near_timestamp(db):
    """Returns correct number of before/after bars."""
    ref = _utc(2020, 7, 10)
    # 5 bars before ref, 3 bars after
    for i in range(5, 0, -1):
        from datetime import timedelta
        _make_price(db, "EURUSD", ref - timedelta(days=i), close=1.09 + i * 0.001)
    _make_price(db, "EURUSD", ref, close=1.100)
    for i in range(1, 4):
        from datetime import timedelta
        _make_price(db, "EURUSD", ref + timedelta(days=i), close=1.101 + i * 0.001)
    await db.flush()

    bars = await get_prices_near_timestamp(
        db, "EURUSD", ref, before_bars=3, after_bars=2
    )
    def _aware(dt: datetime) -> datetime:
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    before = [b for b in bars if _aware(b.datetime_utc) <= ref]
    after  = [b for b in bars if _aware(b.datetime_utc) >  ref]
    assert len(before) == 3
    assert len(after)  == 2


# ===========================================================================
# 7. trade_is_near_high_impact_event
# ===========================================================================

@pytest.mark.asyncio
async def test_trade_near_high_impact_returns_true(db):
    ref = _utc(2020, 8, 5, 13, 0)
    _make_event(db, "USD", _utc(2020, 8, 5, 13, 15), name="NFP", impact="high")
    await db.flush()

    result = await trade_is_near_high_impact_event(
        db, symbol="EURUSD", timestamp_utc=ref, window_minutes=60
    )
    assert result is True


@pytest.mark.asyncio
async def test_trade_near_low_impact_returns_false(db):
    ref = _utc(2020, 8, 6, 13, 0)
    _make_event(db, "USD", _utc(2020, 8, 6, 13, 15), name="Housing Starts", impact="low")
    await db.flush()

    result = await trade_is_near_high_impact_event(
        db, symbol="EURUSD", timestamp_utc=ref, window_minutes=60
    )
    assert result is False


@pytest.mark.asyncio
async def test_trade_near_keyword_event_returns_true(db):
    """An event tagged as 'medium' but with a high-impact keyword should return True."""
    ref = _utc(2020, 9, 1, 14, 0)
    _make_event(db, "USD", _utc(2020, 9, 1, 14, 20),
                name="FOMC Minutes", impact="medium")
    await db.flush()

    result = await trade_is_near_high_impact_event(
        db, symbol="EURUSD", timestamp_utc=ref, window_minutes=60
    )
    assert result is True
