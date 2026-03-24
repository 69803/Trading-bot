"""
Tests for the historical economic events pipeline.

Covers:
  - _normalise_te_item: maps raw Trading Economics dicts to EconomicEventRecord
  - _fetch_trading_economics: mocked TE library (no real API calls)
  - get_historical_economic_events routing:
      * no API key + no CSV → returns [] and logs clearly
      * API key present    → calls TE and returns records
      * csv_path provided  → uses CSV provider
  - is_high_impact_event: all required keywords (CPI, NFP, Interest Rate,
      FOMC, ECB, Fed) plus impact-flag path
  - import_historical_economic_events: end-to-end DB insertion with mocked data
  - Idempotency: second insert of same events → 0 new rows
"""
from __future__ import annotations

import sys
import uuid
from datetime import date, datetime, timezone
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.historical_event import HistoricalEvent
from app.services.historical_economic_events_service import (
    EconomicEventRecord,
    _normalise_te_item,
    _str_or_none,
    get_historical_economic_events,
    is_high_impact_event,
)
from app.services.historical_data_importer import import_historical_economic_events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(year: int, month: int, day: int, hour: int = 12) -> datetime:
    return datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)


def _te_item(
    date_str:   str  = "2020-05-01 14:00:00",
    currency:   str  = "USD",
    country:    str  = "United States",
    event:      str  = "CPI m/m",
    importance: str  = "3",
    actual:     str  = "0.3%",
    forecast:   str  = "0.2%",
    previous:   str  = "0.1%",
) -> dict:
    """Build a dict that mimics a Trading Economics calendar item."""
    return {
        "Date":       date_str,
        "Currency":   currency,
        "Country":    country,
        "Event":      event,
        "Importance": importance,
        "Actual":     actual,
        "Forecast":   forecast,
        "Previous":   previous,
    }


def _sample_records(n: int = 3) -> List[EconomicEventRecord]:
    """Return a list of ready-made EconomicEventRecord objects for DB tests."""
    events = [
        ("CPI m/m",             "high",   "USD"),
        ("Non-Farm Payrolls",   "high",   "USD"),
        ("ECB Interest Rate",   "high",   "EUR"),
    ]
    return [
        EconomicEventRecord(
            event_datetime_utc = _utc(2020, 1, i + 1),
            country            = "US" if cur == "USD" else "EU",
            currency           = cur,
            event_name         = name,
            impact             = impact,
            actual             = "0.3%",
            forecast           = "0.2%",
            previous           = "0.1%",
            source             = "trading_economics",
        )
        for i, (name, impact, cur) in enumerate(events[:n])
    ]


# ===========================================================================
# 1. _normalise_te_item
# ===========================================================================

class TestNormaliseTEItem:

    def test_complete_item_returns_record(self):
        item = _te_item()
        rec = _normalise_te_item(item)
        assert rec is not None
        assert rec.currency    == "USD"
        assert rec.event_name  == "CPI m/m"
        assert rec.impact      == "high"          # importance "3" → high
        assert rec.actual      == "0.3%"
        assert rec.forecast    == "0.2%"
        assert rec.previous    == "0.1%"
        assert rec.source      == "trading_economics"
        assert rec.event_datetime_utc.tzinfo is not None  # UTC-aware

    def test_missing_date_returns_none(self):
        item = _te_item(date_str="")
        item["Date"] = ""
        assert _normalise_te_item(item) is None

    def test_importance_3_maps_to_high(self):
        rec = _normalise_te_item(_te_item(importance="3"))
        assert rec is not None
        assert rec.impact == "high"

    def test_importance_2_maps_to_medium(self):
        rec = _normalise_te_item(_te_item(importance="2"))
        assert rec is not None
        assert rec.impact == "medium"

    def test_importance_1_maps_to_low(self):
        rec = _normalise_te_item(_te_item(importance="1"))
        assert rec is not None
        assert rec.impact == "low"

    def test_empty_actual_becomes_none(self):
        rec = _normalise_te_item(_te_item(actual=""))
        assert rec is not None
        assert rec.actual is None

    def test_nan_forecast_becomes_none(self):
        rec = _normalise_te_item(_te_item(forecast="nan"))
        assert rec is not None
        assert rec.forecast is None

    def test_currency_uppercased(self):
        item = _te_item(currency="eur")
        item["Currency"] = "eur"
        rec = _normalise_te_item(item)
        assert rec is not None
        assert rec.currency == "EUR"

    def test_lowercase_keys_also_work(self):
        """TE sometimes returns lowercase keys."""
        item = {
            "date":       "2020-06-01 10:00:00",
            "currency":   "GBP",
            "country":    "United Kingdom",
            "event":      "BOE Rate Decision",
            "importance": "3",
            "actual":     "0.10%",
            "forecast":   "0.10%",
            "previous":   "0.10%",
        }
        rec = _normalise_te_item(item)
        assert rec is not None
        assert rec.currency == "GBP"
        assert rec.impact   == "high"


# ===========================================================================
# 2. _fetch_trading_economics (mocked TE library)
# ===========================================================================

class TestFetchTradingEconomics:

    def _make_fake_te(self, return_data: list) -> MagicMock:
        fake = MagicMock()
        fake.getCalendarData.return_value = return_data
        return fake

    def test_returns_normalised_records(self):
        raw = [_te_item("2020-01-02 14:00:00", importance="3"),
               _te_item("2020-01-03 14:00:00", event="NFP", importance="3")]
        fake_te = self._make_fake_te(raw)

        with patch.dict(sys.modules, {"tradingeconomics": fake_te}):
            from app.services import historical_economic_events_service as svc
            records = svc._fetch_trading_economics(
                start      = date(2020, 1, 1),
                end        = date(2020, 1, 31),
                countries  = None,
                currencies = None,
                api_key    = "fake-key",
            )

        assert len(records) == 2
        assert all(r.source == "trading_economics" for r in records)

    def test_empty_response_returns_empty_list(self):
        fake_te = self._make_fake_te([])

        with patch.dict(sys.modules, {"tradingeconomics": fake_te}):
            from app.services import historical_economic_events_service as svc
            records = svc._fetch_trading_economics(
                start=date(2020, 1, 1), end=date(2020, 1, 31),
                countries=None, currencies=None, api_key="fake-key",
            )

        assert records == []

    def test_te_exception_returns_empty_list(self):
        fake_te = MagicMock()
        fake_te.getCalendarData.side_effect = RuntimeError("network error")

        with patch.dict(sys.modules, {"tradingeconomics": fake_te}):
            from app.services import historical_economic_events_service as svc
            records = svc._fetch_trading_economics(
                start=date(2020, 1, 1), end=date(2020, 1, 31),
                countries=None, currencies=None, api_key="fake-key",
            )

        assert records == []

    def test_te_not_installed_returns_empty_list(self):
        """If tradingeconomics package is absent, should return [] gracefully."""
        # Remove the module from sys.modules to simulate it being absent
        original = sys.modules.pop("tradingeconomics", None)
        try:
            from app.services import historical_economic_events_service as svc
            records = svc._fetch_trading_economics(
                start=date(2020, 1, 1), end=date(2020, 1, 31),
                countries=None, currencies=None, api_key="fake-key",
            )
            assert records == []
        finally:
            if original is not None:
                sys.modules["tradingeconomics"] = original

    def test_currency_filter_applied(self):
        raw = [
            _te_item("2020-01-02 14:00:00", currency="USD"),
            _te_item("2020-01-03 14:00:00", currency="EUR"),
        ]
        fake_te = self._make_fake_te(raw)

        with patch.dict(sys.modules, {"tradingeconomics": fake_te}):
            from app.services import historical_economic_events_service as svc
            records = svc._fetch_trading_economics(
                start=date(2020, 1, 1), end=date(2020, 1, 31),
                countries=None, currencies=["USD"], api_key="fake-key",
            )

        assert all(r.currency == "USD" for r in records)
        assert len(records) == 1


# ===========================================================================
# 3. get_historical_economic_events routing
# ===========================================================================

class TestGetHistoricalEconomicEventsRouting:

    def test_no_key_no_csv_returns_empty(self):
        """Without API key or CSV, must return [] and NOT crash."""
        with patch("app.services.historical_economic_events_service.settings") as mock_settings:
            mock_settings.TRADING_ECONOMICS_API_KEY = ""
            records = get_historical_economic_events(
                start_date = date(2020, 1, 1),
                end_date   = date(2020, 1, 31),
            )
        assert records == []

    def test_key_present_calls_te_provider(self):
        fake_te = MagicMock()
        fake_te.getCalendarData.return_value = [_te_item()]

        with patch.dict(sys.modules, {"tradingeconomics": fake_te}):
            with patch("app.services.historical_economic_events_service.settings") as mock_settings:
                mock_settings.TRADING_ECONOMICS_API_KEY = "real-key"
                records = get_historical_economic_events(
                    start_date = date(2020, 1, 1),
                    end_date   = date(2020, 1, 31),
                )

        assert len(records) == 1
        assert records[0].source == "trading_economics"

    def test_csv_path_overrides_api_key(self, tmp_path):
        """csv_path takes priority even when an API key is set."""
        csv_file = tmp_path / "events.csv"
        csv_file.write_text(
            "datetime,currency,event_name,impact,actual,forecast,previous\n"
            "2020-06-01 12:00:00,USD,CPI m/m,high,0.3%,0.2%,0.1%\n"
        )

        with patch("app.services.historical_economic_events_service.settings") as mock_settings:
            mock_settings.TRADING_ECONOMICS_API_KEY = "some-key"
            records = get_historical_economic_events(
                start_date = date(2020, 1, 1),
                end_date   = date(2020, 12, 31),
                csv_path   = str(csv_file),
            )

        assert len(records) == 1
        assert records[0].currency   == "USD"
        assert records[0].event_name == "CPI m/m"
        assert records[0].source     == "csv"


# ===========================================================================
# 4. is_high_impact_event — all required keywords
# ===========================================================================

class TestIsHighImpactEventKeywords:

    def _evt(self, name: str, impact: str = "low") -> EconomicEventRecord:
        return EconomicEventRecord(
            event_datetime_utc = _utc(2020, 1, 1),
            country="US", currency="USD",
            event_name=name, impact=impact,
            actual=None, forecast=None, previous=None,
            source="test",
        )

    # Required by spec: CPI, NFP, Interest Rate, FOMC, ECB, Fed
    def test_cpi(self):
        assert is_high_impact_event(self._evt("CPI m/m", impact="low"))

    def test_nfp(self):
        assert is_high_impact_event(self._evt("Non-Farm Payrolls", impact="low"))

    def test_interest_rate(self):
        assert is_high_impact_event(self._evt("Federal Interest Rate Decision", impact="medium"))

    def test_fomc(self):
        assert is_high_impact_event(self._evt("FOMC Statement", impact="low"))

    def test_ecb(self):
        assert is_high_impact_event(self._evt("ECB Monetary Policy Meeting", impact="medium"))

    def test_fed(self):
        assert is_high_impact_event(self._evt("Fed Chair Powell Speaks", impact="medium"))

    def test_high_impact_flag_alone_is_sufficient(self):
        assert is_high_impact_event(self._evt("Retail Sales", impact="high"))

    def test_gdp_keyword(self):
        assert is_high_impact_event(self._evt("GDP q/q", impact="medium"))

    def test_unemployment_keyword(self):
        assert is_high_impact_event(self._evt("Unemployment Rate", impact="medium"))

    def test_medium_non_keyword_is_not_high(self):
        assert not is_high_impact_event(self._evt("Retail Sales", impact="medium"))

    def test_low_non_keyword_is_not_high(self):
        assert not is_high_impact_event(self._evt("Housing Starts", impact="low"))


# ===========================================================================
# 5. import_historical_economic_events — end-to-end DB insertion
# ===========================================================================

@pytest.mark.asyncio
async def test_import_events_inserts_new_rows(db: AsyncSession):
    """Mocked records should be persisted to the DB correctly."""
    records = _sample_records(3)

    with patch(
        "app.services.historical_data_importer.get_historical_economic_events",
        return_value=records,
    ):
        inserted = await import_historical_economic_events(
            db         = db,
            start_date = date(2020, 1, 1),
            end_date   = date(2020, 1, 31),
        )

    assert inserted == 3

    result = await db.execute(
        __import__("sqlalchemy", fromlist=["select"]).select(HistoricalEvent)
    )
    rows = result.scalars().all()
    assert len(rows) == 3
    currencies = {r.currency for r in rows}
    assert "USD" in currencies
    assert "EUR" in currencies


@pytest.mark.asyncio
async def test_import_events_idempotent(db: AsyncSession):
    """Inserting the same records twice → second call inserts 0 rows."""
    records = _sample_records(2)

    patch_target = "app.services.historical_data_importer.get_historical_economic_events"

    with patch(patch_target, return_value=records):
        first = await import_historical_economic_events(
            db=db, start_date=date(2020, 1, 1), end_date=date(2020, 1, 31),
        )

    await db.flush()

    with patch(patch_target, return_value=records):
        second = await import_historical_economic_events(
            db=db, start_date=date(2020, 1, 1), end_date=date(2020, 1, 31),
        )

    assert first  == 2
    assert second == 0


@pytest.mark.asyncio
async def test_import_events_no_api_key_inserts_zero(db: AsyncSession):
    """When get_historical_economic_events returns [] (no key), insert 0 rows."""
    with patch(
        "app.services.historical_data_importer.get_historical_economic_events",
        return_value=[],
    ):
        inserted = await import_historical_economic_events(
            db=db, start_date=date(2020, 1, 1), end_date=date(2020, 1, 31),
        )

    assert inserted == 0


@pytest.mark.asyncio
async def test_import_events_correct_field_values(db: AsyncSession):
    """Verify all fields land in the DB with the right values."""
    records = [EconomicEventRecord(
        event_datetime_utc = _utc(2020, 3, 6, 13),
        country            = "US",
        currency           = "USD",
        event_name         = "Non-Farm Payrolls",
        impact             = "high",
        actual             = "275K",
        forecast           = "200K",
        previous           = "225K",
        source             = "trading_economics",
    )]

    with patch(
        "app.services.historical_data_importer.get_historical_economic_events",
        return_value=records,
    ):
        await import_historical_economic_events(
            db=db, start_date=date(2020, 3, 1), end_date=date(2020, 3, 31),
        )

    from sqlalchemy import select
    result = await db.execute(select(HistoricalEvent).where(HistoricalEvent.currency == "USD"))
    row = result.scalars().first()

    assert row is not None
    assert row.event_name == "Non-Farm Payrolls"
    assert row.impact     == "high"
    assert row.actual     == "275K"
    assert row.forecast   == "200K"
    assert row.previous   == "225K"
    assert row.source     == "trading_economics"
    assert row.country    == "US"
    utc_dt = row.event_datetime_utc
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    assert utc_dt == _utc(2020, 3, 6, 13)
