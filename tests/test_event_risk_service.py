"""
Tests for event_risk_service.py

Covers:
  - symbol_to_currencies: all supported symbols + fallback
  - assess_event_risk_for_trade:
      * no events in DB → NONE (bot runs normally)
      * high-impact event → BLOCK
      * medium-impact event → REDUCE
      * low-impact event → NONE
      * multiple events → high-impact dominates
      * unrelated currency event → NONE (does not affect trade)
      * event outside time window → NONE
      * high-impact by keyword (not just flag)
      * DB query error → NONE (no crash)

All tests use the in-memory SQLite fixture from conftest.py — no network calls.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.historical_event import HistoricalEvent
from app.services.event_risk_service import (
    EventRisk,
    SYMBOL_CURRENCIES,
    assess_event_risk_for_trade,
    symbol_to_currencies,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(delta_minutes: float = 0) -> datetime:
    """Return a UTC-aware datetime offset from now by delta_minutes."""
    return datetime.now(timezone.utc) + timedelta(minutes=delta_minutes)


def _make_event(
    db: AsyncSession,
    currency:   str,
    impact:     str,
    event_name: str = "CPI m/m",
    offset_min: float = 30,        # minutes from now (positive = future)
) -> HistoricalEvent:
    """Insert a HistoricalEvent into the test DB and return it."""
    ev = HistoricalEvent(
        id                 = uuid.uuid4(),
        event_datetime_utc = _utc(offset_min),
        country            = "US",
        currency           = currency,
        event_name         = event_name,
        impact             = impact,
        actual             = None,
        forecast           = "0.2%",
        previous           = "0.1%",
        source             = "test",
    )
    db.add(ev)
    return ev


# ===========================================================================
# 1. symbol_to_currencies
# ===========================================================================

class TestSymbolToCurrencies:

    def test_eurusd(self):
        assert symbol_to_currencies("EURUSD") == {"EUR", "USD"}

    def test_gbpusd(self):
        assert symbol_to_currencies("GBPUSD") == {"GBP", "USD"}

    def test_usdjpy(self):
        assert symbol_to_currencies("USDJPY") == {"USD", "JPY"}

    def test_xauusd(self):
        assert symbol_to_currencies("XAUUSD") == {"XAU", "USD"}

    def test_xagusd(self):
        assert symbol_to_currencies("XAGUSD") == {"XAG", "USD"}

    def test_oil_maps_to_usd_only(self):
        assert symbol_to_currencies("OIL") == {"USD"}

    def test_clf_maps_to_usd_only(self):
        assert symbol_to_currencies("CL=F") == {"USD"}

    def test_lowercase_normalised(self):
        assert symbol_to_currencies("eurusd") == {"EUR", "USD"}

    def test_slash_normalised(self):
        assert symbol_to_currencies("EUR/USD") == {"EUR", "USD"}

    def test_unknown_six_char_fallback(self):
        result = symbol_to_currencies("AUDNZD")
        assert result == {"AUD", "NZD"}

    def test_all_explicit_symbols_present(self):
        """Every symbol in the mapping can be looked up without error."""
        for sym in SYMBOL_CURRENCIES:
            result = symbol_to_currencies(sym)
            assert isinstance(result, set)
            assert len(result) >= 1


# ===========================================================================
# 2. assess_event_risk_for_trade
# ===========================================================================

@pytest.mark.asyncio
async def test_no_events_returns_none(db: AsyncSession):
    """Empty DB → NONE, no crash."""
    risk = await assess_event_risk_for_trade(db, "EURUSD")
    assert risk.level  == "NONE"
    assert risk.event  is None


@pytest.mark.asyncio
async def test_high_impact_event_blocks(db: AsyncSession):
    """A high-impact USD event within the window blocks an EURUSD trade."""
    _make_event(db, currency="USD", impact="high", offset_min=20)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD")

    assert risk.level == "BLOCK"
    assert risk.event is not None
    assert "high-impact" in risk.reason


@pytest.mark.asyncio
async def test_medium_impact_event_reduces(db: AsyncSession):
    """A medium-impact EUR event within the window → REDUCE."""
    _make_event(db, currency="EUR", impact="medium",
                event_name="Retail Sales", offset_min=45)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD")

    assert risk.level == "REDUCE"
    assert risk.event is not None
    assert "medium-impact" in risk.reason


@pytest.mark.asyncio
async def test_low_impact_event_is_none(db: AsyncSession):
    """A low-impact event within the window does not restrict the trade."""
    _make_event(db, currency="USD", impact="low",
                event_name="Housing Starts", offset_min=15)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD")

    assert risk.level == "NONE"


@pytest.mark.asyncio
async def test_high_impact_dominates_medium(db: AsyncSession):
    """When both high and medium events exist, BLOCK takes priority."""
    _make_event(db, currency="USD", impact="medium",
                event_name="Retail Sales", offset_min=10)
    _make_event(db, currency="EUR", impact="high",
                event_name="ECB Rate Decision", offset_min=25)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD")

    assert risk.level == "BLOCK"


@pytest.mark.asyncio
async def test_unrelated_currency_does_not_affect_trade(db: AsyncSession):
    """A JPY high-impact event should not block an EURUSD trade."""
    _make_event(db, currency="JPY", impact="high",
                event_name="BOJ Rate Decision", offset_min=30)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD")

    assert risk.level == "NONE"


@pytest.mark.asyncio
async def test_unrelated_currency_does_affect_usdjpy(db: AsyncSession):
    """The same JPY event DOES block a USDJPY trade."""
    _make_event(db, currency="JPY", impact="high",
                event_name="BOJ Rate Decision", offset_min=30)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "USDJPY")

    assert risk.level == "BLOCK"


@pytest.mark.asyncio
async def test_event_outside_window_ignored(db: AsyncSession):
    """An event 3 hours away falls outside the ±60 min window → NONE."""
    _make_event(db, currency="USD", impact="high",
                event_name="NFP", offset_min=180)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD", window_minutes=60)

    assert risk.level == "NONE"


@pytest.mark.asyncio
async def test_event_inside_custom_window(db: AsyncSession):
    """An event 90 min away is inside a 120 min window."""
    _make_event(db, currency="USD", impact="high",
                event_name="FOMC Statement", offset_min=90)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD", window_minutes=120)

    assert risk.level == "BLOCK"


@pytest.mark.asyncio
async def test_high_impact_by_keyword_not_just_flag(db: AsyncSession):
    """An event tagged 'medium' but with keyword 'NFP' should still BLOCK."""
    _make_event(db, currency="USD", impact="medium",
                event_name="Non-Farm Payrolls", offset_min=20)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD")

    # NFP is a HIGH_IMPACT_KEYWORD → overrides 'medium' flag → BLOCK
    assert risk.level == "BLOCK"


@pytest.mark.asyncio
async def test_oil_checks_usd_events(db: AsyncSession):
    """OIL maps to USD, so a USD high-impact event should block OIL trades."""
    _make_event(db, currency="USD", impact="high",
                event_name="Fed Interest Rate Decision", offset_min=15)
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "OIL")

    assert risk.level == "BLOCK"


@pytest.mark.asyncio
async def test_db_query_error_returns_none(db: AsyncSession):
    """If the DB query raises an exception, return NONE (never crash)."""
    with patch(
        "app.services.event_risk_service.get_events_near_timestamp",
        new_callable=AsyncMock,
        side_effect=RuntimeError("connection lost"),
    ):
        risk = await assess_event_risk_for_trade(db, "EURUSD")

    assert risk.level  == "NONE"
    assert "DB query error" in risk.reason


@pytest.mark.asyncio
async def test_past_event_inside_window_still_triggers(db: AsyncSession):
    """An event 30 min IN THE PAST is within ±60 min window and should fire."""
    _make_event(db, currency="USD", impact="high",
                event_name="CPI m/m", offset_min=-30)   # 30 min ago
    await db.flush()

    risk = await assess_event_risk_for_trade(db, "EURUSD", window_minutes=60)

    assert risk.level == "BLOCK"
