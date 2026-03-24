"""Historical Economic Events Service.

Fetches and normalises macro economic calendar events (CPI, NFP, rate
decisions, …) from one or more providers into a uniform EconomicEventRecord
dataclass.

Provider hierarchy
──────────────────
1. TradingEconomicsProvider  — if TRADING_ECONOMICS_API_KEY is set
2. CSVProvider               — loads from a local CSV file path
3. NullProvider              — returns empty list with an informational log

Trading Economics free tier
────────────────────────────
  The free tier exposes recent data. For full historical coverage you need a
  paid plan. Set TRADING_ECONOMICS_API_KEY in your .env to enable this
  provider. See https://tradingeconomics.com/api/

CSV format (for manual data import)
─────────────────────────────────────
  Expected columns (case-insensitive):
    datetime, country, currency, event, impact, actual, forecast, previous
  datetime must be parseable by pandas (ISO 8601 recommended).
  Pass csv_path=... to get_historical_economic_events() to use this provider.

High-impact detection
──────────────────────
  is_high_impact_event(event) returns True when:
    - impact == "high"                                OR
    - event_name contains a keyword in HIGH_IMPACT_KEYWORDS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from app.core.config import settings
from app.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# High-impact keyword set
# ---------------------------------------------------------------------------

HIGH_IMPACT_KEYWORDS = frozenset({
    "CPI", "NFP", "FOMC", "ECB", "INTEREST RATE",
    "FED", "FEDERAL RESERVE", "BANK OF ENGLAND", "BOE",
    "BANK OF JAPAN", "BOJ", "NON-FARM", "NONFARM",
    "PAYROLL", "GDP", "UNEMPLOYMENT",
})

# Currency → ISO country code mapping used when the provider only gives currency
_CURRENCY_TO_COUNTRY: Dict[str, str] = {
    "USD": "US", "EUR": "EU", "GBP": "GB", "JPY": "JP",
    "AUD": "AU", "CAD": "CA", "CHF": "CH", "NZD": "NZ",
    "CNY": "CN", "CNH": "CN", "SEK": "SE", "NOK": "NO",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class EconomicEventRecord:
    """One normalised historical economic event."""

    event_datetime_utc: datetime    # UTC-aware
    country:            Optional[str]
    currency:           str
    event_name:         str
    impact:             str         # "high" | "medium" | "low"
    actual:             Optional[str]
    forecast:           Optional[str]
    previous:           Optional[str]
    source:             str         # provider identifier


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_historical_economic_events(
    start_date:  date | datetime | str,
    end_date:    date | datetime | str,
    countries:   Optional[List[str]] = None,
    currencies:  Optional[List[str]] = None,
    csv_path:    Optional[str]       = None,
) -> List[EconomicEventRecord]:
    """
    Return historical economic events in the given date range.

    Parameters
    ----------
    start_date / end_date:
        Inclusive date range (``date``, ``datetime``, or ISO string).
    countries:
        Optional filter — ISO 2-letter country codes (e.g. ["US", "EU"]).
    currencies:
        Optional filter — currency codes (e.g. ["USD", "EUR"]).
    csv_path:
        If provided, load events from this local CSV file instead of any API.

    Returns
    -------
    List[EconomicEventRecord] sorted oldest → newest.
    Returns an empty list when no provider is configured.
    """
    start = _to_date(start_date)
    end   = _to_date(end_date)

    # --- CSV path takes priority (explicit user override) ---
    if csv_path:
        return _load_from_csv(csv_path, start, end, countries, currencies)

    # --- Trading Economics (requires API key) ---
    if settings.TRADING_ECONOMICS_API_KEY:
        return _fetch_trading_economics(
            start, end, countries, currencies,
            api_key=settings.TRADING_ECONOMICS_API_KEY,
        )

    # --- No provider configured ---
    log.info(
        "Historical economic events: no provider configured — "
        "set TRADING_ECONOMICS_API_KEY or pass csv_path=... "
        "to import_historical_economic_events()",
    )
    return []


def is_high_impact_event(event: EconomicEventRecord) -> bool:
    """
    Return True if the event should be treated as high-impact.

    Conditions (any one is sufficient):
      - ``impact == "high"``
      - ``event_name`` contains a well-known high-impact keyword
    """
    if event.impact.lower() == "high":
        return True
    name_upper = event.event_name.upper()
    return any(kw in name_upper for kw in HIGH_IMPACT_KEYWORDS)


# ---------------------------------------------------------------------------
# Trading Economics provider
# ---------------------------------------------------------------------------

def _fetch_trading_economics(
    start: date,
    end:   date,
    countries:  Optional[List[str]],
    currencies: Optional[List[str]],
    api_key:    str,
) -> List[EconomicEventRecord]:
    """Fetch events from the Trading Economics calendar API."""
    try:
        import tradingeconomics as te  # optional dependency
    except ImportError:
        log.warning(
            "tradingeconomics package not installed — "
            "run: pip install tradingeconomics"
        )
        return []

    try:
        te.login(api_key)
        raw = te.getCalendarData(
            d1=str(start),
            d2=str(end),
        )
    except Exception as exc:
        log.warning(
            "Economic calendar unavailable — continuing without filter",
            provider="trading_economics",
            error=str(exc),
        )
        return []

    records: List[EconomicEventRecord] = []
    for item in (raw or []):
        try:
            rec = _normalise_te_item(item)
            if rec is None:
                continue
            if countries  and rec.country  not in countries:
                continue
            if currencies and rec.currency not in currencies:
                continue
            records.append(rec)
        except Exception:
            continue   # silently skip malformed rows

    records.sort(key=lambda r: r.event_datetime_utc)
    log.info(
        "Trading Economics events fetched",
        start=str(start), end=str(end),
        total=len(records),
    )
    return records


def _normalise_te_item(item: dict) -> Optional[EconomicEventRecord]:
    """Convert one Trading Economics dict to EconomicEventRecord."""
    dt_str = item.get("Date") or item.get("date") or ""
    if not dt_str:
        return None

    dt_utc = _parse_datetime_utc(dt_str)
    if dt_utc is None:
        return None

    currency = (item.get("Currency") or item.get("currency") or "").upper()
    country  = (item.get("Country")  or item.get("country")  or
                _CURRENCY_TO_COUNTRY.get(currency))
    name     = (item.get("Event")    or item.get("event")    or "").strip()
    impact   = _normalise_impact(item.get("Importance") or item.get("importance") or "")

    return EconomicEventRecord(
        event_datetime_utc = dt_utc,
        country            = country,
        currency           = currency,
        event_name         = name,
        impact             = impact,
        actual             = _str_or_none(item.get("Actual")   or item.get("actual")),
        forecast           = _str_or_none(item.get("Forecast") or item.get("forecast")),
        previous           = _str_or_none(item.get("Previous") or item.get("previous")),
        source             = "trading_economics",
    )


# ---------------------------------------------------------------------------
# CSV provider
# ---------------------------------------------------------------------------

def _load_from_csv(
    csv_path:   str,
    start:      date,
    end:        date,
    countries:  Optional[List[str]],
    currencies: Optional[List[str]],
) -> List[EconomicEventRecord]:
    """Load events from a user-supplied CSV file."""
    try:
        import pandas as pd
    except ImportError:
        log.error("pandas not installed — run: pip install pandas")
        return []

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        log.error(
            "Economic calendar unavailable — continuing without filter",
            csv_path=csv_path, error=str(exc),
        )
        return []

    # Normalise column names
    df.columns = [c.strip().lower() for c in df.columns]
    col_map = {
        "date":     "datetime",
        "time":     "datetime",
        "event":    "event_name",
        "name":     "event_name",
        "title":    "event_name",
    }
    df.rename(columns=col_map, inplace=True)

    required = {"datetime", "currency", "event_name"}
    missing  = required - set(df.columns)
    if missing:
        log.error(
            "CSV is missing required columns",
            missing=list(missing), available=list(df.columns),
        )
        return []

    records: List[EconomicEventRecord] = []
    for _, row in df.iterrows():
        try:
            dt_utc = _parse_datetime_utc(str(row["datetime"]))
            if dt_utc is None:
                continue
            if dt_utc.date() < start or dt_utc.date() > end:
                continue

            currency = str(row.get("currency", "")).upper().strip()
            country  = str(row.get("country",  "")).upper().strip() or \
                       _CURRENCY_TO_COUNTRY.get(currency)
            name     = str(row.get("event_name", "")).strip()
            impact   = _normalise_impact(str(row.get("impact", "low")))

            if countries  and country  not in countries:
                continue
            if currencies and currency not in currencies:
                continue

            records.append(EconomicEventRecord(
                event_datetime_utc = dt_utc,
                country            = country or None,
                currency           = currency,
                event_name         = name,
                impact             = impact,
                actual             = _str_or_none(row.get("actual")),
                forecast           = _str_or_none(row.get("forecast")),
                previous           = _str_or_none(row.get("previous")),
                source             = "csv",
            ))
        except Exception:
            continue

    records.sort(key=lambda r: r.event_datetime_utc)
    log.info(
        "CSV economic events loaded",
        csv_path=csv_path, start=str(start), end=str(end),
        total=len(records),
    )
    return records


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _to_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _parse_datetime_utc(value: str) -> Optional[datetime]:
    """Parse an ISO-like datetime string into a UTC-aware datetime."""
    import pandas as pd
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.to_pydatetime().replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _normalise_impact(value: str) -> str:
    """Map provider-specific importance strings to high / medium / low."""
    v = str(value).lower().strip()
    if v in ("high", "3", "red", "high impact"):
        return "high"
    if v in ("medium", "moderate", "2", "orange", "medium impact"):
        return "medium"
    return "low"


def _str_or_none(value) -> Optional[str]:
    """Return a stripped string or None for empty / NaN values."""
    if value is None:
        return None
    s = str(value).strip()
    return None if s in ("", "nan", "None", "NaN") else s
