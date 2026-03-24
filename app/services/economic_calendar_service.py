"""
Economic Calendar Service — Forex Factory scraper.

Fetches the Forex Factory economic calendar page and filters upcoming high /
medium impact events. Results are cached in-memory for 5 minutes so the bot
never hammers the page on every cycle.

Timezone handling
-----------------
ForexFactory displays all times in America/New_York (US Eastern).
Conversion to UTC uses ``zoneinfo.ZoneInfo("America/New_York")`` so that
DST transitions are handled correctly by the OS / tzdata package —
no manual offset arithmetic needed.

On any scraping or parse failure the service returns an empty list and logs
a warning, allowing the bot to continue without the calendar filter.
"""
from __future__ import annotations

import asyncio
import calendar as _cal
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from app.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Timezone constant — single source of truth
# ---------------------------------------------------------------------------

_NY_TZ = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CALENDAR_URL  = "https://www.forexfactory.com/calendar"
CACHE_TTL_SEC = 300   # re-fetch at most once every 5 minutes
FETCH_TIMEOUT = 15    # seconds per HTTP request

# Pre/post-event safety windows (minutes)
PRE_EVENT_BLOCK_MIN = 10   # ≤ 10 min before high-impact → BLOCK all trades
PRE_EVENT_SKIP_MIN  = 30   # ≤ 30 min before high-impact → SKIP trade
POST_EVENT_SKIP_MIN = 10   # <  10 min after  high-impact → SKIP (volatility)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Cache-Control":   "max-age=0",
}

# Keywords that always classify an event as high-impact regardless of icon
_HIGH_IMPACT_KEYWORDS = frozenset({
    "CPI", "NFP", "FOMC", "ECB", "INTEREST RATE",
    "NON-FARM", "NONFARM", "PAYROLL", "FEDERAL RESERVE",
    "BANK OF ENGLAND", "BOE", "BOJ", "BANK OF JAPAN",
    "GDP", "UNEMPLOYMENT",
})


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Event:
    event_time:  datetime   # UTC-aware
    currency:    str        # "USD", "EUR", etc.
    name:        str
    impact:      str        # "high" | "medium" | "low" | "holiday"

    # Filled by get_upcoming_events() at query time — NOT at parse time.
    # minutes_until_event: positive = event is X min in the future
    #                      negative = event passed X min ago
    # minutes_since_event: positive = event passed X min ago
    #                      negative = event is X min in the future
    minutes_until_event: float = 0.0
    minutes_since_event: float = 0.0


# ---------------------------------------------------------------------------
# In-memory cache (async-safe via asyncio.Lock)
# ---------------------------------------------------------------------------

@dataclass
class _Cache:
    events:     List[Event]        = field(default_factory=list)
    fetched_at: Optional[datetime] = None

_cache      = _Cache()
_cache_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def get_upcoming_events(
    minutes_ahead: int = 60,
    minutes_past:  int = POST_EVENT_SKIP_MIN,
) -> List[Event]:
    """
    Return economic events in the window [now − minutes_past, now + minutes_ahead].

    Including recently-past events lets callers enforce a post-release
    volatility cooldown without a separate fetch.

    Uses a 5-minute in-memory cache — safe to call on every bot cycle.
    Returns an empty list on any network or parse failure.

    Each returned Event has:
      minutes_until_event  — positive = future, negative = past
      minutes_since_event  — positive = past,   negative = future
    """
    async with _cache_lock:
        now = datetime.now(timezone.utc)
        stale = (
            _cache.fetched_at is None
            or (now - _cache.fetched_at).total_seconds() > CACHE_TTL_SEC
        )
        if stale:
            _cache.events     = await _fetch_and_parse()
            _cache.fetched_at = now

    now      = datetime.now(timezone.utc)
    earliest = now - timedelta(minutes=minutes_past)
    latest   = now + timedelta(minutes=minutes_ahead)

    result: List[Event] = []
    for ev in _cache.events:
        if earliest <= ev.event_time <= latest:
            delta_min               = (ev.event_time - now).total_seconds() / 60
            ev.minutes_until_event  =  delta_min    # +future / -past
            ev.minutes_since_event  = -delta_min    # +past   / -future
            result.append(ev)

    return result


def is_high_impact_event(event: Event) -> bool:
    """
    True if the event should be treated as high-impact.

    Conditions (any one is sufficient):
      - impact == "high"  (Forex Factory red icon)
      - name contains a well-known high-impact keyword (CPI, NFP, FOMC, …)
    """
    if event.impact == "high":
        return True
    name_upper = event.name.upper()
    return any(kw in name_upper for kw in _HIGH_IMPACT_KEYWORDS)


def is_medium_impact_event(event: Event) -> bool:
    """True if impact == 'medium' (Forex Factory orange icon)."""
    return event.impact == "medium"


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

async def _fetch_and_parse() -> List[Event]:
    """Download the Forex Factory calendar page and return parsed events."""
    try:
        async with httpx.AsyncClient(
            headers=_HEADERS,
            follow_redirects=True,
            timeout=FETCH_TIMEOUT,
        ) as client:
            resp = await client.get(CALENDAR_URL)
            resp.raise_for_status()
            html = resp.text
    except Exception as exc:
        log.warning(
            "Economic calendar unavailable — continuing without filter",
            error=str(exc),
        )
        return []

    try:
        events = _parse_html(html)
        log.info("Economic calendar fetched", total_events=len(events))
        return events
    except Exception as exc:
        log.warning(
            "Economic calendar parse error — continuing without filter",
            error=str(exc),
        )
        return []


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def _parse_html(html: str) -> List[Event]:
    """Parse Forex Factory HTML into a list of Event objects."""
    soup  = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="calendar__table")
    if table is None:
        log.warning(
            "Economic calendar: calendar__table not found — "
            "page layout may have changed"
        )
        return []

    events:       List[Event]  = []
    current_date: Optional[date] = None
    today         = datetime.now(_NY_TZ).date()   # use NY date, not UTC

    for row in table.find_all("tr", class_="calendar__row"):
        classes = row.get("class", [])

        # Day-breaker rows carry only a date header — no event data
        if "calendar__row--day-breaker" in classes:
            cell = row.find("td")
            if cell:
                current_date = _parse_date_cell(cell.get_text(strip=True), today)
            continue

        ev = _parse_event_row(row, current_date, today)
        if ev is not None:
            events.append(ev)

    return events


def _parse_event_row(
    row,
    current_date: Optional[date],
    today: date,
) -> Optional[Event]:
    """Parse one calendar row. Returns None on any error or missing data."""
    try:
        # ── Date (update context if this row carries a new date) ─────────────
        date_cell = row.find("td", class_="calendar__date")
        if date_cell:
            text = date_cell.get_text(strip=True)
            if text:
                parsed = _parse_date_cell(text, today)
                if parsed:
                    current_date = parsed

        if current_date is None:
            return None

        # ── Time ─────────────────────────────────────────────────────────────
        time_cell = row.find("td", class_="calendar__time")
        if time_cell is None:
            return None
        time_str = time_cell.get_text(strip=True)
        if not time_str or time_str.lower() in ("all day", "tentative"):
            return None

        event_time_utc = _parse_time_to_utc(time_str, current_date)
        if event_time_utc is None:
            return None

        # ── Currency ─────────────────────────────────────────────────────────
        cur_cell = row.find("td", class_="calendar__currency")
        if cur_cell is None:
            return None
        currency = cur_cell.get_text(strip=True).upper()
        if not currency:
            return None

        # ── Impact level ─────────────────────────────────────────────────────
        impact   = "low"
        imp_cell = row.find("td", class_="calendar__impact")
        if imp_cell:
            span = imp_cell.find("span", class_="impact")
            if span:
                span_classes = " ".join(span.get("class", []))
                if   "impact--high"    in span_classes:
                    impact = "high"
                elif "impact--medium"  in span_classes:
                    impact = "medium"
                elif "impact--holiday" in span_classes:
                    impact = "holiday"
                # else: stays "low"

        # ── Event name ───────────────────────────────────────────────────────
        name    = ""
        ev_cell = row.find("td", class_="calendar__event")
        if ev_cell:
            title_span = ev_cell.find("span", class_="calendar__event-title")
            name = (title_span or ev_cell).get_text(strip=True)

        if not name:
            return None

        return Event(
            event_time=event_time_utc,
            currency=currency,
            name=name,
            impact=impact,
        )

    except Exception:
        return None   # silently skip broken rows


# ---------------------------------------------------------------------------
# Time / date helpers
# ---------------------------------------------------------------------------

def _parse_date_cell(text: str, today: date) -> Optional[date]:
    """
    Parse a Forex Factory date header such as "Mon Mar 22" into a ``date``.

    Year is inferred: if the parsed month/day is more than 30 days in the
    past it belongs to next year (handles year-boundary look-ahead).
    """
    m = re.search(r"([A-Za-z]{3})\s+(\d{1,2})\s*$", text.strip())
    if not m:
        return None
    try:
        abbr  = m.group(1).capitalize()
        day   = int(m.group(2))
        month = {v[:3]: k for k, v in enumerate(_cal.month_abbr) if v}[abbr]
        year  = today.year
        candidate = date(year, month, day)
        if (today - candidate).days > 30:
            candidate = date(year + 1, month, day)
        return candidate
    except Exception:
        return None


def _parse_time_to_utc(time_str: str, event_date: date) -> Optional[datetime]:
    """
    Parse a Forex Factory time string (e.g. ``"2:00am"``, ``"11:30pm"``) that
    is expressed in America/New_York time and return an equivalent UTC-aware
    ``datetime``.

    zoneinfo handles DST automatically — no manual offset arithmetic needed.
    """
    m = re.match(r"^(\d{1,2}):(\d{2})(am|pm)$", time_str.strip().lower())
    if not m:
        return None
    try:
        hour, minute, meridiem = int(m.group(1)), int(m.group(2)), m.group(3)
        if meridiem == "pm" and hour != 12:
            hour += 12
        elif meridiem == "am" and hour == 12:
            hour = 0

        # Build a timezone-aware datetime in America/New_York, then convert.
        ny_dt  = datetime(event_date.year, event_date.month, event_date.day,
                          hour, minute, tzinfo=_NY_TZ)
        return ny_dt.astimezone(timezone.utc)
    except Exception:
        return None
