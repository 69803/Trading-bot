"""NYSE/NASDAQ market hours — pure Python, no external API.

Regular session : Mon–Fri 09:30–16:00 America/New_York
Early close     : 13:00 ET on certain half-days (day before major holidays)
Pre-market      : 04:00–09:30 ET
After-hours     : 16:00–20:00 ET

Returns a plain dict so the FastAPI endpoint can return it directly.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = timezone.utc

# ── NYSE observed holidays 2025–2027 ─────────────────────────────────────────
_NYSE_HOLIDAYS: frozenset[date] = frozenset([
    # 2025
    date(2025, 1, 1),  date(2025, 1, 20),  date(2025, 2, 17),
    date(2025, 4, 18), date(2025, 5, 26),  date(2025, 6, 19),
    date(2025, 7, 4),  date(2025, 9, 1),   date(2025, 11, 27),
    date(2025, 12, 25),
    # 2026
    date(2026, 1, 1),  date(2026, 1, 19),  date(2026, 2, 16),
    date(2026, 4, 3),  date(2026, 5, 25),  date(2026, 6, 19),
    date(2026, 7, 3),  date(2026, 9, 7),   date(2026, 11, 26),
    date(2026, 12, 25),
    # 2027
    date(2027, 1, 1),  date(2027, 1, 18),  date(2027, 2, 15),
    date(2027, 3, 26), date(2027, 5, 31),  date(2027, 6, 18),
    date(2027, 7, 5),  date(2027, 9, 6),   date(2027, 11, 25),
    date(2027, 12, 24),
])

# ── Early-close half-days (13:00 ET) ─────────────────────────────────────────
_EARLY_CLOSE: frozenset[date] = frozenset([
    date(2025, 7, 3),   date(2025, 11, 28), date(2025, 12, 24),
    date(2026, 7, 2),   date(2026, 11, 27), date(2026, 12, 24),
    date(2027, 11, 26), date(2027, 12, 23),
])

_PRE_OPEN  = time(4, 0)
_OPEN      = time(9, 30)
_CLOSE_REG = time(16, 0)
_CLOSE_EAR = time(13, 0)
_AFTER_END = time(20, 0)


def _is_trading_day(d: date) -> bool:
    return d.weekday() < 5 and d not in _NYSE_HOLIDAYS


def _close_time(d: date) -> time:
    return _CLOSE_EAR if d in _EARLY_CLOSE else _CLOSE_REG


def _next_open_dt(from_et: datetime) -> datetime:
    """Return the next regular-session open after *from_et* (ET-aware)."""
    d, t = from_et.date(), from_et.time()
    # If today is a trading day and we're before the open, next open is today
    if _is_trading_day(d) and t < _OPEN:
        return datetime(d.year, d.month, d.day, 9, 30, tzinfo=ET)
    d += timedelta(days=1)
    while not _is_trading_day(d):
        d += timedelta(days=1)
    return datetime(d.year, d.month, d.day, 9, 30, tzinfo=ET)


def get_nyse_status() -> dict:
    """Return current NYSE/NASDAQ session status as a plain dict."""
    now    = datetime.now(ET)
    d, t   = now.date(), now.time()
    trading = _is_trading_day(d)
    close_t = _close_time(d)
    is_open = trading and _OPEN <= t < close_t

    if not trading or t >= _AFTER_END or t < _PRE_OPEN:
        session = "closed"
    elif t < _OPEN:
        session = "pre-market"
    elif t < close_t:
        session = "regular"
    else:
        session = "after-hours"

    next_open_et = _next_open_dt(now)
    next_open    = next_open_et.astimezone(UTC).isoformat()

    next_close = None
    if is_open:
        nc_et      = datetime(d.year, d.month, d.day, close_t.hour, close_t.minute, tzinfo=ET)
        next_close = nc_et.astimezone(UTC).isoformat()

    return {
        "is_open":    is_open,
        "session":    session,      # "regular" | "pre-market" | "after-hours" | "closed"
        "next_open":  next_open,    # ISO-8601 UTC string
        "next_close": next_close,   # ISO-8601 UTC string or null
        "market":     "NYSE",
        "timezone":   "America/New_York",
    }
