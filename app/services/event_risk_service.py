"""
Event Risk Service — DB-backed economic event guardrail.

Queries the ``historical_events`` table to detect whether a scheduled macro
event falls within a configurable time window around an intended trade.

This is the offline complement to ``economic_calendar_service.py`` (which
scrapes Forex Factory live). When events have been pre-loaded via the
historical importer, this service provides an always-available filter that
works even when the Forex Factory scraper is unavailable.

If no events exist in the DB the service returns ``EventRisk(level="NONE")``
and the bot continues normally — zero crash risk.

Risk levels (highest priority wins when multiple events overlap)
────────────────────────────────────────────────────────────────
  BLOCK  — high-impact event within ±window_minutes → trade blocked
  REDUCE — medium-impact event within ±window_minutes → position size halved
  NONE   — no relevant event → trade proceeds normally

Symbol → currency mapping
─────────────────────────
  EURUSD  → EUR, USD
  GBPUSD  → GBP, USD
  USDJPY  → USD, JPY
  XAUUSD  → XAU, USD   (gold — XAU events rarely in DB; USD events still apply)
  XAGUSD  → XAG, USD
  OIL     → USD         (USD-denominated commodity, no "OIL" currency code)
  CL=F    → USD
  Unknown → 3+3 char split fallback (e.g. AUDNZD → AUD, NZD)
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.historical_event import HistoricalEvent
from app.services.backtest_data_service import get_events_near_timestamp
from app.services.historical_economic_events_service import HIGH_IMPACT_KEYWORDS

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_WINDOW_MINUTES: int = 60   # look ±60 min around the trade timestamp

# Explicit symbol → currency code mapping.
# Add new symbols here as the bot's symbol list grows.
SYMBOL_CURRENCIES: Dict[str, Set[str]] = {
    # Majors
    "EURUSD": {"EUR", "USD"},
    "GBPUSD": {"GBP", "USD"},
    "USDJPY": {"USD", "JPY"},
    "AUDUSD": {"AUD", "USD"},
    "USDCAD": {"USD", "CAD"},
    "USDCHF": {"USD", "CHF"},
    "NZDUSD": {"NZD", "USD"},
    "USDMXN": {"USD", "MXN"},
    # Metals
    "XAUUSD": {"XAU", "USD"},
    "XAGUSD": {"XAG", "USD"},
    # Commodities — priced in USD, so only USD events apply
    "OIL":  {"USD"},
    "CL=F": {"USD"},
    # Crypto
    "BTCUSD": {"USD"},
    "ETHUSD": {"USD"},
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class EventRisk:
    """
    Result of :func:`assess_event_risk_for_trade`.

    Attributes
    ----------
    level:
        ``"BLOCK"`` — block the trade.
        ``"REDUCE"`` — allow with 50 % reduced position size.
        ``"NONE"`` — no restriction.
    reason:
        Human-readable explanation, included in bot logs.
    event:
        The :class:`HistoricalEvent` row that triggered this level, or
        ``None`` when level is ``"NONE"``.
    """
    level:  str                           # "BLOCK" | "REDUCE" | "NONE"
    reason: str
    event:  Optional[HistoricalEvent] = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def assess_event_risk_for_trade(
    db:             AsyncSession,
    symbol:         str,
    now:            Optional[datetime] = None,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
) -> EventRisk:
    """
    Query ``historical_events`` for events near *now* that are related to *symbol*.

    Returns the highest-priority :class:`EventRisk` found. Returns
    ``EventRisk(level="NONE")`` when the DB is empty or has no matching events
    — the bot continues normally in that case.

    Parameters
    ----------
    db:
        Open async DB session.
    symbol:
        Trading symbol (e.g. ``"EURUSD"``, ``"XAUUSD"``, ``"OIL"``).
    now:
        Reference timestamp (UTC-aware). Defaults to ``datetime.now(UTC)``.
    window_minutes:
        Half-width of the search window in minutes (searches both before and
        after *now*).
    """
    if now is None:
        now = datetime.now(timezone.utc)

    currencies = symbol_to_currencies(symbol)

    try:
        events: List[HistoricalEvent] = await get_events_near_timestamp(
            db             = db,
            timestamp_utc  = now,
            minutes_before = window_minutes,
            minutes_after  = window_minutes,
            currencies     = list(currencies),
        )
    except Exception as exc:
        # Never let a DB query failure crash the bot cycle.
        log.warning(
            "Event risk DB query failed — proceeding without DB event filter",
            symbol=symbol,
            error=str(exc),
        )
        return EventRisk(level="NONE", reason=f"DB query error: {exc}")

    if not events:
        log.debug(
            "No historical events in DB window",
            symbol=symbol,
            currencies=sorted(currencies),
            window_minutes=window_minutes,
        )
        return EventRisk(level="NONE", reason="no events in DB window")

    # Log each detected event for full observability
    for ev in events:
        ev_dt = _ensure_utc(ev.event_datetime_utc)
        delta_min = (ev_dt - now).total_seconds() / 60
        log.info(
            "DB HISTORICAL EVENT IN WINDOW",
            symbol    = symbol,
            event_name= ev.event_name,
            currency  = ev.currency,
            impact    = ev.impact,
            minutes   = round(delta_min, 1),
        )

    # Scan all events; keep the most restrictive outcome.
    # BLOCK(2) > REDUCE(1) > NONE(0)
    best_score  = 0
    best_event  = None
    best_reason = "no relevant events"

    for ev in events:
        ev_dt     = _ensure_utc(ev.event_datetime_utc)
        delta_min = (ev_dt - now).total_seconds() / 60
        timing    = (
            f"in {delta_min:.0f}min" if delta_min >= 0
            else f"{abs(delta_min):.0f}min ago"
        )

        if _is_high_impact(ev):
            score  = 2
            reason = f"high-impact event: {ev.event_name} ({ev.currency}) {timing}"
        elif _is_medium_impact(ev):
            score  = 1
            reason = f"medium-impact event: {ev.event_name} ({ev.currency}) {timing}"
        else:
            continue

        if score > best_score:
            best_score  = score
            best_event  = ev
            best_reason = reason

    level_map = {2: "BLOCK", 1: "REDUCE", 0: "NONE"}
    result = EventRisk(
        level  = level_map[best_score],
        reason = best_reason,
        event  = best_event,
    )

    if result.level != "NONE":
        log.warning(
            "DB EVENT RISK",
            symbol = symbol,
            level  = result.level,
            reason = result.reason,
        )

    return result


def symbol_to_currencies(symbol: str) -> Set[str]:
    """
    Return the set of currency codes relevant to *symbol*.

    Uses the explicit :data:`SYMBOL_CURRENCIES` mapping first.
    Falls back to simple 3+3 character splitting for unknown symbols
    (e.g. ``"AUDNZD"`` → ``{"AUD", "NZD"}``).
    """
    upper = symbol.upper().replace("/", "").replace("_", "").replace("-", "")
    if upper in SYMBOL_CURRENCIES:
        return set(SYMBOL_CURRENCIES[upper])   # return a copy

    # Generic fallback
    if len(upper) >= 6:
        return {upper[:3], upper[3:6]}
    return {upper}


# ---------------------------------------------------------------------------
# Internal impact helpers  (operate on HistoricalEvent ORM rows)
# ---------------------------------------------------------------------------

def _is_high_impact(event: HistoricalEvent) -> bool:
    """True when impact == 'high' OR event name contains a high-impact keyword."""
    if event.impact.lower() == "high":
        return True
    return any(kw in event.event_name.upper() for kw in HIGH_IMPACT_KEYWORDS)


def _is_medium_impact(event: HistoricalEvent) -> bool:
    """True when impact == 'medium'."""
    return event.impact.lower() == "medium"


def _ensure_utc(dt: datetime) -> datetime:
    """Return a UTC-aware datetime; attach UTC if naive."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
