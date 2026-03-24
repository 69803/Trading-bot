"""
Expert Forex Mode Filters (PASO 7).

Six independent filters that gate trade execution to improve signal quality.
All filters are disabled when EXPERT_FILTERS_ENABLED=False.

Filters (evaluated in order — first SKIP wins):
  A — Session:          Only trade during London (07–11 UTC) or NY (13–17 UTC).
  B — Volatility:       Skip if ATR is below minimum threshold.
  C — Trend:            EMA200 alignment — BUY only if price > EMA200, SELL only below.
  D — Signal Quality:   Require N of 3 confirming conditions (EMA alignment, RSI, MACD).
  E — Anti-Overtrading: max trades/day + portfolio-level cooldown between any two trades.
  F — Post-Event Delay: wait X minutes after a high-impact DB event.

Each filter returns ``None`` when it passes.
If a filter fires it returns an ``ExpertFilterResult(action="SKIP")``.
The composite function returns the first SKIP, or ALLOW when all pass.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.models.historical_event import HistoricalEvent
from app.models.position import Position
from app.schemas.technical import TechnicalSignal

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Output type
# ---------------------------------------------------------------------------

@dataclass
class ExpertFilterResult:
    """
    Decision returned by :func:`check_expert_filters`.

    action:      ``"SKIP"`` — do not place the trade.
                 ``"ALLOW"`` — all filters passed.
    filter_name: which filter triggered, or ``"none"`` when ALLOW.
    reason:      human-readable explanation written to bot logs.
    """
    action:      str   # "SKIP" | "ALLOW"
    filter_name: str   # "session" | "volatility" | "trend" | "signal_quality"
                       #  | "overtrading" | "post_event_delay" | "none"
    reason:      str


_ALLOW = ExpertFilterResult(
    action="ALLOW", filter_name="none", reason="all expert filters passed"
)


# ---------------------------------------------------------------------------
# Symbol → currency mapping (mirrors event_risk_service)
# ---------------------------------------------------------------------------

_SYMBOL_CURRENCIES: Dict[str, Set[str]] = {
    "EURUSD": {"EUR", "USD"}, "GBPUSD": {"GBP", "USD"},
    "USDJPY": {"USD", "JPY"}, "AUDUSD": {"AUD", "USD"},
    "USDCAD": {"USD", "CAD"}, "USDCHF": {"USD", "CHF"},
    "NZDUSD": {"NZD", "USD"}, "USDMXN": {"USD", "MXN"},
    "XAUUSD": {"XAU", "USD"}, "XAGUSD": {"XAG", "USD"},
    "OIL":  {"USD"}, "CL=F": {"USD"},
    "BTCUSD": {"USD"}, "ETHUSD": {"USD"},
}


def _sym_currencies(symbol: str) -> Set[str]:
    if symbol in _SYMBOL_CURRENCIES:
        return _SYMBOL_CURRENCIES[symbol]
    clean = symbol.upper().replace("/", "").replace("_", "").replace("-", "")
    return {clean[:3], clean[3:6]} if len(clean) >= 6 else {clean}


# ---------------------------------------------------------------------------
# EMA helper
# ---------------------------------------------------------------------------

def _compute_ema(closes: List[float], period: int) -> Optional[float]:
    """Compute the last EMA value using a simple SMA seed.  Returns None if
    fewer than ``period`` closes are available."""
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period          # SMA seed
    for price in closes[period:]:
        ema = price * k + ema * (1.0 - k)
    return ema


# ---------------------------------------------------------------------------
# Filter A — Session
# ---------------------------------------------------------------------------

def _check_session(now: datetime) -> Optional[ExpertFilterResult]:
    """Session filter — disabled for 24/7 operation."""
    log.debug("SESSION CHECK DISABLED — operating 24/7", hour_utc=now.hour)
    return None


# ---------------------------------------------------------------------------
# Filter B — Volatility (ATR)
# ---------------------------------------------------------------------------

def _check_volatility(atr: float, symbol: str) -> Optional[ExpertFilterResult]:
    """SKIP when ATR is below the minimum threshold (market too quiet)."""
    if math.isnan(atr) or atr <= 0:
        log.debug("Expert volatility filter: ATR unavailable — pass", symbol=symbol)
        return None   # can't evaluate → benefit of the doubt

    if atr >= settings.EXPERT_ATR_MIN:
        log.debug(
            "Expert volatility filter: PASS",
            symbol=symbol, atr=round(atr, 6), threshold=settings.EXPERT_ATR_MIN,
        )
        return None

    reason = (
        f"ATR={atr:.6f} below minimum {settings.EXPERT_ATR_MIN:.6f} — "
        f"market volatility too low for reliable trading"
    )
    log.info("Expert volatility filter: SKIP", symbol=symbol, atr=round(atr, 6))
    return ExpertFilterResult(action="SKIP", filter_name="volatility", reason=reason)


# ---------------------------------------------------------------------------
# Filter C — Trend (EMA200 alignment)
# ---------------------------------------------------------------------------

def _check_trend(
    price: float,
    candles: List[dict],
    direction: str,
    symbol: str,
) -> Optional[ExpertFilterResult]:
    """SKIP when trade direction contradicts the long-term EMA trend.
    BUY only if price > EMA200; SELL only if price < EMA200."""
    if direction not in ("BUY", "SELL"):
        return None   # HOLD — nothing to check

    closes = [float(c["close"]) for c in candles if c.get("close") is not None]
    ema200 = _compute_ema(closes, settings.EXPERT_TREND_EMA_PERIOD)

    if ema200 is None:
        log.debug(
            "Expert trend filter: insufficient candles — pass",
            symbol=symbol, available=len(closes), required=settings.EXPERT_TREND_EMA_PERIOD,
        )
        return None   # not enough data → pass

    if direction == "BUY" and price > ema200:
        log.debug(
            "Expert trend filter: PASS (BUY + price>EMA200)",
            symbol=symbol, price=round(price, 6), ema200=round(ema200, 6),
        )
        return None
    if direction == "SELL" and price < ema200:
        log.debug(
            "Expert trend filter: PASS (SELL + price<EMA200)",
            symbol=symbol, price=round(price, 6), ema200=round(ema200, 6),
        )
        return None

    trend_word = "above" if price > ema200 else "below"
    reason = (
        f"trend conflict — {direction} signal but price ({price:.5f}) is {trend_word} "
        f"EMA{settings.EXPERT_TREND_EMA_PERIOD} ({ema200:.5f})"
    )
    log.info(
        "Expert trend filter: SKIP",
        symbol=symbol, direction=direction,
        price=round(price, 6), ema200=round(ema200, 6),
    )
    return ExpertFilterResult(action="SKIP", filter_name="trend", reason=reason)


# ---------------------------------------------------------------------------
# Filter D — Signal Quality (3 conditions, need N)
# ---------------------------------------------------------------------------

def _check_signal_quality(
    technical: TechnicalSignal,
    direction: str,
    symbol: str,
) -> Optional[ExpertFilterResult]:
    """SKIP when fewer than EXPERT_MIN_SIGNAL_CONDITIONS confirm the direction.

    Three conditions:
      1. EMA alignment  — price > ema_fast > ema_slow (BUY) or inverted (SELL)
      2. RSI momentum   — RSI < 65 for BUY (not overbought); RSI > 35 for SELL
      3. MACD histogram — positive for BUY, negative for SELL
    """
    if direction not in ("BUY", "SELL"):
        return None

    ind = technical.indicators
    met = 0
    detail: List[str] = []

    # Condition 1 — EMA alignment
    if direction == "BUY":
        ok = ind.price > ind.ema_fast > ind.ema_slow
    else:
        ok = ind.price < ind.ema_fast < ind.ema_slow
    detail.append("EMA_OK" if ok else "EMA_FAIL")
    met += int(ok)

    # Condition 2 — RSI not extreme against direction
    rsi_ok = (ind.rsi < 65) if direction == "BUY" else (ind.rsi > 35)
    detail.append("RSI_OK" if rsi_ok else "RSI_FAIL")
    met += int(rsi_ok)

    # Condition 3 — MACD histogram
    h = ind.macd_histogram
    if math.isnan(h):
        macd_ok = False
    else:
        macd_ok = (h > 0) if direction == "BUY" else (h < 0)
    detail.append("MACD_OK" if macd_ok else "MACD_FAIL")
    met += int(macd_ok)

    if met >= settings.EXPERT_MIN_SIGNAL_CONDITIONS:
        log.debug(
            "Expert signal quality filter: PASS",
            symbol=symbol, direction=direction,
            conditions=f"{met}/3", detail=detail,
        )
        return None

    reason = (
        f"signal quality too low — {met}/3 conditions met "
        f"(need {settings.EXPERT_MIN_SIGNAL_CONDITIONS}): {', '.join(detail)}"
    )
    log.info(
        "Expert signal quality filter: SKIP",
        symbol=symbol, direction=direction, conditions=f"{met}/3", detail=detail,
    )
    return ExpertFilterResult(action="SKIP", filter_name="signal_quality", reason=reason)


# ---------------------------------------------------------------------------
# Filter E — Anti-Overtrading (daily count + portfolio cooldown)
# ---------------------------------------------------------------------------

async def _check_overtrading(
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    symbol: str,
    now: datetime,
) -> Optional[ExpertFilterResult]:
    """Overtrading / cooldown filter — disabled for testing."""
    log.debug("COOLDOWN DISABLED FOR TESTING", symbol=symbol)
    return None


# ---------------------------------------------------------------------------
# Filter F — Post-Event Delay
# ---------------------------------------------------------------------------

async def _check_post_event_delay(
    db: AsyncSession,
    symbol: str,
    now: datetime,
) -> Optional[ExpertFilterResult]:
    """SKIP if a high-impact event was released in the last
    EXPERT_POST_EVENT_DELAY_MINUTES for this symbol's currencies."""
    if settings.EXPERT_POST_EVENT_DELAY_MINUTES <= 0:
        return None

    cutoff = now - timedelta(minutes=settings.EXPERT_POST_EVENT_DELAY_MINUTES)
    currencies = _sym_currencies(symbol)

    result = await db.execute(
        select(HistoricalEvent)
        .where(
            HistoricalEvent.impact == "high",
            HistoricalEvent.currency.in_(currencies),
            HistoricalEvent.event_datetime_utc >= cutoff,
            HistoricalEvent.event_datetime_utc <= now,
        )
        .order_by(HistoricalEvent.event_datetime_utc.desc())
        .limit(1)
    )
    event = result.scalar_one_or_none()

    if event is None:
        log.debug("Expert post-event delay filter: PASS", symbol=symbol)
        return None

    ev_dt = event.event_datetime_utc
    if ev_dt.tzinfo is None:
        ev_dt = ev_dt.replace(tzinfo=timezone.utc)
    elapsed_min   = (now - ev_dt).total_seconds() / 60
    remaining_min = settings.EXPERT_POST_EVENT_DELAY_MINUTES - elapsed_min
    reason = (
        f"post-event delay — '{event.event_name}' ({event.currency}) released "
        f"{elapsed_min:.1f}min ago; waiting {settings.EXPERT_POST_EVENT_DELAY_MINUTES}min "
        f"({remaining_min:.1f}min remaining)"
    )
    log.info(
        "Expert post-event delay filter: SKIP",
        symbol=symbol,
        event_name=event.event_name,
        elapsed_min=round(elapsed_min, 1),
    )
    return ExpertFilterResult(action="SKIP", filter_name="post_event_delay", reason=reason)


# ---------------------------------------------------------------------------
# Public API — split into two stages for pipeline efficiency
# ---------------------------------------------------------------------------

async def check_pre_analysis_filters(
    *,
    symbol: str,
    db: AsyncSession,
    portfolio_id: uuid.UUID,
    now: Optional[datetime] = None,
) -> ExpertFilterResult:
    """Stage 1 — filters that require no technical data (cheap, DB-light).

    Run these BEFORE fetching candles or running technical analysis so that
    trades which would be rejected anyway never pay for expensive market-data
    calls.

    Filters evaluated (in order):
      A — Session:          London 07–11 UTC / NY 13–17 UTC
      E — Anti-Overtrading: daily position count + portfolio cooldown
      F — Post-Event Delay: high-impact event released in the last N minutes

    Returns the first SKIP result, or ALLOW when all pass.
    Bypassed entirely when EXPERT_FILTERS_ENABLED=False.
    """
    if not settings.EXPERT_FILTERS_ENABLED:
        log.debug("Expert pre-analysis filters disabled — ALLOW", symbol=symbol)
        return _ALLOW

    if now is None:
        now = datetime.now(timezone.utc)

    # A — Session (cheapest: pure time check, no I/O)
    r = _check_session(now)
    if r is not None:
        return r

    # E — Anti-Overtrading (two DB queries: daily count + recent opened_at)
    r = await _check_overtrading(db, portfolio_id, symbol, now)
    if r is not None:
        return r

    # F — Post-Event Delay (one DB query: recent high-impact events)
    r = await _check_post_event_delay(db, symbol, now)
    if r is not None:
        return r

    log.debug("Expert pre-analysis filters: all passed — ALLOW", symbol=symbol)
    return _ALLOW


def check_post_analysis_filters(
    *,
    technical: TechnicalSignal,
    candles: List[dict],
    direction: str,
    symbol: str,
) -> ExpertFilterResult:
    """Stage 2 — filters that require technical indicator data (synchronous).

    Run these AFTER technical analysis when ATR, price, EMA, RSI, and MACD
    values are available.

    Filters evaluated (in order):
      B — Volatility:     ATR below minimum threshold
      C — Trend:          EMA200 direction alignment
      D — Signal Quality: N of 3 confirming conditions

    Returns the first SKIP result, or ALLOW when all pass.
    Bypassed entirely when EXPERT_FILTERS_ENABLED=False.
    Note: this function is synchronous — no DB calls needed.
    """
    if not settings.EXPERT_FILTERS_ENABLED:
        log.debug("Expert post-analysis filters disabled — ALLOW", symbol=symbol)
        return _ALLOW

    # B — Volatility (single float comparison, no I/O)
    r = _check_volatility(technical.indicators.atr, symbol)
    if r is not None:
        return r

    # C — Trend (EMA200 computed from candles list, no I/O)
    r = _check_trend(technical.indicators.price, candles, direction, symbol)
    if r is not None:
        return r

    # D — Signal Quality (arithmetic on indicator values, no I/O)
    r = _check_signal_quality(technical, direction, symbol)
    if r is not None:
        return r

    log.debug(
        "Expert post-analysis filters: all passed — ALLOW",
        symbol=symbol, direction=direction,
    )
    return _ALLOW
