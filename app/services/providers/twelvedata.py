"""
Twelve Data market data provider — Forex + Commodities.

Endpoints used:
  Base: https://api.twelvedata.com

  Latest price:
    GET /price?symbol={symbol}&apikey={key}
    → {"price": "1.08245"}

  Time series (candles):
    GET /time_series?symbol={symbol}&interval={tf}&outputsize={n}&apikey={key}
    → {"values": [{datetime, open, high, low, close, volume}, ...]}
    Note: returned newest-first by default; we sort ascending.

Auth: ?apikey=<TWELVE_DATA_API_KEY>  (query-param)

Free plan limits:
  800 API credits/day · 8 requests/minute
  Forex: real-time (no delay)
  Commodities: real-time on free plan

Symbols supported:
  Forex  — EUR/USD, GBP/USD, USD/JPY, USD/CHF, AUD/USD, USD/CAD, NZD/USD,
            EUR/GBP, EUR/JPY, GBP/JPY, EUR/CHF, AUD/JPY, GBP/CHF
  Metals — XAU/USD (Gold), XAG/USD (Silver), XPT/USD (Platinum)
  Energy — WTI/USD (WTI Crude), BRENT/USD (Brent), XNG/USD (Natural Gas)

Fallback:
  GBM simulation when API key is missing or request fails.
"""
from __future__ import annotations

import asyncio
import math
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from app.core.logger import get_logger
from .base import MarketDataProvider

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.twelvedata.com"
TIMEOUT  = 10.0

FOREX_SYMBOLS: List[str] = [
    # Majors
    "EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "USD/CAD", "NZD/USD",
    # Minors / Popular
    "EUR/GBP", "EUR/JPY", "GBP/JPY", "EUR/CHF", "AUD/JPY", "GBP/CHF",
]

COMMODITY_SYMBOLS: List[str] = [
    # Metals
    "XAU/USD",   # Gold
    "XAG/USD",   # Silver
    "XPT/USD",   # Platinum
    # Energy
    "WTI/USD",   # WTI Crude Oil
    "BRENT/USD", # Brent Crude Oil
    "XNG/USD",   # Natural Gas
]

SYMBOLS: List[str] = FOREX_SYMBOLS + COMMODITY_SYMBOLS

# Twelve Data interval strings
TWELVE_DATA_INTERVAL: Dict[str, str] = {
    "1m":  "1min",
    "5m":  "5min",
    "15m": "15min",
    "30m": "30min",
    "1h":  "1h",
    "4h":  "4h",
    "1d":  "1day",
}

TIMEFRAME_DELTA: Dict[str, timedelta] = {
    "1m":  timedelta(minutes=1),
    "5m":  timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h":  timedelta(hours=1),
    "4h":  timedelta(hours=4),
    "1d":  timedelta(days=1),
}

# GBM seed prices
FALLBACK_BASE_PRICES: Dict[str, float] = {
    # Forex — Majors
    "EUR/USD": 1.0850,  "GBP/USD": 1.2700,  "USD/JPY": 149.50,
    "USD/CHF": 0.9050,  "AUD/USD": 0.6500,  "USD/CAD": 1.3600,
    "NZD/USD": 0.6000,
    # Forex — Minors
    "EUR/GBP": 0.8550,  "EUR/JPY": 162.50,  "GBP/JPY": 189.00,
    "EUR/CHF": 0.9800,  "AUD/JPY": 97.00,   "GBP/CHF": 1.1500,
    # Commodities — Metals
    "XAU/USD": 2300.0,  "XAG/USD": 27.00,   "XPT/USD": 900.0,
    # Commodities — Energy
    "WTI/USD":  80.0,   "BRENT/USD": 84.0,  "XNG/USD": 2.50,
}

FALLBACK_VOLATILITY: Dict[str, float] = {
    # Forex — per-step vol must match gbm.py VOLATILITY to keep pip moves consistent.
    # BUG HISTORY: was 0.003 (10× too high).  With price ~1.15 that produced
    # mean steps of ~26 pips per get_current_price() call, completely overrunning
    # the 1-pip TP/SL epsilon and causing persistent missed closes in simulation.
    "EUR/USD": 0.0003, "GBP/USD": 0.0004, "USD/JPY": 0.0003,
    "USD/CHF": 0.0003, "AUD/USD": 0.0004, "USD/CAD": 0.0003,
    "NZD/USD": 0.0005,
    "EUR/GBP": 0.0003, "EUR/JPY": 0.0004, "GBP/JPY": 0.0005,
    "EUR/CHF": 0.0003, "AUD/JPY": 0.0004, "GBP/CHF": 0.0003,
    # Commodities — also scaled down by 10× to match intended per-tick granularity
    "XAU/USD": 0.0008, "XAG/USD": 0.0012, "XPT/USD": 0.0010,
    "WTI/USD": 0.0015, "BRENT/USD": 0.0014, "XNG/USD": 0.0025,
}

FALLBACK_SPREAD_PCT = 0.0002


# ---------------------------------------------------------------------------
# GBM fallback
# ---------------------------------------------------------------------------

class _GBMFallback:
    """
    GBM price simulation for TwelveData fallback (no API key).

    Advancement is intentionally separated from reads so that multiple
    callers within the same bot cycle (UI endpoint, bot evaluation, candle
    fetch) all see the SAME price snapshot.  Only advance_price() mutates
    state; peek_price() is a pure read.

    The scheduler calls advance_price() via TwelveDataProvider.update_price()
    every 30 s, giving a deterministic price cadence that matches the stock
    GBM (which is also advanced only by the scheduler).
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = dict(FALLBACK_BASE_PRICES)
        self._prev:   Dict[str, float] = dict(FALLBACK_BASE_PRICES)
        # Pre-seed rolling history with a real GBM walk so indicators have
        # meaningful variance from the first cycle.
        self._history: Dict[str, List[float]] = {}
        for sym, base in FALLBACK_BASE_PRICES.items():
            vol = FALLBACK_VOLATILITY.get(sym, 0.0005)
            rng_h = random.Random(hash(sym) & 0x7FFFFFFF)
            p = base
            hist: List[float] = []
            for _ in range(500):
                shock = rng_h.gauss(0, 1)
                p = round(p * math.exp(-0.5 * vol ** 2 + vol * shock), 5)
                hist.append(p)
            self._history[sym] = hist
            self._prices[sym] = hist[-1]
            self._prev[sym]   = hist[-2] if len(hist) >= 2 else base

    def peek_price(self, symbol: str) -> float:
        """Return the current cached price WITHOUT advancing the walk."""
        return self._prices.get(symbol, FALLBACK_BASE_PRICES.get(symbol, 1.0))

    def advance_price(self, symbol: str) -> float:
        """Advance the GBM walk one step and return the new price."""
        price = self._prices.get(symbol, FALLBACK_BASE_PRICES.get(symbol, 1.0))
        vol   = FALLBACK_VOLATILITY.get(symbol, 0.0005)
        shock = random.gauss(0, 1)
        new_price = round(price * math.exp(-0.5 * vol ** 2 + vol * shock), 5)
        self._prev[symbol]   = price
        self._prices[symbol] = new_price
        hist = self._history.setdefault(symbol, [price])
        hist.append(new_price)
        if len(hist) > 2000:
            self._history[symbol] = hist[-1000:]
        return new_price

    # Keep next_price as an alias so existing call-sites (e.g. get_quote)
    # that intentionally want a fresh tick still work without changes.
    def next_price(self, symbol: str) -> float:
        return self.advance_price(symbol)

    def prev_price(self, symbol: str) -> float:
        return self._prev.get(symbol, FALLBACK_BASE_PRICES.get(symbol, 1.0))

    def candles(self, symbol: str, timeframe: str, limit: int) -> List[dict]:
        delta  = TIMEFRAME_DELTA.get(timeframe, timedelta(hours=1))
        vol    = FALLBACK_VOLATILITY.get(symbol, 0.0005)
        base   = FALLBACK_BASE_PRICES.get(symbol, 1.0)
        now    = datetime.now(timezone.utc)

        # Build from the rolling price history that advances every 30 s via
        # advance_price(). This ensures EMA/RSI change every cycle instead of
        # being re-seeded to the same sequence each call.
        hist   = self._history.get(symbol, [base])
        prices = hist[-(limit + 1):]  # one extra for open of first candle
        n      = len(prices)

        result: List[dict] = []
        for i in range(1, n):
            close_p = prices[i]
            open_p  = prices[i - 1]
            spread  = abs(close_p - open_p)
            high_p  = round(max(open_p, close_p) + spread * 0.5, 5)
            low_p   = round(max(min(open_p, close_p) - spread * 0.5, base * 0.01), 5)
            ts      = now - delta * (n - 1 - i)
            result.append({
                "timestamp": ts.isoformat(),
                "open":      round(open_p,  5),
                "high":      high_p,
                "low":       low_p,
                "close":     round(close_p, 5),
                "volume":    0,
            })
        return result[-limit:]

    def historical_candles(self, symbol: str, timeframe: str, start_date, end_date) -> List[dict]:
        delta = TIMEFRAME_DELTA.get(timeframe, timedelta(hours=1))
        vol   = FALLBACK_VOLATILITY.get(symbol, 0.005)
        base  = FALLBACK_BASE_PRICES.get(symbol, 1.0)
        seed  = hash(symbol + timeframe) & 0x7FFFFFFF
        rng   = random.Random(seed)

        def _to_dt(d, end: bool = False) -> datetime:
            if isinstance(d, datetime):
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            h, m = (23, 59) if end else (0, 0)
            return datetime(d.year, d.month, d.day, h, m, tzinfo=timezone.utc)

        start = _to_dt(start_date)
        end   = _to_dt(end_date, end=True)
        ts    = start - delta * 400
        price = base * rng.uniform(0.95, 1.05)
        result: List[dict] = []
        while ts <= end:
            open_ = price
            ticks = [price * math.exp(-0.5 * vol**2 + vol * rng.gauss(0, 1)) for _ in range(4)]
            price = max(ticks[-1], base * 0.3)
            result.append({
                "timestamp": ts.isoformat(),
                "open":      round(open_,            5),
                "high":      round(max(open_, *ticks), 5),
                "low":       round(min(open_, *ticks), 5),
                "close":     round(price,            5),
                "volume":    0,
                "symbol":    symbol,
                "timeframe": timeframe,
            })
            ts += delta
        return result


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class TwelveDataProvider(MarketDataProvider):
    """
    Fetches real forex and commodity data from Twelve Data REST API.

    Falls back to GBM simulation when:
      - TWELVE_DATA_API_KEY is not set
      - A request fails (network, 4xx, rate limit)

    All external calls run in asyncio.to_thread — event loop never blocked.
    """

    # Price snapshot TTL: slightly shorter than the 60 s bot cycle so all
    # calls within one cycle share the same live price (SL/TP detection).
    _SNAPSHOT_TTL = 55.0

    # Candle snapshot TTL: 1h candles only change once per hour — no point
    # re-fetching them every 60 s bot cycle.  3600 s caps API usage at
    # ~24 candle calls/day per symbol instead of ~1440.
    _CANDLE_SNAPSHOT_TTL = 3600.0

    def __init__(self, api_key: str) -> None:
        self._has_keys = bool(api_key)
        self._api_key  = api_key
        self._fallback = _GBMFallback()
        self._prev_prices: Dict[str, float] = {}

        # Cycle-level snapshot caches: { symbol: (value, monotonic_ts) }
        self._price_snapshot:  Dict[str, Tuple[float, float]] = {}
        self._candle_snapshot: Dict[Tuple[str, str, int], Tuple[List[dict], float]] = {}

        if self._has_keys:
            log.info("TwelveDataProvider ready", symbols_count=len(SYMBOLS))
        else:
            log.warning(
                "TwelveDataProvider: TWELVE_DATA_API_KEY not set "
                "— forex/commodity calls will use GBM fallback"
            )

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        p = dict(params or {})
        p["apikey"] = self._api_key
        url = f"{BASE_URL}{path}"
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.get(url, params=p)
        if resp.status_code == 401:
            raise PermissionError("Twelve Data: 401 Unauthorized — check TWELVE_DATA_API_KEY")
        if resp.status_code == 429:
            raise RuntimeError("Twelve Data: 429 rate limit hit (8 req/min on free plan)")
        resp.raise_for_status()
        data = resp.json()
        # Twelve Data returns {"status": "error", "message": "..."} for invalid symbols
        if isinstance(data, dict) and data.get("status") == "error":
            raise ValueError(f"Twelve Data error: {data.get('message', 'unknown')}")
        return data

    # ------------------------------------------------------------------
    # MarketDataProvider interface
    # ------------------------------------------------------------------

    def get_all_symbols(self) -> List[str]:
        return list(SYMBOLS)

    async def get_current_price(self, symbol: str) -> float:
        # ── Cycle-level snapshot: return cached price if still fresh ──────────
        # All calls within the same 60 s bot cycle share one snapshot so that
        # pre-guard, position-eval, and entry checks all see the same price.
        # The cache is populated on the first call and expires after _SNAPSHOT_TTL.
        now_mono = time.monotonic()
        cached = self._price_snapshot.get(symbol)
        if cached is not None:
            price, ts = cached
            if now_mono - ts < self._SNAPSHOT_TTL:
                log.debug("MARKET DATA FETCH", source="CYCLE_SNAPSHOT",
                          symbol=symbol, price=round(price, 6))
                return price

        # ── First call this cycle: fetch a fresh price ────────────────────────
        if not self._has_keys:
            # Read the scheduler-advanced price WITHOUT advancing state.
            # Price is only mutated by update_price() / the 30 s scheduler job.
            price = self._fallback.peek_price(symbol)
            log.info("MARKET DATA FETCH", source="GBM_SIMULATION",
                     symbol=symbol, price=round(price, 6))
            self._price_snapshot[symbol] = (price, now_mono)
            return price

        def _fetch() -> float:
            data  = self._get("/price", {"symbol": symbol})
            p = data.get("price")
            if p is None:
                raise ValueError(f"No price in Twelve Data response for {symbol}: {data}")
            return float(p)

        try:
            price = await asyncio.to_thread(_fetch)
            self._prev_prices[symbol] = price
            log.info("MARKET DATA FETCH", source="TWELVEDATA_REAL",
                     symbol=symbol, price=round(price, 6))
        except Exception as exc:
            log.error(
                "MARKET DATA UNAVAILABLE — simulated price blocked (live mode)",
                symbol=symbol, error=str(exc),
            )
            raise

        self._price_snapshot[symbol] = (price, now_mono)
        return price

    async def get_quote(self, symbol: str) -> dict:
        if not self._has_keys:
            price  = self._fallback.next_price(symbol)
            prev   = self._fallback.prev_price(symbol)
            spread = price * FALLBACK_SPREAD_PCT
            change = price - prev
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     round(change,                        5),
                "change_pct": round((change / prev * 100) if prev else 0.0, 4),
                "bid":        round(price - spread,                5),
                "ask":        round(price + spread,                5),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }

        try:
            price  = await self.get_current_price(symbol)
            prev   = self._prev_prices.get(symbol, price)
            change = price - prev
            spread = price * FALLBACK_SPREAD_PCT
            self._prev_prices[symbol] = price
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     round(change,                        5),
                "change_pct": round((change / prev * 100) if prev else 0.0, 4),
                "bid":        round(price - spread,                5),
                "ask":        round(price + spread,                5),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            log.warning("TwelveData get_quote failed, using fallback",
                        symbol=symbol, error=str(exc))
            price  = self._fallback.next_price(symbol)
            spread = price * FALLBACK_SPREAD_PCT
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     0.0,
                "change_pct": 0.0,
                "bid":        round(price - spread, 5),
                "ask":        round(price + spread, 5),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 200) -> List[dict]:
        # ── Cycle-level snapshot ──────────────────────────────────────────────
        # The pre-guard pass and _process_symbol both call get_candles for the
        # same symbol+timeframe within one 60 s cycle.  Return the same slice
        # so indicator values are identical across all checks in that cycle.
        now_mono = time.monotonic()
        snap_key = (symbol, timeframe, limit)
        cached_c = self._candle_snapshot.get(snap_key)
        if cached_c is not None:
            candles_c, ts_c = cached_c
            if now_mono - ts_c < self._CANDLE_SNAPSHOT_TTL:
                log.debug("MARKET DATA CANDLES", source="CYCLE_SNAPSHOT",
                          symbol=symbol, timeframe=timeframe, limit=limit)
                return candles_c

        # ── First call this cycle: fetch fresh candles ────────────────────────
        if not self._has_keys:
            log.info("MARKET DATA CANDLES", source="GBM_SIMULATION",
                     symbol=symbol, timeframe=timeframe, limit=limit)
            candles = self._fallback.candles(symbol, timeframe, limit)
            self._candle_snapshot[snap_key] = (candles, now_mono)
            return candles

        interval = TWELVE_DATA_INTERVAL.get(timeframe, "1h")

        def _fetch() -> List[dict]:
            data = self._get("/time_series", {
                "symbol":     symbol,
                "interval":   interval,
                "outputsize": min(limit, 5000),
                "order":      "ASC",  # oldest first
            })
            values = data.get("values", [])
            return [
                {
                    "timestamp": datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S")
                                 .replace(tzinfo=timezone.utc).isoformat()
                                 if " " in v["datetime"]
                                 else datetime.strptime(v["datetime"], "%Y-%m-%d")
                                      .replace(tzinfo=timezone.utc).isoformat(),
                    "open":   float(v["open"]),
                    "high":   float(v["high"]),
                    "low":    float(v["low"]),
                    "close":  float(v["close"]),
                    "volume": int(float(v.get("volume") or 0)),
                }
                for v in values
            ]

        try:
            candles = await asyncio.to_thread(_fetch)
            candles = candles[-limit:]
            log.info("MARKET DATA CANDLES", source="TWELVEDATA_REAL",
                     symbol=symbol, timeframe=timeframe, count=len(candles))
        except Exception as exc:
            log.error(
                "MARKET DATA UNAVAILABLE — simulated candles blocked (live mode)",
                symbol=symbol, error=str(exc),
            )
            raise

        self._candle_snapshot[snap_key] = (candles, now_mono)
        return candles

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_date, end_date
    ) -> List[dict]:
        if not self._has_keys:
            return self._fallback.historical_candles(symbol, timeframe, start_date, end_date)

        interval = TWELVE_DATA_INTERVAL.get(timeframe, "1day")

        def _to_str(d) -> str:
            if isinstance(d, datetime):
                return d.strftime("%Y-%m-%d")
            return d.strftime("%Y-%m-%d")

        def _fetch() -> List[dict]:
            data = self._get("/time_series", {
                "symbol":     symbol,
                "interval":   interval,
                "start_date": _to_str(start_date),
                "end_date":   _to_str(end_date),
                "outputsize": 5000,
                "order":      "ASC",
            })
            values = data.get("values", [])
            return [
                {
                    "timestamp": datetime.strptime(v["datetime"], "%Y-%m-%d %H:%M:%S")
                                 .replace(tzinfo=timezone.utc).isoformat()
                                 if " " in v["datetime"]
                                 else datetime.strptime(v["datetime"], "%Y-%m-%d")
                                      .replace(tzinfo=timezone.utc).isoformat(),
                    "open":      float(v["open"]),
                    "high":      float(v["high"]),
                    "low":       float(v["low"]),
                    "close":     float(v["close"]),
                    "volume":    int(float(v.get("volume") or 0)),
                    "symbol":    symbol,
                    "timeframe": timeframe,
                }
                for v in values
            ]

        try:
            return await asyncio.to_thread(_fetch)
        except Exception as exc:
            log.warning("TwelveData get_historical_candles failed, using fallback",
                        symbol=symbol, error=str(exc))
            return self._fallback.historical_candles(symbol, timeframe, start_date, end_date)

    async def update_price(self, symbol: str) -> float:
        """
        Called by the scheduler every 30 s to advance the simulated price walk.

        BUG HISTORY: previously returned _prev_prices (stale) — the GBM walk
        was NEVER advanced by the scheduler for forex symbols.  Every call to
        get_current_price() advanced state instead, meaning UI page loads were
        silently mutating the price seen by the next bot evaluation cycle.
        """
        if not self._has_keys:
            price = self._fallback.advance_price(symbol)
            log.info("MARKET DATA UPDATE", source="GBM_SIMULATION", symbol=symbol, price=round(price, 6))
            return price
        # Real TwelveData: prices are fetched live; nothing to advance.
        return self._prev_prices.get(symbol, FALLBACK_BASE_PRICES.get(symbol, 1.0))
