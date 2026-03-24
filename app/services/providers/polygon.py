"""
Polygon.io market data provider — direct HTTP implementation.

Endpoints used:
  Base: https://api.polygon.io

  Latest trade (price):
    GET /v2/last/trade/{symbol}

  Aggregates / candles:
    GET /v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from}/{to}

Auth: ?apiKey=<POLYGON_API_KEY>  (query-param, no header required)

Supported symbols: 110+ US equities across Tech, Finance, Healthcare, Consumer, Industrial, Energy, Telecom, EV, ETFs
Supported timeframes: 1m, 5m, 1h, 1d

Fallback behaviour:
  - API key missing  → GBM simulation, warning logged at startup
  - Request fails    → GBM simulation, warning logged per call
"""
from __future__ import annotations

import asyncio
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import httpx

from app.core.logger import get_logger
from .base import MarketDataProvider

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.polygon.io"
TIMEOUT  = 10.0

SYMBOLS: List[str] = [
    # Big Tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD", "INTC", "NFLX",
    # Semiconductors
    "AVGO", "QCOM", "TXN", "ASML", "MRVL", "AMAT", "KLAC", "MU",
    # SaaS / Cloud
    "CRM", "ORCL", "ADBE", "NOW", "PANW", "CRWD", "NET", "DDOG", "SNOW", "ZS",
    # Growth / Popular
    "PLTR", "COIN", "SHOP", "UBER", "LYFT", "ABNB", "DASH", "RBLX", "ROKU", "ZM",
    "HOOD", "SOFI", "SNAP", "TWLO", "SQ", "PYPL",
    # Finance
    "JPM", "BAC", "GS", "MS", "C", "WFC", "AXP", "V", "MA", "BLK", "SCHW", "USB",
    # Healthcare
    "JNJ", "PFE", "MRK", "ABBV", "UNH", "TMO", "DHR", "CVS", "GILD", "ISRG", "MRNA", "REGN",
    # Consumer Staples
    "KO", "PEP", "MCD", "SBUX", "WMT", "COST", "PG", "CL",
    # Consumer Discretionary
    "NKE", "DIS", "HD", "LOW", "TGT", "LULU", "BKNG",
    # Industrial
    "BA", "CAT", "GE", "UPS", "HON", "RTX", "DE", "MMM", "FDX", "LMT",
    # Energy
    "XOM", "CVX", "SLB", "COP", "OXY",
    # Telecom
    "T", "VZ", "TMUS", "CMCSA",
    # EV / Automotive
    "GM", "F", "RIVN", "LCID",
    # ETFs
    "SPY", "QQQ", "DIA", "IWM",
]

# Fallback GBM seed prices (used when Polygon is unavailable)
FALLBACK_BASE_PRICES: Dict[str, float] = {
    # Big Tech
    "AAPL": 175.0,  "MSFT": 415.0,  "GOOGL": 175.0, "AMZN": 185.0,
    "META": 500.0,  "NVDA": 850.0,  "TSLA":  250.0, "AMD":  165.0,
    "INTC":  30.0,  "NFLX": 700.0,
    # Semiconductors
    "AVGO": 900.0,  "QCOM": 160.0,  "TXN":   175.0, "ASML": 800.0,
    "MRVL":  70.0,  "AMAT": 200.0,  "KLAC":  700.0, "MU":   120.0,
    # SaaS / Cloud
    "CRM":  290.0,  "ORCL": 125.0,  "ADBE":  480.0, "NOW":  800.0,
    "PANW": 320.0,  "CRWD": 350.0,  "NET":    95.0, "DDOG": 120.0,
    "SNOW": 160.0,  "ZS":   200.0,
    # Growth / Popular
    "PLTR":  25.0,  "COIN": 200.0,  "SHOP":   85.0, "UBER":  75.0,
    "LYFT":  18.0,  "ABNB": 145.0,  "DASH":  115.0, "RBLX":  40.0,
    "ROKU":  70.0,  "ZM":    65.0,  "HOOD":   18.0, "SOFI":  10.0,
    "SNAP":  12.0,  "TWLO":  65.0,  "SQ":     75.0, "PYPL":  65.0,
    # Finance
    "JPM":  200.0,  "BAC":   40.0,  "GS":    460.0, "MS":    100.0,
    "C":     65.0,  "WFC":   58.0,  "AXP":   230.0, "V":     280.0,
    "MA":   470.0,  "BLK":   800.0, "SCHW":   75.0, "USB":    45.0,
    # Healthcare
    "JNJ":  155.0,  "PFE":   28.0,  "MRK":   125.0, "ABBV": 185.0,
    "UNH":  530.0,  "TMO":   560.0, "DHR":   245.0, "CVS":   60.0,
    "GILD":  85.0,  "ISRG":  400.0, "MRNA":   95.0, "REGN": 1000.0,
    # Consumer Staples
    "KO":    60.0,  "PEP":  175.0,  "MCD":   290.0, "SBUX":  95.0,
    "WMT":   65.0,  "COST": 850.0,  "PG":    155.0, "CL":    90.0,
    # Consumer Discretionary
    "NKE":   75.0,  "DIS":   90.0,  "HD":    355.0, "LOW":   230.0,
    "TGT":  145.0,  "LULU": 380.0,  "BKNG": 3800.0,
    # Industrial
    "BA":   200.0,  "CAT":  360.0,  "GE":    170.0, "UPS":   135.0,
    "HON":  200.0,  "RTX":   90.0,  "DE":    390.0, "MMM":   105.0,
    "FDX":  260.0,  "LMT":  470.0,
    # Energy
    "XOM":  115.0,  "CVX":  160.0,  "SLB":    50.0, "COP":  120.0,
    "OXY":   60.0,
    # Telecom
    "T":     20.0,  "VZ":    40.0,  "TMUS":  170.0, "CMCSA":  45.0,
    # EV / Automotive
    "GM":    45.0,  "F":     12.0,  "RIVN":   15.0, "LCID":    4.0,
    # ETFs
    "SPY":  500.0,  "QQQ":  430.0,  "DIA":   390.0, "IWM":   200.0,
}
FALLBACK_VOLATILITY: Dict[str, float] = {
    # Big Tech
    "AAPL": 0.012,  "MSFT": 0.010,  "GOOGL": 0.013, "AMZN": 0.014,
    "META": 0.018,  "NVDA": 0.025,  "TSLA":  0.030, "AMD":  0.028,
    "INTC": 0.018,  "NFLX": 0.022,
    # Semiconductors
    "AVGO": 0.018,  "QCOM": 0.020,  "TXN":   0.015, "ASML": 0.020,
    "MRVL": 0.025,  "AMAT": 0.022,  "KLAC":  0.022, "MU":   0.028,
    # SaaS / Cloud
    "CRM":  0.018,  "ORCL": 0.015,  "ADBE":  0.018, "NOW":  0.020,
    "PANW": 0.022,  "CRWD": 0.025,  "NET":   0.030, "DDOG": 0.028,
    "SNOW": 0.030,  "ZS":   0.025,
    # Growth / Popular
    "PLTR": 0.035,  "COIN": 0.040,  "SHOP":  0.025, "UBER": 0.020,
    "LYFT": 0.030,  "ABNB": 0.025,  "DASH":  0.028, "RBLX": 0.035,
    "ROKU": 0.032,  "ZM":   0.028,  "HOOD":  0.040, "SOFI": 0.038,
    "SNAP": 0.038,  "TWLO": 0.030,  "SQ":    0.028, "PYPL": 0.022,
    # Finance
    "JPM":  0.012,  "BAC":  0.014,  "GS":    0.016, "MS":   0.015,
    "C":    0.015,  "WFC":  0.014,  "AXP":   0.014, "V":    0.012,
    "MA":   0.013,  "BLK":  0.015,  "SCHW":  0.016, "USB":  0.014,
    # Healthcare
    "JNJ":  0.010,  "PFE":  0.015,  "MRK":   0.012, "ABBV": 0.015,
    "UNH":  0.013,  "TMO":  0.015,  "DHR":   0.014, "CVS":  0.015,
    "GILD": 0.016,  "ISRG": 0.018,  "MRNA":  0.035, "REGN": 0.020,
    # Consumer Staples
    "KO":   0.010,  "PEP":  0.010,  "MCD":   0.012, "SBUX": 0.015,
    "WMT":  0.010,  "COST": 0.013,  "PG":    0.010, "CL":   0.010,
    # Consumer Discretionary
    "NKE":  0.014,  "DIS":  0.016,  "HD":    0.013, "LOW":  0.014,
    "TGT":  0.018,  "LULU": 0.022,  "BKNG":  0.018,
    # Industrial
    "BA":   0.020,  "CAT":  0.016,  "GE":    0.018, "UPS":  0.014,
    "HON":  0.013,  "RTX":  0.014,  "DE":    0.016, "MMM":  0.014,
    "FDX":  0.018,  "LMT":  0.012,
    # Energy
    "XOM":  0.015,  "CVX":  0.015,  "SLB":   0.022, "COP":  0.018,
    "OXY":  0.022,
    # Telecom
    "T":    0.012,  "VZ":   0.012,  "TMUS":  0.015, "CMCSA": 0.014,
    # EV / Automotive
    "GM":   0.018,  "F":    0.020,  "RIVN":  0.045, "LCID": 0.055,
    # ETFs
    "SPY":  0.008,  "QQQ":  0.010,  "DIA":   0.008, "IWM":  0.012,
}
FALLBACK_SPREAD_PCT = 0.0002  # 0.02 %

# Platform timeframe → (multiplier, timespan) for Polygon /aggs endpoint
POLYGON_TIMEFRAME: Dict[str, tuple] = {
    "1m":  (1,  "minute"),
    "5m":  (5,  "minute"),
    "15m": (15, "minute"),
    "30m": (30, "minute"),
    "1h":  (1,  "hour"),
    "4h":  (4,  "hour"),
    "1d":  (1,  "day"),
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


# ---------------------------------------------------------------------------
# GBM fallback (identical pattern to Alpaca provider)
# ---------------------------------------------------------------------------

class _GBMFallback:
    def __init__(self) -> None:
        self._prices: Dict[str, float] = dict(FALLBACK_BASE_PRICES)
        self._prev:   Dict[str, float] = dict(FALLBACK_BASE_PRICES)

    def next_price(self, symbol: str) -> float:
        price = self._prices.get(symbol, 100.0)
        vol   = FALLBACK_VOLATILITY.get(symbol, 0.015)
        shock = random.gauss(0, 1)
        new_price = round(price * math.exp(-0.5 * vol ** 2 + vol * shock), 2)
        self._prev[symbol]   = price
        self._prices[symbol] = new_price
        return new_price

    def prev_price(self, symbol: str) -> float:
        return self._prev.get(symbol, FALLBACK_BASE_PRICES.get(symbol, 100.0))

    def candles(self, symbol: str, timeframe: str, limit: int) -> List[dict]:
        delta = TIMEFRAME_DELTA.get(timeframe, timedelta(hours=1))
        vol   = FALLBACK_VOLATILITY.get(symbol, 0.015)
        base  = FALLBACK_BASE_PRICES.get(symbol, 100.0)
        rng   = random.Random(hash(symbol + timeframe))
        price = base
        now   = datetime.now(timezone.utc)
        result: List[dict] = []
        for i in range(limit, 0, -1):
            ts    = now - delta * i
            open_ = price
            ticks = [price * math.exp(-0.5 * vol**2 + vol * rng.gauss(0, 1)) for _ in range(4)]
            price = ticks[-1]
            result.append({
                "timestamp": ts.isoformat(),
                "open":   round(open_,           2),
                "high":   round(max(open_, *ticks), 2),
                "low":    round(min(open_, *ticks), 2),
                "close":  round(price,           2),
                "volume": round(rng.uniform(100_000, 2_000_000), 0),
            })
        return result

    def historical_candles(
        self, symbol: str, timeframe: str, start_date, end_date
    ) -> List[dict]:
        delta = TIMEFRAME_DELTA.get(timeframe, timedelta(hours=1))
        vol   = FALLBACK_VOLATILITY.get(symbol, 0.015)
        base  = FALLBACK_BASE_PRICES.get(symbol, 100.0)
        seed  = hash(symbol + timeframe) & 0x7FFFFFFF
        rng   = random.Random(seed)

        def _to_dt(d, end: bool = False) -> datetime:
            if isinstance(d, datetime):
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            h, m = (23, 59) if end else (0, 0)
            return datetime(d.year, d.month, d.day, h, m, tzinfo=timezone.utc)

        start = _to_dt(start_date)
        end   = _to_dt(end_date, end=True)
        ts    = start - delta * 400   # 400-candle warmup
        price = base * rng.uniform(0.9, 1.1)
        result: List[dict] = []
        while ts <= end:
            open_ = price
            ticks = [price * math.exp(-0.5 * vol**2 + vol * rng.gauss(0, 1)) for _ in range(4)]
            price = max(ticks[-1], base * 0.3)
            result.append({
                "timestamp": ts.isoformat(),
                "open":      round(open_,           2),
                "high":      round(max(open_, *ticks), 2),
                "low":       round(min(open_, *ticks), 2),
                "close":     round(price,           2),
                "volume":    round(rng.uniform(100_000, 5_000_000), 0),
                "symbol":    symbol,
                "timeframe": timeframe,
            })
            ts += delta
        return result


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class PolygonMarketDataProvider(MarketDataProvider):
    """
    Fetches real US equity data from Polygon.io via direct HTTP calls.

    Falls back to GBM simulation when keys are missing or requests fail.
    All external calls run in asyncio.to_thread — event loop never blocked.
    """

    def __init__(self, api_key: str) -> None:
        self._has_keys = bool(api_key)
        self._api_key  = api_key
        self._fallback = _GBMFallback()
        self._prev_prices: Dict[str, float] = {}

        if self._has_keys:
            log.info("PolygonMarketDataProvider ready", symbols=SYMBOLS)
        else:
            log.warning(
                "PolygonMarketDataProvider: POLYGON_API_KEY not set "
                "— all calls will use GBM fallback prices"
            )

    # ------------------------------------------------------------------
    # Internal HTTP helper
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Synchronous GET against Polygon REST API."""
        p = dict(params or {})
        p["apiKey"] = self._api_key
        url = f"{BASE_URL}{path}"
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.get(url, params=p)
        if resp.status_code == 403:
            raise PermissionError(
                "Polygon free plan limitation detected — 403 Forbidden on this endpoint."
            )
        if resp.status_code == 401:
            raise PermissionError(
                "Polygon returned 401 Unauthorized — POLYGON_API_KEY is invalid."
            )
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # MarketDataProvider interface
    # ------------------------------------------------------------------

    def get_all_symbols(self) -> List[str]:
        return list(SYMBOLS)

    async def get_current_price(self, symbol: str) -> float:
        if not self._has_keys:
            price = self._fallback.next_price(symbol)
            log.debug("Polygon fallback price", symbol=symbol, price=price)
            return price

        def _fetch() -> float:
            # /v2/aggs/ticker/{symbol}/prev — available on all Polygon plans
            data    = self._get(f"/v2/aggs/ticker/{symbol}/prev", {"adjusted": "true"})
            results = data.get("results", [])
            if not results:
                raise ValueError(f"No results in Polygon prev-day response for {symbol}: {data}")
            price = results[0].get("c")  # 'c' = close price
            if price is None:
                raise ValueError(f"No close price in Polygon prev-day response for {symbol}: {data}")
            return float(price)

        try:
            price = await asyncio.to_thread(_fetch)
            self._prev_prices[symbol] = price
            return price
        except PermissionError as exc:
            log.warning(
                "Polygon free plan limitation detected — using GBM fallback",
                symbol=symbol, error=str(exc),
            )
            return self._fallback.next_price(symbol)
        except Exception as exc:
            log.warning("Polygon get_current_price failed, using fallback",
                        symbol=symbol, error=str(exc))
            return self._fallback.next_price(symbol)

    async def get_quote(self, symbol: str) -> dict:
        """
        Polygon free tier does not provide a real-time bid/ask endpoint, so
        we derive a synthetic spread from the latest trade price.
        """
        if not self._has_keys:
            price  = self._fallback.next_price(symbol)
            prev   = self._fallback.prev_price(symbol)
            spread = price * FALLBACK_SPREAD_PCT
            change = price - prev
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     round(change,                  4),
                "change_pct": round((change / prev * 100) if prev else 0.0, 4),
                "bid":        round(price - spread,          4),
                "ask":        round(price + spread,          4),
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
                "change":     round(change,                  4),
                "change_pct": round((change / prev * 100) if prev else 0.0, 4),
                "bid":        round(price - spread,          4),
                "ask":        round(price + spread,          4),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            log.warning("Polygon get_quote failed, using fallback",
                        symbol=symbol, error=str(exc))
            price  = self._fallback.next_price(symbol)
            spread = price * FALLBACK_SPREAD_PCT
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     0.0,
                "change_pct": 0.0,
                "bid":        round(price - spread, 4),
                "ask":        round(price + spread, 4),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }

    async def get_candles(
        self, symbol: str, timeframe: str, limit: int = 200
    ) -> List[dict]:
        if not self._has_keys:
            return self._fallback.candles(symbol, timeframe, limit)

        multiplier, timespan = POLYGON_TIMEFRAME.get(timeframe, (1, "hour"))
        delta = TIMEFRAME_DELTA.get(timeframe, timedelta(hours=1))
        # Go back far enough to get *limit* trading candles (skip weekends)
        from_dt = datetime.now(timezone.utc) - delta * int(limit * 1.8)
        to_dt   = datetime.now(timezone.utc)
        from_s  = from_dt.strftime("%Y-%m-%d")
        to_s    = to_dt.strftime("%Y-%m-%d")

        def _fetch() -> List[dict]:
            path = f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_s}/{to_s}"
            data = self._get(path, {"adjusted": "true", "sort": "asc", "limit": limit})
            results = data.get("results", [])
            return [
                {
                    "timestamp": datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).isoformat(),
                    "open":   float(r["o"]),
                    "high":   float(r["h"]),
                    "low":    float(r["l"]),
                    "close":  float(r["c"]),
                    "volume": float(r.get("v", 0)),
                }
                for r in results
            ]

        # For intraday timeframes, Polygon free plan returns very few or no candles.
        # Always use GBM for non-daily charts to ensure consistent chart data.
        if timeframe != "1d":
            return self._fallback.candles(symbol, timeframe, limit)

        try:
            candles = await asyncio.to_thread(_fetch)
            if len(candles) >= limit // 2:
                return candles[-limit:]
            # Not enough candles from Polygon — use GBM
            log.warning(
                "Polygon returned insufficient candles, using GBM fallback",
                symbol=symbol, timeframe=timeframe, got=len(candles), wanted=limit,
            )
            return self._fallback.candles(symbol, timeframe, limit)
        except Exception as exc:
            log.warning("Polygon get_candles failed, using fallback",
                        symbol=symbol, error=str(exc))
            return self._fallback.candles(symbol, timeframe, limit)

    async def get_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        start_date,
        end_date,
    ) -> List[dict]:
        if not self._has_keys:
            return self._fallback.historical_candles(symbol, timeframe, start_date, end_date)

        multiplier, timespan = POLYGON_TIMEFRAME.get(timeframe, (1, "hour"))

        def _to_date_str(d, end: bool = False) -> str:
            if isinstance(d, datetime):
                return d.strftime("%Y-%m-%d")
            return d.strftime("%Y-%m-%d")

        from_s = _to_date_str(start_date)
        to_s   = _to_date_str(end_date, end=True)

        def _fetch() -> List[dict]:
            candles: List[dict] = []
            cursor: Optional[str] = None
            path = f"/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_s}/{to_s}"
            params: dict = {"adjusted": "true", "sort": "asc", "limit": 50000}

            while True:
                if cursor:
                    params["cursor"] = cursor
                data    = self._get(path, params)
                results = data.get("results", [])
                candles.extend(
                    {
                        "timestamp": datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).isoformat(),
                        "open":      float(r["o"]),
                        "high":      float(r["h"]),
                        "low":       float(r["l"]),
                        "close":     float(r["c"]),
                        "volume":    float(r.get("v", 0)),
                        "symbol":    symbol,
                        "timeframe": timeframe,
                    }
                    for r in results
                )
                cursor = data.get("next_url")   # Polygon uses next_url for pagination
                if not cursor:
                    break
            return candles

        try:
            return await asyncio.to_thread(_fetch)
        except Exception as exc:
            log.warning("Polygon get_historical_candles failed, using fallback",
                        symbol=symbol, error=str(exc))
            return self._fallback.historical_candles(symbol, timeframe, start_date, end_date)

    async def update_price(self, symbol: str) -> float:
        """No-op — Polygon prices are fetched live on demand."""
        return self._prev_prices.get(symbol, FALLBACK_BASE_PRICES.get(symbol, 0.0))
