"""
Binance market data provider — Crypto spot.

Endpoints used (all public — no API key required):
  Base: https://api.binance.com

  Latest price:
    GET /api/v3/ticker/price?symbol={symbol}
    → {"symbol": "ETHUSDT", "price": "2534.82000000"}

  24h ticker (for get_quote):
    GET /api/v3/ticker/24hr?symbol={symbol}
    → {bidPrice, askPrice, priceChange, priceChangePercent, ...}

  Klines (candles):
    GET /api/v3/klines?symbol={symbol}&interval={interval}&limit={n}
    → [[open_time_ms, open, high, low, close, volume, ...], ...]
    Note: returned oldest-first by default.

Rate limits (public endpoints):
  1200 requests/minute — no daily credit cap.
  Weight per endpoint: 1 (price), 1 (klines per symbol).

Symbols supported (add more to CRYPTO_SYMBOLS as needed):
  ETHUSDT, BTCUSDT, BNBUSDT, SOLUSDT, ADAUSDT, XRPUSDT,
  DOGEUSDT, AVAXUSDT, DOTUSDT, MATICUSDT

No GBM fallback — if the API is unavailable the provider raises.
Callers (bot_service._evaluate_open_positions) already handle this
with fail-closed logic: skip SL/TP evaluation for the cycle.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from app.core.logger import get_logger
from .base import MarketDataProvider

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.binance.com"
TIMEOUT  = 10.0

CRYPTO_SYMBOLS: List[str] = [
    "ETHUSDT",
    "BTCUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "MATICUSDT",
]

# Binance interval strings — bot always sends "1h"; map covers common cases.
BINANCE_INTERVAL: Dict[str, str] = {
    "1m":  "1m",
    "5m":  "5m",
    "15m": "15m",
    "30m": "30m",
    "1h":  "1h",
    "4h":  "4h",
    "1d":  "1d",
}


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class BinanceProvider(MarketDataProvider):
    """
    Fetches real crypto spot data from Binance public REST API.

    No API key is required for market data endpoints.
    No GBM fallback — raises on failure so callers can fail-closed.

    All external calls run in asyncio.to_thread — event loop never blocked.
    """

    # Price snapshot TTL: slightly shorter than the 60 s bot cycle so all
    # calls within one cycle share the same live price (SL/TP detection).
    _SNAPSHOT_TTL = 55.0

    # Candle snapshot TTL: 1h candles only change once per hour.
    # Caps API usage at ~24 candle calls/day per symbol.
    _CANDLE_SNAPSHOT_TTL = 3600.0

    def __init__(self) -> None:
        self._prev_prices: Dict[str, float] = {}

        # Cycle-level snapshot caches: { symbol: (value, monotonic_ts) }
        self._price_snapshot:  Dict[str, Tuple[float, float]] = {}
        self._candle_snapshot: Dict[Tuple[str, str, int], Tuple[List[dict], float]] = {}

        log.info("BinanceProvider ready", symbols_count=len(CRYPTO_SYMBOLS))

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[dict] = None) -> object:
        url = f"{BASE_URL}{path}"
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.get(url, params=params or {})
        if resp.status_code == 429:
            raise RuntimeError("Binance: 429 rate limit hit")
        if resp.status_code == 418:
            raise RuntimeError("Binance: 418 IP banned (too many 429s)")
        resp.raise_for_status()
        data = resp.json()
        # Binance returns {"code": -XXXX, "msg": "..."} for API errors
        if isinstance(data, dict) and "code" in data and int(data["code"]) < 0:
            raise ValueError(f"Binance error {data['code']}: {data.get('msg', 'unknown')}")
        return data

    # ------------------------------------------------------------------
    # MarketDataProvider interface
    # ------------------------------------------------------------------

    def get_all_symbols(self) -> List[str]:
        return list(CRYPTO_SYMBOLS)

    async def get_current_price(self, symbol: str) -> float:
        # ── Cycle-level snapshot ──────────────────────────────────────────
        now_mono = time.monotonic()
        cached = self._price_snapshot.get(symbol)
        if cached is not None:
            price, ts = cached
            if now_mono - ts < self._SNAPSHOT_TTL:
                log.debug("MARKET DATA FETCH", source="CYCLE_SNAPSHOT",
                          symbol=symbol, price=round(price, 6))
                return price

        def _fetch() -> float:
            data = self._get("/api/v3/ticker/price", {"symbol": symbol})
            return float(data["price"])

        try:
            price = await asyncio.to_thread(_fetch)
            self._prev_prices[symbol] = price
            log.info("MARKET DATA FETCH", source="BINANCE_REAL",
                     symbol=symbol, price=round(price, 4))
        except Exception as exc:
            log.error(
                "MARKET DATA UNAVAILABLE — Binance price fetch failed",
                symbol=symbol, error=str(exc),
            )
            raise

        self._price_snapshot[symbol] = (price, now_mono)
        return price

    async def get_quote(self, symbol: str) -> dict:
        def _fetch() -> dict:
            return self._get("/api/v3/ticker/24hr", {"symbol": symbol})

        try:
            data  = await asyncio.to_thread(_fetch)
            price = float(data["lastPrice"])
            prev  = self._prev_prices.get(symbol, price)
            self._prev_prices[symbol] = price
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     round(float(data["priceChange"]),        4),
                "change_pct": round(float(data["priceChangePercent"]), 4),
                "bid":        round(float(data["bidPrice"]),           4),
                "ask":        round(float(data["askPrice"]),           4),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            log.error("Binance get_quote failed", symbol=symbol, error=str(exc))
            raise

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 200) -> List[dict]:
        # ── Cycle-level snapshot ──────────────────────────────────────────
        now_mono = time.monotonic()
        snap_key = (symbol, timeframe, limit)
        cached_c = self._candle_snapshot.get(snap_key)
        if cached_c is not None:
            candles_c, ts_c = cached_c
            if now_mono - ts_c < self._CANDLE_SNAPSHOT_TTL:
                log.debug("MARKET DATA CANDLES", source="CYCLE_SNAPSHOT",
                          symbol=symbol, timeframe=timeframe, limit=limit)
                return candles_c

        interval = BINANCE_INTERVAL.get(timeframe, "1h")

        def _fetch() -> List[dict]:
            rows = self._get("/api/v3/klines", {
                "symbol":   symbol,
                "interval": interval,
                "limit":    min(limit, 1000),  # Binance max per request: 1000
            })
            return [
                {
                    "timestamp": datetime.fromtimestamp(
                        row[0] / 1000, tz=timezone.utc
                    ).isoformat(),
                    "open":   float(row[1]),
                    "high":   float(row[2]),
                    "low":    float(row[3]),
                    "close":  float(row[4]),
                    "volume": float(row[5]),
                }
                for row in rows
            ]

        try:
            candles = await asyncio.to_thread(_fetch)
            log.info("MARKET DATA CANDLES", source="BINANCE_REAL",
                     symbol=symbol, timeframe=timeframe, count=len(candles))
        except Exception as exc:
            log.error(
                "MARKET DATA UNAVAILABLE — Binance candles fetch failed",
                symbol=symbol, error=str(exc),
            )
            raise

        self._candle_snapshot[snap_key] = (candles, now_mono)
        return candles

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_date, end_date
    ) -> List[dict]:
        interval = BINANCE_INTERVAL.get(timeframe, "1d")

        def _to_ms(d) -> int:
            if isinstance(d, datetime):
                dt = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            else:
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)

        def _fetch() -> List[dict]:
            # Binance returns max 1000 candles per request; for historical
            # ranges that fit, this is sufficient.
            rows = self._get("/api/v3/klines", {
                "symbol":    symbol,
                "interval":  interval,
                "startTime": _to_ms(start_date),
                "endTime":   _to_ms(end_date),
                "limit":     1000,
            })
            return [
                {
                    "timestamp": datetime.fromtimestamp(
                        row[0] / 1000, tz=timezone.utc
                    ).isoformat(),
                    "open":      float(row[1]),
                    "high":      float(row[2]),
                    "low":       float(row[3]),
                    "close":     float(row[4]),
                    "volume":    float(row[5]),
                    "symbol":    symbol,
                    "timeframe": timeframe,
                }
                for row in rows
            ]

        try:
            return await asyncio.to_thread(_fetch)
        except Exception as exc:
            log.error(
                "Binance get_historical_candles failed",
                symbol=symbol, error=str(exc),
            )
            raise

    async def update_price(self, symbol: str) -> float:
        """No-op for live providers — prices are fetched on demand."""
        return self._prev_prices.get(symbol, 0.0)
