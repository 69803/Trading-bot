"""
KuCoin market data provider — Crypto spot.

Replaces BinanceProvider to avoid HTTP 451 geo-blocks on Render.
KuCoin public REST API works globally without an API key.

Endpoints used (all public — no API key required):
  Base: https://api.kucoin.com

  Latest price:
    GET /api/v1/market/orderbook/level1?symbol={symbol}
    → {"code":"200000","data":{"price":"2534.82","bestBid":"...","bestAsk":"...",...}}

  24h ticker (for get_quote):
    GET /api/v1/market/stats?symbol={symbol}
    → {"code":"200000","data":{"last","changeRate","changePrice","buy","sell",...}}
    Note: changeRate is a decimal fraction (0.0123 = 1.23%).

  Candles:
    GET /api/v1/market/candles?type={interval}&symbol={symbol}&startAt={ts}&endAt={ts}
    → {"code":"200000","data":[[time_sec, open, close, high, low, volume, turnover],...]}
    Note: returned newest-first; reversed internally to oldest-first.
    KuCoin column order: [0]=time [1]=open [2]=close [3]=high [4]=low [5]=volume

Rate limits (public endpoints):
  ~30 requests / 10 s — no daily cap.
  Max 1 500 candles per request.

Symbol mapping (internal — transparent to the rest of the system):
  ETHUSDT  → ETH-USDT
  BTCUSDT  → BTC-USDT
  BNBUSDT  → BNB-USDT
  SOLUSDT  → SOL-USDT
  ADAUSDT  → ADA-USDT
  XRPUSDT  → XRP-USDT
  DOGEUSDT → DOGE-USDT
  AVAXUSDT → AVAX-USDT
  DOTUSDT  → DOT-USDT
  MATICUSDT→ MATIC-USDT

  Rule: strip trailing "USDT", insert dash → "<BASE>-USDT"
  Reverse: drop the dash → back to the canonical symbol used by the bot.
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

BASE_URL = "https://api.kucoin.com"
TIMEOUT  = 10.0

CRYPTO_SYMBOLS: List[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "AVAXUSDT",
    "DOTUSDT",
    "MATICUSDT",
]

# KuCoin interval strings
KUCOIN_INTERVAL: Dict[str, str] = {
    "1m":  "1min",
    "5m":  "5min",
    "15m": "15min",
    "30m": "30min",
    "1h":  "1hour",
    "4h":  "4hour",
    "1d":  "1day",
}

# Seconds per interval — used to compute startAt for candle requests
_INTERVAL_SECONDS: Dict[str, int] = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "30m": 1_800,
    "1h":  3_600,
    "4h":  14_400,
    "1d":  86_400,
}


# ---------------------------------------------------------------------------
# Symbol helpers
# ---------------------------------------------------------------------------

def _to_kucoin_symbol(symbol: str) -> str:
    """ETHUSDT → ETH-USDT"""
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4] + "-USDT"
    return s  # pass through unchanged if unexpected format


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class KuCoinProvider(MarketDataProvider):
    """
    Fetches real crypto spot data from KuCoin public REST API.

    No API key is required for market data endpoints.
    No GBM fallback — raises on failure so callers can fail-closed.

    All external calls run in asyncio.to_thread — event loop never blocked.
    """

    # Price snapshot TTL: slightly shorter than the 60 s bot cycle so all
    # calls within one cycle share the same live price (SL/TP detection).
    _SNAPSHOT_TTL = 55.0

    # Candle snapshot TTL: 1 h candles only change once per hour.
    _CANDLE_SNAPSHOT_TTL = 3600.0

    def __init__(self) -> None:
        self._prev_prices: Dict[str, float] = {}

        # Cycle-level snapshot caches: { symbol: (value, monotonic_ts) }
        self._price_snapshot:  Dict[str, Tuple[float, float]] = {}
        self._candle_snapshot: Dict[Tuple[str, str, int], Tuple[List[dict], float]] = {}

        log.info("KuCoinProvider ready", symbols_count=len(CRYPTO_SYMBOLS))

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[dict] = None) -> object:
        url = f"{BASE_URL}{path}"
        with httpx.Client(timeout=TIMEOUT) as client:
            resp = client.get(url, params=params or {})
        if resp.status_code == 429:
            raise RuntimeError("KuCoin: 429 rate limit hit")
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != "200000":
            raise ValueError(f"KuCoin error {data.get('code')}: {data.get('msg', 'unknown')}")
        return data["data"]

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

        kucoin_sym = _to_kucoin_symbol(symbol)

        def _fetch() -> float:
            data = self._get("/api/v1/market/orderbook/level1", {"symbol": kucoin_sym})
            return float(data["price"])

        try:
            price = await asyncio.to_thread(_fetch)
            self._prev_prices[symbol] = price
            log.info("MARKET DATA FETCH", source="KUCOIN_REAL",
                     symbol=symbol, price=round(price, 4))
        except Exception as exc:
            log.error(
                "MARKET DATA UNAVAILABLE — KuCoin price fetch failed",
                symbol=symbol, error=str(exc),
            )
            raise

        self._price_snapshot[symbol] = (price, now_mono)
        return price

    async def get_quote(self, symbol: str) -> dict:
        kucoin_sym = _to_kucoin_symbol(symbol)

        def _fetch() -> dict:
            return self._get("/api/v1/market/stats", {"symbol": kucoin_sym})

        try:
            data  = await asyncio.to_thread(_fetch)
            price = float(data["last"])
            # changeRate is a decimal fraction (e.g. 0.0123 = 1.23 %)
            change_pct = round(float(data["changeRate"]) * 100, 4)
            change     = round(float(data["changePrice"]), 4)
            self._prev_prices[symbol] = price
            return {
                "symbol":     symbol,
                "price":      price,
                "change":     change,
                "change_pct": change_pct,
                "bid":        round(float(data["buy"]),  4),
                "ask":        round(float(data["sell"]), 4),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }
        except Exception as exc:
            log.error("KuCoin get_quote failed", symbol=symbol, error=str(exc))
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

        kucoin_sym = _to_kucoin_symbol(symbol)
        interval   = KUCOIN_INTERVAL.get(timeframe, "1hour")
        bar_sec    = _INTERVAL_SECONDS.get(timeframe, 3600)
        clamped    = min(limit, 1500)  # KuCoin max per request
        now_ts     = int(time.time())
        start_ts   = now_ts - clamped * bar_sec

        def _fetch() -> List[dict]:
            rows = self._get("/api/v1/market/candles", {
                "symbol":  kucoin_sym,
                "type":    interval,
                "startAt": start_ts,
                "endAt":   now_ts,
            })
            # KuCoin returns newest-first; reverse to oldest-first
            # KuCoin column order: [time_sec, open, close, high, low, volume, turnover]
            return [
                {
                    "timestamp": datetime.fromtimestamp(
                        int(row[0]), tz=timezone.utc
                    ).isoformat(),
                    "open":   float(row[1]),
                    "high":   float(row[3]),
                    "low":    float(row[4]),
                    "close":  float(row[2]),
                    "volume": float(row[5]),
                }
                for row in reversed(rows)
            ]

        try:
            candles = await asyncio.to_thread(_fetch)
            log.info("MARKET DATA CANDLES", source="KUCOIN_REAL",
                     symbol=symbol, timeframe=timeframe, count=len(candles))
        except Exception as exc:
            log.error(
                "MARKET DATA UNAVAILABLE — KuCoin candles fetch failed",
                symbol=symbol, error=str(exc),
            )
            raise

        self._candle_snapshot[snap_key] = (candles, now_mono)
        return candles

    async def get_historical_candles(
        self, symbol: str, timeframe: str, start_date, end_date
    ) -> List[dict]:
        kucoin_sym = _to_kucoin_symbol(symbol)
        interval   = KUCOIN_INTERVAL.get(timeframe, "1day")

        def _to_sec(d) -> int:
            if isinstance(d, datetime):
                dt = d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            else:
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            return int(dt.timestamp())

        def _fetch() -> List[dict]:
            rows = self._get("/api/v1/market/candles", {
                "symbol":  kucoin_sym,
                "type":    interval,
                "startAt": _to_sec(start_date),
                "endAt":   _to_sec(end_date),
            })
            # KuCoin returns newest-first; reverse to oldest-first
            return [
                {
                    "timestamp": datetime.fromtimestamp(
                        int(row[0]), tz=timezone.utc
                    ).isoformat(),
                    "open":      float(row[1]),
                    "high":      float(row[3]),
                    "low":       float(row[4]),
                    "close":     float(row[2]),
                    "volume":    float(row[5]),
                    "symbol":    symbol,
                    "timeframe": timeframe,
                }
                for row in reversed(rows)
            ]

        try:
            return await asyncio.to_thread(_fetch)
        except Exception as exc:
            log.error(
                "KuCoin get_historical_candles failed",
                symbol=symbol, error=str(exc),
            )
            raise

    async def update_price(self, symbol: str) -> float:
        """No-op for live providers — prices are fetched on demand."""
        return self._prev_prices.get(symbol, 0.0)
