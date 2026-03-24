"""
GBM (Geometric Brownian Motion) market data provider.

Generates realistic simulated OHLCV prices — no real API calls.

Supports all symbols exposed in the UI:
  - Stocks / ETFs  (NVDA, AAPL, TSLA …)
  - Forex pairs    (EURUSD, GBPUSD …)
  - Crypto         (BTCUSD, ETHUSD …)
  - Commodities    (XAUUSD, USOIL …)

Used as:
  - Primary provider when MARKET_DATA_PROVIDER=gbm
  - Last-resort fallback inside other providers
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import numpy as np

from .base import MarketDataProvider

# ---------------------------------------------------------------------------
# Full symbol catalogue (mirrors frontend constants.ts ASSET_CLASSES)
# ---------------------------------------------------------------------------

BASE_PRICES: Dict[str, float] = {
    # ── Big Tech ───────────────────────────────────────────────────────────
    "AAPL": 175.0,  "MSFT": 415.0,  "GOOGL": 175.0, "AMZN": 185.0,
    "META": 500.0,  "NVDA": 850.0,  "TSLA":  250.0, "AMD":  165.0,
    "INTC":  30.0,  "NFLX": 700.0,
    # ── Semiconductors ─────────────────────────────────────────────────────
    "AVGO": 900.0,  "QCOM": 160.0,  "TXN":   175.0, "ASML": 800.0,
    "MRVL":  70.0,  "AMAT": 200.0,  "KLAC":  700.0, "MU":   120.0,
    # ── SaaS / Cloud ───────────────────────────────────────────────────────
    "CRM":  290.0,  "ORCL": 125.0,  "ADBE":  480.0, "NOW":  800.0,
    "PANW": 320.0,  "CRWD": 350.0,  "NET":    95.0, "DDOG": 120.0,
    "SNOW": 160.0,  "ZS":   200.0,
    # ── Growth / Popular ───────────────────────────────────────────────────
    "PLTR":  25.0,  "COIN": 200.0,  "SHOP":   85.0, "UBER":  75.0,
    "LYFT":  18.0,  "ABNB": 145.0,  "DASH":  115.0, "RBLX":  40.0,
    "ROKU":  70.0,  "ZM":    65.0,  "HOOD":   18.0, "SOFI":  10.0,
    "SNAP":  12.0,  "TWLO":  65.0,  "SQ":     75.0, "PYPL":  65.0,
    # ── Finance ────────────────────────────────────────────────────────────
    "JPM":  200.0,  "BAC":   40.0,  "GS":    460.0, "MS":    100.0,
    "C":     65.0,  "WFC":   58.0,  "AXP":   230.0, "V":     280.0,
    "MA":   470.0,  "BLK":   800.0, "SCHW":   75.0, "USB":    45.0,
    # ── Healthcare ─────────────────────────────────────────────────────────
    "JNJ":  155.0,  "PFE":   28.0,  "MRK":   125.0, "ABBV": 185.0,
    "UNH":  530.0,  "TMO":   560.0, "DHR":   245.0, "CVS":   60.0,
    "GILD":  85.0,  "ISRG":  400.0, "MRNA":   95.0, "REGN": 1000.0,
    # ── Consumer Staples ───────────────────────────────────────────────────
    "KO":    60.0,  "PEP":  175.0,  "MCD":   290.0, "SBUX":  95.0,
    "WMT":   65.0,  "COST": 850.0,  "PG":    155.0, "CL":    90.0,
    # ── Consumer Discretionary ─────────────────────────────────────────────
    "NKE":   75.0,  "DIS":   90.0,  "HD":    355.0, "LOW":   230.0,
    "TGT":  145.0,  "LULU": 380.0,  "BKNG": 3800.0,
    # ── Industrial ─────────────────────────────────────────────────────────
    "BA":   200.0,  "CAT":  360.0,  "GE":    170.0, "UPS":   135.0,
    "HON":  200.0,  "RTX":   90.0,  "DE":    390.0, "MMM":   105.0,
    "FDX":  260.0,  "LMT":  470.0,
    # ── Energy ─────────────────────────────────────────────────────────────
    "XOM":  115.0,  "CVX":  160.0,  "SLB":    50.0, "COP":   120.0,
    "OXY":   60.0,
    # ── Telecom ────────────────────────────────────────────────────────────
    "T":     20.0,  "VZ":    40.0,  "TMUS":  170.0, "CMCSA":  45.0,
    # ── EV / Automotive ────────────────────────────────────────────────────
    "GM":    45.0,  "F":     12.0,  "RIVN":   15.0, "LCID":    4.0,
    # ── ETFs ───────────────────────────────────────────────────────────────
    "SPY":  500.0,  "QQQ":  430.0,  "DIA":   390.0, "IWM":   200.0,
    # ── Forex (no-slash notation used by the UI) ───────────────────────────
    "EURUSD": 1.0850, "GBPUSD": 1.2650, "USDJPY": 151.50,
    "AUDUSD": 0.6550, "USDCAD": 1.3600, "USDCHF": 0.9050,
    "NZDUSD": 0.6050, "EURGBP": 0.8580,
    # ── Crypto ─────────────────────────────────────────────────────────────
    "BTCUSD":  67500.0, "ETHUSD":  3500.0, "BNBUSD":   580.0,
    "SOLUSD":   185.0,  "ADAUSD":    0.60, "XRPUSD":    0.65,
    # ── Commodities ────────────────────────────────────────────────────────
    "XAUUSD": 2300.0, "XAGUSD":  27.0, "USOIL":  75.0,
    "UKOIL":   78.0,  "NATURALGAS": 2.5,
}

VOLATILITY: Dict[str, float] = {
    # Stocks — roughly proportional to beta
    "AAPL": 0.012,  "MSFT": 0.010,  "GOOGL": 0.013, "AMZN": 0.014,
    "META": 0.018,  "NVDA": 0.025,  "TSLA":  0.030, "AMD":  0.028,
    "INTC": 0.018,  "NFLX": 0.022,
    "AVGO": 0.018,  "QCOM": 0.020,  "TXN":   0.015, "ASML": 0.020,
    "MRVL": 0.025,  "AMAT": 0.022,  "KLAC":  0.022, "MU":   0.028,
    "CRM":  0.018,  "ORCL": 0.015,  "ADBE":  0.018, "NOW":  0.020,
    "PANW": 0.022,  "CRWD": 0.025,  "NET":   0.030, "DDOG": 0.028,
    "SNOW": 0.030,  "ZS":   0.025,
    "PLTR": 0.035,  "COIN": 0.040,  "SHOP":  0.025, "UBER": 0.020,
    "LYFT": 0.030,  "ABNB": 0.025,  "DASH":  0.028, "RBLX": 0.035,
    "ROKU": 0.032,  "ZM":   0.028,  "HOOD":  0.040, "SOFI": 0.038,
    "SNAP": 0.038,  "TWLO": 0.030,  "SQ":    0.028, "PYPL": 0.022,
    "JPM":  0.012,  "BAC":  0.014,  "GS":    0.016, "MS":   0.015,
    "C":    0.015,  "WFC":  0.014,  "AXP":   0.014, "V":    0.012,
    "MA":   0.013,  "BLK":  0.015,  "SCHW":  0.016, "USB":  0.014,
    "JNJ":  0.010,  "PFE":  0.015,  "MRK":   0.012, "ABBV": 0.015,
    "UNH":  0.013,  "TMO":  0.015,  "DHR":   0.014, "CVS":  0.015,
    "GILD": 0.016,  "ISRG": 0.018,  "MRNA":  0.035, "REGN": 0.020,
    "KO":   0.010,  "PEP":  0.010,  "MCD":   0.012, "SBUX": 0.015,
    "WMT":  0.010,  "COST": 0.013,  "PG":    0.010, "CL":   0.010,
    "NKE":  0.014,  "DIS":  0.016,  "HD":    0.013, "LOW":  0.014,
    "TGT":  0.018,  "LULU": 0.022,  "BKNG":  0.018,
    "BA":   0.020,  "CAT":  0.016,  "GE":    0.018, "UPS":  0.014,
    "HON":  0.013,  "RTX":  0.014,  "DE":    0.016, "MMM":  0.014,
    "FDX":  0.018,  "LMT":  0.012,
    "XOM":  0.015,  "CVX":  0.015,  "SLB":   0.022, "COP":  0.018,
    "OXY":  0.022,
    "T":    0.012,  "VZ":   0.012,  "TMUS":  0.015, "CMCSA": 0.014,
    "GM":   0.018,  "F":    0.020,  "RIVN":  0.045, "LCID": 0.055,
    "SPY":  0.008,  "QQQ":  0.010,  "DIA":   0.008, "IWM":  0.012,
    # Forex
    "EURUSD": 0.0003, "GBPUSD": 0.0004, "USDJPY": 0.0003,
    "AUDUSD": 0.0005, "USDCAD": 0.0004, "USDCHF": 0.0004,
    "NZDUSD": 0.0005, "EURGBP": 0.0003,
    # Crypto
    "BTCUSD": 0.008,  "ETHUSD": 0.010, "BNBUSD": 0.012,
    "SOLUSD": 0.015,  "ADAUSD": 0.018, "XRPUSD": 0.018,
    # Commodities
    "XAUUSD": 0.006, "XAGUSD": 0.010, "USOIL":  0.015,
    "UKOIL":  0.015, "NATURALGAS": 0.025,
}

SPREADS: Dict[str, float] = {
    # Forex (tight)
    "EURUSD": 0.00002, "GBPUSD": 0.00003, "USDJPY": 0.02,
    "AUDUSD": 0.00003, "USDCAD": 0.00003, "USDCHF": 0.00003,
    "NZDUSD": 0.00004, "EURGBP": 0.00003,
    # Crypto (wider)
    "BTCUSD": 10.0,  "ETHUSD": 1.5,   "BNBUSD": 0.3,
    "SOLUSD": 0.05,  "ADAUSD": 0.001, "XRPUSD": 0.001,
    # Commodities
    "XAUUSD": 0.30,  "XAGUSD": 0.02,  "USOIL": 0.03,
    "UKOIL":  0.03,  "NATURALGAS": 0.005,
}

TIMEFRAME_MINUTES: Dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "4h": 240, "1d": 1440,
}


def _decimals(symbol: str) -> int:
    """Return appropriate decimal precision for a symbol."""
    if symbol in ("BTCUSD",):
        return 2
    if symbol in ("ETHUSD", "BNBUSD", "XAUUSD"):
        return 2
    if symbol in ("USDJPY",):
        return 3
    if symbol in ("SOLUSD",):
        return 3
    if symbol in ("NATURALGAS", "USOIL", "UKOIL"):
        return 3
    if symbol in ("ADAUSD", "XRPUSD"):
        return 5
    if symbol in ("EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", "EURGBP"):
        return 5
    # Default: stocks / ETFs use 2 decimal places
    return 2


class GBMMarketDataProvider(MarketDataProvider):
    """
    Geometric Brownian Motion provider.

    Supports all symbols in the full catalogue above.
    Never raises for unknown symbols — falls back to a sane default ($100, 1.5% vol).
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = dict(BASE_PRICES)
        self._prev_prices: Dict[str, float] = dict(BASE_PRICES)
        # Pre-seed with a real GBM walk so EMA/RSI have meaningful variance
        # from the first cycle instead of a flat line.
        self._price_history: Dict[str, List[float]] = {}
        for s, p in BASE_PRICES.items():
            vol = VOLATILITY.get(s, 0.015)
            dec = _decimals(s)
            rng = random.Random(hash(s) & 0x7FFFFFFF)
            price = p
            hist: List[float] = []
            for _ in range(500):
                shock = rng.gauss(0, 1)
                price = round(price * math.exp(-0.5 * vol ** 2 + vol * shock), dec)
            hist_price = price
            for _ in range(500):
                shock = rng.gauss(0, 1)
                hist_price = round(hist_price * math.exp(-0.5 * vol ** 2 + vol * shock), dec)
                hist.append(hist_price)
            self._price_history[s] = hist
            self._prices[s] = hist[-1]
            self._prev_prices[s] = hist[-2] if len(hist) >= 2 else p

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _vol(self, symbol: str) -> float:
        return VOLATILITY.get(symbol, 0.015)

    def _base(self, symbol: str) -> float:
        return BASE_PRICES.get(symbol, 100.0)

    @staticmethod
    def _gbm_step(price: float, vol: float, drift: float = 0.0) -> float:
        shock = random.gauss(0, 1)
        return price * math.exp((drift - 0.5 * vol ** 2) + vol * shock)

    @staticmethod
    def _gbm_step_rng(
        price: float, vol: float, rng: random.Random, drift: float = 0.0
    ) -> float:
        shock = rng.gauss(0, 1)
        return price * math.exp((drift - 0.5 * vol ** 2) + vol * shock)

    # ------------------------------------------------------------------
    # MarketDataProvider interface
    # ------------------------------------------------------------------

    def get_all_symbols(self) -> List[str]:
        return list(BASE_PRICES.keys())

    async def get_current_price(self, symbol: str) -> float:
        price = self._prices.get(symbol, self._base(symbol))
        vol = self._vol(symbol)
        new_price = self._gbm_step(price, vol)
        dec = _decimals(symbol)
        new_price = round(new_price, dec)
        self._prev_prices[symbol] = price
        self._prices[symbol] = new_price
        history = self._price_history.setdefault(symbol, [self._base(symbol)] * 250)
        history.append(new_price)
        if len(history) > 1000:
            self._price_history[symbol] = history[-1000:]
        return new_price

    async def get_quote(self, symbol: str) -> dict:
        price = await self.get_current_price(symbol)
        prev = self._prev_prices.get(symbol, self._base(symbol))
        change = price - prev
        change_pct = (change / prev) * 100 if prev else 0.0
        spread = SPREADS.get(symbol, price * 0.0002)
        dec = _decimals(symbol)
        return {
            "symbol": symbol,
            "price": price,
            "change": round(change, dec),
            "change_pct": round(change_pct, 4),
            "bid": round(price - spread / 2, dec),
            "ask": round(price + spread / 2, dec),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def get_candles(
        self, symbol: str, timeframe: str, limit: int = 200
    ) -> List[dict]:
        tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 60)
        now = datetime.now(timezone.utc)
        dec = _decimals(symbol)
        vol = self._vol(symbol)
        base = self._base(symbol)

        # Use the rolling price history (updated each cycle by get_current_price)
        # so candles change every cycle instead of being re-seeded each call.
        hist = self._price_history.get(symbol, [base])
        # +1 so we have an open price for the first candle
        prices = hist[-(limit + 1):]
        n = len(prices)

        candles: List[dict] = []
        for i in range(1, n):
            close_p = prices[i]
            open_p  = prices[i - 1]
            spread  = abs(close_p - open_p)
            high_p  = round(max(open_p, close_p) + spread * vol * 2, dec)
            low_p   = round(max(min(open_p, close_p) - spread * vol * 2, base * 0.01), dec)
            ts = now - timedelta(minutes=tf_minutes * (n - 1 - i))
            candles.append({
                "timestamp": ts.isoformat(),
                "open":   round(open_p,  dec),
                "high":   high_p,
                "low":    low_p,
                "close":  round(close_p, dec),
                "volume": round(1000.0 * base / max(base, 1.0), 2),
            })
        return candles[-limit:]

    async def get_historical_candles(
        self,
        symbol: str,
        timeframe: str,
        start_date,
        end_date,
    ) -> List[dict]:
        tf_minutes = TIMEFRAME_MINUTES.get(timeframe, 60)
        dec = _decimals(symbol)
        base = self._base(symbol)
        vol = self._vol(symbol)
        seed = hash(symbol + timeframe) & 0x7FFFFFFF
        rng = random.Random(seed)
        np_rng = np.random.default_rng(seed)  # noqa: F841 (kept for reproducibility)

        def _to_dt(d, end: bool = False) -> datetime:
            if isinstance(d, datetime):
                return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
            h, m = (23, 59) if end else (0, 0)
            return datetime(d.year, d.month, d.day, h, m, tzinfo=timezone.utc)

        start_dt = _to_dt(start_date)
        end_dt = _to_dt(end_date, end=True)

        warmup_count = 400
        warmup_start = start_dt - timedelta(minutes=tf_minutes * warmup_count)
        total_minutes = int((end_dt - warmup_start).total_seconds() / 60)
        total_candles = max(warmup_count + 100, total_minutes // max(tf_minutes, 1) + 1)

        price = base * rng.uniform(0.92, 1.08)
        candles: List[dict] = []
        current_ts = warmup_start

        for _ in range(total_candles):
            if current_ts > end_dt:
                break
            open_p = price
            t1 = self._gbm_step_rng(open_p, vol, rng)
            t2 = self._gbm_step_rng(t1, vol, rng)
            t3 = self._gbm_step_rng(t2, vol, rng)
            t4 = self._gbm_step_rng(t3, vol, rng)
            ticks = [t1, t2, t3, t4]
            high_p = max(open_p, *ticks)
            low_p = min(open_p, *ticks)
            close_p = ticks[-1]
            # Light mean-reversion to keep price realistic
            close_p = close_p * (1 + 0.0001 * (base - close_p) / base)
            close_p = max(close_p, base * 0.3)
            candles.append({
                "timestamp": current_ts.isoformat(),
                "open":      round(open_p,  dec),
                "high":      round(high_p,  dec),
                "low":       round(low_p,   dec),
                "close":     round(close_p, dec),
                "volume":    round(rng.uniform(200, 8000) * 1000 + 500, 2),
                "symbol":    symbol,
                "timeframe": timeframe,
            })
            price = close_p
            current_ts += timedelta(minutes=tf_minutes)

        return candles

    async def update_price(self, symbol: str) -> float:
        """Advance the GBM walk for *symbol*. Called by the scheduler."""
        return await self.get_current_price(symbol)
