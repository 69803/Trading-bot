"""
TrendMaster Strategy Engine
===========================
Trend Following • Forex • Scalping (5m)

Entry Conditions (ALL six must be TRUE simultaneously):

LONG:
  1. close > EMA50          — price is above the trend filter
  2. EMA9 crossed above EMA21 in the last 2 candles  — fresh bullish crossover
  3. MACD histogram > 0  AND  MACD line > signal line — momentum confirmed
  4. 45 ≤ RSI ≤ 75          — ideal entry zone, not over-extended
  5. ATR within normal range — no news spike (current ATR < 2× recent avg)
  6. Current candle bullish  — close > open

SHORT:
  1. close < EMA50
  2. EMA9 crossed below EMA21 in the last 2 candles
  3. MACD histogram < 0  AND  MACD line < signal line
  4. 25 ≤ RSI ≤ 55
  5. ATR within normal range
  6. Current candle bearish  — close < open

Risk sizing (applied by risk_manager):
  SL = entry ± ATR × 1.5
  TP = entry ∓ ATR × 3.0   → ratio 1:2
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional

from app.core.logger import get_logger
from app.schemas.technical import IndicatorValues, TechnicalSignal
from app.utils.indicators import (
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_atr,
)

log = get_logger(__name__)

# ── Strategy constants ────────────────────────────────────────────────────────
EMA_FAST   = 9
EMA_SLOW   = 21
EMA_FILTER = 50      # price must be on this side to qualify
RSI_PERIOD = 14
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
ATR_PERIOD = 14

LONG_RSI_MIN,  LONG_RSI_MAX  = 45.0, 75.0
SHORT_RSI_MIN, SHORT_RSI_MAX = 25.0, 55.0

# ATR spike gate: skip if current ATR > this multiple of its recent average
ATR_SPIKE_MULT = 2.0

# Crossover look-back: how many candles back to search for a valid crossover
CROSSOVER_LOOKBACK = 3   # "in the last 2 velas" → check candles[-3:]

# ATR multipliers for SL/TP — exported so risk_manager can use them
ATR_SL_MULT = 1.5
ATR_TP_MULT = 3.0

# Minimum candles required
MIN_CANDLES = 80    # EMA50 needs 50 seed + warm-up buffer


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "5m",
) -> TechnicalSignal:
    """
    Run the TrendMaster strategy on the given OHLCV candles.

    Args:
        symbol:   Trading pair, e.g. "EUR/USD".
        candles:  List of OHLCV dicts (oldest → newest).
        timeframe: Label for logging ("5m", "1m", …).

    Returns:
        TechnicalSignal with direction BUY / SELL / HOLD,
        confidence 0–100, and a reasons list.
    """
    now = datetime.now(timezone.utc)
    n   = len(candles)

    if n < MIN_CANDLES:
        reason = f"SKIPPED [insufficient_data]: need {MIN_CANDLES} candles, got {n}"
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    closes = [float(c["close"]) for c in candles]
    opens  = [float(c["open"])  for c in candles]
    highs  = [float(c["high"])  for c in candles]
    lows   = [float(c["low"])   for c in candles]

    # ── Indicators ────────────────────────────────────────────────────────────
    ema9_s  = calculate_ema(closes, EMA_FAST)
    ema21_s = calculate_ema(closes, EMA_SLOW)
    ema50_s = calculate_ema(closes, EMA_FILTER)
    rsi_s   = calculate_rsi(closes, RSI_PERIOD)
    macd_line, macd_sig_line, macd_hist = calculate_macd(
        closes, MACD_FAST, MACD_SLOW, MACD_SIG
    )
    atr_s   = calculate_atr(highs, lows, closes, ATR_PERIOD)

    # ── Current values ────────────────────────────────────────────────────────
    price    = closes[-1]
    open_cur = opens[-1]
    ema9     = _last(ema9_s)
    ema21    = _last(ema21_s)
    ema50    = _last(ema50_s)
    rsi      = _last(rsi_s)
    macd_v   = _last(macd_line)
    macd_sv  = _last(macd_sig_line)
    macd_hv  = _last(macd_hist)
    atr      = _last(atr_s)

    # Guard: NaN in any core indicator
    core = {"ema9": ema9, "ema21": ema21, "ema50": ema50,
            "rsi": rsi, "macd": macd_v, "macd_hist": macd_hv}
    bad  = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol, candles=n)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── ATR spike check ───────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_spike  = (atr is not None and atr > atr_avg * ATR_SPIKE_MULT)

    # ── EMA9/21 crossover in last CROSSOVER_LOOKBACK candles ─────────────────
    bull_cross  = _crossed_up(ema9_s,  ema21_s, CROSSOVER_LOOKBACK)
    bear_cross  = _crossed_down(ema9_s, ema21_s, CROSSOVER_LOOKBACK)

    # ── Candle direction ──────────────────────────────────────────────────────
    candle_bull = price > open_cur
    candle_bear = price < open_cur

    # ── Log snapshot ──────────────────────────────────────────────────────────
    log.info(
        "TRENDMASTER SNAPSHOT",
        symbol=symbol, candles=n,
        price=round(price, 5),
        ema9=round(ema9, 5), ema21=round(ema21, 5), ema50=round(ema50, 5),
        rsi=round(rsi, 2),
        macd_hist=round(macd_hv, 6), macd_line=round(macd_v, 6), macd_sig=round(macd_sv, 6),
        atr=round(atr, 6), atr_avg=round(atr_avg, 6), atr_spike=atr_spike,
        bull_cross=bull_cross, bear_cross=bear_cross,
        candle_bull=candle_bull, candle_bear=candle_bear,
    )

    # ── LONG conditions ───────────────────────────────────────────────────────
    long_conds = {
        "price_above_ema50":  price > ema50,
        "ema9_21_bull_cross": bull_cross,
        "macd_hist_positive": macd_hv > 0,
        "macd_line_above_sig": macd_v > macd_sv,
        "rsi_in_zone":        LONG_RSI_MIN <= rsi <= LONG_RSI_MAX,
        "atr_normal":         not atr_spike,
        "candle_bullish":     candle_bull,
    }
    long_ok = all(long_conds.values())

    # ── SHORT conditions ──────────────────────────────────────────────────────
    short_conds = {
        "price_below_ema50":  price < ema50,
        "ema9_21_bear_cross": bear_cross,
        "macd_hist_negative": macd_hv < 0,
        "macd_line_below_sig": macd_v < macd_sv,
        "rsi_in_zone":        SHORT_RSI_MIN <= rsi <= SHORT_RSI_MAX,
        "atr_normal":         not atr_spike,
        "candle_bearish":     candle_bear,
    }
    short_ok = all(short_conds.values())

    # ── Log which conditions passed/failed ────────────────────────────────────
    log.info(
        "TRENDMASTER CONDITIONS",
        symbol=symbol,
        long=long_conds,
        short=short_conds,
        long_ok=long_ok,
        short_ok=short_ok,
    )

    # ── Build reasons list ────────────────────────────────────────────────────
    reasons: List[str] = []

    if long_ok:
        direction  = "BUY"
        confidence = _confidence(long_conds, rsi, macd_hv)
        reasons = [
            f"TrendMaster LONG: EMA9({ema9:.5f}) > EMA21({ema21:.5f}) crossover ✓",
            f"Price({price:.5f}) > EMA50({ema50:.5f}) ✓",
            f"MACD hist +{macd_hv:.6f} ✓",
            f"RSI {rsi:.1f} in [{LONG_RSI_MIN}–{LONG_RSI_MAX}] ✓",
            f"Candle bullish ✓  ATR normal ✓",
            f"SL=ATR×{ATR_SL_MULT}  TP=ATR×{ATR_TP_MULT}",
        ]
    elif short_ok:
        direction  = "SELL"
        confidence = _confidence(short_conds, rsi, macd_hv)
        reasons = [
            f"TrendMaster SHORT: EMA9({ema9:.5f}) < EMA21({ema21:.5f}) crossover ✓",
            f"Price({price:.5f}) < EMA50({ema50:.5f}) ✓",
            f"MACD hist {macd_hv:.6f} ✓",
            f"RSI {rsi:.1f} in [{SHORT_RSI_MIN}–{SHORT_RSI_MAX}] ✓",
            f"Candle bearish ✓  ATR normal ✓",
            f"SL=ATR×{ATR_SL_MULT}  TP=ATR×{ATR_TP_MULT}",
        ]
    else:
        direction  = "HOLD"
        confidence = 0
        # Build specific reason why each side failed
        failed_long  = [k for k, v in long_conds.items()  if not v]
        failed_short = [k for k, v in short_conds.items() if not v]
        reasons = [
            f"HOLD — no valid entry",
            f"LONG failed: {failed_long}",
            f"SHORT failed: {failed_short}",
        ]
        log.info(
            "TRENDMASTER DECISION: HOLD",
            symbol=symbol,
            failed_long=failed_long,
            failed_short=failed_short,
        )

    if direction in ("BUY", "SELL"):
        log.info(
            f"TRENDMASTER DECISION: {direction}",
            symbol=symbol,
            confidence=confidence,
            rsi=round(rsi, 2),
            atr=round(atr, 6),
        )

    indicators = IndicatorValues(
        price          = round(price,   5),
        rsi            = round(rsi,     2),
        ema_fast       = round(ema9,    5),
        ema_slow       = round(ema21,   5),
        macd           = round(macd_v,  6),
        macd_signal    = round(macd_sv, 6),
        macd_histogram = round(macd_hv, 6),
        atr            = round(atr if atr and not math.isnan(atr) else 0.0, 6),
        volume_ratio   = 1.0,
        adx            = 0.0,
    )

    return TechnicalSignal(
        symbol         = symbol,
        timeframe      = timeframe,
        direction      = direction,
        confidence     = confidence,
        reasons        = reasons,
        indicators     = indicators,
        analyzed_at    = now,
        candles_used   = n,
        trend_strength = "strong" if confidence >= 70 else "moderate" if confidence >= 50 else "weak",
        composite_score= confidence if direction == "BUY" else (-confidence if direction == "SELL" else 0),
        score_breakdown= [],
        hold_reason    = reasons[0] if direction == "HOLD" else None,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _confidence(conds: dict, rsi: float, macd_hv: float) -> int:
    """
    Compute 0-100 confidence when ALL conditions are met.
    Base = 60. Bonuses for RSI ideal zone and strong MACD histogram.
    """
    score = 60
    # RSI in sweet spot (50-65 for long, 35-50 for short)
    if 50 <= rsi <= 65 or 35 <= rsi <= 50:
        score += 15
    # Strong MACD histogram
    if abs(macd_hv) > 0.0002:
        score += 15
    elif abs(macd_hv) > 0.00005:
        score += 8
    return min(100, score)


def _crossed_up(fast: List[float], slow: List[float], lookback: int) -> bool:
    """Return True if fast crossed above slow within the last *lookback* candles."""
    n = min(len(fast), len(slow), lookback + 1)
    if n < 2:
        return False
    f = fast[-n:]
    s = slow[-n:]
    for i in range(1, len(f)):
        fv, sv = f[i], s[i]
        fp, sp = f[i - 1], s[i - 1]
        if any(math.isnan(v) for v in (fv, sv, fp, sp)):
            continue
        if fp <= sp and fv > sv:
            return True
    return False


def _crossed_down(fast: List[float], slow: List[float], lookback: int) -> bool:
    """Return True if fast crossed below slow within the last *lookback* candles."""
    n = min(len(fast), len(slow), lookback + 1)
    if n < 2:
        return False
    f = fast[-n:]
    s = slow[-n:]
    for i in range(1, len(f)):
        fv, sv = f[i], s[i]
        fp, sp = f[i - 1], s[i - 1]
        if any(math.isnan(v) for v in (fv, sv, fp, sp)):
            continue
        if fp >= sp and fv < sv:
            return True
    return False


def _last(series: List[float]) -> Optional[float]:
    if not series:
        return None
    v = series[-1]
    return None if math.isnan(v) else v


def _hold(
    symbol: str, timeframe: str, candles: List[dict],
    now: datetime, reason: str,
) -> TechnicalSignal:
    price = float(candles[-1]["close"]) if candles else 0.0
    return TechnicalSignal(
        symbol       = symbol,
        timeframe    = timeframe,
        direction    = "HOLD",
        confidence   = 0,
        reasons      = [reason],
        indicators   = IndicatorValues(
            price=price, rsi=0, ema_fast=0, ema_slow=0,
            macd=0, macd_signal=0, macd_histogram=0, atr=0, volume_ratio=1.0,
        ),
        analyzed_at  = now,
        candles_used = len(candles),
        hold_reason  = reason,
        composite_score = 0,
        score_breakdown = [],
    )
