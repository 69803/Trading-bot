"""
Mean Reversion Strategy Engine
===============================
Mean Reversion • Forex • 15m signal / 5m entry

Core principle: price strays from its statistical average but always returns.
Operate ONLY in ranging markets (ADX < 25). Never trade trending markets.

Entry Conditions (ALL must be met simultaneously):

LONG — reversal from lower extreme:
  1. ADX < 25                    — range market confirmed
  2. BB Width within normal range (not expanding > 150% avg)
  3. candle LOW  <= BB lower band  — price touched the extreme
  4. RSI < 30                    — oversold confirmed
  5. Stochastic %K < 20 + bullish cross (%K crossed above %D)
  6. Current candle CLOSES above the lower band (close > bb_lower)
  7. ATR within normal range     — no news spike

SHORT — reversal from upper extreme:
  1. ADX < 25
  2. BB Width within normal range
  3. candle HIGH >= BB upper band — price touched the extreme
  4. RSI > 70                    — overbought confirmed
  5. Stochastic %K > 80 + bearish cross (%K crossed below %D)
  6. Current candle CLOSES below the upper band (close < bb_upper)
  7. ATR within normal range

Risk:
  SL = ATR × 2.0
  TP = ATR × 3.0  → ~1:1.5 R:R targeting SMA20

Emergency exits (handled by bot_service):
  - ADX > 30 while in trade → close
  - 2 candles close outside band → close
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional

from app.core.logger import get_logger
from app.schemas.technical import IndicatorValues, TechnicalSignal
from app.utils.indicators import (
    calculate_adx,
    calculate_atr,
    calculate_bollinger_bands,
    calculate_rsi,
    calculate_stochastic,
)

log = get_logger(__name__)

# ── Strategy constants ────────────────────────────────────────────────────────
BB_PERIOD      = 20
BB_STD         = 2.0
RSI_PERIOD     = 14
STOCH_K        = 5
STOCH_D        = 3
STOCH_SMOOTH   = 3
ADX_PERIOD     = 14
ATR_PERIOD     = 14

# ADX thresholds
ADX_RANGE_MAX  = 25.0   # above this → trend → NO entry
ADX_EXIT       = 30.0   # above this → emergency exit signal

# RSI thresholds
LONG_RSI_MAX   = 30.0   # oversold threshold
SHORT_RSI_MIN  = 70.0   # overbought threshold

# Stochastic thresholds
STOCH_OVERSOLD   = 20.0
STOCH_OVERBOUGHT = 80.0

# ATR spike gate
ATR_SPIKE_MULT = 2.0

# BB Width filter: bands expanding too fast = possible trend breakout
BB_WIDTH_EXPAND_MAX = 1.50   # if width > 150% of its average → avoid entry
BB_WIDTH_LOOKBACK   = 20     # periods to compute average width

# ATR multipliers for risk_manager (exported)
ATR_SL_MULT = 2.0
ATR_TP_MULT = 3.0   # TP = SMA20 (middle band) in practice; ATR×3 as hard cap

# Minimum candles needed
MIN_CANDLES = 80


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "15m",
) -> TechnicalSignal:
    """
    Run the Mean Reversion strategy.

    Returns TechnicalSignal with direction BUY / SELL / HOLD.
    """
    now = datetime.now(timezone.utc)
    n   = len(candles)

    if n < MIN_CANDLES:
        reason = f"SKIPPED [insufficient_data]: need {MIN_CANDLES} candles, got {n}"
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    closes = [float(c["close"]) for c in candles]
    highs  = [float(c["high"])  for c in candles]
    lows   = [float(c["low"])   for c in candles]

    # ── Indicators ────────────────────────────────────────────────────────────
    bb_upper_s, bb_mid_s, bb_lower_s = calculate_bollinger_bands(closes, BB_PERIOD, BB_STD)
    rsi_s                             = calculate_rsi(closes, RSI_PERIOD)
    stoch_k_s, stoch_d_s             = calculate_stochastic(highs, lows, closes, STOCH_K, STOCH_D, STOCH_SMOOTH)
    adx_s                             = calculate_adx(highs, lows, closes, ADX_PERIOD)
    atr_s                             = calculate_atr(highs, lows, closes, ATR_PERIOD)

    # ── Current values ────────────────────────────────────────────────────────
    price    = closes[-1]
    high_cur = highs[-1]
    low_cur  = lows[-1]
    bb_upper = _last(bb_upper_s)
    bb_mid   = _last(bb_mid_s)
    bb_lower = _last(bb_lower_s)
    rsi      = _last(rsi_s)
    stoch_k  = _last(stoch_k_s)
    stoch_d  = _last(stoch_d_s)
    adx      = _last(adx_s)
    atr      = _last(atr_s)

    # Guard: NaN in core indicators
    core = {"bb_upper": bb_upper, "bb_mid": bb_mid, "bb_lower": bb_lower,
            "rsi": rsi, "stoch_k": stoch_k, "adx": adx}
    bad  = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol, candles=n)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── ATR spike check ───────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_spike  = bool(atr is not None and atr > atr_avg * ATR_SPIKE_MULT)

    # ── BB Width filter ───────────────────────────────────────────────────────
    # Measure if bands are expanding (possible trend starting → avoid entry)
    bb_widths: List[float] = []
    for u, m, l in zip(bb_upper_s, bb_mid_s, bb_lower_s):
        if u is None or m is None or l is None:
            continue
        if math.isnan(u) or math.isnan(m) or math.isnan(l) or m == 0:
            continue
        bb_widths.append(u - l)

    bb_width_cur = bb_upper - bb_lower
    if len(bb_widths) >= BB_WIDTH_LOOKBACK:
        bb_width_avg = sum(bb_widths[-BB_WIDTH_LOOKBACK:]) / BB_WIDTH_LOOKBACK
    else:
        bb_width_avg = bb_width_cur  # not enough data → neutral

    bb_width_ratio = (bb_width_cur / bb_width_avg) if bb_width_avg > 0 else 1.0
    bb_expanding   = bb_width_ratio > BB_WIDTH_EXPAND_MAX

    # ── Stochastic crosses ────────────────────────────────────────────────────
    stoch_bull_cross = _stoch_crossed_up(stoch_k_s, stoch_d_s, lookback=3)
    stoch_bear_cross = _stoch_crossed_down(stoch_k_s, stoch_d_s, lookback=3)

    log.info(
        "MEAN REVERSION SNAPSHOT",
        symbol=symbol, candles=n,
        price=round(price, 5),
        bb_upper=round(bb_upper, 5), bb_mid=round(bb_mid, 5), bb_lower=round(bb_lower, 5),
        bb_width_ratio=round(bb_width_ratio, 2), bb_expanding=bb_expanding,
        rsi=round(rsi, 2),
        stoch_k=round(stoch_k, 2) if stoch_k else None,
        stoch_d=round(stoch_d, 2) if stoch_d else None,
        adx=round(adx, 2),
        atr=round(atr, 6) if atr else None,
        atr_spike=atr_spike,
        stoch_bull_cross=stoch_bull_cross,
        stoch_bear_cross=stoch_bear_cross,
    )

    # ── Hard gate: trending market ────────────────────────────────────────────
    if adx >= ADX_RANGE_MAX:
        reason = (
            f"HOLD — ADX {adx:.1f} >= {ADX_RANGE_MAX} "
            f"(trending market — Mean Reversion disabled)"
        )
        log.info("MEAN REVERSION: " + reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── Hard gate: BB bands expanding → possible breakout ─────────────────────
    if bb_expanding:
        reason = (
            f"HOLD — BB width ratio {bb_width_ratio:.2f} > {BB_WIDTH_EXPAND_MAX} "
            f"(bands expanding — possible trend forming, no entry)"
        )
        log.info("MEAN REVERSION: " + reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── LONG conditions ───────────────────────────────────────────────────────
    # low_cur  <= bb_lower → candle touched/pierced the lower band (wick or body)
    # price    >  bb_lower → candle closed back INSIDE the band (reversal confirmed)
    long_conds = {
        "adx_range":           adx < ADX_RANGE_MAX,
        "bb_not_expanding":    not bb_expanding,
        "low_touched_band":    low_cur <= bb_lower,
        "closed_inside_band":  price > bb_lower,
        "rsi_oversold":        rsi < LONG_RSI_MAX,
        "stoch_oversold":      stoch_k is not None and stoch_k < STOCH_OVERSOLD,
        "stoch_bull_cross":    stoch_bull_cross,
        "atr_normal":          not atr_spike,
    }
    long_ok = all(long_conds.values())

    # ── SHORT conditions ──────────────────────────────────────────────────────
    # high_cur >= bb_upper → candle touched/pierced the upper band
    # price    <  bb_upper → candle closed back INSIDE the band
    short_conds = {
        "adx_range":           adx < ADX_RANGE_MAX,
        "bb_not_expanding":    not bb_expanding,
        "high_touched_band":   high_cur >= bb_upper,
        "closed_inside_band":  price < bb_upper,
        "rsi_overbought":      rsi > SHORT_RSI_MIN,
        "stoch_overbought":    stoch_k is not None and stoch_k > STOCH_OVERBOUGHT,
        "stoch_bear_cross":    stoch_bear_cross,
        "atr_normal":          not atr_spike,
    }
    short_ok = all(short_conds.values())

    log.info(
        "MEAN REVERSION CONDITIONS",
        symbol=symbol,
        long=long_conds, short=short_conds,
        long_ok=long_ok, short_ok=short_ok,
    )

    # ── Direction ─────────────────────────────────────────────────────────────
    reasons: List[str] = []

    if long_ok:
        direction  = "BUY"
        confidence = _confidence(rsi, stoch_k, adx, bullish=True)
        reasons = [
            f"Mean Reversion LONG: low({low_cur:.5f}) touched BB lower({bb_lower:.5f}) ✓",
            f"Closed back inside: price({price:.5f}) > BB lower({bb_lower:.5f}) ✓",
            f"RSI {rsi:.1f} < {LONG_RSI_MAX} (oversold) ✓",
            f"Stochastic %K {stoch_k:.1f} < {STOCH_OVERSOLD} + bullish cross ✓",
            f"ADX {adx:.1f} < {ADX_RANGE_MAX} (range) ✓  BB width ratio {bb_width_ratio:.2f} ✓  ATR normal ✓",
            f"TP target: SMA20 = {bb_mid:.5f}  |  SL = ATR × {ATR_SL_MULT}",
        ]
        log.info("MEAN REVERSION DECISION: BUY", symbol=symbol, confidence=confidence,
                 rsi=round(rsi, 2), adx=round(adx, 2))

    elif short_ok:
        direction  = "SELL"
        confidence = _confidence(rsi, stoch_k, adx, bullish=False)
        reasons = [
            f"Mean Reversion SHORT: high({high_cur:.5f}) touched BB upper({bb_upper:.5f}) ✓",
            f"Closed back inside: price({price:.5f}) < BB upper({bb_upper:.5f}) ✓",
            f"RSI {rsi:.1f} > {SHORT_RSI_MIN} (overbought) ✓",
            f"Stochastic %K {stoch_k:.1f} > {STOCH_OVERBOUGHT} + bearish cross ✓",
            f"ADX {adx:.1f} < {ADX_RANGE_MAX} (range) ✓  BB width ratio {bb_width_ratio:.2f} ✓  ATR normal ✓",
            f"TP target: SMA20 = {bb_mid:.5f}  |  SL = ATR × {ATR_SL_MULT}",
        ]
        log.info("MEAN REVERSION DECISION: SELL", symbol=symbol, confidence=confidence,
                 rsi=round(rsi, 2), adx=round(adx, 2))

    else:
        direction  = "HOLD"
        confidence = 0
        failed_long  = [k for k, v in long_conds.items()  if not v]
        failed_short = [k for k, v in short_conds.items() if not v]
        reasons = [
            f"HOLD — no Mean Reversion entry",
            f"LONG failed: {failed_long}",
            f"SHORT failed: {failed_short}",
        ]
        log.info("MEAN REVERSION DECISION: HOLD", symbol=symbol,
                 failed_long=failed_long, failed_short=failed_short)

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(bb_lower, 5),   # repurposed: bb_lower
        ema_slow       = round(bb_upper, 5),   # repurposed: bb_upper
        macd           = round(bb_mid, 5),     # repurposed: bb_mid / SMA20
        macd_signal    = round(stoch_k if stoch_k else 0.0, 2),
        macd_histogram = round(stoch_d if stoch_d else 0.0, 2),
        atr            = round(atr if atr and not math.isnan(atr) else 0.0, 6),
        volume_ratio   = round(bb_width_ratio, 2),  # BB width ratio for monitoring
        adx            = round(adx, 2),
    )

    return TechnicalSignal(
        symbol          = symbol,
        timeframe       = timeframe,
        direction       = direction,
        confidence      = confidence,
        reasons         = reasons,
        indicators      = indicators,
        analyzed_at     = now,
        candles_used    = n,
        trend_strength  = "strong" if confidence >= 70 else "moderate" if confidence >= 50 else "weak",
        composite_score = confidence if direction == "BUY" else (-confidence if direction == "SELL" else 0),
        score_breakdown = [],
        hold_reason     = reasons[0] if direction == "HOLD" else None,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _confidence(rsi: float, stoch_k: Optional[float], adx: float, bullish: bool) -> int:
    """60 base + bonuses for extreme RSI, extreme Stochastic, and low ADX."""
    score = 60
    if bullish:
        if rsi < 25:       score += 15   # strong oversold
        elif rsi < 30:     score += 8
        if stoch_k is not None and stoch_k < 10:   score += 15
        elif stoch_k is not None and stoch_k < 20:  score += 8
    else:
        if rsi > 75:       score += 15   # strong overbought
        elif rsi > 70:     score += 8
        if stoch_k is not None and stoch_k > 90:   score += 15
        elif stoch_k is not None and stoch_k > 80:  score += 8
    if adx < 15:           score += 10   # very clean range
    return min(100, score)


def _stoch_crossed_up(k: List[float], d: List[float], lookback: int = 3) -> bool:
    n = min(len(k), len(d), lookback + 1)
    if n < 2:
        return False
    ks, ds = k[-n:], d[-n:]
    for i in range(1, len(ks)):
        kp, dp, kc, dc = ks[i-1], ds[i-1], ks[i], ds[i]
        if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in (kp, dp, kc, dc)):
            continue
        if kp <= dp and kc > dc:
            return True
    return False


def _stoch_crossed_down(k: List[float], d: List[float], lookback: int = 3) -> bool:
    n = min(len(k), len(d), lookback + 1)
    if n < 2:
        return False
    ks, ds = k[-n:], d[-n:]
    for i in range(1, len(ks)):
        kp, dp, kc, dc = ks[i-1], ds[i-1], ks[i], ds[i]
        if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in (kp, dp, kc, dc)):
            continue
        if kp >= dp and kc < dc:
            return True
    return False


def _last(series: List[float]) -> Optional[float]:
    if not series:
        return None
    v = series[-1]
    return None if (isinstance(v, float) and math.isnan(v)) else v


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
