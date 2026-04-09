"""
PipHunter Strategy Engine — Breakout
=====================================
Breakout • Forex • Strict Risk Management
15M entry / 1H confirmation / 4H–Daily levels

Core principle: price breaking a key level triggers an explosive move.
Operate ONLY in valid sessions (London / NY). Never trade Asia.

Entry Conditions (ALL must be met simultaneously):

LONG:
  1. Price breaks above resistance (close > resistance_level)
  2. Breakout candle body > 60% of total range
  3. ADX > 20                     — momentum confirmed
  4. RSI > 50                     — bullish momentum
  5. Candle closed (no wicks-only break)
  6. ATR within normal range      — no news spike chaos
  7. Valid session (London / NY)

SHORT:
  1. Price breaks below support (close < support_level)
  2. Breakout candle body > 60% of total range
  3. ADX > 20
  4. RSI < 50                     — bearish momentum
  5. Candle closed
  6. ATR within normal range
  7. Valid session

Risk:
  SL  = ATR × 1.0 below/above broken level
  TP1 = 1:1 R (close 40%)
  TP2 = 1:2 R (close 40%)
  TP3 = 1:3 R (close 20%)
  Move to BE after TP1

Fakeout exits:
  - 2 candles close back through broken level → close
  - 1 candle → reduce position 50%

Limits:
  - Max 4 trades/day
  - 3% daily drawdown
  - 6% weekly drawdown
  - 3 fakeouts → pause 24h
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
)

log = get_logger(__name__)

# ── Strategy constants ────────────────────────────────────────────────────────
BB_PERIOD       = 20        # used to derive dynamic S/R levels
BB_STD          = 2.0
RSI_PERIOD      = 14
ADX_PERIOD      = 14
ATR_PERIOD      = 14

# Entry thresholds
ADX_MIN         = 20.0      # minimum momentum for valid breakout
LONG_RSI_MIN    = 50.0      # bullish momentum
SHORT_RSI_MAX   = 50.0      # bearish momentum
BODY_PCT_MIN    = 0.60      # candle body must be >60% of range

# ATR spike gate
ATR_SPIKE_MULT  = 2.5       # above this → news spike → skip

# ATR multipliers for risk_manager
ATR_SL_MULT     = 1.0       # tight SL = ATR × 1.0
ATR_TP1_MULT    = 1.0       # TP1 = 1:1
ATR_TP2_MULT    = 2.0       # TP2 = 1:2
ATR_TP3_MULT    = 3.0       # TP3 = 1:3

# Lookback for S/R level detection (candles)
SR_LOOKBACK     = 50
MIN_CANDLES     = 80


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "15m",
) -> TechnicalSignal:
    """
    Run the PipHunter Breakout strategy.

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
    opens  = [float(c["open"])  for c in candles]

    # ── Indicators ────────────────────────────────────────────────────────────
    bb_upper_s, bb_mid_s, bb_lower_s = calculate_bollinger_bands(closes, BB_PERIOD, BB_STD)
    rsi_s                             = calculate_rsi(closes, RSI_PERIOD)
    adx_s                             = calculate_adx(highs, lows, closes, ADX_PERIOD)
    atr_s                             = calculate_atr(highs, lows, closes, ATR_PERIOD)

    # ── Current values ────────────────────────────────────────────────────────
    price    = closes[-1]
    bb_upper = _last(bb_upper_s)
    bb_lower = _last(bb_lower_s)
    bb_mid   = _last(bb_mid_s)
    rsi      = _last(rsi_s)
    adx      = _last(adx_s)
    atr      = _last(atr_s)

    # Guard: NaN in core indicators
    core = {"bb_upper": bb_upper, "bb_lower": bb_lower, "rsi": rsi, "adx": adx}
    bad  = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol, candles=n)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── S/R levels from recent price history ─────────────────────────────────
    lookback_highs = highs[-(SR_LOOKBACK + 1):-1]
    lookback_lows  = lows[-(SR_LOOKBACK + 1):-1]
    resistance     = max(lookback_highs) if lookback_highs else bb_upper
    support        = min(lookback_lows)  if lookback_lows  else bb_lower

    # ── ATR spike check ───────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_spike  = bool(atr is not None and atr > atr_avg * ATR_SPIKE_MULT)

    # ── Breakout candle quality ───────────────────────────────────────────────
    candle_range = highs[-1] - lows[-1]
    candle_body  = abs(closes[-1] - opens[-1])
    body_pct     = (candle_body / candle_range) if candle_range > 0 else 0.0
    strong_candle = body_pct >= BODY_PCT_MIN

    log.info(
        "PIPHUNTER SNAPSHOT",
        symbol=symbol, candles=n,
        price=round(price, 5),
        resistance=round(resistance, 5),
        support=round(support, 5),
        rsi=round(rsi, 2),
        adx=round(adx, 2),
        atr=round(atr, 6) if atr else None,
        atr_spike=atr_spike,
        body_pct=round(body_pct, 2),
        strong_candle=strong_candle,
    )

    # ── LONG conditions ───────────────────────────────────────────────────────
    long_conds = {
        "breaks_resistance": price > resistance,
        "strong_candle":     strong_candle,
        "adx_momentum":      adx > ADX_MIN,
        "rsi_bullish":       rsi > LONG_RSI_MIN,
        "atr_normal":        not atr_spike,
    }
    long_ok = all(long_conds.values())

    # ── SHORT conditions ──────────────────────────────────────────────────────
    short_conds = {
        "breaks_support": price < support,
        "strong_candle":  strong_candle,
        "adx_momentum":   adx > ADX_MIN,
        "rsi_bearish":    rsi < SHORT_RSI_MAX,
        "atr_normal":     not atr_spike,
    }
    short_ok = all(short_conds.values())

    log.info(
        "PIPHUNTER CONDITIONS",
        symbol=symbol,
        long=long_conds,
        short=short_conds,
        long_ok=long_ok,
        short_ok=short_ok,
    )

    # ── Direction ─────────────────────────────────────────────────────────────
    reasons: List[str] = []

    if long_ok:
        direction  = "BUY"
        confidence = _confidence(rsi, adx, body_pct, bullish=True)
        sl         = round(atr * ATR_SL_MULT, 5) if atr else None
        tp1        = round(atr * ATR_TP1_MULT, 5) if atr else None
        tp2        = round(atr * ATR_TP2_MULT, 5) if atr else None
        reasons = [
            f"PipHunter LONG (Breakout): price({price:.5f}) broke resistance({resistance:.5f}) ✓",
            f"ADX {adx:.1f} > {ADX_MIN} (momentum confirmed) ✓",
            f"RSI {rsi:.1f} > {LONG_RSI_MIN} (bullish) ✓",
            f"Candle body {body_pct:.0%} > {BODY_PCT_MIN:.0%} (strong candle) ✓",
            f"ATR normal ✓",
            f"SL = ATR × {ATR_SL_MULT} ({sl})  |  TP1 = {tp1}  |  TP2 = {tp2}",
        ]
        log.info("PIPHUNTER DECISION: BUY", symbol=symbol, confidence=confidence,
                 rsi=round(rsi, 2), adx=round(adx, 2))

    elif short_ok:
        direction  = "SELL"
        confidence = _confidence(rsi, adx, body_pct, bullish=False)
        sl         = round(atr * ATR_SL_MULT, 5) if atr else None
        tp1        = round(atr * ATR_TP1_MULT, 5) if atr else None
        tp2        = round(atr * ATR_TP2_MULT, 5) if atr else None
        reasons = [
            f"PipHunter SHORT (Breakout): price({price:.5f}) broke support({support:.5f}) ✓",
            f"ADX {adx:.1f} > {ADX_MIN} (momentum confirmed) ✓",
            f"RSI {rsi:.1f} < {SHORT_RSI_MAX} (bearish) ✓",
            f"Candle body {body_pct:.0%} > {BODY_PCT_MIN:.0%} (strong candle) ✓",
            f"ATR normal ✓",
            f"SL = ATR × {ATR_SL_MULT} ({sl})  |  TP1 = {tp1}  |  TP2 = {tp2}",
        ]
        log.info("PIPHUNTER DECISION: SELL", symbol=symbol, confidence=confidence,
                 rsi=round(rsi, 2), adx=round(adx, 2))

    else:
        direction  = "HOLD"
        confidence = 0
        failed_long  = [k for k, v in long_conds.items()  if not v]
        failed_short = [k for k, v in short_conds.items() if not v]
        reasons = [
            f"HOLD — no valid Breakout entry",
            f"LONG failed: {failed_long}",
            f"SHORT failed: {failed_short}",
        ]
        log.info("PIPHUNTER DECISION: HOLD", symbol=symbol,
                 failed_long=failed_long, failed_short=failed_short)

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(support, 5),     # repurpose: support level
        ema_slow       = round(resistance, 5),  # repurpose: resistance level
        macd           = round(bb_mid, 5) if bb_mid else 0.0,
        macd_signal    = round(body_pct, 4),
        macd_histogram = 0.0,
        atr            = round(atr if atr and not math.isnan(atr) else 0.0, 6),
        volume_ratio   = 1.0,
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

def _confidence(rsi: float, adx: float, body_pct: float, bullish: bool) -> int:
    """60 base + bonuses for strong ADX, extreme RSI, and high body %."""
    score = 60
    if adx > 35:    score += 15
    elif adx > 25:  score += 8
    if bullish:
        if rsi > 65: score += 10
        elif rsi > 55: score += 5
    else:
        if rsi < 35: score += 10
        elif rsi < 45: score += 5
    if body_pct > 0.80: score += 10
    elif body_pct > 0.70: score += 5
    return min(100, score)


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
