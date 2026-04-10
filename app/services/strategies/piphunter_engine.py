"""
Breakout Strategy Engine
========================
Breakout • Forex • Strict Risk Management
15M entry timeframe

Core principle: price breaking a validated key level (3+ touches) triggers an explosive move.

Entry Conditions (ALL must be met simultaneously):

LONG:
  1. Price closes above resistance (3+ touches in last 100 candles)
  2. Candle body > 60% of total range
  3. Candle body > ATR × 0.7  (absolute size filter)
  4. ADX > 20 AND ADX rising (adx[-1] > adx[-3])
  5. DI+ > DI-                (directional bias confirms upside)
  6. RSI > 50                 (bullish momentum)
  7. BB squeeze active        (width near recent minimum)
  8. ATR within normal range  (no news spike chaos)

SHORT:
  1. Price closes below support (3+ touches)
  2. Candle body > 60% of total range
  3. Candle body > ATR × 0.7
  4. ADX > 20 AND ADX rising
  5. DI- > DI+
  6. RSI < 50
  7. BB squeeze active
  8. ATR within normal range

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
  - 3% daily drawdown / 6% weekly drawdown
  - 3 fakeouts → pause 24h
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional

from app.core.logger import get_logger
from app.schemas.technical import IndicatorValues, TechnicalSignal
from app.utils.indicators import (
    calculate_adx_full,
    calculate_atr,
    calculate_bollinger_bands,
    calculate_rsi,
)

log = get_logger(__name__)

# ── Strategy constants ────────────────────────────────────────────────────────
BB_PERIOD       = 20
BB_STD          = 2.0
RSI_PERIOD      = 14
ADX_PERIOD      = 14
ATR_PERIOD      = 14

ADX_MIN         = 20.0      # minimum ADX for valid breakout
LONG_RSI_MIN    = 50.0
SHORT_RSI_MAX   = 50.0
BODY_PCT_MIN    = 0.60      # candle body > 60% of range
BODY_ATR_MIN    = 0.70      # candle body > ATR × 0.7 (absolute filter)

ATR_SPIKE_MULT  = 2.5       # above this → news spike → skip
BB_SQUEEZE_MAX  = 1.30      # squeeze: current width ≤ min × 1.30

ATR_SL_MULT     = 1.0
ATR_TP_MULT     = 1.0       # kept for bot_service compat (TP1)
ATR_TP1_MULT    = 1.0
ATR_TP2_MULT    = 2.0
ATR_TP3_MULT    = 3.0

SR_LOOKBACK     = 100       # candles to scan for key levels
SR_MIN_TOUCHES  = 3         # touches required to qualify as key level
MIN_CANDLES     = 120


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "15m",
) -> TechnicalSignal:
    """
    Run the Breakout strategy.

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
    adx_s, di_plus_s, di_minus_s     = calculate_adx_full(highs, lows, closes, ADX_PERIOD)
    atr_s                             = calculate_atr(highs, lows, closes, ATR_PERIOD)

    # ── Current values ────────────────────────────────────────────────────────
    price    = closes[-1]
    bb_upper = _last(bb_upper_s)
    bb_lower = _last(bb_lower_s)
    bb_mid   = _last(bb_mid_s)
    rsi      = _last(rsi_s)
    adx      = _last(adx_s)
    di_plus  = _last(di_plus_s)
    di_minus = _last(di_minus_s)
    atr      = _last(atr_s)

    # Guard: NaN in core indicators
    core = {"bb_upper": bb_upper, "bb_lower": bb_lower, "rsi": rsi, "adx": adx, "atr": atr}
    bad  = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol, candles=n)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── ATR spike check ───────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_spike  = bool(atr > atr_avg * ATR_SPIKE_MULT)

    # ── Key S/R level detection (3+ touches) ─────────────────────────────────
    lb = min(SR_LOOKBACK, n - 1)
    lk_highs  = highs[-(lb + 1):-1]
    lk_lows   = lows[-(lb + 1):-1]
    tol       = atr * 0.3  # touch tolerance = ATR × 0.3

    resistance = _key_resistance(lk_highs, tol, SR_MIN_TOUCHES) or max(lk_highs)
    support    = _key_support(lk_lows,   tol, SR_MIN_TOUCHES) or min(lk_lows)

    # ── ADX rising check ──────────────────────────────────────────────────────
    adx_rising = False
    if len(adx_s) >= 3:
        adx_prev = adx_s[-3]
        if adx_prev is not None and not math.isnan(adx_prev):
            adx_rising = adx > adx_prev

    # ── BB squeeze check ──────────────────────────────────────────────────────
    valid_pairs = [
        (u, l) for u, l in zip(bb_upper_s[-lb:], bb_lower_s[-lb:])
        if u is not None and l is not None and not math.isnan(u) and not math.isnan(l)
    ]
    bb_widths    = [u - l for u, l in valid_pairs]
    bb_width_min = min(bb_widths) if bb_widths else 0.0
    bb_width_cur = bb_upper - bb_lower
    bb_squeeze   = bool(bb_width_min > 0 and bb_width_cur <= bb_width_min * BB_SQUEEZE_MAX)

    # ── Breakout candle quality ───────────────────────────────────────────────
    candle_range = highs[-1] - lows[-1]
    candle_body  = abs(closes[-1] - opens[-1])
    body_pct     = (candle_body / candle_range) if candle_range > 0 else 0.0
    strong_candle = body_pct >= BODY_PCT_MIN and candle_body >= atr * BODY_ATR_MIN

    log.info(
        "BREAKOUT SNAPSHOT",
        symbol=symbol, candles=n,
        price=round(price, 5),
        resistance=round(resistance, 5),
        support=round(support, 5),
        rsi=round(rsi, 2),
        adx=round(adx, 2),
        adx_rising=adx_rising,
        di_plus=round(di_plus, 2) if di_plus else None,
        di_minus=round(di_minus, 2) if di_minus else None,
        bb_squeeze=bb_squeeze,
        bb_width_cur=round(bb_width_cur, 6),
        bb_width_min=round(bb_width_min, 6),
        atr=round(atr, 6),
        atr_spike=atr_spike,
        body_pct=round(body_pct, 2),
        strong_candle=strong_candle,
    )

    # ── LONG conditions ───────────────────────────────────────────────────────
    long_conds = {
        "breaks_resistance": price > resistance,
        "strong_candle":     strong_candle,
        "adx_momentum":      adx > ADX_MIN,
        "adx_rising":        adx_rising,
        "di_plus_dominant":  (di_plus is not None and di_minus is not None and di_plus > di_minus),
        "rsi_bullish":       rsi > LONG_RSI_MIN,
        "bb_squeeze":        bb_squeeze,
        "atr_normal":        not atr_spike,
    }
    long_ok = all(long_conds.values())

    # ── SHORT conditions ──────────────────────────────────────────────────────
    short_conds = {
        "breaks_support":    price < support,
        "strong_candle":     strong_candle,
        "adx_momentum":      adx > ADX_MIN,
        "adx_rising":        adx_rising,
        "di_minus_dominant": (di_plus is not None and di_minus is not None and di_minus > di_plus),
        "rsi_bearish":       rsi < SHORT_RSI_MAX,
        "bb_squeeze":        bb_squeeze,
        "atr_normal":        not atr_spike,
    }
    short_ok = all(short_conds.values())

    log.info(
        "BREAKOUT CONDITIONS",
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
        confidence = _confidence(rsi, adx, body_pct, di_plus or 0, di_minus or 0, bullish=True)
        sl         = round(atr * ATR_SL_MULT, 5)
        tp1        = round(atr * ATR_TP1_MULT, 5)
        tp2        = round(atr * ATR_TP2_MULT, 5)
        reasons = [
            f"Breakout LONG: price({price:.5f}) broke resistance({resistance:.5f}) ✓",
            f"ADX {adx:.1f} > {ADX_MIN} {'↑' if adx_rising else '→'} (momentum) ✓",
            f"DI+ {di_plus:.1f} > DI- {di_minus:.1f} (direction confirmed) ✓",
            f"RSI {rsi:.1f} > {LONG_RSI_MIN} (bullish) ✓",
            f"Candle body {body_pct:.0%} > {BODY_PCT_MIN:.0%} | body {candle_body:.5f} > ATR×0.7 ✓",
            f"BB squeeze ✓  |  SL={sl}  TP1={tp1}  TP2={tp2}",
        ]
        log.info("BREAKOUT DECISION: BUY", symbol=symbol, confidence=confidence,
                 rsi=round(rsi, 2), adx=round(adx, 2))

    elif short_ok:
        direction  = "SELL"
        confidence = _confidence(rsi, adx, body_pct, di_plus or 0, di_minus or 0, bullish=False)
        sl         = round(atr * ATR_SL_MULT, 5)
        tp1        = round(atr * ATR_TP1_MULT, 5)
        tp2        = round(atr * ATR_TP2_MULT, 5)
        reasons = [
            f"Breakout SHORT: price({price:.5f}) broke support({support:.5f}) ✓",
            f"ADX {adx:.1f} > {ADX_MIN} {'↑' if adx_rising else '→'} (momentum) ✓",
            f"DI- {di_minus:.1f} > DI+ {di_plus:.1f} (direction confirmed) ✓",
            f"RSI {rsi:.1f} < {SHORT_RSI_MAX} (bearish) ✓",
            f"Candle body {body_pct:.0%} > {BODY_PCT_MIN:.0%} | body {candle_body:.5f} > ATR×0.7 ✓",
            f"BB squeeze ✓  |  SL={sl}  TP1={tp1}  TP2={tp2}",
        ]
        log.info("BREAKOUT DECISION: SELL", symbol=symbol, confidence=confidence,
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
        log.info("BREAKOUT DECISION: HOLD", symbol=symbol,
                 failed_long=failed_long, failed_short=failed_short)

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(support, 5),      # repurpose: support level
        ema_slow       = round(resistance, 5),   # repurpose: resistance level
        macd           = round(bb_mid, 5) if bb_mid else 0.0,
        macd_signal    = round(body_pct, 4),
        macd_histogram = round(di_plus - di_minus, 2) if (di_plus and di_minus) else 0.0,
        atr            = round(atr if not math.isnan(atr) else 0.0, 6),
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


# ── Key level helpers ─────────────────────────────────────────────────────────

def _key_resistance(highs: List[float], tol: float, min_touches: int) -> Optional[float]:
    """
    Find the highest level that has min_touches or more highs within tolerance.
    Iterates candidates from highest to lowest (closest to a potential breakout).
    """
    for candidate in sorted(highs, reverse=True):
        touches = sum(1 for h in highs if abs(h - candidate) <= tol)
        if touches >= min_touches:
            return candidate
    return None


def _key_support(lows: List[float], tol: float, min_touches: int) -> Optional[float]:
    """
    Find the lowest level that has min_touches or more lows within tolerance.
    Iterates candidates from lowest to highest (closest to a potential breakdown).
    """
    for candidate in sorted(lows):
        touches = sum(1 for l in lows if abs(l - candidate) <= tol)
        if touches >= min_touches:
            return candidate
    return None


# ── Confidence scorer ─────────────────────────────────────────────────────────

def _confidence(rsi: float, adx: float, body_pct: float,
                di_plus: float, di_minus: float, bullish: bool) -> int:
    score = 60
    # ADX bonus
    if adx > 35:    score += 12
    elif adx > 25:  score += 6
    # DI spread bonus
    di_diff = abs(di_plus - di_minus)
    if di_diff > 15: score += 8
    elif di_diff > 8: score += 4
    # RSI bonus
    if bullish:
        if rsi > 65: score += 10
        elif rsi > 55: score += 5
    else:
        if rsi < 35: score += 10
        elif rsi < 45: score += 5
    # Body quality bonus
    if body_pct > 0.80: score += 10
    elif body_pct > 0.70: score += 5
    return min(100, score)


# ── Fallbacks ─────────────────────────────────────────────────────────────────

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
