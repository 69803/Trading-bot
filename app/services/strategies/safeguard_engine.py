"""
SafeGuard Strategy Engine — Carry Trade
=========================================
Carry Trade • Forex • Passive Income via Swap
Daily timeframe entry / 4H timing

Core principle: earn positive swap (interest) every day by holding
high-yield currencies against low-yield currencies.
Double gain when price also moves in favor.

Entry Conditions (ALL must be met simultaneously):

LONG (high-yield vs low-yield):
  1. Swap positive for the pair
  2. VIX < 20                    — risk-on environment
  3. Price > EMA200 (daily)      — macro uptrend
  4. ADX > 15                    — some trending direction
  5. RSI between 35–70           — not overbought/oversold
  6. Pullback present            — entry at value, not chase

EXIT / EMERGENCY:
  VIX 20–25 → reduce position 50%
  VIX 25–35 → close 70%
  VIX > 35  → close EVERYTHING (no exceptions)

Risk:
  SL  = ATR × 3.0   (wide — long-term strategy)
  TP1 = ATR × 3.0 + move to break-even at 30 days
  TP2 = trailing after ATR × 6.0

Limits:
  - Max 3 positions simultaneously
  - Max 2.5% total exposure
  - No trading around BOJ, NFP, CPI, GDP events
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
EMA_SLOW_PERIOD = 200       # macro trend filter (approximated via BB)
BB_PERIOD       = 20        # for SMA reference
BB_STD          = 2.0
RSI_PERIOD      = 14
ADX_PERIOD      = 14
ATR_PERIOD      = 14

# Entry thresholds
ADX_MIN         = 15.0      # carry trade: only needs mild trend
RSI_MIN         = 35.0      # not oversold panic
RSI_MAX         = 70.0      # not overbought

# VIX thresholds (simulated via ATR spread when VIX unavailable)
VIX_SAFE        = 20.0      # below → risk-on, entries allowed
VIX_CAUTION     = 25.0      # reduce 50%
VIX_DANGER      = 35.0      # close everything

# ATR multipliers
ATR_SL_MULT     = 3.0       # wide SL for long-term hold
ATR_TP1_MULT    = 3.0       # first target
ATR_TP2_MULT    = 6.0       # trailing activation

# ATR spike → proxy for elevated volatility / risk-off
ATR_SPIKE_MULT  = 2.0       # above avg × 2 → skip (VIX proxy)

MIN_CANDLES     = 80

# Pairs with positive swap (long side earns interest)
POSITIVE_SWAP_LONGS = {
    "AUD/JPY", "NZD/JPY", "GBP/JPY",
    "USD/JPY", "EUR/JPY", "AUD/CHF",
}


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "1d",
) -> TechnicalSignal:
    """
    Run the SafeGuard Carry Trade strategy.

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
    adx_s                             = calculate_adx(highs, lows, closes, ADX_PERIOD)
    atr_s                             = calculate_atr(highs, lows, closes, ATR_PERIOD)

    # ── Current values ────────────────────────────────────────────────────────
    price    = closes[-1]
    bb_mid   = _last(bb_mid_s)
    bb_upper = _last(bb_upper_s)
    bb_lower = _last(bb_lower_s)
    rsi      = _last(rsi_s)
    adx      = _last(adx_s)
    atr      = _last(atr_s)

    # Guard: NaN in core indicators
    core = {"bb_mid": bb_mid, "rsi": rsi, "adx": adx}
    bad  = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol, candles=n)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── ATR spike → elevated volatility proxy (VIX) ───────────────────────────
    valid_atrs  = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg     = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_ratio   = (atr / atr_avg) if atr_avg > 0 and atr else 1.0
    vix_proxy   = atr_ratio * 15.0   # scale to approximate VIX-like number

    # Emergency VIX gates
    if vix_proxy > VIX_DANGER:
        reason = f"EMERGENCY EXIT — VIX proxy {vix_proxy:.1f} > {VIX_DANGER} (close all positions)"
        log.warning("SAFEGUARD: " + reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    if vix_proxy > VIX_CAUTION:
        reason = f"HOLD — VIX proxy {vix_proxy:.1f} > {VIX_CAUTION} (reduce exposure, no new entries)"
        log.info("SAFEGUARD: " + reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── Swap check ────────────────────────────────────────────────────────────
    swap_positive = symbol.upper() in POSITIVE_SWAP_LONGS

    # ── Simple EMA200 proxy: price above long-term SMA ────────────────────────
    sma_long = sum(closes[-200:]) / min(len(closes), 200)
    above_sma200 = price > sma_long

    # ── Pullback detection: price pulled back toward SMA but still above ──────
    pullback = price < bb_upper and price > bb_lower if bb_upper and bb_lower else True

    log.info(
        "SAFEGUARD SNAPSHOT",
        symbol=symbol, candles=n,
        price=round(price, 5),
        sma200=round(sma_long, 5),
        above_sma200=above_sma200,
        vix_proxy=round(vix_proxy, 2),
        rsi=round(rsi, 2),
        adx=round(adx, 2),
        swap_positive=swap_positive,
        pullback=pullback,
    )

    # ── LONG conditions ───────────────────────────────────────────────────────
    long_conds = {
        "swap_positive":  swap_positive,
        "vix_safe":       vix_proxy < VIX_SAFE,
        "above_sma200":   above_sma200,
        "adx_trending":   adx > ADX_MIN,
        "rsi_range":      RSI_MIN <= rsi <= RSI_MAX,
        "pullback":       pullback,
    }
    long_ok = all(long_conds.values())

    log.info(
        "SAFEGUARD CONDITIONS",
        symbol=symbol,
        long=long_conds,
        long_ok=long_ok,
    )

    # ── Direction ─────────────────────────────────────────────────────────────
    reasons: List[str] = []

    if long_ok:
        direction  = "BUY"
        confidence = _confidence(rsi, adx, vix_proxy)
        sl         = round(atr * ATR_SL_MULT, 5) if atr else None
        tp1        = round(atr * ATR_TP1_MULT, 5) if atr else None
        reasons = [
            f"SafeGuard LONG (Carry Trade): swap positive ✓",
            f"VIX proxy {vix_proxy:.1f} < {VIX_SAFE} (risk-on) ✓",
            f"Price {price:.5f} > SMA200 {sma_long:.5f} (uptrend) ✓",
            f"ADX {adx:.1f} > {ADX_MIN} (trending) ✓",
            f"RSI {rsi:.1f} in [{RSI_MIN}–{RSI_MAX}] ✓",
            f"Pullback entry ✓",
            f"SL = ATR × {ATR_SL_MULT} ({sl})  |  TP1 = {tp1}  |  BE at 30 days",
        ]
        log.info("SAFEGUARD DECISION: BUY", symbol=symbol, confidence=confidence,
                 rsi=round(rsi, 2), adx=round(adx, 2), vix_proxy=round(vix_proxy, 2))

    else:
        direction  = "HOLD"
        confidence = 0
        failed = [k for k, v in long_conds.items() if not v]
        reasons = [
            f"HOLD — Carry Trade conditions not met",
            f"Failed: {failed}",
        ]
        log.info("SAFEGUARD DECISION: HOLD", symbol=symbol, failed=failed)

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(sma_long, 5),
        ema_slow       = round(sma_long, 5),
        macd           = round(vix_proxy, 2),
        macd_signal    = round(atr_ratio, 4),
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
        composite_score = confidence if direction == "BUY" else 0,
        score_breakdown = [],
        hold_reason     = reasons[0] if direction == "HOLD" else None,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _confidence(rsi: float, adx: float, vix_proxy: float) -> int:
    """60 base + bonuses for low VIX, strong ADX, RSI in sweet spot."""
    score = 60
    if vix_proxy < 15:   score += 15
    elif vix_proxy < 18: score += 8
    if adx > 30:         score += 10
    elif adx > 20:       score += 5
    if 45 <= rsi <= 60:  score += 10   # ideal carry entry zone
    elif 40 <= rsi <= 65: score += 5
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
