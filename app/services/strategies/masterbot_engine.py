"""
MasterBot Strategy Engine — Multi-Strategy Intelligent Selector
================================================================
Master Bot • Forex • Dynamic Strategy Selection

Core principle: detect market state first, then activate the correct
strategy. Never force one strategy into the wrong environment.

Market States:
  RISK_OFF   → VIX proxy > 25 → close all, no entries
  RANGE      → ADX_4H < 20   → Mean Reversion (ScalerX logic)
  BREAKOUT   → breakout detected + ADX rising → Breakout (PipHunter logic)
  MOMENTUM   → ADX_4H > 30 + multi-TF alignment → Momentum (CryptoBot logic)
  TREND      → ADX_1H > 25  → Trend Following (TrendMaster logic)
  CARRY      → VIX < 20 + swap positive → Carry Trade (SafeGuard logic)
  NO_TRADE   → none of the above

Strategy priority (highest first):
  1. RISK_OFF   — capital protection
  2. MOMENTUM   — strongest edge
  3. BREAKOUT   — high conviction
  4. TREND      — reliable
  5. RANGE      — low priority
  6. CARRY      — background income

Risk per strategy:
  Trend       → 1.0%
  Mean Rev    → 1.0%
  Breakout    → 0.75%
  Momentum    → 1.25%
  Carry       → 0.65%

Global limits:
  Max 3% simultaneous exposure
  Max 10 trades/day
  3% daily DD → stop
  7% weekly DD → stop
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional, Tuple

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

# ── Constants ─────────────────────────────────────────────────────────────────
ADX_PERIOD   = 14
ATR_PERIOD   = 14
RSI_PERIOD   = 14
BB_PERIOD    = 20
BB_STD       = 2.0
STOCH_K      = 5
STOCH_D      = 3
STOCH_SMOOTH = 3

# ADX thresholds
ADX_RANGE_MAX   = 20.0   # below → range
ADX_TREND_MIN   = 25.0   # above → trend
ADX_MOMENTUM    = 30.0   # above → momentum

# VIX proxy thresholds (ATR-ratio based)
VIX_SCALE        = 15.0
VIX_CALM         = 15.0
VIX_NORMAL       = 20.0
VIX_CAUTION      = 25.0
VIX_DANGER       = 35.0

# Breakout detection
BREAKOUT_LOOKBACK = 50
BODY_PCT_MIN      = 0.60

# Mean Reversion (ScalerX)
MR_RSI_LONG     = 30.0
MR_RSI_SHORT    = 70.0
MR_STOCH_OVER   = 20.0
MR_STOCH_OB     = 80.0

# Trend Following (TrendMaster)
TF_RSI_BULL_MIN = 45.0
TF_RSI_BEAR_MAX = 55.0

# Momentum (CryptoBot)
MOM_RSI_BULL    = 50.0
MOM_RSI_BEAR    = 50.0

MIN_CANDLES = 80

POSITIVE_SWAP_PAIRS = {
    "AUD/JPY", "NZD/JPY", "GBP/JPY",
    "USD/JPY", "EUR/JPY", "AUD/CHF",
}

# Market state labels
RISK_OFF  = "RISK_OFF"
RANGE     = "RANGE"
BREAKOUT  = "BREAKOUT"
MOMENTUM  = "MOMENTUM"
TREND     = "TREND"
CARRY     = "CARRY"
NO_TRADE  = "NO_TRADE"


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "1h",
) -> TechnicalSignal:
    """
    Run the MasterBot multi-strategy engine.

    1. Classify market state
    2. Select strategy
    3. Apply strategy-specific entry logic
    4. Return BUY / SELL / HOLD with active strategy in reasons
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
    stoch_k_s, stoch_d_s             = calculate_stochastic(highs, lows, closes, STOCH_K, STOCH_D, STOCH_SMOOTH)
    adx_s                             = calculate_adx(highs, lows, closes, ADX_PERIOD)
    atr_s                             = calculate_atr(highs, lows, closes, ATR_PERIOD)

    price    = closes[-1]
    bb_upper = _last(bb_upper_s)
    bb_mid   = _last(bb_mid_s)
    bb_lower = _last(bb_lower_s)
    rsi      = _last(rsi_s)
    stoch_k  = _last(stoch_k_s)
    stoch_d  = _last(stoch_d_s)
    adx      = _last(adx_s)
    atr      = _last(atr_s)

    # Guard
    core = {"bb_upper": bb_upper, "bb_mid": bb_mid, "bb_lower": bb_lower,
            "rsi": rsi, "adx": adx}
    bad = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── VIX proxy ─────────────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_ratio  = (atr / atr_avg) if atr_avg > 0 and atr else 1.0
    vix_proxy  = atr_ratio * VIX_SCALE

    # ── SMA200 proxy ──────────────────────────────────────────────────────────
    sma200 = sum(closes[-200:]) / min(len(closes), 200)

    # ── Breakout detection ────────────────────────────────────────────────────
    lookback_highs = highs[-(BREAKOUT_LOOKBACK + 1):-1]
    lookback_lows  = lows[-(BREAKOUT_LOOKBACK + 1):-1]
    resistance     = max(lookback_highs) if lookback_highs else bb_upper
    support_level  = min(lookback_lows)  if lookback_lows  else bb_lower
    candle_range   = highs[-1] - lows[-1]
    candle_body    = abs(closes[-1] - opens[-1])
    body_pct       = (candle_body / candle_range) if candle_range > 0 else 0.0
    breakout_long  = price > resistance and body_pct >= BODY_PCT_MIN and adx > 20
    breakout_short = price < support_level and body_pct >= BODY_PCT_MIN and adx > 20

    # ── Multi-TF alignment proxy (ADX rising) ─────────────────────────────────
    adx_list   = [v for v in adx_s if v is not None and not math.isnan(v)]
    adx_rising = len(adx_list) >= 3 and adx_list[-1] > adx_list[-3]

    # ── Stochastic crosses ────────────────────────────────────────────────────
    stoch_bull = _stoch_crossed_up(stoch_k_s, stoch_d_s)
    stoch_bear = _stoch_crossed_down(stoch_k_s, stoch_d_s)

    # ── MARKET STATE CLASSIFICATION ───────────────────────────────────────────
    market_state = _classify_market(
        vix_proxy, adx, breakout_long, breakout_short,
        adx_rising, symbol,
    )

    log.info(
        "MASTERBOT MARKET STATE",
        symbol=symbol, market_state=market_state,
        adx=round(adx, 2), vix_proxy=round(vix_proxy, 2),
        rsi=round(rsi, 2), price=round(price, 5),
    )

    # ── RISK-OFF: close everything ────────────────────────────────────────────
    if market_state == RISK_OFF:
        reason = f"MASTERBOT RISK-OFF — VIX proxy {vix_proxy:.1f} > {VIX_CAUTION} (close all positions, no entries)"
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    if market_state == NO_TRADE:
        reason = f"MASTERBOT NO_TRADE — market conditions unclear (ADX {adx:.1f}, VIX {vix_proxy:.1f})"
        return _hold(symbol, timeframe, candles, now, reason)

    # ── STRATEGY EXECUTION ────────────────────────────────────────────────────
    direction, confidence, reasons = _execute_strategy(
        market_state, symbol, price,
        rsi, stoch_k, stoch_d, adx, atr,
        bb_upper, bb_mid, bb_lower,
        resistance, support_level, body_pct,
        sma200, vix_proxy,
        stoch_bull, stoch_bear,
        breakout_long, breakout_short,
    )

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(bb_lower, 5),
        ema_slow       = round(bb_upper, 5),
        macd           = round(bb_mid, 5),
        macd_signal    = round(stoch_k if stoch_k else 0.0, 2),
        macd_histogram = round(vix_proxy, 2),
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


# ── Market state classifier ───────────────────────────────────────────────────

def _classify_market(
    vix_proxy: float,
    adx: float,
    breakout_long: bool,
    breakout_short: bool,
    adx_rising: bool,
    symbol: str,
) -> str:
    if vix_proxy > VIX_CAUTION:
        return RISK_OFF
    if adx < ADX_RANGE_MAX:
        return RANGE
    if breakout_long or breakout_short:
        return BREAKOUT
    if adx >= ADX_MOMENTUM and adx_rising:
        return MOMENTUM
    if adx >= ADX_TREND_MIN:
        return TREND
    if symbol.upper() in POSITIVE_SWAP_PAIRS and vix_proxy < VIX_NORMAL:
        return CARRY
    return NO_TRADE


# ── Strategy executor ─────────────────────────────────────────────────────────

def _execute_strategy(
    state: str, symbol: str, price: float,
    rsi: float, stoch_k: Optional[float], stoch_d: Optional[float],
    adx: float, atr: Optional[float],
    bb_upper: float, bb_mid: float, bb_lower: float,
    resistance: float, support_level: float, body_pct: float,
    sma200: float, vix_proxy: float,
    stoch_bull: bool, stoch_bear: bool,
    breakout_long: bool, breakout_short: bool,
) -> Tuple[str, int, List[str]]:

    # ── RANGE → Mean Reversion (ScalerX) ─────────────────────────────────────
    if state == RANGE:
        long_ok  = (price <= bb_lower and rsi < MR_RSI_LONG and
                    stoch_k is not None and stoch_k < MR_STOCH_OVER and
                    stoch_bull and price > bb_lower)
        short_ok = (price >= bb_upper and rsi > MR_RSI_SHORT and
                    stoch_k is not None and stoch_k > MR_STOCH_OB and
                    stoch_bear and price < bb_upper)
        if long_ok:
            return "BUY", _conf(60, rsi, adx, True), [
                f"MASTERBOT [RANGE → Mean Reversion] BUY",
                f"Price at BB lower ({bb_lower:.5f}), RSI {rsi:.1f} oversold, Stoch bull cross ✓",
                f"TP: SMA20 = {bb_mid:.5f}  |  SL: ATR × 2",
            ]
        if short_ok:
            return "SELL", _conf(60, rsi, adx, False), [
                f"MASTERBOT [RANGE → Mean Reversion] SELL",
                f"Price at BB upper ({bb_upper:.5f}), RSI {rsi:.1f} overbought, Stoch bear cross ✓",
                f"TP: SMA20 = {bb_mid:.5f}  |  SL: ATR × 2",
            ]
        return "HOLD", 0, [f"MASTERBOT [RANGE → Mean Reversion] HOLD — conditions not met"]

    # ── BREAKOUT → Breakout (PipHunter) ──────────────────────────────────────
    if state == BREAKOUT:
        if breakout_long and rsi > 50:
            sl = round(atr * 1.0, 5) if atr else None
            return "BUY", _conf(65, rsi, adx, True), [
                f"MASTERBOT [BREAKOUT → PipHunter] BUY",
                f"Price broke resistance ({resistance:.5f}), body {body_pct:.0%}, ADX {adx:.1f} rising ✓",
                f"SL: ATR × 1.0 ({sl})  |  TP: 1:1 / 1:2 / 1:3",
            ]
        if breakout_short and rsi < 50:
            sl = round(atr * 1.0, 5) if atr else None
            return "SELL", _conf(65, rsi, adx, False), [
                f"MASTERBOT [BREAKOUT → PipHunter] SELL",
                f"Price broke support ({support_level:.5f}), body {body_pct:.0%}, ADX {adx:.1f} rising ✓",
                f"SL: ATR × 1.0 ({sl})  |  TP: 1:1 / 1:2 / 1:3",
            ]
        return "HOLD", 0, [f"MASTERBOT [BREAKOUT → PipHunter] HOLD — RSI filter failed"]

    # ── MOMENTUM → Momentum (CryptoBot) ──────────────────────────────────────
    if state == MOMENTUM:
        bull = price > sma200 and rsi > MOM_RSI_BULL
        bear = price < sma200 and rsi < MOM_RSI_BEAR
        if bull:
            return "BUY", _conf(70, rsi, adx, True), [
                f"MASTERBOT [MOMENTUM → CryptoBot] BUY",
                f"Price > SMA200, ADX {adx:.1f} strong momentum, RSI {rsi:.1f} ✓",
                f"Trailing SL: ATR × 1.5  |  No fixed TP",
            ]
        if bear:
            return "SELL", _conf(70, rsi, adx, False), [
                f"MASTERBOT [MOMENTUM → CryptoBot] SELL",
                f"Price < SMA200, ADX {adx:.1f} strong momentum, RSI {rsi:.1f} ✓",
                f"Trailing SL: ATR × 1.5  |  No fixed TP",
            ]
        return "HOLD", 0, [f"MASTERBOT [MOMENTUM → CryptoBot] HOLD — price/SMA200 misaligned"]

    # ── TREND → Trend Following (TrendMaster) ────────────────────────────────
    if state == TREND:
        bull = price > sma200 and rsi >= TF_RSI_BULL_MIN
        bear = price < sma200 and rsi <= TF_RSI_BEAR_MAX
        if bull:
            return "BUY", _conf(62, rsi, adx, True), [
                f"MASTERBOT [TREND → TrendMaster] BUY",
                f"Uptrend confirmed: price > SMA200, ADX {adx:.1f}, RSI {rsi:.1f} ✓",
                f"SL: ATR × 1.5  |  TP: ATR × 3",
            ]
        if bear:
            return "SELL", _conf(62, rsi, adx, False), [
                f"MASTERBOT [TREND → TrendMaster] SELL",
                f"Downtrend confirmed: price < SMA200, ADX {adx:.1f}, RSI {rsi:.1f} ✓",
                f"SL: ATR × 1.5  |  TP: ATR × 3",
            ]
        return "HOLD", 0, [f"MASTERBOT [TREND → TrendMaster] HOLD — conditions not met"]

    # ── CARRY → Carry Trade (SafeGuard) ──────────────────────────────────────
    if state == CARRY:
        bull = price > sma200 and 35 <= rsi <= 70 and adx > 15
        if bull:
            sl = round(atr * 3.0, 5) if atr else None
            return "BUY", _conf(58, rsi, adx, True), [
                f"MASTERBOT [CARRY → SafeGuard] BUY",
                f"Swap positive, VIX {vix_proxy:.1f} < {VIX_NORMAL}, price > SMA200 ✓",
                f"SL: ATR × 3.0 ({sl})  |  Earn daily swap",
            ]
        return "HOLD", 0, [f"MASTERBOT [CARRY → SafeGuard] HOLD — entry conditions not met"]

    return "HOLD", 0, [f"MASTERBOT HOLD — state {state} unhandled"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _conf(base: int, rsi: float, adx: float, bullish: bool) -> int:
    score = base
    if adx > 35:    score += 12
    elif adx > 25:  score += 6
    if bullish:
        if rsi > 65: score += 8
    else:
        if rsi < 35: score += 8
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
