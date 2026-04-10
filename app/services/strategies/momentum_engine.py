"""
Momentum Strategy Engine
=========================
Momentum • Forex • Lo que sube, sigue subiendo

Based on Jegadeesh & Titman (1993): assets with strong recent performance
tend to continue performing well. In Forex: central bank divergence +
institutional flows create sustained directional moves.

Momentum Score system (0–8 points):
  1. price > EMA200           → macro uptrend  (+1 LONG)
  2. ADX > 25                 → momentum active (+1 both)
  3. DI+ > DI-                → bullish directional (+1 LONG)
  4. MACD > 0 + above signal  → momentum confirmed (+1 LONG)
  5. RSI(10) 50–75            → momentum zone, not exhausted (+1 LONG)
  6. ROC(14) > 0              → price accelerating (+1 LONG)
  7. EMA50 > EMA200           → medium-term above long-term (+1 LONG)
  8. No MACD divergence       → momentum still valid (+1 both)
  (mirror conditions apply for SHORT)

Entry thresholds:
  Score 7–8 → enter with 1.5× size flag
  Score 6   → enter with 1.0× size flag
  Score 5   → enter with 0.5× size flag (reduced)
  Score < 5 → HOLD

Risk:
  SL = ATR × 1.5  (below EMA50 for pullback entries)
  TP = ATR × 4.0  (no fixed TP — trailing in practice; 4.0 is hard cap)

Emergency exits (monitored by bot_service):
  - ADX falls below 20 for 2+ candles → start closing
  - MACD + ROC divergence both confirmed → close
  - Price breaks EMA200 in reverse → macro trend changed
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from app.core.logger import get_logger
from app.schemas.technical import IndicatorValues, TechnicalSignal
from app.utils.indicators import (
    calculate_adx_full,
    calculate_atr,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
)

log = get_logger(__name__)

# ── Indicator parameters ──────────────────────────────────────────────────────
RSI_PERIOD     = 10      # shorter than standard — faster momentum capture
EMA_FAST       = 50      # medium-term trend
EMA_SLOW       = 200     # macro trend
MACD_FAST      = 12
MACD_SLOW      = 26
MACD_SIG       = 9
ADX_PERIOD     = 14
ATR_PERIOD     = 14
ROC_PERIOD     = 14      # Rate of Change period

# ── Thresholds ────────────────────────────────────────────────────────────────
ADX_MIN        = 25.0    # below → no momentum → HOLD
ADX_STRONG     = 35.0    # above → high-confidence entries

LONG_RSI_MIN   = 50.0    # momentum zone (NOT overbought logic)
LONG_RSI_MAX   = 75.0
SHORT_RSI_MIN  = 25.0
SHORT_RSI_MAX  = 50.0

# ATR spike gate (news filter)
ATR_SPIKE_MULT = 2.5     # wider than scalping bots — momentum allows more ATR

# ── Score thresholds ──────────────────────────────────────────────────────────
SCORE_FULL     = 6       # full position size
SCORE_STRONG   = 7       # 1.5× size multiplier
SCORE_REDUCED  = 5       # 0.5× — reduced size entry

# ── ATR multipliers (exported for risk_manager) ───────────────────────────────
ATR_SL_MULT    = 1.5
ATR_TP_MULT    = 4.0     # wide hard cap; trailing stop manages in practice

MIN_CANDLES    = 250     # EMA200 needs 200 seed + warm-up


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "1h",
) -> TechnicalSignal:
    """
    Run the Momentum strategy on 1H candles.

    Uses EMA200 as the macro trend context (approximates the daily timeframe),
    ADX/MACD/RSI/ROC for momentum confirmation, and a 0–8 score system to
    gate entries and size positions.
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
    ema50_s  = calculate_ema(closes, EMA_FAST)
    ema200_s = calculate_ema(closes, EMA_SLOW)
    rsi_s    = calculate_rsi(closes, RSI_PERIOD)
    macd_line_s, macd_sig_s, macd_hist_s = calculate_macd(closes, MACD_FAST, MACD_SLOW, MACD_SIG)
    adx_s, di_plus_s, di_minus_s         = calculate_adx_full(highs, lows, closes, ADX_PERIOD)
    atr_s    = calculate_atr(highs, lows, closes, ATR_PERIOD)

    # ── Current values ────────────────────────────────────────────────────────
    price      = closes[-1]
    ema50      = _last(ema50_s)
    ema200     = _last(ema200_s)
    rsi        = _last(rsi_s)
    macd_v     = _last(macd_line_s)
    macd_sv    = _last(macd_sig_s)
    macd_hv    = _last(macd_hist_s)
    adx        = _last(adx_s)
    di_plus    = _last(di_plus_s)
    di_minus   = _last(di_minus_s)
    atr        = _last(atr_s)

    # Guard: NaN in core indicators
    core = {"ema50": ema50, "ema200": ema200, "rsi": rsi,
            "macd": macd_v, "adx": adx, "di_plus": di_plus, "di_minus": di_minus}
    bad  = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol, candles=n)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── ATR spike check ───────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_spike  = bool(atr is not None and atr > atr_avg * ATR_SPIKE_MULT)

    # ── ROC(14): Rate of Change ───────────────────────────────────────────────
    roc = _calculate_roc(closes, ROC_PERIOD)

    # ── MACD divergence detection ─────────────────────────────────────────────
    # Bullish divergence (in downtrend): price lower low, MACD hist higher low
    # Bearish divergence (in uptrend):  price higher high, MACD hist lower high
    macd_bear_div = _macd_bearish_divergence(closes, macd_hist_s, lookback=10)
    macd_bull_div = _macd_bullish_divergence(closes, macd_hist_s, lookback=10)

    log.info(
        "MOMENTUM SNAPSHOT",
        symbol=symbol, candles=n,
        price=round(price, 5),
        ema50=round(ema50, 5), ema200=round(ema200, 5),
        rsi=round(rsi, 2), roc=round(roc, 4),
        adx=round(adx, 2), di_plus=round(di_plus, 2), di_minus=round(di_minus, 2),
        macd=round(macd_v, 6), macd_sig=round(macd_sv, 6), macd_hist=round(macd_hv, 6),
        atr_spike=atr_spike, macd_bear_div=macd_bear_div, macd_bull_div=macd_bull_div,
    )

    # ── Hard gate: ADX too low → no momentum ─────────────────────────────────
    if adx < ADX_MIN:
        reason = f"MOMENTUM HOLD — ADX {adx:.1f} < {ADX_MIN} (no momentum active)"
        log.info(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── Momentum Score ────────────────────────────────────────────────────────
    score_long, score_short = _momentum_score(
        price, ema50, ema200, adx, di_plus, di_minus,
        macd_v, macd_sv, macd_hv,
        rsi, roc, macd_bear_div, macd_bull_div,
    )

    log.info(
        "MOMENTUM SCORE",
        symbol=symbol,
        score_long=score_long, score_short=score_short,
    )

    # ── Entry decision ────────────────────────────────────────────────────────
    direction, confidence, reasons, size_mult = _entry_decision(
        score_long, score_short, symbol,
        price, ema50, ema200, adx, di_plus, di_minus,
        rsi, roc, macd_v, macd_sv, macd_hv, atr, atr_spike,
    )

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(ema50, 5),
        ema_slow       = round(ema200, 5),
        macd           = round(macd_v, 6),
        macd_signal    = round(macd_sv, 6),
        macd_histogram = round(macd_hv, 6),
        atr            = round(atr if atr and not math.isnan(atr) else 0.0, 6),
        volume_ratio   = round(size_mult, 2),  # Momentum Score size multiplier
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


# ── Momentum Score (0–8) ──────────────────────────────────────────────────────

def _momentum_score(
    price: float, ema50: float, ema200: float,
    adx: float, di_plus: float, di_minus: float,
    macd_v: float, macd_sv: float, macd_hv: float,
    rsi: float, roc: float,
    macd_bear_div: bool, macd_bull_div: bool,
) -> Tuple[int, int]:
    """
    Returns (score_long, score_short). Max 8 each.

    Score criteria (document section 8.3, adapted to single TF):
      1. Price vs EMA200 (macro direction)
      2. ADX > 25
      3. DI directionality
      4. MACD alignment
      5. RSI in momentum zone
      6. ROC sign
      7. EMA50 vs EMA200 alignment
      8. No divergence
    """
    sl = 0  # long score
    ss = 0  # short score

    # 1. Macro trend
    if price > ema200:  sl += 1
    if price < ema200:  ss += 1

    # 2. ADX strength
    if adx > ADX_MIN:
        sl += 1; ss += 1

    # 3. Directional indicators
    if di_plus > di_minus:   sl += 1
    if di_minus > di_plus:   ss += 1

    # 4. MACD alignment
    if macd_v > 0 and macd_v > macd_sv:   sl += 1
    if macd_v < 0 and macd_v < macd_sv:   ss += 1

    # 5. RSI in momentum zone
    if LONG_RSI_MIN <= rsi <= LONG_RSI_MAX:    sl += 1
    if SHORT_RSI_MIN <= rsi <= SHORT_RSI_MAX:  ss += 1

    # 6. ROC direction
    if roc > 0:  sl += 1
    if roc < 0:  ss += 1

    # 7. EMA alignment (medium vs long)
    if ema50 > ema200:  sl += 1
    if ema50 < ema200:  ss += 1

    # 8. No divergence (subtract if divergence active)
    if not macd_bear_div:  sl += 1
    if not macd_bull_div:  ss += 1

    return sl, ss


# ── Entry decision ────────────────────────────────────────────────────────────

def _entry_decision(
    score_long: int, score_short: int, symbol: str,
    price: float, ema50: float, ema200: float,
    adx: float, di_plus: float, di_minus: float,
    rsi: float, roc: float,
    macd_v: float, macd_sv: float, macd_hv: float,
    atr: Optional[float], atr_spike: bool,
) -> Tuple[str, int, List[str], float]:
    """
    Returns (direction, confidence, reasons, size_multiplier).
    size_multiplier: 1.5 = strong, 1.0 = normal, 0.5 = reduced, 0.0 = no entry
    """

    # ATR spike always blocks entry
    if atr_spike:
        return "HOLD", 0, ["MOMENTUM HOLD — ATR spike detected (news event?)"], 0.0

    # ── LONG ─────────────────────────────────────────────────────────────────
    if score_long >= SCORE_REDUCED:
        if score_long >= SCORE_STRONG:
            size_mult = 1.5
            size_tag  = "STRONG [1.5× size]"
        elif score_long >= SCORE_FULL:
            size_mult = 1.0
            size_tag  = "FULL [1.0× size]"
        else:
            size_mult = 0.5
            size_tag  = "REDUCED [0.5× size]"

        confidence = _conf(score_long, adx)
        reasons = [
            f"Momentum BUY {size_tag} — Score {score_long}/8",
            f"Price({price:.5f}) > EMA200({ema200:.5f}) ✓  EMA50({ema50:.5f}) > EMA200 ✓",
            f"ADX {adx:.1f} (momentum active)  DI+({di_plus:.1f}) > DI-({di_minus:.1f}) ✓",
            f"MACD {macd_v:.6f} > signal ✓  Hist {macd_hv:.6f}",
            f"RSI(10) {rsi:.1f} in [{LONG_RSI_MIN}–{LONG_RSI_MAX}] ✓  ROC {roc:.4f} > 0 ✓",
            f"SL: ATR × {ATR_SL_MULT}  TP: ATR × {ATR_TP_MULT} (trailing in practice)",
        ]
        log.info("MOMENTUM DECISION: BUY", symbol=symbol,
                 score=score_long, confidence=confidence, size_mult=size_mult)
        return "BUY", confidence, reasons, size_mult

    # ── SHORT ─────────────────────────────────────────────────────────────────
    if score_short >= SCORE_REDUCED:
        if score_short >= SCORE_STRONG:
            size_mult = 1.5
            size_tag  = "STRONG [1.5× size]"
        elif score_short >= SCORE_FULL:
            size_mult = 1.0
            size_tag  = "FULL [1.0× size]"
        else:
            size_mult = 0.5
            size_tag  = "REDUCED [0.5× size]"

        confidence = _conf(score_short, adx)
        reasons = [
            f"Momentum SELL {size_tag} — Score {score_short}/8",
            f"Price({price:.5f}) < EMA200({ema200:.5f}) ✓  EMA50({ema50:.5f}) < EMA200 ✓",
            f"ADX {adx:.1f} (momentum active)  DI-({di_minus:.1f}) > DI+({di_plus:.1f}) ✓",
            f"MACD {macd_v:.6f} < signal ✓  Hist {macd_hv:.6f}",
            f"RSI(10) {rsi:.1f} in [{SHORT_RSI_MIN}–{SHORT_RSI_MAX}] ✓  ROC {roc:.4f} < 0 ✓",
            f"SL: ATR × {ATR_SL_MULT}  TP: ATR × {ATR_TP_MULT} (trailing in practice)",
        ]
        log.info("MOMENTUM DECISION: SELL", symbol=symbol,
                 score=score_short, confidence=confidence, size_mult=size_mult)
        return "SELL", confidence, reasons, size_mult

    # ── HOLD ──────────────────────────────────────────────────────────────────
    reasons = [
        f"MOMENTUM HOLD — Score insuficiente (LONG: {score_long}/8, SHORT: {score_short}/8, mín: {SCORE_REDUCED})",
        f"ADX {adx:.1f}  DI+({di_plus:.1f}) / DI-({di_minus:.1f})  RSI {rsi:.1f}  ROC {roc:.4f}",
    ]
    log.info("MOMENTUM DECISION: HOLD", symbol=symbol,
             score_long=score_long, score_short=score_short)
    return "HOLD", 0, reasons, 0.0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _calculate_roc(closes: List[float], period: int) -> float:
    """Rate of Change: ((close - close[period]) / close[period]) × 100"""
    if len(closes) <= period:
        return 0.0
    prev = closes[-(period + 1)]
    if prev == 0:
        return 0.0
    return ((closes[-1] - prev) / prev) * 100.0


def _macd_bearish_divergence(closes: List[float], hist: List[float], lookback: int = 10) -> bool:
    """
    Bearish divergence: price makes higher high but MACD histogram makes lower high.
    Indicates momentum weakening in an uptrend.
    """
    valid_hist = [h for h in hist[-lookback:] if h is not None and not math.isnan(h)]
    if len(valid_hist) < 4 or len(closes) < lookback:
        return False
    price_window = closes[-lookback:]
    # Price higher high in second half vs first half
    mid = lookback // 2
    price_first_max = max(closes[-lookback: -mid]) if mid > 0 else closes[-lookback]
    price_last_max  = max(closes[-mid:])
    hist_window     = [h for h in hist[-lookback:] if h is not None and not math.isnan(h)]
    if len(hist_window) < mid:
        return False
    hist_first_max  = max(hist_window[:mid])
    hist_last_max   = max(hist_window[mid:])
    # Price makes higher high but MACD makes lower high
    return price_last_max > price_first_max and hist_last_max < hist_first_max


def _macd_bullish_divergence(closes: List[float], hist: List[float], lookback: int = 10) -> bool:
    """
    Bullish divergence: price makes lower low but MACD histogram makes higher low.
    Indicates momentum weakening in a downtrend.
    """
    if len(closes) < lookback:
        return False
    mid = lookback // 2
    price_first_min = min(closes[-lookback: -mid]) if mid > 0 else closes[-lookback]
    price_last_min  = min(closes[-mid:])
    hist_window     = [h for h in hist[-lookback:] if h is not None and not math.isnan(h)]
    if len(hist_window) < mid:
        return False
    hist_first_min  = min(hist_window[:mid])
    hist_last_min   = min(hist_window[mid:])
    return price_last_min < price_first_min and hist_last_min > hist_first_min


def _conf(score: int, adx: float) -> int:
    """Confidence = base from score + ADX strength bonus."""
    base = 50 + (score - SCORE_REDUCED) * 8   # 50, 58, 66, 74, 82
    if adx > 40:     base += 12
    elif adx > 35:   base += 8
    elif adx > ADX_STRONG: base += 5
    return min(100, base)


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
            macd=0, macd_signal=0, macd_histogram=0, atr=0, volume_ratio=0.0,
        ),
        analyzed_at  = now,
        candles_used = len(candles),
        hold_reason  = reason,
        composite_score = 0,
        score_breakdown = [],
    )
