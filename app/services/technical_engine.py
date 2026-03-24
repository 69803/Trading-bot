"""
Technical Analysis Engine — Phase 2 (v2 — professional strategy).

Converts raw OHLCV candles into a structured TechnicalSignal by combining
EMA trend, RSI level + momentum, MACD, ADX trend strength, and volume.

Scoring model
─────────────
  Component      Weight   Trigger condition
  ───────────    ──────   ──────────────────────────────────────────────
  EMA            ±40      fast > slow (trend) or crossover (+40)
  RSI level      ±15      > 45 confirms BUY, < 55 confirms SELL
  RSI momentum   ±10      3-bar RSI slope — rising slope boosts BUY
  MACD           ±20      histogram direction / crossover
  Volume         ±10      amplifier (high vol → boost, low vol → cut)
  ADX            ±15      trend strength bonus/penalty (not a gate)
  ──────────────────────────────────────────────────────────────────────
  Total          ±110

Sideways market gate (ADX)
──────────────────────────
  If ADX < ADX_SIDEWAYS (15) AND no fresh EMA crossover:
    → early HOLD,  log "SKIPPED: weak trend"

Trade quality thresholds
─────────────────────────
  score ≥ BUY_THRESHOLD  (25) → BUY
  score ≤ SELL_THRESHOLD (-25) → SELL
  otherwise                   → HOLD

Signal strength labels in logs
──────────────────────────────
  score 25-39, ADX < 25  → "TRADE: signal accepted (weak trend)"
  score 25-39, ADX >= 25 → "TRADE: moderate trend confirmed"
  score ≥ 40,  ADX >= 30 → "TRADE: strong trend confirmed"

The module is intentionally pure (no DB, no HTTP calls).
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from app.core.logger import get_logger
from app.schemas.technical import IndicatorValues, ScoreFactor, TechnicalSignal
from app.utils.indicators import (
    calculate_adx,
    calculate_atr,
    calculate_ema,
    calculate_macd,
    calculate_rsi,
    calculate_volume_ratio,
)

log = get_logger(__name__)

# Minimum candles required to produce a valid signal
MIN_CANDLES = 60   # raised from 50: need enough history for ADX

# Score thresholds — require multi-indicator confirmation
BUY_THRESHOLD  =  10   # TESTING: lowered from 25 — easier to trigger BUY
SELL_THRESHOLD = -10   # TESTING: raised from -25 — easier to trigger SELL

# EMA trend-clarity filters
MIN_EMA_GAP_PCT      = 0.02   # min separation to count as a trend
CROSSOVER_LOOKBACK   = 10     # bars to inspect for recent whipsaws
MAX_RECENT_CROSSOVERS = 3     # choppy if more than 3 reversals

# ADX trend strength thresholds
# ADX_SIDEWAYS lowered to 15: EURUSD on 1h regularly trades at ADX 12-18 even
# during genuine directional moves. ADX=20 was blocking every non-trending hour.
ADX_PERIOD    = 14
ADX_SIDEWAYS  = 15   # below this + no fresh crossover → skip (sideways market)
ADX_MODERATE  = 25   # moderate trend — normal trading
ADX_STRONG    = 35   # strong trend — high-confidence entries

# RSI momentum: look-back for slope calculation
RSI_SLOPE_BARS = 3


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "1h",
    ema_fast_period: int = 5,
    ema_slow_period: int = 10,
    rsi_period: int = 14,
    rsi_overbought: float = 55.0,
    rsi_oversold: float = 45.0,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    atr_period: int = 14,
    volume_period: int = 20,
) -> TechnicalSignal:
    """
    Analyze OHLCV candles and return a structured TechnicalSignal.

    Args:
        symbol:          Trading symbol (e.g. "NVDA").
        candles:         List of OHLCV dicts with keys:
                         open, high, low, close, volume, timestamp.
                         Must be ordered oldest → newest.
        timeframe:       Candle timeframe label (e.g. "1h").
        ema_fast_period: Fast EMA period.
        ema_slow_period: Slow EMA period.
        rsi_period:      RSI look-back period.
        rsi_overbought:  RSI overbought threshold.
        rsi_oversold:    RSI oversold threshold.
        macd_fast:       MACD fast EMA.
        macd_slow:       MACD slow EMA.
        macd_signal:     MACD signal EMA.
        atr_period:      ATR look-back period.
        volume_period:   Volume ratio look-back period.

    Returns:
        TechnicalSignal with direction, confidence (0-100) and reasons.
    """
    n = len(candles)
    now = datetime.now(timezone.utc)

    # ── Guard: not enough data ───────────────────────────────────────────────
    if n < MIN_CANDLES:
        log.warning(
            "Insufficient candles for technical analysis",
            symbol=symbol, got=n, need=MIN_CANDLES,
        )
        # DEBUG: show last candle timestamps so we know if data is stale
        if candles:
            last5 = candles[-5:] if n >= 5 else candles
            log.warning(
                "CANDLE DEBUG: last candles received",
                symbol=symbol,
                count=n,
                last_timestamps=[c.get("timestamp", "?") for c in last5],
                last_closes=[round(float(c["close"]), 5) for c in last5],
            )
        return _hold_signal(symbol, timeframe, candles, now, reason="Insufficient candle history")

    # ── Extract OHLCV arrays ─────────────────────────────────────────────────
    closes  = [float(c["close"])  for c in candles]
    highs   = [float(c["high"])   for c in candles]
    lows    = [float(c["low"])    for c in candles]
    volumes = [float(c.get("volume", 0)) for c in candles]

    # ── Calculate indicators ─────────────────────────────────────────────────
    ema_fast_series = calculate_ema(closes, ema_fast_period)
    ema_slow_series = calculate_ema(closes, ema_slow_period)
    rsi_series      = calculate_rsi(closes, rsi_period)
    macd_line, macd_sig_line, macd_hist = calculate_macd(
        closes, macd_fast, macd_slow, macd_signal
    )
    atr_series      = calculate_atr(highs, lows, closes, atr_period)
    adx_series      = calculate_adx(highs, lows, closes, ADX_PERIOD)
    vol_ratio       = calculate_volume_ratio(volumes, volume_period)

    # ── Latest values ────────────────────────────────────────────────────────
    ema_fast  = _safe_last(ema_fast_series)
    ema_slow  = _safe_last(ema_slow_series)
    rsi       = _safe_last(rsi_series)
    macd_val  = _safe_last(macd_line)
    macd_sval = _safe_last(macd_sig_line)
    macd_hval = _safe_last(macd_hist)
    atr_val   = _safe_last(atr_series)
    adx_val   = _safe_last(adx_series)
    price     = closes[-1]

    # Previous-bar values for crossover detection and RSI momentum
    ema_fast_prev  = _safe_nth(ema_fast_series, -2)
    ema_slow_prev  = _safe_nth(ema_slow_series, -2)
    macd_hist_prev = _safe_nth(macd_hist, -2)
    rsi_prev3      = _safe_nth(rsi_series, -(RSI_SLOPE_BARS + 1))  # 3 bars ago

    # ── DEBUG: log candle quality and raw indicator snapshot ─────────────────
    last5 = candles[-5:] if n >= 5 else candles
    log.info(
        "CANDLE DEBUG",
        symbol=symbol,
        total_candles=n,
        last_timestamps=[c.get("timestamp", "?") for c in last5],
        last_closes=[round(float(c["close"]), 5) for c in last5],
        latest_close=round(closes[-1], 5),
    )
    log.info(
        "INDICATOR SNAPSHOT",
        symbol=symbol,
        price=round(price, 5),
        ema_fast=round(ema_fast, 5) if ema_fast is not None and not math.isnan(ema_fast) else None,
        ema_slow=round(ema_slow, 5) if ema_slow is not None and not math.isnan(ema_slow) else None,
        rsi=round(rsi, 2) if rsi is not None and not math.isnan(rsi) else None,
        macd_hist=round(macd_hval, 6) if macd_hval is not None and not math.isnan(macd_hval) else None,
        adx=round(adx_val, 2) if adx_val is not None and not math.isnan(adx_val) else None,
    )

    # ── Return HOLD if any core indicator is NaN ─────────────────────────────
    core_vals = (ema_fast, ema_slow, rsi, macd_val, macd_sval, macd_hval)
    nan_indicators = [
        name for name, val in zip(
            ("ema_fast", "ema_slow", "rsi", "macd", "macd_signal", "macd_histogram"),
            core_vals,
        )
        if val is None or math.isnan(val)
    ]
    if nan_indicators:
        log.warning(
            "TECH DEBUG: NaN indicators — HOLD forced",
            symbol=symbol,
            nan_indicators=nan_indicators,
            candles=n,
        )
        return _hold_signal(
            symbol, timeframe, candles, now,
            reason="Indicators still warming up — need more candle history",
        )

    # Safe ADX: default to 25 (neutral) when still warming up
    adx = adx_val if (adx_val is not None and not math.isnan(adx_val)) else 25.0

    # ── Score each component ─────────────────────────────────────────────────
    score          = 0
    reasons:        List[str]       = []
    factors:        List[ScoreFactor] = []
    hold_reason:    Optional[str]   = None
    trend_strength: str             = "weak"
    ema_crossover:  Optional[str]   = None
    macd_crossover: Optional[str]   = None
    rsi_extreme:    Optional[str]   = None

    # ── 1. EMA (±40) — primary signal, trend direction ──────────────────────
    ema_gap_pct = abs(ema_fast - ema_slow) / ema_slow * 100 if ema_slow else 0

    lookback = CROSSOVER_LOOKBACK
    recent_fast = ema_fast_series[-lookback:] if len(ema_fast_series) >= lookback else ema_fast_series
    recent_slow = ema_slow_series[-lookback:] if len(ema_slow_series) >= lookback else ema_slow_series
    recent_crossovers = _count_crossovers(recent_fast, recent_slow)

    bullish_cross = (
        ema_fast_prev is not None
        and ema_slow_prev is not None
        and ema_fast_prev <= ema_slow_prev
        and ema_fast > ema_slow
    )
    bearish_cross = (
        ema_fast_prev is not None
        and ema_slow_prev is not None
        and ema_fast_prev >= ema_slow_prev
        and ema_fast < ema_slow
    )

    # ── SIDEWAYS MARKET GATE (ADX) ───────────────────────────────────────────
    fresh_crossover = bullish_cross or bearish_cross
    if adx < ADX_SIDEWAYS and not fresh_crossover:
        hold_reason = (
            f"ADX {adx:.1f} below sideways threshold ({ADX_SIDEWAYS}) "
            f"with no fresh EMA crossover — market is ranging"
        )
        log.info(
            "TECH DECISION: HOLD — sideways market gate",
            symbol=symbol,
            hold_reason=hold_reason,
            adx=round(adx, 1), adx_threshold=ADX_SIDEWAYS,
            ema_gap_pct=round(ema_gap_pct, 4),
            recent_crossovers=recent_crossovers,
            rsi=round(rsi, 2),
            macd_hist=round(macd_hval, 6),
        )
        return _hold_signal(
            symbol, timeframe, candles, now,
            reason=f"SKIPPED: weak trend — ADX {adx:.1f} < {ADX_SIDEWAYS} (sideways market)",
            hold_reason=hold_reason,
        )

    # EMA scoring
    if ema_gap_pct < MIN_EMA_GAP_PCT:
        reason_str = (
            f"EMA gap too small ({ema_gap_pct:.3f}% < {MIN_EMA_GAP_PCT}%) — flat, no direction"
        )
        reasons.append(reason_str)
        factors.append(ScoreFactor(
            name="EMA_GAP", domain="trend", points=0, max_points=40, reason=reason_str,
        ))
    elif recent_crossovers > MAX_RECENT_CROSSOVERS:
        ema_pts = 10 if ema_fast > ema_slow else -10
        score += ema_pts
        reason_str = (
            f"EMA choppy — {recent_crossovers} crossovers in {lookback} bars "
            f"(max {MAX_RECENT_CROSSOVERS}) — half weight"
        )
        reasons.append(reason_str)
        factors.append(ScoreFactor(
            name="EMA_CHOPPY", domain="trend", points=ema_pts, max_points=40, reason=reason_str,
        ))
    elif bullish_cross:
        score += 40
        reason_str = (
            f"EMA bullish crossover: fast {ema_fast:.5f} crossed above slow {ema_slow:.5f} "
            f"(gap {ema_gap_pct:.3f}%)"
        )
        reasons.append(reason_str)
        factors.append(ScoreFactor(
            name="EMA_CROSSOVER", domain="trend", points=40, max_points=40, reason=reason_str,
        ))
        ema_crossover = "bullish"
    elif ema_fast > ema_slow:
        score += 30
        reason_str = (
            f"EMA bullish trend: fast {ema_fast:.5f} > slow {ema_slow:.5f} "
            f"(gap {ema_gap_pct:.3f}%)"
        )
        reasons.append(reason_str)
        factors.append(ScoreFactor(
            name="EMA_TREND", domain="trend", points=30, max_points=40, reason=reason_str,
        ))
    elif bearish_cross:
        score -= 40
        reason_str = (
            f"EMA bearish crossover: fast {ema_fast:.5f} crossed below slow {ema_slow:.5f} "
            f"(gap {ema_gap_pct:.3f}%)"
        )
        reasons.append(reason_str)
        factors.append(ScoreFactor(
            name="EMA_CROSSOVER", domain="trend", points=-40, max_points=40, reason=reason_str,
        ))
        ema_crossover = "bearish"
    elif ema_fast < ema_slow:
        score -= 30
        reason_str = (
            f"EMA bearish trend: fast {ema_fast:.5f} < slow {ema_slow:.5f} "
            f"(gap {ema_gap_pct:.3f}%)"
        )
        reasons.append(reason_str)
        factors.append(ScoreFactor(
            name="EMA_TREND", domain="trend", points=-30, max_points=40, reason=reason_str,
        ))
    else:
        reasons.append("EMA equal — no direction")

    # ── 2. RSI (±15) — confirmation, not a blocker ───────────────────────────
    if rsi <= 30:
        score += 15
        reason_str = f"RSI {rsi:.1f} — strongly oversold (BUY confirmation)"
        rsi_extreme = "oversold"
        rsi_pts = 15
    elif rsi <= 45:
        score += 10
        reason_str = f"RSI {rsi:.1f} — oversold (BUY confirmation)"
        rsi_extreme = "oversold"
        rsi_pts = 10
    elif rsi <= 55:
        score += 5
        reason_str = f"RSI {rsi:.1f} — neutral-bullish (mild BUY lean)"
        rsi_pts = 5
    elif rsi <= 65:
        score -= 5
        reason_str = f"RSI {rsi:.1f} — neutral-bearish (mild SELL lean)"
        rsi_pts = -5
    elif rsi <= 70:
        score -= 10
        reason_str = f"RSI {rsi:.1f} — overbought (SELL confirmation)"
        rsi_extreme = "overbought"
        rsi_pts = -10
    else:
        score -= 15
        reason_str = f"RSI {rsi:.1f} — strongly overbought (SELL confirmation)"
        rsi_extreme = "overbought"
        rsi_pts = -15
    reasons.append(reason_str)
    factors.append(ScoreFactor(
        name="RSI_LEVEL", domain="momentum", points=rsi_pts, max_points=15, reason=reason_str,
    ))

    # ── 3. MACD (±20) — secondary confirmation ───────────────────────────────
    macd_bull_cross = (
        macd_hist_prev is not None
        and not math.isnan(macd_hist_prev)
        and macd_hist_prev < 0
        and macd_hval > 0
    )
    macd_bear_cross = (
        macd_hist_prev is not None
        and not math.isnan(macd_hist_prev)
        and macd_hist_prev > 0
        and macd_hval < 0
    )

    if macd_bull_cross:
        macd_pts = 20
        reason_str = f"MACD bullish crossover (hist turned +{macd_hval:.5f})"
        macd_crossover = "bullish"
    elif macd_hval > 0:
        macd_pts = 15 if macd_hval > (macd_hist_prev or 0) else 8
        trend_lbl = "gaining" if macd_hval > (macd_hist_prev or 0) else "positive"
        reason_str = f"MACD {trend_lbl} (line {macd_val:.5f}, hist {macd_hval:.5f})"
    elif macd_bear_cross:
        macd_pts = -20
        reason_str = f"MACD bearish crossover (hist turned {macd_hval:.5f})"
        macd_crossover = "bearish"
    elif macd_hval < 0:
        macd_pts = -(15 if macd_hval < (macd_hist_prev or 0) else 8)
        trend_lbl = "weakening" if macd_hval < (macd_hist_prev or 0) else "negative"
        reason_str = f"MACD {trend_lbl} (line {macd_val:.5f}, hist {macd_hval:.5f})"
    else:
        macd_pts = 0
        reason_str = f"MACD flat (line {macd_val:.5f}, hist {macd_hval:.5f})"
    score += macd_pts
    reasons.append(reason_str)
    factors.append(ScoreFactor(
        name="MACD_HIST", domain="momentum", points=macd_pts, max_points=20, reason=reason_str,
    ))

    # ── 4. RSI momentum (±10) — 3-bar RSI slope ──────────────────────────────
    rsi_slope = 0.0
    if rsi_prev3 is not None and not math.isnan(rsi_prev3):
        rsi_slope = (rsi - rsi_prev3) / RSI_SLOPE_BARS

    if rsi_slope >= 1.5:
        rsi_mom_pts = 10
        reason_str = f"RSI momentum bullish (slope +{rsi_slope:.1f} pts/bar)"
    elif rsi_slope >= 0.5:
        rsi_mom_pts = 5
        reason_str = f"RSI rising (slope +{rsi_slope:.1f} pts/bar)"
    elif rsi_slope <= -1.5:
        rsi_mom_pts = -10
        reason_str = f"RSI momentum bearish (slope {rsi_slope:.1f} pts/bar)"
    elif rsi_slope <= -0.5:
        rsi_mom_pts = -5
        reason_str = f"RSI falling (slope {rsi_slope:.1f} pts/bar)"
    else:
        rsi_mom_pts = 0
        reason_str = f"RSI slope flat ({rsi_slope:+.2f} pts/bar)"
    score += rsi_mom_pts
    if rsi_mom_pts != 0:
        reasons.append(reason_str)
    factors.append(ScoreFactor(
        name="RSI_MOMENTUM", domain="momentum", points=rsi_mom_pts, max_points=10, reason=reason_str,
    ))

    # ── 5. Volume amplifier (±10) ────────────────────────────────────────────
    if vol_ratio >= 2.0:
        vol_pts = 10 if score > 0 else -10
        reason_str = f"High volume confirmation ({vol_ratio:.1f}× average)"
    elif vol_ratio >= 1.5:
        vol_pts = 5 if score > 0 else -5
        reason_str = f"Above-average volume ({vol_ratio:.1f}×)"
    elif vol_ratio < 0.5:
        vol_pts = -8 if score > 0 else 8
        reason_str = f"Low volume ({vol_ratio:.1f}×) — weak confirmation"
    else:
        vol_pts = 0
        reason_str = f"Normal volume ({vol_ratio:.1f}×)"
    score += vol_pts
    if vol_pts != 0:
        reasons.append(reason_str)
    factors.append(ScoreFactor(
        name="VOLUME", domain="volatility", points=vol_pts, max_points=10, reason=reason_str,
    ))

    # ── 6. ADX trend strength modifier (±15) ─────────────────────────────────
    if adx >= ADX_STRONG:
        adx_pts = 15 if score > 0 else -15
        reason_str = f"ADX {adx:.1f} — strong trend (>= {ADX_STRONG})"
    elif adx >= ADX_MODERATE:
        adx_pts = 8 if score > 0 else -8
        reason_str = f"ADX {adx:.1f} — moderate trend"
    elif adx >= ADX_SIDEWAYS:
        adx_pts = 0
        reason_str = f"ADX {adx:.1f} — borderline trend (weakly trending)"
    else:
        # Only reachable with fresh crossover overriding the gate
        adx_pts = 0
        reason_str = f"ADX {adx:.1f} — sideways (crossover exception)"
    score += adx_pts
    if reason_str:
        reasons.append(reason_str)
    factors.append(ScoreFactor(
        name="ADX_STRENGTH", domain="trend", points=adx_pts, max_points=15, reason=reason_str,
    ))

    # ── Clamp score to [-110, 110] ───────────────────────────────────────────
    score = max(-110, min(110, score))

    # ── Build score breakdown summary string ─────────────────────────────────
    factor_summary = "  |  ".join(
        f"{f.name}({f.points:+d})" for f in factors if f.points != 0
    )

    # ── TECH DEBUG: full score breakdown ─────────────────────────────────────
    log.info(
        "TECH SCORE BREAKDOWN",
        symbol=symbol,
        candles=n,
        latest_close=round(price, 5),
        ema_fast=round(ema_fast, 5),
        ema_slow=round(ema_slow, 5),
        ema_gap_pct=round(ema_gap_pct, 4),
        rsi=round(rsi, 2),
        rsi_slope=round(rsi_slope, 3),
        macd=round(macd_val, 6),
        macd_hist=round(macd_hval, 6),
        atr=round(atr_val if atr_val and not math.isnan(atr_val) else 0.0, 5),
        adx=round(adx, 2),
        volume_ratio=round(vol_ratio, 3),
        bullish_cross=bullish_cross,
        bearish_cross=bearish_cross,
        recent_crossovers=recent_crossovers,
        choppy=(recent_crossovers > MAX_RECENT_CROSSOVERS),
        buy_threshold=BUY_THRESHOLD,
        sell_threshold=SELL_THRESHOLD,
        composite_score=score,
        score_factors=factor_summary,
        will_buy=(score >= BUY_THRESHOLD),
        will_sell=(score <= SELL_THRESHOLD),
    )

    # ── Direction from composite score ───────────────────────────────────────
    if score >= BUY_THRESHOLD:
        direction  = "BUY"
        confidence = min(100, score)
        if score >= 40 and adx >= ADX_STRONG:
            trend_strength = "strong"
            trade_label = "TECH DECISION: BUY — strong trend"
        elif score >= 25 and adx >= ADX_MODERATE:
            trend_strength = "moderate"
            trade_label = "TECH DECISION: BUY — moderate trend"
        else:
            trend_strength = "weak"
            trade_label = "TECH DECISION: BUY — weak trend"
        log.info(
            trade_label,
            symbol=symbol, score=score, confidence=confidence,
            adx=round(adx, 1), factors=factor_summary,
        )
    elif score <= SELL_THRESHOLD:
        direction  = "SELL"
        confidence = min(100, abs(score))
        if score <= -40 and adx >= ADX_STRONG:
            trend_strength = "strong"
            trade_label = "TECH DECISION: SELL — strong trend"
        elif score <= -25 and adx >= ADX_MODERATE:
            trend_strength = "moderate"
            trade_label = "TECH DECISION: SELL — moderate trend"
        else:
            trend_strength = "weak"
            trade_label = "TECH DECISION: SELL — weak trend"
        log.info(
            trade_label,
            symbol=symbol, score=score, confidence=confidence,
            adx=round(adx, 1), factors=factor_summary,
        )
    else:
        direction      = "HOLD"
        confidence     = 0
        trend_strength = "sideways" if adx < ADX_SIDEWAYS else "weak"
        hold_reason    = (
            f"Score {score:+d} in dead zone (threshold ±{BUY_THRESHOLD}); "
            f"ADX={adx:.1f}  factors: {factor_summary}"
        )
        reasons.append(f"HOLD: {hold_reason}")
        log.info(
            "TECH DECISION: HOLD — score below threshold",
            symbol=symbol, score=score,
            buy_threshold=BUY_THRESHOLD, sell_threshold=SELL_THRESHOLD,
            adx=round(adx, 1), factors=factor_summary,
        )

    indicators = IndicatorValues(
        price          = round(price,     5),
        rsi            = round(rsi,       2),
        ema_fast       = round(ema_fast,  5),
        ema_slow       = round(ema_slow,  5),
        macd           = round(macd_val,  6),
        macd_signal    = round(macd_sval, 6),
        macd_histogram = round(macd_hval, 6),
        atr            = round(atr_val if atr_val and not math.isnan(atr_val) else 0.0, 5),
        volume_ratio   = round(vol_ratio, 3),
        adx            = round(adx, 2),
    )

    log.debug(
        "Technical signal generated",
        symbol=symbol, direction=direction,
        confidence=confidence, score=score,
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
        trend_strength  = trend_strength,
        ema_crossover   = ema_crossover,
        macd_crossover  = macd_crossover,
        rsi_extreme     = rsi_extreme,
        # Observability fields
        composite_score = score,
        score_breakdown = factors,
        hold_reason     = hold_reason,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_crossovers(fast: List[float], slow: List[float]) -> int:
    """Count EMA crossovers (direction changes) within the given window."""
    count = 0
    n = min(len(fast), len(slow))
    for i in range(1, n):
        f_prev, s_prev = fast[i - 1], slow[i - 1]
        f_cur,  s_cur  = fast[i],     slow[i]
        if any(math.isnan(v) for v in (f_prev, s_prev, f_cur, s_cur)):
            continue
        if f_prev <= s_prev and f_cur > s_cur:
            count += 1
        elif f_prev >= s_prev and f_cur < s_cur:
            count += 1
    return count


def _safe_last(series: List[float]) -> Optional[float]:
    """Return the last non-NaN value, or None if the series is empty/all-NaN."""
    if not series:
        return None
    v = series[-1]
    return None if math.isnan(v) else v


def _safe_nth(series: List[float], idx: int) -> Optional[float]:
    """Return series[idx] if valid and not NaN, else None."""
    try:
        v = series[idx]
        return None if math.isnan(v) else v
    except IndexError:
        return None


def _hold_signal(
    symbol: str,
    timeframe: str,
    candles: List[dict],
    now: datetime,
    reason: str,
    hold_reason: Optional[str] = None,
) -> TechnicalSignal:
    """Return a safe HOLD signal when analysis cannot be completed."""
    price = float(candles[-1]["close"]) if candles else 0.0
    return TechnicalSignal(
        symbol      = symbol,
        timeframe   = timeframe,
        direction   = "HOLD",
        confidence  = 0,
        reasons     = [reason],
        indicators  = IndicatorValues(
            price          = price,
            rsi            = float("nan"),
            ema_fast       = float("nan"),
            ema_slow       = float("nan"),
            macd           = float("nan"),
            macd_signal    = float("nan"),
            macd_histogram = float("nan"),
            atr            = float("nan"),
            volume_ratio   = 1.0,
        ),
        analyzed_at  = now,
        candles_used = len(candles),
        hold_reason  = hold_reason or reason,
    )
