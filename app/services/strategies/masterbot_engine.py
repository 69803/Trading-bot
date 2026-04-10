"""
APEX System — MasterBot Multi-Strategy Engine  v2.0
====================================================
Master Bot • Forex • 5-Module Adaptive Strategy

Architecture:
  1. Market Regime Detector  → classify current environment
  2. Risk Semaphore          → 5 levels, controls position sizing
  3. Module Selector         → activate the right strategy per regime
  4. Signal Generator        → entry logic per module
  5. Risk Parameters         → dynamic ATR mults per module

Market Regimes (priority order):
  RISK_OFF   → VIX proxy > 25   → emergency stop: no entries, signal close
  MOMENTUM   → ADX > 30 + rising + multi-TF confirmation
  BREAKOUT   → price breaks key level + body pct filter + ADX > 20
  TREND      → ADX 25–30 + price vs SMA200
  RANGE      → ADX < 20         → Mean Reversion at BB extremes
  CARRY      → VIX < 20 + positive-swap pair + range conditions
  NO_TRADE   → transition / ambiguous

Risk Semaphore:
  Level 1 GREEN   → VIX < 15   → size_mult = 1.00  (full risk)
  Level 2 YELLOW  → VIX 15-20  → size_mult = 0.70  (reduced risk)
  Level 3 ORANGE  → VIX 20-25  → size_mult = 0.50  (minimal risk)
  Level 4 RED     → VIX 25-30  → size_mult = 0.00  (no new entries)
  Level 5 BLACK   → VIX > 30   → size_mult = 0.00  (emergency close)

Module ATR parameters:
  Trend Following → SL × 1.5 / TP × 3.0   (1:2 R:R)
  Mean Reversion  → SL × 2.0 / TP × 2.0   (1:1 R:R, TP = SMA20)
  Momentum        → SL × 1.5 / TP × 4.0   (trailing high reward)
  Breakout        → SL × 1.0 / TP × 2.5   (tight SL, 1:2.5 R:R)
  Carry Trade     → SL × 3.0 / TP × 4.0   (wide, long hold)

Risk per trade:
  Trend       → 1.00%
  Mean Rev    → 1.00%
  Breakout    → 0.75%
  Momentum    → 1.25%
  Carry       → 0.65%

Global limits:
  Max 3% simultaneous exposure
  Max 10 trades / day
  3% daily DD  → stop
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

# ── Module-level ATR multiplier exports (read by bot_service after analyze()) ─
# These are updated on every call to analyze() to reflect the active module.
ATR_SL_MULT: float = 1.5
ATR_TP_MULT: float = 3.0

# ── Indicator periods ─────────────────────────────────────────────────────────
ADX_PERIOD   = 14
ATR_PERIOD   = 14
RSI_PERIOD   = 14
BB_PERIOD    = 20
BB_STD       = 2.0
STOCH_K      = 5
STOCH_D      = 3
STOCH_SMOOTH = 3

# ── ADX regime thresholds ─────────────────────────────────────────────────────
ADX_RANGE_MAX   = 20.0    # below this  → RANGE
ADX_TREND_MIN   = 25.0    # above this  → TREND
ADX_MOMENTUM    = 30.0    # above this + rising → MOMENTUM

# ── VIX proxy (ATR-ratio × scale) thresholds ─────────────────────────────────
VIX_SCALE        = 15.0
VIX_CALM         = 15.0   # semaphore GREEN  → full size
VIX_NORMAL       = 20.0   # semaphore YELLOW → 0.7×
VIX_CAUTION      = 25.0   # semaphore ORANGE → 0.5×  /  RED → no entries
VIX_DANGER       = 30.0   # semaphore BLACK  → emergency
VIX_EXTREME      = 35.0   # never trade above this

# ── Risk Semaphore ────────────────────────────────────────────────────────────
SEM_GREEN   = "GREEN"     # VIX < 15     size_mult = 1.00
SEM_YELLOW  = "YELLOW"    # VIX 15-20    size_mult = 0.70
SEM_ORANGE  = "ORANGE"    # VIX 20-25    size_mult = 0.50
SEM_RED     = "RED"       # VIX 25-30    size_mult = 0.00  (pause entries)
SEM_BLACK   = "BLACK"     # VIX > 30     size_mult = 0.00  (emergency close)

SEM_SIZE_MULT = {
    SEM_GREEN:  1.00,
    SEM_YELLOW: 0.70,
    SEM_ORANGE: 0.50,
    SEM_RED:    0.00,
    SEM_BLACK:  0.00,
}

# ── Breakout detection ────────────────────────────────────────────────────────
BREAKOUT_LOOKBACK = 50
BODY_PCT_MIN      = 0.60

# ── Mean Reversion (RANGE module) ─────────────────────────────────────────────
MR_RSI_LONG     = 35.0
MR_RSI_SHORT    = 65.0
MR_STOCH_OVER   = 25.0
MR_STOCH_OB     = 75.0

# ── Trend Following (TREND module) ────────────────────────────────────────────
TF_RSI_BULL_MIN = 45.0
TF_RSI_BEAR_MAX = 55.0

# ── Momentum (MOMENTUM module) ────────────────────────────────────────────────
MOM_RSI_BULL    = 50.0
MOM_RSI_BEAR    = 50.0

MIN_CANDLES = 80

# Pairs that earn positive swap on long (Carry module)
POSITIVE_SWAP_PAIRS = {
    "AUD/JPY", "NZD/JPY", "GBP/JPY",
    "USD/JPY", "EUR/JPY", "AUD/CHF",
}

# Market regime labels
RISK_OFF  = "RISK_OFF"
RANGE     = "RANGE"
BREAKOUT  = "BREAKOUT"
MOMENTUM  = "MOMENTUM"
TREND     = "TREND"
CARRY     = "CARRY"
NO_TRADE  = "NO_TRADE"

# ATR mults per module
_MODULE_ATR: dict[str, tuple[float, float]] = {
    MOMENTUM: (1.5, 4.0),  # SL, TP
    BREAKOUT: (1.0, 2.5),
    TREND:    (1.5, 3.0),
    RANGE:    (2.0, 2.0),
    CARRY:    (3.0, 4.0),
}


# ── Public API ────────────────────────────────────────────────────────────────

def analyze(
    symbol: str,
    candles: List[dict],
    timeframe: str = "1h",
) -> TechnicalSignal:
    """
    Run the APEX multi-strategy engine.

    Pipeline:
      1. Compute indicators
      2. Calculate VIX proxy → Risk Semaphore level
      3. Classify market regime
      4. Semaphore gate: RED/BLACK → HOLD (no entries)
      5. Select and apply module strategy
      6. Set dynamic ATR_SL_MULT / ATR_TP_MULT globals
      7. Embed size_multiplier in volume_ratio field
    """
    global ATR_SL_MULT, ATR_TP_MULT

    now = datetime.now(timezone.utc)
    n   = len(candles)

    if n < MIN_CANDLES:
        reason = f"APEX SKIPPED [insufficient_data]: need {MIN_CANDLES} candles, got {n}"
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    closes = [float(c["close"]) for c in candles]
    highs  = [float(c["high"])  for c in candles]
    lows   = [float(c["low"])   for c in candles]
    opens  = [float(c["open"])  for c in candles]

    # ── 1. Indicators ─────────────────────────────────────────────────────────
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

    # Guard against NaN core indicators
    core = {"bb_upper": bb_upper, "bb_mid": bb_mid, "bb_lower": bb_lower,
            "rsi": rsi, "adx": adx}
    bad = [k for k, v in core.items() if v is None or math.isnan(v)]
    if bad:
        reason = f"APEX SKIPPED [insufficient_data]: NaN in {bad}"
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason)

    # ── 2. VIX proxy → Risk Semaphore ─────────────────────────────────────────
    valid_atrs = [v for v in atr_s if v is not None and not math.isnan(v)]
    atr_avg    = sum(valid_atrs[-ATR_PERIOD:]) / max(len(valid_atrs[-ATR_PERIOD:]), 1)
    atr_ratio  = (atr / atr_avg) if atr_avg > 0 and atr else 1.0
    vix_proxy  = atr_ratio * VIX_SCALE

    semaphore    = _risk_semaphore(vix_proxy)
    size_mult    = SEM_SIZE_MULT[semaphore]

    # ── 3. Derived signals ────────────────────────────────────────────────────
    sma200 = sum(closes[-200:]) / min(len(closes), 200)

    # Breakout detection (key level breach + strong body)
    lookback_highs = highs[-(BREAKOUT_LOOKBACK + 1):-1]
    lookback_lows  = lows[-(BREAKOUT_LOOKBACK + 1):-1]
    resistance     = max(lookback_highs) if lookback_highs else bb_upper
    support_level  = min(lookback_lows)  if lookback_lows  else bb_lower
    candle_range   = highs[-1] - lows[-1]
    candle_body    = abs(closes[-1] - opens[-1])
    body_pct       = (candle_body / candle_range) if candle_range > 0 else 0.0
    breakout_long  = price > resistance and body_pct >= BODY_PCT_MIN and adx > 20
    breakout_short = price < support_level and body_pct >= BODY_PCT_MIN and adx > 20

    # ADX rising (multi-TF alignment proxy)
    adx_list   = [v for v in adx_s if v is not None and not math.isnan(v)]
    adx_rising = len(adx_list) >= 3 and adx_list[-1] > adx_list[-3]

    # Stochastic crosses
    stoch_bull = _stoch_crossed_up(stoch_k_s, stoch_d_s)
    stoch_bear = _stoch_crossed_down(stoch_k_s, stoch_d_s)

    # ── 4. Market Regime Classification ──────────────────────────────────────
    market_state = _classify_market(
        vix_proxy, adx, breakout_long, breakout_short,
        adx_rising, symbol,
    )

    log.info(
        "APEX MARKET REGIME",
        symbol       = symbol,
        market_state = market_state,
        semaphore    = semaphore,
        size_mult    = size_mult,
        adx          = round(adx, 2),
        vix_proxy    = round(vix_proxy, 2),
        rsi          = round(rsi, 2),
        price        = round(price, 5),
    )

    # ── 5. Risk Semaphore gate ────────────────────────────────────────────────
    if semaphore == SEM_BLACK:
        ATR_SL_MULT, ATR_TP_MULT = 1.5, 3.0
        reason = (
            f"APEX [BLACK SEMAPHORE] — VIX proxy {vix_proxy:.1f} > {VIX_DANGER} "
            f"— emergency: no entries, close open positions"
        )
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason, size_mult=0.0,
                     vix_proxy=vix_proxy, adx=adx, rsi=rsi, atr=atr,
                     bb_upper=bb_upper, bb_mid=bb_mid, bb_lower=bb_lower,
                     stoch_k=stoch_k)

    if semaphore == SEM_RED:
        ATR_SL_MULT, ATR_TP_MULT = 1.5, 3.0
        reason = (
            f"APEX [RED SEMAPHORE] — VIX proxy {vix_proxy:.1f} in danger zone "
            f"[{VIX_CAUTION}-{VIX_DANGER}] — pausing new entries"
        )
        log.warning(reason, symbol=symbol)
        return _hold(symbol, timeframe, candles, now, reason, size_mult=0.0,
                     vix_proxy=vix_proxy, adx=adx, rsi=rsi, atr=atr,
                     bb_upper=bb_upper, bb_mid=bb_mid, bb_lower=bb_lower,
                     stoch_k=stoch_k)

    if market_state == NO_TRADE:
        ATR_SL_MULT, ATR_TP_MULT = 1.5, 3.0
        reason = (
            f"APEX [NO_TRADE] — regime unclear: "
            f"ADX {adx:.1f} (transition zone), VIX {vix_proxy:.1f}"
        )
        return _hold(symbol, timeframe, candles, now, reason, size_mult=size_mult,
                     vix_proxy=vix_proxy, adx=adx, rsi=rsi, atr=atr,
                     bb_upper=bb_upper, bb_mid=bb_mid, bb_lower=bb_lower,
                     stoch_k=stoch_k)

    # ── 6. Module strategy execution ──────────────────────────────────────────
    direction, confidence, reasons = _execute_module(
        market_state, symbol, price,
        rsi, stoch_k, stoch_d, adx, atr,
        bb_upper, bb_mid, bb_lower,
        resistance, support_level, body_pct,
        sma200, vix_proxy, semaphore,
        stoch_bull, stoch_bear,
        breakout_long, breakout_short,
    )

    # ── 7. Update dynamic ATR mults ───────────────────────────────────────────
    sl_m, tp_m = _MODULE_ATR.get(market_state, (1.5, 3.0))
    ATR_SL_MULT = sl_m
    ATR_TP_MULT = tp_m

    indicators = IndicatorValues(
        price          = round(price, 5),
        rsi            = round(rsi, 2),
        ema_fast       = round(bb_lower, 5),
        ema_slow       = round(bb_upper, 5),
        macd           = round(bb_mid, 5),
        macd_signal    = round(stoch_k if stoch_k else 0.0, 2),
        macd_histogram = round(vix_proxy, 2),
        atr            = round(atr if atr and not math.isnan(atr) else 0.0, 6),
        volume_ratio   = round(size_mult, 2),  # Risk Semaphore size multiplier
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


# ── Risk Semaphore ─────────────────────────────────────────────────────────────

def _risk_semaphore(vix_proxy: float) -> str:
    """Map VIX proxy to a semaphore level."""
    if vix_proxy > VIX_DANGER:
        return SEM_BLACK
    if vix_proxy > VIX_CAUTION:
        return SEM_RED
    if vix_proxy > VIX_NORMAL:
        return SEM_ORANGE
    if vix_proxy > VIX_CALM:
        return SEM_YELLOW
    return SEM_GREEN


# ── Market Regime Classifier ───────────────────────────────────────────────────
# Priority: Crisis > Momentum > Breakout > Trend > Range > Carry

def _classify_market(
    vix_proxy: float,
    adx: float,
    breakout_long: bool,
    breakout_short: bool,
    adx_rising: bool,
    symbol: str,
) -> str:
    # 1. Crisis first — override everything
    if vix_proxy > VIX_CAUTION:
        return RISK_OFF

    # 2. Momentum — strongest edge (ADX > 30 AND rising)
    if adx >= ADX_MOMENTUM and adx_rising:
        return MOMENTUM

    # 3. Breakout — high conviction (price breaks key level)
    if breakout_long or breakout_short:
        return BREAKOUT

    # 4. Trend — ADX in 25-30 range
    if adx >= ADX_TREND_MIN:
        return TREND

    # 5. Range — ADX below 20 (mean reversion)
    if adx < ADX_RANGE_MAX:
        return RANGE

    # 6. Carry — background income (low VIX, positive-swap pairs)
    if symbol.upper() in POSITIVE_SWAP_PAIRS and vix_proxy < VIX_NORMAL:
        return CARRY

    # Transition zone (ADX 20-25, no clear signal)
    return NO_TRADE


# ── Module Strategy Executor ───────────────────────────────────────────────────

def _execute_module(
    state: str, symbol: str, price: float,
    rsi: float, stoch_k: Optional[float], stoch_d: Optional[float],
    adx: float, atr: Optional[float],
    bb_upper: float, bb_mid: float, bb_lower: float,
    resistance: float, support_level: float, body_pct: float,
    sma200: float, vix_proxy: float, semaphore: str,
    stoch_bull: bool, stoch_bear: bool,
    breakout_long: bool, breakout_short: bool,
) -> Tuple[str, int, List[str]]:

    sem_tag = f"[{semaphore} SEM × {SEM_SIZE_MULT[semaphore]:.0%}]"

    # ── Module 1: MOMENTUM ───────────────────────────────────────────────────
    # Strongest edge. Entry: price vs SMA200 + RSI filter.
    # ATR: SL×1.5, TP×4.0 (trailing high reward)
    if state == MOMENTUM:
        bull = price > sma200 and rsi > MOM_RSI_BULL
        bear = price < sma200 and rsi < MOM_RSI_BEAR
        if bull:
            return "BUY", _conf(72, rsi, adx, True), [
                f"APEX {sem_tag} [MOMENTUM] BUY — ADX {adx:.1f} (strong+rising)",
                f"Price {price:.5f} > SMA200 {sma200:.5f}, RSI {rsi:.1f} momentum confirmed ✓",
                f"SL: ATR×1.5  TP: ATR×4.0 (trailing)  |  R:R ~1:2.7",
            ]
        if bear:
            return "SELL", _conf(72, rsi, adx, False), [
                f"APEX {sem_tag} [MOMENTUM] SELL — ADX {adx:.1f} (strong+rising)",
                f"Price {price:.5f} < SMA200 {sma200:.5f}, RSI {rsi:.1f} bearish momentum ✓",
                f"SL: ATR×1.5  TP: ATR×4.0 (trailing)  |  R:R ~1:2.7",
            ]
        return "HOLD", 0, [
            f"APEX [MOMENTUM] HOLD — price/SMA200 misaligned "
            f"(price {price:.5f} vs SMA200 {sma200:.5f}, RSI {rsi:.1f})"
        ]

    # ── Module 2: BREAKOUT ───────────────────────────────────────────────────
    # High conviction breakout. Entry: confirmed break + body filter + RSI.
    # ATR: SL×1.0, TP×2.5
    if state == BREAKOUT:
        if breakout_long and rsi > 50:
            sl = round(atr * 1.0, 5) if atr else None
            return "BUY", _conf(68, rsi, adx, True), [
                f"APEX {sem_tag} [BREAKOUT] BUY — broke resistance {resistance:.5f}",
                f"Body {body_pct:.0%} ≥ {BODY_PCT_MIN:.0%}, ADX {adx:.1f} confirming, RSI {rsi:.1f} ✓",
                f"SL: ATR×1.0 ({sl})  TP: ATR×2.5  |  R:R 1:2.5",
            ]
        if breakout_short and rsi < 50:
            sl = round(atr * 1.0, 5) if atr else None
            return "SELL", _conf(68, rsi, adx, False), [
                f"APEX {sem_tag} [BREAKOUT] SELL — broke support {support_level:.5f}",
                f"Body {body_pct:.0%} ≥ {BODY_PCT_MIN:.0%}, ADX {adx:.1f} confirming, RSI {rsi:.1f} ✓",
                f"SL: ATR×1.0 ({sl})  TP: ATR×2.5  |  R:R 1:2.5",
            ]
        return "HOLD", 0, [
            f"APEX [BREAKOUT] HOLD — RSI filter failed "
            f"(breakout_long={breakout_long}, RSI {rsi:.1f}; breakout_short={breakout_short})"
        ]

    # ── Module 3: TREND FOLLOWING ────────────────────────────────────────────
    # Reliable trend entry. Entry: price vs SMA200 + RSI in trend range.
    # ATR: SL×1.5, TP×3.0
    if state == TREND:
        bull = price > sma200 and rsi >= TF_RSI_BULL_MIN
        bear = price < sma200 and rsi <= TF_RSI_BEAR_MAX
        if bull:
            return "BUY", _conf(64, rsi, adx, True), [
                f"APEX {sem_tag} [TREND] BUY — uptrend confirmed",
                f"Price {price:.5f} > SMA200 {sma200:.5f}, ADX {adx:.1f}, RSI {rsi:.1f} ✓",
                f"SL: ATR×1.5  TP: ATR×3.0  |  R:R 1:2",
            ]
        if bear:
            return "SELL", _conf(64, rsi, adx, False), [
                f"APEX {sem_tag} [TREND] SELL — downtrend confirmed",
                f"Price {price:.5f} < SMA200 {sma200:.5f}, ADX {adx:.1f}, RSI {rsi:.1f} ✓",
                f"SL: ATR×1.5  TP: ATR×3.0  |  R:R 1:2",
            ]
        return "HOLD", 0, [
            f"APEX [TREND] HOLD — conditions not met "
            f"(price vs SMA200: {price:.5f} vs {sma200:.5f}, RSI {rsi:.1f})"
        ]

    # ── Module 4: MEAN REVERSION (RANGE) ────────────────────────────────────
    # Range-bound market. Entry at BB extremes + stochastic confirmation.
    # ATR: SL×2.0, TP = SMA20 (BB mid, encoded as TP×2.0)
    if state == RANGE:
        long_ok  = (
            price <= bb_lower * 1.002 and
            rsi < MR_RSI_LONG and
            stoch_k is not None and stoch_k < MR_STOCH_OVER and
            stoch_bull
        )
        short_ok = (
            price >= bb_upper * 0.998 and
            rsi > MR_RSI_SHORT and
            stoch_k is not None and stoch_k > MR_STOCH_OB and
            stoch_bear
        )
        if long_ok:
            return "BUY", _conf(60, rsi, adx, True), [
                f"APEX {sem_tag} [MEAN REVERSION] BUY at BB lower",
                f"Price {price:.5f} ≤ BB lower {bb_lower:.5f}, RSI {rsi:.1f} oversold, Stoch bull cross ✓",
                f"TP: SMA20 = {bb_mid:.5f}  SL: ATR×2.0  |  R:R ~1:1",
            ]
        if short_ok:
            return "SELL", _conf(60, rsi, adx, False), [
                f"APEX {sem_tag} [MEAN REVERSION] SELL at BB upper",
                f"Price {price:.5f} ≥ BB upper {bb_upper:.5f}, RSI {rsi:.1f} overbought, Stoch bear cross ✓",
                f"TP: SMA20 = {bb_mid:.5f}  SL: ATR×2.0  |  R:R ~1:1",
            ]
        return "HOLD", 0, [
            f"APEX [MEAN REVERSION] HOLD — BB+RSI+Stoch conditions not met "
            f"(price {price:.5f}, BB {bb_lower:.5f}–{bb_upper:.5f}, RSI {rsi:.1f})"
        ]

    # ── Module 5: CARRY TRADE ────────────────────────────────────────────────
    # Background income. Long only on positive-swap pairs, stable conditions.
    # ATR: SL×3.0, TP×4.0 (wide, designed for days-long holds)
    if state == CARRY:
        # Carry: only long, price above SMA200, RSI neutral zone, ADX > 15
        bull = price > sma200 and 35 <= rsi <= 65 and adx > 15
        if bull:
            sl = round(atr * 3.0, 5) if atr else None
            return "BUY", _conf(58, rsi, adx, True), [
                f"APEX {sem_tag} [CARRY] BUY — positive swap + stable conditions",
                f"{symbol} earns daily swap. VIX {vix_proxy:.1f} calm, price > SMA200 ✓",
                f"SL: ATR×3.0 ({sl})  TP: ATR×4.0 (trailing)  |  Earn daily carry",
            ]
        return "HOLD", 0, [
            f"APEX [CARRY] HOLD — conditions not met "
            f"(price vs SMA200: {price:.5f} vs {sma200:.5f}, RSI {rsi:.1f})"
        ]

    return "HOLD", 0, [f"APEX HOLD — state '{state}' not handled"]


# ── Helpers ────────────────────────────────────────────────────────────────────

def _conf(base: int, rsi: float, adx: float, bullish: bool) -> int:
    """Compute confidence score from base + ADX strength + RSI extremity."""
    score = base
    if adx > 40:     score += 15
    elif adx > 35:   score += 12
    elif adx > 25:   score += 6
    if bullish:
        if rsi > 65: score += 8
        if rsi > 70: score += 4
    else:
        if rsi < 35: score += 8
        if rsi < 30: score += 4
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
    size_mult: float = 0.0,
    vix_proxy: float = 0.0,
    adx: float = 0.0,
    rsi: float = 0.0,
    atr: Optional[float] = None,
    bb_upper: float = 0.0,
    bb_mid: float = 0.0,
    bb_lower: float = 0.0,
    stoch_k: Optional[float] = None,
) -> TechnicalSignal:
    price = float(candles[-1]["close"]) if candles else 0.0
    return TechnicalSignal(
        symbol       = symbol,
        timeframe    = timeframe,
        direction    = "HOLD",
        confidence   = 0,
        reasons      = [reason],
        indicators   = IndicatorValues(
            price          = price,
            rsi            = round(rsi, 2) if rsi else 0.0,
            ema_fast       = round(bb_lower, 5) if bb_lower else 0.0,
            ema_slow       = round(bb_upper, 5) if bb_upper else 0.0,
            macd           = round(bb_mid, 5) if bb_mid else 0.0,
            macd_signal    = round(stoch_k, 2) if stoch_k else 0.0,
            macd_histogram = round(vix_proxy, 2) if vix_proxy else 0.0,
            atr            = round(atr, 6) if atr and not math.isnan(atr) else 0.0,
            volume_ratio   = round(size_mult, 2),
            adx            = round(adx, 2) if adx else 0.0,
        ),
        analyzed_at  = now,
        candles_used = len(candles),
        hold_reason  = reason,
        composite_score = 0,
        score_breakdown = [],
    )
