"""
Risk Manager — Phase 5.

Performs all pre-trade risk checks and computes exact position sizing
and price levels (stop-loss, take-profit) before any order is placed.

This module is a pure function layer — it receives all state as arguments
and returns a RiskAssessment.  No DB calls, no HTTP calls.

Checks performed (in order)
────────────────────────────
1. Decision is actionable (BUY or SELL).
2. Global open-position cap (risk_settings.max_open_positions).
3. Sufficient cash balance.
4. Position size cap (risk_settings.max_position_size_pct × equity).
5. ATR-based SL/TP calculation (falls back to pct-based if ATR unavailable).
6. Risk-per-trade cap (loss at SL ≤ max_position_size_pct × equity).
7. Minimum risk/reward ratio (warn-only — does not reject the trade).

Position sizing
───────────────
    invest = min(config.investment_amount, equity × max_position_size_pct)

ATR-based SL/TP  (preferred)
    sl_distance = ATR × ATR_SL_MULT      (default: ATR × 2.0)
    tp_distance = ATR × ATR_TP_MULT      (default: ATR × 3.0)
    For BUY:  sl = entry − sl_distance,  tp = entry + tp_distance
    For SELL: sl = entry + sl_distance,  tp = entry − tp_distance

Pct-based SL/TP  (fallback when ATR is 0 or NaN)
    sl_distance = entry × stop_loss_pct
    tp_distance = entry × take_profit_pct
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional

from app.core.logger import get_logger
from app.models.risk_settings import RiskSettings
from app.schemas.decision import FinalDecision
from app.schemas.risk_assessment import RiskAssessment
from app.schemas.technical import TechnicalSignal

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Tuneable constants
# ---------------------------------------------------------------------------
ATR_SL_MULT      = 2.0    # stop-loss = entry ± ATR × this
ATR_TP_MULT      = 1.0    # take-profit = entry ± ATR × this
MIN_RR_RATIO     = 1.0    # warn if risk/reward below this (not a hard reject)
MIN_ATR_FRACTION = 0.0001 # ATR must be > entry × this to be considered valid
MIN_SL_TP_DIST   = 0.00010 # minimum absolute distance from entry (~1 pip for EURUSD)

# Volatility-adjusted sizing: reference ATR as fraction of price
# If current ATR/price > VOLATILITY_REF_PCT, position size is scaled down
VOLATILITY_REF_PCT = 0.005   # 0.5% of price = "normal" ATR for forex
VOLATILITY_MIN_MULT = 0.25   # never shrink below 25% of base size


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def assess(
    decision: FinalDecision,
    technical: TechnicalSignal,
    equity: float,
    open_positions_count: int,
    risk_settings: RiskSettings,
    invest_amount: float,
) -> RiskAssessment:
    """
    Evaluate a FinalDecision against current portfolio state and risk rules.

    Args:
        decision:             Output of decision_engine.decide().
        technical:            Output of technical_engine.analyze() — provides
                              ATR and current price.
        equity:               Current portfolio equity (cash_balance is fine for
                              paper trading; use cash + unrealised for live).
        open_positions_count: Number of currently open positions (all symbols).
        risk_settings:        User's RiskSettings DB row.
        invest_amount:        Dollar amount requested by StrategyConfig.

    Returns:
        RiskAssessment — approved=True with sizing details, or
                         approved=False with rejection_reason.
    """
    now       = datetime.now(timezone.utc)
    symbol    = decision.symbol
    direction = decision.direction

    # ── 1. Decision must be actionable ──────────────────────────────────────
    if direction not in ("BUY", "SELL"):
        return _reject(now, f"Direction {direction!r} is not actionable")

    # ── 2. Max open positions ────────────────────────────────────────────────
    max_pos = int(risk_settings.max_open_positions)
    if open_positions_count >= max_pos:
        return _reject(
            now,
            f"Max open positions reached ({open_positions_count}/{max_pos})",
        )

    # ── 3. Cash balance sufficient ───────────────────────────────────────────
    if equity <= 0:
        return _reject(now, "Portfolio equity is zero or negative")

    # invest_amount is pre-sized by bot_service (floored + safety buffer),
    # so we only need to reject if equity itself is too low to cover it.
    if equity < invest_amount:
        return _reject(now, f"Insufficient equity (${equity:.2f}) for trade")

    # ── 4. Position size ─────────────────────────────────────────────────────
    position_size = round(min(invest_amount, equity), 2)
    position_size = max(0.01, position_size)

    # Volatility-adjusted sizing (optional): scale down when ATR is elevated
    if (
        getattr(risk_settings, "volatility_sizing_enabled", False)
        and technical.indicators.atr > 0
        and not math.isnan(technical.indicators.atr)
        and technical.indicators.price > 0
    ):
        atr_pct = technical.indicators.atr / technical.indicators.price
        if atr_pct > VOLATILITY_REF_PCT:
            vol_mult = max(VOLATILITY_MIN_MULT, VOLATILITY_REF_PCT / atr_pct)
            position_size = round(position_size * vol_mult, 2)
            position_size = max(0.01, position_size)
            log.info(
                "Volatility-adjusted position size",
                symbol=symbol,
                atr_pct=round(atr_pct * 100, 3),
                ref_pct=round(VOLATILITY_REF_PCT * 100, 3),
                vol_multiplier=round(vol_mult, 3),
                adjusted_size=position_size,
            )

    # ── 5. Entry price & ATR ─────────────────────────────────────────────────
    entry_price = technical.indicators.price
    atr_value   = technical.indicators.atr

    if entry_price <= 0:
        return _reject(now, f"Invalid entry price: {entry_price}")

    # Decide SL/TP method
    atr_valid = (
        atr_value > 0
        and not math.isnan(atr_value)
        and atr_value > entry_price * MIN_ATR_FRACTION
    )

    if atr_valid:
        sl_price, tp_price, method = _atr_levels(
            direction, entry_price, atr_value,
        )
    else:
        sl_price, tp_price, method = _pct_levels(
            direction, entry_price, risk_settings,
        )

    # Enforce minimum SL/TP distance — never allow near-entry levels
    _min_dist = max(entry_price * MIN_ATR_FRACTION, MIN_SL_TP_DIST)
    if sl_price is not None and abs(sl_price - entry_price) < _min_dist:
        original_sl = sl_price
        sl_price = (entry_price + _min_dist) if direction == "SELL" else (entry_price - _min_dist)
        log.warning(
            "SL too close to entry — enforced minimum distance",
            symbol=symbol, method=method,
            original_sl=round(original_sl, 8),
            enforced_sl=round(sl_price, 8),
            min_dist=round(_min_dist, 8),
        )
    if tp_price is not None and abs(tp_price - entry_price) < _min_dist:
        original_tp = tp_price
        tp_price = (entry_price - _min_dist) if direction == "SELL" else (entry_price + _min_dist)
        log.warning(
            "TP too close to entry — enforced minimum distance",
            symbol=symbol, method=method,
            original_tp=round(original_tp, 8),
            enforced_tp=round(tp_price, 8),
            min_dist=round(_min_dist, 8),
        )

    # ── 6. Risk-per-trade accounting (informational) ─────────────────────────
    sl_distance  = abs(entry_price - sl_price) if sl_price else 0.0
    quantity     = position_size / entry_price if entry_price > 0 else 0.0
    risk_dollars = quantity * sl_distance

    # ── 7. Risk/reward ratio (warn only) ─────────────────────────────────────
    tp_distance = abs(entry_price - tp_price) if tp_price else 0.0
    rr_ratio    = (tp_distance / sl_distance) if sl_distance > 0 else 0.0

    if rr_ratio < MIN_RR_RATIO and rr_ratio > 0:
        log.warning(
            "Low risk/reward ratio",
            symbol=symbol, rr=round(rr_ratio, 2), minimum=MIN_RR_RATIO,
        )

    log.info(
        "Risk assessment approved",
        symbol=symbol, direction=direction,
        size=position_size, sl=sl_price, tp=tp_price,
        rr=round(rr_ratio, 2), method=method,
        atr=atr_value,
    )

    return RiskAssessment(
        approved              = True,
        position_size_dollars = position_size,
        stop_loss_price       = round(sl_price, 8) if sl_price else None,
        take_profit_price     = round(tp_price, 8) if tp_price else None,
        entry_price           = round(entry_price, 8),
        atr_value             = round(atr_value, 8),
        risk_per_trade_dollars= round(risk_dollars, 4),
        risk_reward_ratio     = round(rr_ratio, 3),
        sizing_method         = method,
        assessed_at           = now,
    )


# ---------------------------------------------------------------------------
# SL/TP calculators
# ---------------------------------------------------------------------------

def _atr_levels(
    direction: str,
    entry: float,
    atr: float,
) -> tuple[float, float, str]:
    """Compute SL and TP using ATR multiples."""
    sl_dist = atr * ATR_SL_MULT
    tp_dist = atr * ATR_TP_MULT
    if direction == "BUY":
        return entry - sl_dist, entry + tp_dist, "atr_based"
    else:  # SELL
        return entry + sl_dist, entry - tp_dist, "atr_based"


def _pct_levels(
    direction: str,
    entry: float,
    risk: RiskSettings,
) -> tuple[float, float, str]:
    """Compute SL and TP using percentage values from RiskSettings."""
    sl_pct = float(risk.stop_loss_pct)
    tp_pct = float(risk.take_profit_pct)
    if direction == "BUY":
        return (
            entry * (1.0 - sl_pct),
            entry * (1.0 + tp_pct),
            "pct_based",
        )
    else:  # SELL
        return (
            entry * (1.0 + sl_pct),
            entry * (1.0 - tp_pct),
            "pct_based",
        )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Trailing stop & break-even helpers (called by bot_service each cycle)
# ---------------------------------------------------------------------------

def update_trailing_stop(
    position,          # Position ORM object
    current_price: float,
    trailing_stop_pct: float,
) -> bool:
    """
    Update trailing stop level on an open position.

    Returns True if the trailing stop was triggered and the position should
    be closed.  The caller is responsible for writing back position fields.

    Logic:
      LONG:  trail stop = price × (1 − pct);  close when price ≤ trailing_stop
      SHORT: trail stop = price × (1 + pct);  close when price ≥ trailing_stop
    """
    if trailing_stop_pct <= 0 or not position.is_open:
        return False

    from decimal import Decimal
    price = Decimal(str(current_price))
    pct   = Decimal(str(trailing_stop_pct))

    if position.side == "long":
        # Update high-water mark
        hwm = position.high_water_mark or position.avg_entry_price
        if price > hwm:
            position.high_water_mark = price
            hwm = price
        new_trail = hwm * (Decimal("1") - pct)
        # Only ever move the stop UP
        old_trail = position.trailing_stop_price
        if old_trail is None or new_trail > old_trail:
            position.trailing_stop_price = new_trail
        # Trigger only when trailing level is AT OR ABOVE entry price.
        # This prevents closing at a loss when price never moved into profit:
        # if HWM == entry, trail = entry×(1−pct) < entry → do NOT fire, let SL handle it.
        if (
            price <= position.trailing_stop_price
            and position.trailing_stop_price >= position.avg_entry_price
        ):
            log.info(
                "Trailing stop triggered",
                symbol=position.symbol, side="long",
                price=float(price), trail_stop=float(position.trailing_stop_price),
                entry=float(position.avg_entry_price),
            )
            return True

    else:  # short
        hwm = position.high_water_mark or position.avg_entry_price
        if price < hwm:
            position.high_water_mark = price
            hwm = price
        new_trail = hwm * (Decimal("1") + pct)
        old_trail = position.trailing_stop_price
        if old_trail is None or new_trail < old_trail:
            position.trailing_stop_price = new_trail
        # Trigger only when trailing level is AT OR BELOW entry price.
        if (
            price >= position.trailing_stop_price
            and position.trailing_stop_price <= position.avg_entry_price
        ):
            log.info(
                "Trailing stop triggered",
                symbol=position.symbol, side="short",
                price=float(price), trail_stop=float(position.trailing_stop_price),
                entry=float(position.avg_entry_price),
            )
            return True

    return False


def check_break_even(
    position,          # Position ORM object
    current_price: float,
    break_even_trigger_pct: float,
) -> bool:
    """
    Move stop-loss to entry price when unrealised gain exceeds trigger %.

    Returns True if break-even was just activated (SL was moved).
    """
    if break_even_trigger_pct <= 0 or not position.is_open:
        return False
    if getattr(position, "break_even_activated", False):
        return False  # already activated

    from decimal import Decimal
    price   = float(current_price)
    entry   = float(position.avg_entry_price)
    trigger = break_even_trigger_pct

    if position.side == "long":
        gain_pct = (price - entry) / entry
        if gain_pct >= trigger:
            position.stop_loss_price = Decimal(str(entry))
            position.break_even_activated = True
            log.info(
                "Break-even activated",
                symbol=position.symbol, side="long",
                entry=entry, gain_pct=round(gain_pct * 100, 3),
                new_sl=entry,
            )
            return True

    else:  # short
        gain_pct = (entry - price) / entry
        if gain_pct >= trigger:
            position.stop_loss_price = Decimal(str(entry))
            position.break_even_activated = True
            log.info(
                "Break-even activated",
                symbol=position.symbol, side="short",
                entry=entry, gain_pct=round(gain_pct * 100, 3),
                new_sl=entry,
            )
            return True

    return False


def _reject(now: datetime, reason: str) -> RiskAssessment:
    log.warning("RISK REJECTED", reason=reason)
    return RiskAssessment(
        approved         = False,
        rejection_reason = reason,
        assessed_at      = now,
    )
