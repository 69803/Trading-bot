"""
Auto-trading bot service — Phase 6.

Each cycle:
  1. Load all users with bot_state.is_running == True
  2. For each user, load strategy config + risk settings + portfolio
  3. Check daily loss limit — auto-stop bot if exceeded
  4. For each configured symbol:
       a. Enforce cooldown (skip if last trade too recent)
       b. Check TP/SL on open positions → close directly if hit
       c. Run the full engine pipeline:
            technical_engine  → TechnicalSignal
            news_service      → List[NewsItem]
            sentiment_engine  → SentimentResult
            decision_engine   → FinalDecision
            risk_manager      → RiskAssessment
       d. Execute trade if RiskAssessment.approved
  5. Update bot_state.last_cycle_at and cycles_run
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logger import get_logger
from app.models.bot_log import BotLog
from app.models.bot_state import BotState
from app.models.order import Order
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.models.risk_settings import RiskSettings
from app.models.strategy_config import StrategyConfig
from app.models.trade import Trade
from app.core.config import settings
from app.schemas.sentiment import SentimentResult
from app.services import order_service
from app.services import decision_engine, risk_manager, sentiment_engine, technical_engine
from app.services.analytics_service import (
    count_consecutive_losses,
    count_trades_last_hour,
    save_performance_snapshot,
)
from app.services.audit_logger import log_decision
from app.services.market_data_router import market_data_router as _market_data_router_for_candles
from app.services.risk_manager import check_break_even, update_trailing_stop
from app.services.economic_calendar_service import (
    PRE_EVENT_BLOCK_MIN,
    PRE_EVENT_SKIP_MIN,
    POST_EVENT_SKIP_MIN,
    get_upcoming_events,
    is_high_impact_event,
    is_medium_impact_event,
)
from app.services.event_risk_service import assess_event_risk_for_trade
from app.services.expert_filters_service import (
    check_post_analysis_filters,
    check_pre_analysis_filters,
)
from app.services.historical_guardrail_service import check_historical_guardrail
from app.services.news_service import get_news

log = get_logger(__name__)

CANDLE_LIMIT = 250              # enough for EMA-200 warm-up

# ── Entry quality thresholds ──────────────────────────────────────────────────
MIN_CONFIDENCE_THRESHOLD = 40   # minimum decision confidence to open any trade (0-100)
MIN_ADX_FOR_ENTRY        = 20   # minimum ADX to confirm a trend exists
COOLDOWN_SECONDS         = 900  # minimum seconds between trades on the same symbol (15 min)
MIN_ATR_PCT              = 0.04 # minimum ATR as % of price (filters micro-range setups)

# Per-symbol stricter rules for noisy pairs
EURUSD_MIN_CONFIDENCE    = 55   # EURUSD requires higher confidence than default
EURUSD_MIN_ADX           = 25   # EURUSD requires stronger trend than default

# Float precision tolerance for TP/SL comparisons.
#
# Root cause of missed closes: the GBM simulator (and real data providers)
# return a price rounded to 5 decimal places.  Decimal→float conversion can
# produce a result that is off by a few ULP.  Example observed in production:
#
#   tp  = Decimal("1.15000") → float = 1.15         (exact)
#   price from get_current_price() = 1.15001         (one GBM tick slightly above)
#   1.15001 <= 1.15  →  False  →  position NOT closed even though UI shows 1.15
#
# The epsilon widens each boundary by one pip (0.0001 for EURUSD = $1 on 10k lot).
# It is applied to the TRIGGER only — the position still fills at the exact TP/SL
# price, not at tp+epsilon.
_TP_SL_EPSILON: float = 0.0001

# Minimum meaningful trade size in USD.
# If the auto-sized investment falls below this, the trade is skipped rather
# than placing a negligibly small order.
MIN_TRADE_AMOUNT = 5.0


# ---------------------------------------------------------------------------
# Public entry point (called by APScheduler)
# ---------------------------------------------------------------------------

async def run_bot_cycle(db: AsyncSession) -> None:
    """Execute one bot cycle for all users with is_running=True."""
    result = await db.execute(
        select(BotState).where(BotState.is_running == True)  # noqa: E712
    )
    running_states: List[BotState] = list(result.scalars().all())

    if not running_states:
        return

    log.info("Bot cycle started", users=len(running_states))

    for state in running_states:
        try:
            await _run_user_cycle(db, state)
        except Exception as exc:
            log.exception("Bot cycle error", user_id=str(state.user_id), error=str(exc))
            state.last_log = f"ERROR: {exc}"
            await db.flush()

    await db.commit()
    log.info("Bot cycle complete")


# ---------------------------------------------------------------------------
# Per-user cycle
# ---------------------------------------------------------------------------

async def _run_user_cycle(db: AsyncSession, state: BotState) -> None:
    user_id = state.user_id

    # Load strategy config — use .first() to tolerate accidental duplicate rows
    log.debug("bot_service._run_user_cycle: querying StrategyConfig", user_id=str(user_id))
    sc_result = await db.execute(
        select(StrategyConfig).where(StrategyConfig.user_id == user_id)
    )
    config: Optional[StrategyConfig] = sc_result.scalars().first()
    if config is None or not config.symbols:
        log.warning("No strategy config or symbols", user_id=str(user_id))
        state.last_log = "No strategy config or symbols"
        return

    # Load portfolio — use .first() to tolerate accidental duplicate rows
    log.debug("bot_service._run_user_cycle: querying Portfolio", user_id=str(user_id))
    port_result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == user_id)
    )
    portfolio: Optional[Portfolio] = port_result.scalars().first()
    if portfolio is None:
        log.warning("No portfolio found", user_id=str(user_id))
        state.last_log = "No portfolio found"
        return

    # Load risk settings — use .first() to tolerate accidental duplicate rows
    log.debug("bot_service._run_user_cycle: querying RiskSettings", user_id=str(user_id))
    risk_result = await db.execute(
        select(RiskSettings).where(RiskSettings.user_id == user_id)
    )
    risk: Optional[RiskSettings] = risk_result.scalars().first()

    # ── Cycle start log + open-position dump (before any guard can return early) ─
    _cycle_ts = datetime.now(timezone.utc)
    print(f"BOT CYCLE RUN at {_cycle_ts.isoformat()} — cycle#{(state.cycles_run or 0) + 1} symbols={config.symbols}", flush=True)
    log.info(
        "BOT CYCLE START",
        user_id=str(user_id),
        cycle_number=(state.cycles_run or 0) + 1,
        cycle_at=_cycle_ts.isoformat(),
        symbols=config.symbols,
        interval_seconds=60,
    )

    # Dump all open positions so every cycle produces visible evidence that
    # evaluation is running and shows the exact DB values of TP/SL/side/entry
    # that will be used — critical for debugging "position never closes" issues.
    _all_open_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == True,  # noqa: E712
        )
    )
    _all_open: List[Position] = list(_all_open_result.scalars().all())
    if _all_open:
        for _p in _all_open:
            log.info(
                "BOT CYCLE OPEN POSITION",
                cycle_at=_cycle_ts.isoformat(),
                position_id=str(_p.id),
                symbol=_p.symbol,
                side=_p.side,
                entry=str(_p.avg_entry_price),
                take_profit=str(_p.take_profit_price),
                stop_loss=str(_p.stop_loss_price),
                prev_evaluated_price=str(_p.prev_evaluated_price),
                opened_at=_p.opened_at.isoformat() if _p.opened_at else None,
                open_seconds=round(
                    (_cycle_ts - (
                        _p.opened_at if _p.opened_at.tzinfo is not None
                        else _p.opened_at.replace(tzinfo=timezone.utc)
                    )).total_seconds(), 1
                ) if _p.opened_at else None,
            )
    else:
        log.info("BOT CYCLE OPEN POSITION", cycle_at=_cycle_ts.isoformat(), open_count=0)

    # ── Unconditional TP/SL pass — BEFORE all risk guards ─────────────────────
    #
    # ROOT CAUSE FIX:
    # The guards below (daily_loss, drawdown, consecutive_losses) all call
    # `return` before _process_symbol is reached.  _evaluate_open_positions is
    # called inside _process_symbol, so any guard firing meant open positions
    # received ZERO TP/SL evaluation for that entire cycle.
    #
    # Reproducing the missed-TP scenario:
    #   1. SHORT opened, price drops through TP.
    #   2. Same cycle: consecutive-loss guard fires → `return` at line ~195.
    #   3. _evaluate_open_positions never called → position stays open.
    #   4. Next cycle: price has bounced back above TP → miss confirmed.
    #
    # Fix: evaluate ALL symbols' open positions here, unconditionally, before
    # any guard can interrupt the cycle.  The guards still gate new-trade
    # opening (inside _process_symbol) — they just no longer block position
    # closing.
    _pre_guard_close_msgs: List[str] = []
    for _sym in config.symbols:
        try:
            _close_msgs, _ = await _evaluate_open_positions(
                db=db, portfolio=portfolio, symbol=_sym, risk=risk,
            )
            for _m in _close_msgs:
                _pre_guard_close_msgs.append(f"{_sym}: {_m}")
        except Exception as _e:
            log.error(
                "PRE-GUARD TP/SL PASS: exception evaluating symbol",
                symbol=_sym, error=str(_e),
            )

    if _pre_guard_close_msgs:
        log.info(
            "PRE-GUARD TP/SL PASS: positions closed before risk guards",
            user_id=str(user_id),
            cycle_at=_cycle_ts.isoformat(),
            closed=_pre_guard_close_msgs,
        )
        await db.flush()
    else:
        log.info(
            "PRE-GUARD TP/SL PASS: no positions closed",
            user_id=str(user_id),
            cycle_at=_cycle_ts.isoformat(),
        )

    # ── Daily loss auto-stop ─────────────────────────────────────────────────
    if risk:
        today_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        daily_pnl_result = await db.execute(
            select(func.sum(Trade.realized_pnl)).where(
                Trade.portfolio_id == portfolio.id,
                Trade.executed_at >= today_start,
                Trade.realized_pnl.isnot(None),  # exclude open BUY trades
            )
        )
        daily_pnl = float(daily_pnl_result.scalar() or 0)
        equity = float(portfolio.cash_balance)
        if daily_pnl < 0 and equity > 0:
            loss_pct = abs(daily_pnl) / equity
            if loss_pct >= float(risk.max_daily_loss_pct):
                state.is_running = False
                msg = (
                    f"AUTO-STOPPED: daily loss {loss_pct:.1%} reached limit "
                    f"{float(risk.max_daily_loss_pct):.1%}"
                )
                state.last_log = msg
                log.warning("Bot auto-stopped: max daily loss", user_id=str(user_id))
                await db.flush()
                return

    # ── Max drawdown auto-stop ────────────────────────────────────────────────
    if risk:
        initial = float(portfolio.initial_capital)
        realized = float(portfolio.realized_pnl or 0)
        if realized < 0 and initial > 0:
            drawdown_pct = abs(realized) / initial
            if drawdown_pct >= float(risk.max_drawdown_pct):
                state.is_running = False
                msg = (
                    f"AUTO-STOPPED: max drawdown {drawdown_pct:.1%} reached limit "
                    f"{float(risk.max_drawdown_pct):.1%}"
                )
                state.last_log = msg
                log.warning("Bot auto-stopped: max drawdown", user_id=str(user_id))
                await db.flush()
                return

    # ── Consecutive loss circuit breaker ─────────────────────────────────────
    if risk and int(risk.max_consecutive_losses or 0) > 0:
        consec_losses = await count_consecutive_losses(db, portfolio.id)
        if consec_losses >= int(risk.max_consecutive_losses):
            msg = (
                f"PAUSED: {consec_losses} consecutive losses "
                f"(limit {int(risk.max_consecutive_losses)}) — skipping this cycle"
            )
            state.last_log = msg
            log.warning(msg, user_id=str(user_id))
            await db.flush()
            return

    # Count total open positions at cycle start (updated locally as BUYs execute)
    open_count_result = await db.execute(
        select(func.count()).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == True,  # noqa: E712
        )
    )
    live_open_positions: int = open_count_result.scalar() or 0

    logs: List[str] = []
    now = datetime.now(timezone.utc)

    for symbol in config.symbols:
        try:
            msg, position_opened = await _process_symbol(
                db=db,
                user_id=user_id,
                portfolio=portfolio,
                config=config,
                risk=risk,
                symbol=symbol,
                total_open_positions=live_open_positions,
            )
            if position_opened:
                live_open_positions += 1
            if msg:
                logs.append(f"{symbol}: {msg}")
        except Exception as exc:
            import traceback as _tb
            logs.append(f"{symbol}: ERROR {exc}")
            log.error(
                "Symbol processing error — FULL TRACEBACK BELOW",
                symbol=symbol,
                error=str(exc),
                error_type=type(exc).__name__,
                traceback=_tb.format_exc(),
            )

    state.last_cycle_at = now
    state.cycles_run = (state.cycles_run or 0) + 1
    cycle_ts = now.strftime("%H:%M:%S")
    body = " | ".join(logs) if logs else "no action"
    state.last_log = f"[{cycle_ts}] cycle#{state.cycles_run} {body}"
    log.info(
        "BOT CYCLE COMPLETE",
        cycle=state.cycles_run,
        cycle_at=cycle_ts,
        last_log=state.last_log,
    )

    # Persist full log history — one row per cycle, never overwritten
    symbol_hint = config.symbols[0] if config.symbols else None
    db.add(BotLog(
        user_id=user_id,
        timestamp=now,
        message=state.last_log,
        symbol=symbol_hint if len(config.symbols) == 1 else None,
    ))

    # Persist a performance snapshot (rate-limited to once per hour)
    try:
        await save_performance_snapshot(db, portfolio.id)
    except Exception as _snap_exc:
        log.warning("Failed to save performance snapshot", error=str(_snap_exc))

    await db.flush()


# ---------------------------------------------------------------------------
# Per-symbol processing — the full engine pipeline
# ---------------------------------------------------------------------------

async def _process_symbol(
    db: AsyncSession,
    user_id: UUID,
    portfolio: Portfolio,
    config: StrategyConfig,
    risk: Optional[RiskSettings],
    symbol: str,
    total_open_positions: int,
) -> tuple[str, bool]:
    """
    Run the full 5-phase engine for one symbol.
    Returns (log_message, position_opened).
    position_opened is True only when a BUY order was successfully filled.
    """

    log.debug("_process_symbol START", symbol=symbol, portfolio_id=str(portfolio.id))

    # ── TP/SL / trailing-stop / break-even — evaluated FIRST, unconditionally ─
    # This MUST run before session filters, economic-event guards, and candle
    # checks.  Filters are only for deciding whether to OPEN new trades; they
    # must never block the closure of an existing position.
    # Uses a fresh live price (not the stale candle close) so the decision
    # matches what the UI is displaying.
    close_msgs, remaining_positions = await _evaluate_open_positions(
        db=db, portfolio=portfolio, symbol=symbol, risk=risk,
    )
    if close_msgs:
        return " | ".join(close_msgs), False

    # ── Cooldown check — skip symbol if a trade was opened too recently ───────
    _last_pos_res = await db.execute(
        select(Position)
        .where(Position.portfolio_id == portfolio.id, Position.symbol == symbol)
        .order_by(Position.opened_at.desc())
        .limit(1)
    )
    _last_pos = _last_pos_res.scalars().first()
    if _last_pos and _last_pos.opened_at:
        _pos_ts = _last_pos.opened_at
        if _pos_ts.tzinfo is None:
            _pos_ts = _pos_ts.replace(tzinfo=timezone.utc)
        _elapsed = (datetime.now(timezone.utc) - _pos_ts).total_seconds()
        if _elapsed < COOLDOWN_SECONDS:
            _remaining = int(COOLDOWN_SECONDS - _elapsed)
            log.info(
                "SKIPPED [cooldown]",
                symbol=symbol,
                elapsed_s=int(_elapsed),
                remaining_s=_remaining,
                cooldown_s=COOLDOWN_SECONDS,
            )
            return f"SKIPPED [cooldown]: symbol={symbol} remaining={_remaining}s", False

    # ── Economic calendar filter ─────────────────────────────────────────────
    # Window: events within 60 min ahead OR up to POST_EVENT_SKIP_MIN min past.
    cal_events     = await get_upcoming_events(minutes_ahead=60, minutes_past=POST_EVENT_SKIP_MIN)
    sym_currencies = _symbol_currencies(symbol)
    relevant_cal   = [e for e in cal_events if e.currency in sym_currencies]

    high_cal = [e for e in relevant_cal if is_high_impact_event(e)]
    mid_cal  = [e for e in relevant_cal if not is_high_impact_event(e) and is_medium_impact_event(e)]

    # Log every relevant event with full timing context
    for ev in relevant_cal:
        log.info(
            "ECONOMIC EVENT DETECTED",
            symbol=symbol,
            name=ev.name,
            currency=ev.currency,
            impact=ev.impact,
            minutes_until=(
                f"{ev.minutes_until_event:.1f}min"
                if ev.minutes_until_event >= 0 else "already passed"
            ),
            minutes_since=(
                f"{ev.minutes_since_event:.1f}min ago"
                if ev.minutes_since_event >= 0 else "not yet"
            ),
        )

    # ── Determine the single most-restrictive economic decision ─────────────
    # Priority (highest wins): BLOCK(3) > SKIP(2) > REDUCE(1) > NONE(0)
    # Scan ALL relevant events before acting — never let list order decide.

    _CAL_BLOCK  = 3
    _CAL_SKIP   = 2
    _CAL_REDUCE = 1
    _CAL_NONE   = 0

    cal_decision       = _CAL_NONE
    cal_trigger_ev     = None
    cal_trigger_reason = ""

    for ev in high_cal:
        # POST-EVENT: volatility window after release
        if 0 <= ev.minutes_since_event < POST_EVENT_SKIP_MIN:
            lvl    = _CAL_SKIP
            reason = (
                f"post-event volatility — {ev.name} ({ev.currency}) "
                f"released {ev.minutes_since_event:.0f}min ago"
            )
        # PRE-EVENT BLOCK: imminent
        elif 0 <= ev.minutes_until_event <= PRE_EVENT_BLOCK_MIN:
            lvl    = _CAL_BLOCK
            reason = (
                f"imminent high-impact event — {ev.name} ({ev.currency}) "
                f"in {ev.minutes_until_event:.0f}min"
            )
        # PRE-EVENT SKIP: approaching
        elif 0 <= ev.minutes_until_event <= PRE_EVENT_SKIP_MIN:
            lvl    = _CAL_SKIP
            reason = (
                f"high-impact event within 30min — {ev.name} ({ev.currency}) "
                f"in {ev.minutes_until_event:.0f}min"
            )
        else:
            continue

        if lvl > cal_decision:
            cal_decision       = lvl
            cal_trigger_ev     = ev
            cal_trigger_reason = reason

    for ev in mid_cal:
        if _CAL_REDUCE > cal_decision:
            cal_decision       = _CAL_REDUCE
            cal_trigger_ev     = ev
            cal_trigger_reason = (
                f"medium-impact event — {ev.name} ({ev.currency}) "
                + (
                    f"in {ev.minutes_until_event:.0f}min"
                    if ev.minutes_until_event >= 0 else "already passed"
                )
            )

    # Map integer back to label for the summary log
    _CAL_LABELS = {_CAL_BLOCK: "BLOCK", _CAL_SKIP: "SKIP",
                   _CAL_REDUCE: "REDUCE", _CAL_NONE: "NONE"}
    log.info(
        "FINAL ECONOMIC DECISION",
        symbol=symbol,
        decision=_CAL_LABELS[cal_decision],
        trigger=(cal_trigger_ev.name if cal_trigger_ev else "—"),
        reason=cal_trigger_reason or "no relevant events",
    )

    cal_reduce_size = False   # flag carried to invest-sizing section
    event_context   = "normal"  # written to position at open time

    if cal_decision == _CAL_BLOCK:
        log.warning(
            "BLOCKED: imminent high-impact event",
            symbol=symbol,
            name=cal_trigger_ev.name,
            currency=cal_trigger_ev.currency,
            impact=cal_trigger_ev.impact,
            minutes_until=f"{cal_trigger_ev.minutes_until_event:.1f}min",
        )
        return f"BLOCKED: {cal_trigger_reason}", False

    if cal_decision == _CAL_SKIP:
        is_post = cal_trigger_ev.minutes_since_event >= 0
        if is_post:
            log.warning(
                "SKIPPED: post-event volatility",
                symbol=symbol,
                name=cal_trigger_ev.name,
                currency=cal_trigger_ev.currency,
                impact=cal_trigger_ev.impact,
                minutes_since=f"{cal_trigger_ev.minutes_since_event:.1f}min ago",
            )
        else:
            log.warning(
                "SKIPPED: high-impact event within 30 minutes",
                symbol=symbol,
                name=cal_trigger_ev.name,
                currency=cal_trigger_ev.currency,
                impact=cal_trigger_ev.impact,
                minutes_until=f"{cal_trigger_ev.minutes_until_event:.1f}min",
            )
        return f"SKIPPED: {cal_trigger_reason}", False

    if cal_decision == _CAL_REDUCE:
        log.warning(
            "WARNING: medium-impact event — size reduced",
            symbol=symbol,
            name=cal_trigger_ev.name,
            currency=cal_trigger_ev.currency,
            impact=cal_trigger_ev.impact,
            minutes_until=(
                f"{cal_trigger_ev.minutes_until_event:.1f}min"
                if cal_trigger_ev.minutes_until_event >= 0 else "already passed"
            ),
        )
        cal_reduce_size = True
        event_context   = "reduced_size_due_to_event"

    # ── DB historical event check (historical_events table) ──────────────────
    # Second, offline-capable layer on top of the live Forex Factory check.
    # Queries pre-loaded scheduled events from the DB.
    # If the DB is empty this returns NONE immediately and costs one fast query.
    db_risk = await assess_event_risk_for_trade(db, symbol)

    if db_risk.level == "BLOCK":
        log.warning(
            "SKIPPED: DB historical event — high-impact",
            symbol=symbol,
            reason=db_risk.reason,
        )
        return f"SKIPPED: {db_risk.reason}", False

    if db_risk.level == "REDUCE" and not cal_reduce_size:
        cal_reduce_size = True
        event_context   = "reduced_size_due_to_event"
        log.warning(
            "SIZE REDUCED: DB historical event — medium-impact",
            symbol=symbol,
            reason=db_risk.reason,
        )

    # ── Historical performance guardrail ─────────────────────────────────────
    # Evidence-based filter: blocks or reduces trades when closed-position
    # history shows consistently poor performance for this symbol, UTC hour,
    # or event-reduced context.  Requires a minimum sample size before firing
    # — below the threshold the rule is skipped (not enough evidence).
    guardrail = await check_historical_guardrail(
        db                   = db,
        portfolio_id         = portfolio.id,
        symbol               = symbol,
        current_hour_utc     = datetime.now(timezone.utc).hour,
        is_event_reduced_trade = cal_reduce_size,
    )

    if guardrail.action == "BLOCK":
        log.warning(
            "BLOCKED: historical performance guardrail",
            symbol  = symbol,
            rule    = guardrail.rule,
            reason  = guardrail.reason,
        )
        return f"BLOCKED: {guardrail.reason}", False

    if guardrail.action == "REDUCE" and not cal_reduce_size:
        cal_reduce_size = True
        log.warning(
            "SIZE REDUCED: historical performance guardrail",
            symbol  = symbol,
            rule    = guardrail.rule,
            reason  = guardrail.reason,
        )

    # ── Expert pre-analysis filters (session / overtrading / post-event) ─────
    # Filters A, E, F: no technical data required — run before the expensive
    # candle fetch + technical analysis so skipped trades cost nothing.
    pre = await check_pre_analysis_filters(
        symbol=symbol, db=db, portfolio_id=portfolio.id,
    )
    if pre.action == "SKIP":
        log.warning(
            "SKIPPED: expert pre-analysis filter",
            symbol=symbol, filter_name=pre.filter_name, reason=pre.reason,
        )
        return f"SKIPPED [{pre.filter_name}]: {pre.reason}", False

    # ── Phase 2: Technical analysis ──────────────────────────────────────────
    candles = await _market_data_router_for_candles.get_candles(symbol, "1h", limit=CANDLE_LIMIT)
    if not candles:
        return "no candle data", False

    # ── Candle quality / staleness check ─────────────────────────────────────
    _last5 = candles[-5:] if len(candles) >= 5 else candles
    _last5_ts    = [c.get("timestamp", "?") for c in _last5]
    _last5_close = [round(float(c["close"]), 5) for c in _last5]
    # Detect stale data: latest candle timestamp more than 2 hours old
    _stale = False
    _latest_ts = candles[-1].get("timestamp")
    if _latest_ts:
        try:
            from datetime import timezone as _tz
            _ts = (
                datetime.fromisoformat(str(_latest_ts).replace("Z", "+00:00"))
                if isinstance(_latest_ts, str)
                else datetime.fromtimestamp(float(_latest_ts), tz=_tz.utc)
            )
            _age_min = (datetime.now(timezone.utc) - _ts).total_seconds() / 60
            _stale = _age_min > 120
        except Exception:
            _age_min = -1
            _stale = False
    else:
        _age_min = -1

    log.info(
        "CANDLE QUALITY CHECK",
        symbol=symbol,
        total_candles=len(candles),
        last5_timestamps=_last5_ts,
        last5_closes=_last5_close,
        latest_candle_age_min=round(_age_min, 1) if _age_min >= 0 else "unknown",
        stale=_stale,
        ema_fast_period=int(config.ema_fast),
        ema_slow_period=int(config.ema_slow),
        rsi_overbought=float(config.rsi_overbought),
        rsi_oversold=float(config.rsi_oversold),
    )

    if _stale:
        log.warning(
            "STALE CANDLES — skipping technical analysis",
            symbol=symbol,
            age_min=round(_age_min, 1),
        )
        return f"SKIPPED: stale candle data ({_age_min:.0f}min old)", False

    technical = technical_engine.analyze(
        symbol=symbol,
        candles=candles,
        timeframe="1h",
        ema_fast_period=int(config.ema_fast),
        ema_slow_period=int(config.ema_slow),
        rsi_period=int(config.rsi_period),
        rsi_overbought=float(config.rsi_overbought),
        rsi_oversold=float(config.rsi_oversold),
    )

    # ── Guard: insufficient data — return before logging any indicator values ──
    if technical.hold_reason and "insufficient_data" in technical.hold_reason:
        log.warning(
            "SKIPPED [insufficient_data]",
            symbol=symbol,
            candles=len(candles),
            reason=technical.hold_reason,
        )
        return f"SKIPPED [insufficient_data]: {technical.hold_reason}", False

    current_price = technical.indicators.price

    # ── Full decision-path trace (emitted every cycle for diagnosis) ──────────
    _ind = technical.indicators
    log.info(
        "DECISION TRACE",
        symbol=symbol,
        # Technical output
        tech_direction=technical.direction,
        tech_confidence=technical.confidence,
        tech_trend_strength=technical.trend_strength,
        tech_reasons=technical.reasons,
        # Raw indicators
        price=_ind.price,
        ema_fast=_ind.ema_fast,
        ema_slow=_ind.ema_slow,
        rsi=_ind.rsi,
        macd=_ind.macd,
        macd_histogram=_ind.macd_histogram,
        atr=_ind.atr,
        adx=_ind.adx,
        volume_ratio=_ind.volume_ratio,
        # Config used
        ema_fast_period=int(config.ema_fast),
        ema_slow_period=int(config.ema_slow),
        rsi_overbought=float(config.rsi_overbought),
        rsi_oversold=float(config.rsi_oversold),
        buy_threshold=25,
        sell_threshold=-25,
        adx_sideways_gate=15,
    )

    # ── Expert post-analysis filters (volatility / trend / signal quality) ───
    # Filters B, C, D: require ATR, price, EMA, RSI, MACD from technical
    # analysis — evaluated synchronously, no further DB calls needed.
    post = check_post_analysis_filters(
        technical=technical,
        candles=candles,
        direction=technical.direction,
        symbol=symbol,
    )
    if post.action == "SKIP":
        log.warning(
            "SKIPPED: expert post-analysis filter",
            symbol=symbol, filter_name=post.filter_name, reason=post.reason,
        )
        return f"SKIPPED [{post.filter_name}]: {post.reason}", False

    # remaining_positions: open positions that did NOT trigger TP/SL.
    # Already populated by _evaluate_open_positions() at the top of this function.
    open_long:  Optional[Position] = next((p for p in remaining_positions if p.side == "long"),  None)
    open_short: Optional[Position] = next((p for p in remaining_positions if p.side == "short"), None)

    # ── Phase 3: News & Sentiment ────────────────────────────────────────────
    # Only use real sentiment when a live news API key is configured.
    # Simulated headlines carry no real information and add noise to decisions.
    real_news_available = bool(settings.NEWS_API_KEY or settings.ALPHA_VANTAGE_KEY)
    if real_news_available:
        news_items = await get_news(symbol, max_items=10)
        _provider  = "newsapi" if settings.NEWS_API_KEY else "alphavantage"
        sentiment  = sentiment_engine.analyze(symbol=symbol, items=news_items, provider=_provider)
        log.info(
            "NEWS SOURCE = REAL (NewsAPI)",
            symbol=symbol,
            articles=sentiment.news_count,
            sentiment=sentiment.label,
            score=round(sentiment.sentiment_score, 3),
            impact=sentiment.impact_score,
        )
    else:
        sentiment = _neutral_sentiment(symbol)
        log.info("NEWS SOURCE = SIMULATED", symbol=symbol, reason="no NEWS_API_KEY configured")

    # ── Max trades per hour throttle ─────────────────────────────────────────
    if risk and int(risk.max_trades_per_hour or 0) > 0:
        trades_this_hour = await count_trades_last_hour(db, portfolio.id)
        if trades_this_hour >= int(risk.max_trades_per_hour):
            log.info(
                "SKIPPED: max trades per hour reached",
                symbol=symbol,
                trades_this_hour=trades_this_hour,
                limit=int(risk.max_trades_per_hour),
            )
            return f"SKIP: max {risk.max_trades_per_hour} trades/hour reached", False

    # ── Phase 4: Decision Engine ─────────────────────────────────────────────
    decision = decision_engine.decide(technical=technical, sentiment=sentiment)

    log.info(
        "DECISION",
        symbol=symbol,
        direction=decision.direction,
        confidence=decision.confidence,
        technical=technical.direction,
        tech_confidence=technical.confidence,
        sentiment=sentiment.label,
        is_actionable=decision.is_actionable,
    )

    # ── Respect allow_buy / allow_sell flags ─────────────────────────────────
    if decision.direction == "BUY" and not config.allow_buy:
        await log_decision(
            db, user_id=user_id, portfolio_id=portfolio.id,
            technical=technical, sentiment=sentiment, decision=decision,
            executed=False, execution_rejection="buy disabled in config",
        )
        return f"hold — buy disabled | tech={technical.direction} sentiment={sentiment.label}", False

    if decision.direction == "SELL" and not config.allow_sell:
        await log_decision(
            db, user_id=user_id, portfolio_id=portfolio.id,
            technical=technical, sentiment=sentiment, decision=decision,
            executed=False, execution_rejection="sell disabled in config",
        )
        return f"hold — sell disabled | tech={technical.direction} sentiment={sentiment.label}", False

    # Non-actionable (HOLD / BLOCKED) — log and return informative message
    if not decision.is_actionable:
        await log_decision(
            db, user_id=user_id, portfolio_id=portfolio.id,
            technical=technical, sentiment=sentiment, decision=decision,
            executed=False,
        )
        override = f" [{decision.override_reason}]" if decision.override_reason else ""
        ind = technical.indicators
        return (
            f"{decision.direction} | tech={technical.direction}({technical.confidence}) "
            f"score={technical.composite_score:+d} "
            f"RSI={ind.rsi:.1f} EMA50={ind.ema_fast:.5f} EMA200={ind.ema_slow:.5f} "
            f"MACD={ind.macd_histogram:+.5f} "
            f"sentiment={sentiment.label}({sentiment.sentiment_score:+.2f})"
            f"{override}"
        ), False

    _sym_upper = symbol.upper()
    _is_eurusd = _sym_upper == "EURUSD"

    # ── Filter A: minimum confidence ─────────────────────────────────────────
    _conf_threshold = EURUSD_MIN_CONFIDENCE if _is_eurusd else MIN_CONFIDENCE_THRESHOLD
    if decision.confidence < _conf_threshold:
        log.info(
            "SKIPPED [low_confidence]",
            symbol=symbol,
            confidence=decision.confidence,
            threshold=_conf_threshold,
            direction=decision.direction,
        )
        return (
            f"SKIPPED [low_confidence]: confidence={decision.confidence} below threshold={_conf_threshold}"
        ), False

    # ── Filter B / D: minimum ADX (trend strength) ───────────────────────────
    _adx_val = technical.indicators.adx if technical.indicators else None
    _adx_threshold = EURUSD_MIN_ADX if _is_eurusd else MIN_ADX_FOR_ENTRY
    if _adx_val is not None and _adx_val < _adx_threshold:
        log.info(
            "SKIPPED [weak_trend]",
            symbol=symbol,
            adx=round(_adx_val, 2),
            threshold=_adx_threshold,
            direction=decision.direction,
        )
        _tag = "eurusd_filter" if _is_eurusd else "weak_trend"
        return (
            f"SKIPPED [{_tag}]: ADX={_adx_val:.1f} below {_adx_threshold} — no clear trend"
        ), False

    # ── Filter E: minimum ATR range — skip micro-range / tight setups ────────
    _atr_val   = technical.indicators.atr if technical.indicators else None
    _price_ref = technical.indicators.price if technical.indicators else current_price
    if _atr_val and _price_ref and _price_ref > 0:
        _atr_pct = _atr_val / _price_ref * 100
        if _atr_pct < MIN_ATR_PCT:
            log.info(
                "SKIPPED [insufficient_range]",
                symbol=symbol,
                atr=round(_atr_val, 6),
                atr_pct=round(_atr_pct, 4),
                min_atr_pct=MIN_ATR_PCT,
                direction=decision.direction,
            )
            return (
                f"SKIPPED [insufficient_range]: ATR={_atr_val:.5f} ({_atr_pct:.3f}%) "
                f"below minimum {MIN_ATR_PCT}% — setup too tight"
            ), False

    # ── Phase 5: Risk Manager ────────────────────────────────────────────────
    if risk is None:
        from app.models.risk_settings import RiskSettings as RS
        risk = RS(
            user_id=user_id,
            max_position_size_pct=Decimal("0.05"),
            max_daily_loss_pct=Decimal("0.02"),
            max_open_positions=10,
            stop_loss_pct=Decimal("0.03"),
            take_profit_pct=Decimal("0.06"),
            max_drawdown_pct=Decimal("0.20"),
        )

    # Always re-read cash_balance from DB — the portfolio object may have
    # been expired by a previous db.commit() inside this same cycle.
    await db.refresh(portfolio)
    equity = float(portfolio.cash_balance)

    log.info(
        "BALANCE CHECK",
        symbol=symbol,
        balance_before_trade=round(equity, 2),
        source="db_fresh_read",
    )

    if equity <= 0:
        log.warning(
            "SKIP TRADE — balance is zero or negative",
            symbol=symbol,
            balance_before_trade=round(equity, 2),
        )
        return f"SKIPPED — balance_before_trade=${equity:.2f} (deposit funds first)", False

    max_pct    = float(risk.max_position_size_pct)
    configured = float(config.investment_amount) if config.investment_amount else 100.0

    # ── AUTO-SIZE investment amount ──────────────────────────────────────────
    # Use int-truncation (floor) instead of round so we never accidentally
    # produce a value >= equity * max_pct at the floating-point level.
    # The -0.01 safety buffer eliminates rounding edge-cases entirely.
    #
    # Example: equity=199.76, max_pct=10%
    #   raw_max   = 19.9759...
    #   safe_max  = int(1997.59) / 100 - 0.01 = 19.97 - 0.01 = 19.96
    #   invest    = min(20.0, 19.96) = 19.96  ← always accepted
    raw_max    = equity * max_pct
    safe_max   = int(raw_max * 100) / 100 - 0.01   # floor to cents, then -1 cent
    safe_max   = max(safe_max, 0.0)                  # clamp to 0 if equity is tiny
    invest_amount = round(min(configured, safe_max), 2)

    capped = invest_amount < configured
    log.info(
        "INVEST SIZING",
        symbol=symbol,
        configured=round(configured, 2),
        balance=round(equity, 2),
        raw_max=round(raw_max, 4),
        safe_max=round(safe_max, 2),
        invest_amount=invest_amount,
        max_pct=f"{max_pct:.0%}",
        capped=capped,
        open_positions=total_open_positions,
        max_open_positions=int(risk.max_open_positions),
    )

    # ── Major news safety: reduce size or skip on high-impact events ─────────
    # Threshold: impact >= 70 (central bank decisions, NFP, war, recession news)
    # Logic:
    #   impact 70-84  → reduce position size by 50%
    #   impact >= 85  → skip trade entirely
    NEWS_IMPACT_REDUCE = 70
    NEWS_IMPACT_SKIP   = 85
    if real_news_available and sentiment.impact_score >= NEWS_IMPACT_SKIP:
        log.warning(
            "SKIPPED: high-impact news — too risky to trade",
            symbol=symbol,
            sentiment=sentiment.label,
            impact=sentiment.impact_score,
            threshold=NEWS_IMPACT_SKIP,
        )
        return (
            f"SKIPPED: high-impact news (impact={sentiment.impact_score}/100, "
            f"sentiment={sentiment.label}) — trade skipped for safety"
        ), False
    elif real_news_available and sentiment.impact_score >= NEWS_IMPACT_REDUCE:
        original_invest = invest_amount
        invest_amount = round(invest_amount * 0.50, 2)
        log.warning(
            "SKIPPED: high-impact news — position size reduced 50%",
            symbol=symbol,
            sentiment=sentiment.label,
            impact=sentiment.impact_score,
            threshold=NEWS_IMPACT_REDUCE,
            original_size=original_invest,
            reduced_size=invest_amount,
        )

    # ── Economic calendar: reduce size on medium-impact event ────────────────
    if cal_reduce_size:
        original_invest = invest_amount
        invest_amount   = round(invest_amount * 0.50, 2)
        log.warning(
            "WARNING: medium-impact event — size reduced",
            symbol=symbol,
            original_size=original_invest,
            reduced_size=invest_amount,
        )

    # ── Minimum trade size guard ─────────────────────────────────────────────
    if invest_amount < MIN_TRADE_AMOUNT:
        log.warning(
            "INVEST TOO SMALL — skipping trade",
            symbol=symbol,
            balance=round(equity, 2),
            invest_amount=invest_amount,
            min_required=MIN_TRADE_AMOUNT,
            max_pct=f"{max_pct:.0%}",
        )
        return (
            f"SKIPPED — balance ${equity:.2f} too low "
            f"(max_trade=${invest_amount:.2f} < min=${MIN_TRADE_AMOUNT:.2f})"
        ), False

    assessment = risk_manager.assess(
        decision             = decision,
        technical            = technical,
        equity               = equity,
        open_positions_count = total_open_positions,
        risk_settings        = risk,
        invest_amount        = invest_amount,
    )

    log.info(
        "RISK RESULT",
        symbol=symbol,
        balance=round(equity, 2),
        requested=round(configured, 2),
        capped_to=round(invest_amount, 2),
        max_allowed=round(safe_max, 2),
        final_order_size=assessment.position_size_dollars,
        approved=assessment.approved,
        rejection_reason=assessment.rejection_reason,
        sizing_method=assessment.sizing_method,
    )

    if not assessment.approved:
        await log_decision(
            db, user_id=user_id, portfolio_id=portfolio.id,
            technical=technical, sentiment=sentiment, decision=decision,
            assessment=assessment, executed=False,
            execution_rejection=assessment.rejection_reason,
        )
        return (
            f"RISK BLOCKED [{assessment.rejection_reason}] | "
            f"direction={decision.direction} confidence={decision.confidence}"
        ), False

    # ── Pre-entry live price guard ────────────────────────────────────────────
    # Candle data can be stale (e.g. GBM fixed-seed candles never change).
    # Fetch live price here to check the signal is still valid: if the live
    # price has already crossed the computed SL, the signal is stale and the
    # trade would close instantly.  Skip rather than open a losing trade.
    try:
        _live_check = await _market_data_router_for_candles.get_current_price(symbol)
    except Exception:
        _live_check = current_price  # fallback: allow trade if price unavailable

    if assessment.stop_loss_price is not None:
        _sl = float(assessment.stop_loss_price)
        if decision.direction == "BUY" and _live_check <= _sl:
            log.warning(
                "SKIP: live price already at/below SL — stale candle signal",
                symbol=symbol,
                live_price=round(_live_check, 6),
                candle_price=round(current_price, 6),
                sl=round(_sl, 6),
                gap_pips=round((_live_check - _sl) * 10000, 1),
            )
            return (
                f"SKIP: live {_live_check:.5f} ≤ SL {_sl:.5f} — "
                f"candle price {current_price:.5f} is stale"
            ), False
        if decision.direction == "SELL" and _live_check >= _sl:
            log.warning(
                "SKIP: live price already at/above SL — stale candle signal",
                symbol=symbol,
                live_price=round(_live_check, 6),
                candle_price=round(current_price, 6),
                sl=round(_sl, 6),
                gap_pips=round((_sl - _live_check) * 10000, 1),
            )
            return (
                f"SKIP: live {_live_check:.5f} ≥ SL {_sl:.5f} — "
                f"candle price {current_price:.5f} is stale"
            ), False

    # ── Execute trade ─────────────────────────────────────────────────────────
    if decision.direction == "BUY":
        if open_long:
            await log_decision(
                db, user_id=user_id, portfolio_id=portfolio.id,
                technical=technical, sentiment=sentiment, decision=decision,
                assessment=assessment, executed=False,
                execution_rejection="long already open",
            )
            return (
                f"hold — long already open @ {float(open_long.avg_entry_price):.5f} "
                f"| tech={technical.direction} sentiment={sentiment.label}"
            ), False

        # Counter-signal: close the short before doing anything else
        if open_short:
            close_msg = await _close_position_directly(
                db=db,
                position=open_short,
                portfolio=portfolio,
                current_price=current_price,
                reason=f"buy_signal_close_short | sentiment={sentiment.label}",
            )
            await log_decision(
                db, user_id=user_id, portfolio_id=portfolio.id,
                technical=technical, sentiment=sentiment, decision=decision,
                assessment=assessment, executed=True,
            )
            return close_msg, False

        log.info(
            "EXECUTING BUY",
            symbol=symbol,
            size=assessment.position_size_dollars,
            entry=current_price,
            sl=assessment.stop_loss_price,
            tp=assessment.take_profit_price,
        )
        order = await order_service.create_order(
            db=db,
            portfolio_id=portfolio.id,
            user_id=user_id,
            symbol=symbol,
            side="buy",
            order_type="market",
            investment_amount=assessment.position_size_dollars,
            event_context=event_context,
            is_paper=True,
        )

        log.info(
            "ORDER RESULT",
            symbol=symbol,
            side="buy",
            status=order.status,
            rejection_reason=order.rejection_reason,
        )

        if order.status != "filled":
            await log_decision(
                db, user_id=user_id, portfolio_id=portfolio.id,
                technical=technical, sentiment=sentiment, decision=decision,
                assessment=assessment, executed=False,
                execution_rejection=order.rejection_reason,
            )
            return f"BUY rejected: {order.rejection_reason}", False

        # Set ATR/pct-derived SL & TP on the newly created position
        if assessment.stop_loss_price or assessment.take_profit_price:
            await _set_position_levels(
                db=db,
                portfolio_id=portfolio.id,
                symbol=symbol,
                sl=assessment.stop_loss_price,
                tp=assessment.take_profit_price,
            )

        log.info("ORDER CREATED — BUY filled", symbol=symbol, order_id=str(order.id))
        await log_decision(
            db, user_id=user_id, portfolio_id=portfolio.id,
            technical=technical, sentiment=sentiment, decision=decision,
            assessment=assessment, executed=True,
        )

        sl_str = f"{assessment.stop_loss_price:.5f}"   if assessment.stop_loss_price   else "n/a"
        tp_str = f"{assessment.take_profit_price:.5f}" if assessment.take_profit_price else "n/a"
        return (
            f"BUY ${assessment.position_size_dollars:.0f} @ {current_price:.5f} | "
            f"SL={sl_str} TP={tp_str} | "
            f"method={assessment.sizing_method} conf={decision.confidence} "
            f"sentiment={sentiment.label}({sentiment.sentiment_score:+.2f})"
        ), True

    else:  # SELL
        if open_short:
            await log_decision(
                db, user_id=user_id, portfolio_id=portfolio.id,
                technical=technical, sentiment=sentiment, decision=decision,
                assessment=assessment, executed=False,
                execution_rejection="short already open",
            )
            return (
                f"hold — short already open @ {float(open_short.avg_entry_price):.5f} "
                f"| tech={technical.direction} sentiment={sentiment.label}"
            ), False

        if open_long:
            close_msg = await _close_position_directly(
                db=db,
                position=open_long,
                portfolio=portfolio,
                current_price=current_price,
                reason=f"sell_signal | sentiment={sentiment.label}",
            )
            await log_decision(
                db, user_id=user_id, portfolio_id=portfolio.id,
                technical=technical, sentiment=sentiment, decision=decision,
                assessment=assessment, executed=True,
            )
            return close_msg, False

        # No open position — open a short
        log.info(
            "EXECUTING SHORT",
            symbol=symbol,
            size=assessment.position_size_dollars,
            entry=current_price,
            sl=assessment.stop_loss_price,
            tp=assessment.take_profit_price,
        )
        order = await order_service.create_order(
            db=db,
            portfolio_id=portfolio.id,
            user_id=user_id,
            symbol=symbol,
            side="sell",
            order_type="market",
            investment_amount=assessment.position_size_dollars,
            event_context=event_context,
            is_paper=True,
        )

        log.info(
            "ORDER RESULT",
            symbol=symbol,
            side="sell",
            status=order.status,
            rejection_reason=order.rejection_reason,
        )

        if order.status != "filled":
            await log_decision(
                db, user_id=user_id, portfolio_id=portfolio.id,
                technical=technical, sentiment=sentiment, decision=decision,
                assessment=assessment, executed=False,
                execution_rejection=order.rejection_reason,
            )
            return f"SHORT rejected: {order.rejection_reason}", False

        # Set ATR-derived SL/TP on the new short position
        if assessment.stop_loss_price or assessment.take_profit_price:
            await _set_position_levels(
                db=db,
                portfolio_id=portfolio.id,
                symbol=symbol,
                sl=assessment.stop_loss_price,
                tp=assessment.take_profit_price,
            )

        await log_decision(
            db, user_id=user_id, portfolio_id=portfolio.id,
            technical=technical, sentiment=sentiment, decision=decision,
            assessment=assessment, executed=True,
        )

        sl_str = f"{assessment.stop_loss_price:.5f}"   if assessment.stop_loss_price   else "n/a"
        tp_str = f"{assessment.take_profit_price:.5f}" if assessment.take_profit_price else "n/a"
        return (
            f"SHORT ${assessment.position_size_dollars:.0f} @ {current_price:.5f} | "
            f"SL={sl_str} TP={tp_str} | "
            f"method={assessment.sizing_method} conf={decision.confidence} "
            f"sentiment={sentiment.label}({sentiment.sentiment_score:+.2f})"
        ), True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _set_position_levels(
    db: AsyncSession,
    portfolio_id: UUID,
    symbol: str,
    sl: Optional[float],
    tp: Optional[float],
) -> None:
    """Update the most recently opened position for symbol with SL/TP prices."""
    log.debug(
        "bot_service._set_position_levels: querying most-recent open position (LIMIT 1)",
        symbol=symbol,
        portfolio_id=str(portfolio_id),
    )
    result = await db.execute(
        select(Position)
        .where(
            Position.portfolio_id == portfolio_id,
            Position.symbol == symbol,
            Position.is_open == True,  # noqa: E712
        )
        .order_by(Position.opened_at.desc())
        .limit(1)
    )
    pos: Optional[Position] = result.scalar_one_or_none()
    if pos:
        if sl is not None:
            pos.stop_loss_price = Decimal(str(round(sl, 8)))
        if tp is not None:
            pos.take_profit_price = Decimal(str(round(tp, 8)))
        await db.flush()


async def _close_position_directly(
    db: AsyncSession,
    position: Position,
    portfolio: Portfolio,
    current_price: float,
    reason: str,
) -> str:
    """
    Close an existing position without opening an opposite one.

    - Marks position as closed (is_open=False, closed_at, closed_price)
    - Calculates realized PnL
    - Credits portfolio: returns investment_amount + PnL
    - Creates a filled Order + Trade record for the close
    """
    now   = datetime.now(timezone.utc)
    price = Decimal(str(current_price))

    if position.side == "long":
        pnl = (price - position.avg_entry_price) * position.quantity
    else:
        pnl = (position.avg_entry_price - price) * position.quantity

    close_side = "sell" if position.side == "long" else "buy"

    # Create a filled close order (Trade.order_id is NOT NULL)
    close_order = Order(
        id=uuid4(),
        portfolio_id=portfolio.id,
        symbol=position.symbol,
        side=close_side,
        order_type="market",
        investment_amount=position.investment_amount,
        quantity=position.quantity,
        filled_quantity=position.quantity,
        avg_fill_price=price,
        status="filled",
        created_at=now,
        updated_at=now,
    )
    db.add(close_order)
    await db.flush()

    close_trade = Trade(
        id=uuid4(),
        order_id=close_order.id,
        portfolio_id=portfolio.id,
        symbol=position.symbol,
        side=close_side,
        quantity=position.quantity,
        price=price,
        commission=Decimal("0"),
        realized_pnl=pnl,
        executed_at=now,
        created_at=now,
    )
    db.add(close_trade)

    position.is_open      = False
    position.closed_at    = now
    position.closed_price = price
    position.realized_pnl = pnl

    invest   = position.investment_amount or (position.avg_entry_price * position.quantity)
    proceeds = invest + pnl
    if proceeds > Decimal("0"):
        portfolio.cash_balance += proceeds
    portfolio.realized_pnl = (portfolio.realized_pnl or Decimal("0")) + pnl
    portfolio.updated_at   = now

    await db.flush()

    sign = "+" if pnl >= 0 else ""
    return (
        f"closed {position.side} ({reason}) @ {float(price):.5f} "
        f"PnL={sign}${float(pnl):.2f}"
    )


def _neutral_sentiment(symbol: str) -> SentimentResult:
    """Return a fully neutral SentimentResult (confidence_modifier = ×1.0, no overrides)."""
    return SentimentResult(
        symbol          = symbol,
        sentiment_score = 0.0,
        impact_score    = 0,
        label           = "neutral",
        news_count      = 0,
        headlines       = [],
        items           = [],
        analyzed_at     = datetime.now(timezone.utc),
        source          = "simulated",
    )


def _symbol_currencies(symbol: str) -> set:
    """
    Extract the two currency codes from a forex symbol.

    Examples:
        "EURUSD"  → {"EUR", "USD"}
        "EUR/USD" → {"EUR", "USD"}
        "XAUUSD"  → {"XAU", "USD"}
    """
    clean = symbol.upper().replace("/", "").replace("_", "").replace("-", "")
    if len(clean) >= 6:
        return {clean[:3], clean[3:6]}
    return {clean}


def _is_active_trading_session() -> bool:
    """Return True during an active trading session (UTC hours).

    When USE_TRADING_SESSIONS=False (default): always returns True (24/7).

    When USE_TRADING_SESSIONS=True and EXPERT_FILTERS_ENABLED=True:
      London session: 07:00–11:00 UTC
      NY session:     13:00–17:00 UTC
      (configurable via EXPERT_SESSION_* settings)

    When USE_TRADING_SESSIONS=True and EXPERT_FILTERS_ENABLED=False:
      Legacy broad window 08:00–22:00 UTC.
    """
    # 24/7 mode: disabled by default, re-enable via USE_TRADING_SESSIONS=true in .env
    if not getattr(settings, "USE_TRADING_SESSIONS", False):
        return True
    hour = datetime.now(timezone.utc).hour
    if settings.EXPERT_FILTERS_ENABLED:
        in_london = settings.EXPERT_SESSION_LONDON_START <= hour < settings.EXPERT_SESSION_LONDON_END
        in_ny     = settings.EXPERT_SESSION_NY_START     <= hour < settings.EXPERT_SESSION_NY_END
        return in_london or in_ny
    # Legacy broad window
    return 8 <= hour < 22


async def _evaluate_open_positions(
    db: AsyncSession,
    portfolio: Portfolio,
    symbol: str,
    risk: Optional[RiskSettings],
) -> tuple[List[str], List[Position]]:
    """
    Evaluate TP/SL / trailing-stop / break-even for every open position on
    *symbol* using THREE independent detection methods — ANY of which fires
    closes the position:

      Method 1 — Snapshot range (existing):
        Uses effective_high / effective_low (live tick + candle wicks) to
        detect whether the TP/SL level is currently inside the price range.

      Method 2 — Price cross (NEW):
        Compares pos.prev_evaluated_price (last cycle) with live_price (this
        cycle).  If price CROSSED a TP/SL level between two evaluations —
        even if it has since bounced back — the position closes.

        SHORT TP cross: prev > take_profit AND current <= take_profit
        SHORT SL cross: prev < stop_loss  AND current >= stop_loss
        LONG  TP cross: prev < take_profit AND current >= take_profit
        LONG  SL cross: prev > stop_loss  AND current <= stop_loss

      Method 3 — Intrabar candle wicks (existing):
        The last 3 closed 1h candles include their full high/low range.
        If a wick crossed TP/SL inside the current candle (between bot ticks),
        the snapshot range catches it.

    After every evaluation (close OR hold), pos.prev_evaluated_price is updated
    to the current live price.  This persists through DB commits so the cross
    detector has a reliable baseline on the next cycle even after a restart.

    Called in TWO places every cycle — both unconditionally:
      1. Pre-guard pass in _run_user_cycle (before daily-loss / drawdown /
         consecutive-loss guards that would otherwise return early and skip all
         symbol processing, leaving positions stuck open indefinitely).
      2. Start of _process_symbol (before session filter and every other guard
         that gates new-trade opening but must not block position closing).

    Returns (close_messages, remaining_open_positions).
    """
    eval_at = datetime.now(timezone.utc).isoformat()

    # ── 1. Live tick price ────────────────────────────────────────────────────
    try:
        live_price: float = await _market_data_router_for_candles.get_current_price(symbol)
    except Exception as exc:
        log.warning(
            "POSITION EVAL SKIPPED: failed to fetch live price",
            eval_at=eval_at,
            symbol=symbol,
            error=str(exc),
            skipped=True,
            reason="price_fetch_error",
        )
        # Return existing DB positions so _process_symbol still knows about them.
        # Without this, remaining_positions = [] lets the bot open a second
        # position on a symbol that already has one.
        try:
            _fallback = await db.execute(
                select(Position).where(
                    Position.portfolio_id == portfolio.id,
                    Position.symbol       == symbol,
                    Position.is_open      == True,  # noqa: E712
                )
            )
            _existing = list(_fallback.scalars().all())
        except Exception:
            _existing = []
        return [], _existing

    # ── 2. Recent candle wicks (intrabar / between-cycle coverage) ────────────
    # Candles are fetched once per symbol invocation; per-position filtering
    # happens inside the loop to exclude wicks that predate pos.opened_at.
    recent_candles: list = []
    candle_wick_high: float = live_price  # symbol-level, used only for START log
    candle_wick_low:  float = live_price
    try:
        recent_candles = await _market_data_router_for_candles.get_candles(symbol, "1h", limit=3)
        if recent_candles:
            candle_wick_high = max(float(c["high"]) for c in recent_candles)
            candle_wick_low  = min(float(c["low"])  for c in recent_candles)
    except Exception as exc:
        log.warning(
            "POSITION EVAL: candle fetch failed — snapshot + cross detection only",
            eval_at=eval_at,
            symbol=symbol,
            error=str(exc),
        )

    # ── 3. Load open positions ────────────────────────────────────────────────
    pos_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id,
            Position.symbol       == symbol,
            Position.is_open      == True,  # noqa: E712
        )
    )
    open_positions: List[Position] = list(pos_result.scalars().all())

    if not open_positions:
        log.debug(
            "POSITION EVAL: no open positions — skip",
            eval_at=eval_at,
            symbol=symbol,
            skipped=True,
            reason="no_open_positions",
        )
        return [], []

    log.info(
        "POSITION EVAL START",
        eval_at=eval_at,
        symbol=symbol,
        open_count=len(open_positions),
        live_price=round(live_price, 6),
        candle_wick_high=round(candle_wick_high, 6),
        candle_wick_low=round(candle_wick_low, 6),
        sides=[p.side for p in open_positions],
    )

    trailing_pct = float(risk.trailing_stop_pct)      if risk else 0.0
    be_trigger   = float(risk.break_even_trigger_pct) if risk else 0.0

    close_messages: List[str] = []
    remaining:      List[Position] = []

    for pos in open_positions:
        entry = float(pos.avg_entry_price)
        sl    = float(pos.stop_loss_price)   if pos.stop_loss_price   else None
        tp    = float(pos.take_profit_price) if pos.take_profit_price else None

        # ── Per-position candle wick range ────────────────────────────────────
        # Only candles whose open-timestamp is >= pos.opened_at contribute.
        # A candle timestamp is its OPEN time; if the candle opened before this
        # position existed we cannot know whether the extreme tick occurred
        # before or after pos.opened_at, so we conservatively exclude it.
        # If no candle qualifies (position opened this cycle) the effective
        # range collapses to live_price only — cross detection and snapshot
        # still apply; only the candle-wick expansion is suppressed.
        pos_wick_high: float = live_price
        pos_wick_low:  float = live_price
        for _c in recent_candles:
            try:
                _c_ts = datetime.fromisoformat(_c["timestamp"])
                if _c_ts.tzinfo is None:
                    _c_ts = _c_ts.replace(tzinfo=timezone.utc)
                if _c_ts >= pos.opened_at:
                    pos_wick_high = max(pos_wick_high, float(_c["high"]))
                    pos_wick_low  = min(pos_wick_low,  float(_c["low"]))
            except (KeyError, ValueError):
                pass  # malformed candle entry — skip silently

        effective_high = max(live_price, pos_wick_high)
        effective_low  = min(live_price, pos_wick_low)

        # ── Previous price (cross-detection baseline) ─────────────────────────
        prev: Optional[float] = (
            float(pos.prev_evaluated_price) if pos.prev_evaluated_price else None
        )

        # ── Method 1: Snapshot range detection ───────────────────────────────
        # _TP_SL_EPSILON widens each boundary by 1 pip so that a float that is
        # 0.00001 away from the stored TP/SL (due to Decimal→float rounding or
        # GBM tick granularity) still triggers.  Close price is always the exact
        # TP/SL level, not tp+epsilon.
        if pos.side == "long":
            tp_hit_snapshot = tp is not None and effective_high >= tp - _TP_SL_EPSILON
            sl_hit_snapshot = sl is not None and effective_low  <= sl + _TP_SL_EPSILON
        else:  # short
            tp_hit_snapshot = tp is not None and effective_low  <= tp + _TP_SL_EPSILON
            sl_hit_snapshot = sl is not None and effective_high >= sl - _TP_SL_EPSILON

        # ── Method 2: Price-cross detection ──────────────────────────────────
        # Fires when price CROSSED the level between this cycle and the last one,
        # even if it has since bounced back past the level.
        # prev is None on a newly-opened position → skip cross check that cycle.
        # Epsilon applied to the "from" side of the cross so that a price that
        # was already at TP±epsilon last cycle still registers the crossing.
        if prev is not None and tp is not None:
            if pos.side == "long":
                tp_crossed = prev < tp + _TP_SL_EPSILON and live_price >= tp - _TP_SL_EPSILON
            else:  # short
                tp_crossed = prev > tp - _TP_SL_EPSILON and live_price <= tp + _TP_SL_EPSILON
        else:
            tp_crossed = False

        if prev is not None and sl is not None:
            if pos.side == "long":
                sl_crossed = prev > sl - _TP_SL_EPSILON and live_price <= sl + _TP_SL_EPSILON
            else:  # short
                sl_crossed = prev < sl + _TP_SL_EPSILON and live_price >= sl - _TP_SL_EPSILON
        else:
            sl_crossed = False

        # ── Combined decision ─────────────────────────────────────────────────
        tp_hit = tp_hit_snapshot or tp_crossed
        sl_hit = sl_hit_snapshot or sl_crossed

        # Identify primary trigger source for logging
        if tp_hit:
            if tp_crossed and not tp_hit_snapshot:
                tp_trigger_src = "price_cross"
            elif not tp_crossed and tp is not None and (
                (pos.side == "long"  and live_price >= tp) or
                (pos.side == "short" and live_price <= tp)
            ):
                tp_trigger_src = "live_snapshot"
            else:
                tp_trigger_src = "candle_wick"
        else:
            tp_trigger_src = "none"

        if sl_hit:
            if sl_crossed and not sl_hit_snapshot:
                sl_trigger_src = "price_cross"
            elif not sl_crossed and sl is not None and (
                (pos.side == "long"  and live_price <= sl) or
                (pos.side == "short" and live_price >= sl)
            ):
                sl_trigger_src = "live_snapshot"
            else:
                sl_trigger_src = "candle_wick"
        else:
            sl_trigger_src = "none"

        # SL takes priority over TP (conservative — avoids overstating profit
        # when both fire in the same evaluation on a gap candle).
        if sl_hit:
            close_price  = sl
            close_reason = "stop_loss"
            trigger_src  = sl_trigger_src
        elif tp_hit:
            close_price  = tp
            close_reason = "take_profit"
            trigger_src  = tp_trigger_src
        else:
            close_price  = None
            close_reason = ""
            trigger_src  = "none"

        log.info(
            "POSITION EVAL",
            eval_at=eval_at,
            symbol=symbol,
            position_id=str(pos.id),
            side=pos.side,
            entry_price=round(entry, 6),
            prev_price=round(prev, 6) if prev is not None else None,
            current_price=round(live_price, 6),
            candle_wick_high=round(candle_wick_high, 6),
            candle_wick_low=round(candle_wick_low, 6),
            effective_high=round(effective_high, 6),
            effective_low=round(effective_low, 6),
            stop_loss=round(sl, 6) if sl is not None else None,
            take_profit=round(tp, 6) if tp is not None else None,
            epsilon=_TP_SL_EPSILON,
            # Detection breakdown
            tp_hit_snapshot=tp_hit_snapshot,
            tp_crossed=tp_crossed,
            sl_hit_snapshot=sl_hit_snapshot,
            sl_crossed=sl_crossed,
            # Combined outcome
            tp_hit=tp_hit,
            sl_hit=sl_hit,
            skipped=False,
            trigger_source=trigger_src,
            close_decision=close_reason or "hold",
        )

        # ── Always update prev_evaluated_price before any close logic ─────────
        # Persisting current live_price here means the NEXT cycle always has a
        # valid baseline for cross-detection, regardless of whether this cycle
        # closes the position or not.
        pos.prev_evaluated_price = Decimal(str(live_price))

        # 1. Break-even: move SL to entry when unrealised gain threshold is met
        if be_trigger > 0 and close_price is None:
            activated = check_break_even(pos, live_price, be_trigger)
            if activated:
                await db.flush()

        # 2. Trailing stop: update level and check for trigger
        if trailing_pct > 0 and close_price is None:
            trail_triggered = update_trailing_stop(pos, live_price, trailing_pct)
            if trail_triggered:
                msg = await _close_position_directly(
                    db=db,
                    position=pos,
                    portfolio=portfolio,
                    current_price=live_price,
                    reason=f"trailing_stop @ {live_price:.5f}",
                )
                log.info(
                    "POSITION EVAL: CLOSED",
                    eval_at=eval_at,
                    symbol=symbol,
                    position_id=str(pos.id),
                    reason="trailing_stop",
                    close_price=round(live_price, 6),
                    close_msg=msg,
                )
                close_messages.append(msg)
                continue
            await db.flush()

        # 3. TP/SL close
        if close_price is not None:
            msg = await _close_position_directly(
                db=db,
                position=pos,
                portfolio=portfolio,
                current_price=close_price,
                reason=f"{close_reason} [src={trigger_src}]",
            )
            log.info(
                "POSITION EVAL: CLOSED",
                eval_at=eval_at,
                symbol=symbol,
                position_id=str(pos.id),
                reason=close_reason,
                trigger_source=trigger_src,
                close_price=round(close_price, 6),
                close_msg=msg,
            )
            close_messages.append(msg)
        else:
            # Position stays open — flush the prev_evaluated_price update
            await db.flush()
            remaining.append(pos)

    return close_messages, remaining


def _check_tp_sl(position: Position, current_price: float) -> tuple[bool, str]:
    """Return (should_close, reason) based on current price vs TP/SL."""
    price = Decimal(str(current_price))

    if position.side == "long":
        if position.stop_loss_price and price <= position.stop_loss_price:
            return True, "stop_loss"
        if position.take_profit_price and price >= position.take_profit_price:
            return True, "take_profit"
    else:
        if position.stop_loss_price and price >= position.stop_loss_price:
            return True, "stop_loss"
        if position.take_profit_price and price <= position.take_profit_price:
            return True, "take_profit"

    return False, ""


# ---------------------------------------------------------------------------
# Limit order filler (unchanged from prior phase)
# ---------------------------------------------------------------------------

async def fill_pending_limit_orders(db: AsyncSession) -> None:
    """Check all pending limit orders. Fill if current price crosses the limit."""
    from app.services.market_data_router import market_data_router

    result = await db.execute(
        select(Order).where(
            Order.status == "pending",
            Order.order_type == "limit",
            Order.limit_price.isnot(None),
        )
    )
    orders: List[Order] = list(result.scalars().all())

    if not orders:
        return

    filled_count = 0
    for order in orders:
        try:
            symbol        = order.symbol
            current_price = Decimal(
                str(await market_data_router.get_current_price(symbol))
            )
            limit = order.limit_price

            should_fill = (
                (order.side == "buy"  and current_price <= limit)
                or (order.side == "sell" and current_price >= limit)
            )
            if not should_fill:
                continue

            port_result = await db.execute(
                select(Portfolio).where(Portfolio.id == order.portfolio_id)
            )
            portfolio: Optional[Portfolio] = port_result.scalar_one_or_none()
            if portfolio is None:
                continue

            fill_price  = limit
            commission  = (fill_price * order.quantity * Decimal("0.001")).quantize(
                Decimal("0.01")
            )
            trade_value = fill_price * order.quantity

            if order.side == "buy":
                total_cost = trade_value + commission
                if portfolio.cash_balance < total_cost:
                    order.status = "rejected"
                    order.rejection_reason = "Insufficient cash at limit fill time"
                    order.updated_at = datetime.now(timezone.utc)
                    continue
                portfolio.cash_balance -= total_cost
            else:
                portfolio.cash_balance += trade_value - commission

            portfolio.updated_at = datetime.now(timezone.utc)

            trade = Trade(
                id=uuid4(),
                order_id=order.id,
                portfolio_id=order.portfolio_id,
                symbol=symbol,
                side=order.side,
                quantity=order.quantity,
                price=fill_price,
                commission=commission,
                realized_pnl=None,
                executed_at=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
            )
            db.add(trade)
            await db.flush()

            order.filled_quantity = order.quantity
            order.avg_fill_price  = fill_price
            order.status          = "filled"
            order.updated_at      = datetime.now(timezone.utc)

            invest_dec = order.investment_amount or (fill_price * order.quantity)
            await order_service._open_or_update_position(
                db, order.portfolio_id, trade, invest_dec
            )
            filled_count += 1

        except Exception as exc:
            log.warning("Limit order fill error", order_id=str(order.id), error=str(exc))

    if filled_count:
        await db.commit()
        log.info("Limit orders filled", count=filled_count)
