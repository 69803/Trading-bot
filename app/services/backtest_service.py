"""
Backtesting engine — Phase 8 (multi-layer).

Replays historical candles through the full Phase 2-5 pipeline:
  Phase 2 — technical_engine  → TechnicalSignal (EMA, RSI, MACD, ATR, Volume)
  Phase 3 — sentiment_engine  → SentimentResult (momentum-simulated headlines)
  Phase 4 — decision_engine   → FinalDecision   (BUY/SELL/HOLD/BLOCKED)
  Phase 5 — risk_manager      → RiskAssessment  (ATR-based SL/TP, sizing)

Sentiment simulation
────────────────────
Real news cannot be fetched for historical dates, so sentiment is derived
from the candle window itself using a momentum proxy:

  momentum = (close[-1] - close[-5]) / close[-5]   (5-candle return)

  momentum > +1.5%  → 3-5 positive NewsItems   (strong uptrend)
  momentum > +0.5%  → 1-2 positive NewsItems
  momentum < -1.5%  → 3-5 negative NewsItems   (strong downtrend)
  momentum < -0.5%  → 1-2 negative NewsItems
  otherwise         → 1-2 neutral NewsItems

This makes sentiment loosely correlated with recent price action while
remaining deterministic (no real API calls in the hot path).

Comparison mode (use_sentiment=False)
──────────────────────────────────────
Setting use_sentiment=False runs the technical engine alone (sentiment
fixed at neutral, confidence_modifier=1.0) so you can compare the two
equity curves in the results.

New result fields vs the legacy engine
───────────────────────────────────────
  decisions_breakdown   {BUY, SELL, HOLD, BLOCKED: count}
  avg_technical_conf    average TechnicalSignal.confidence on action signals
  avg_final_conf        average FinalDecision.confidence on executed trades
  sentiment_overrides   number of times sentiment changed a technical signal
  atr_sl_tp_used        fraction of trades where ATR-based SL/TP was used
"""
from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.backtest_run import BacktestRun
from app.schemas.sentiment import NewsItem, SentimentResult
from app.services import decision_engine, risk_manager, sentiment_engine, technical_engine
from app.services.market_data_router import market_data_router as market_data_service

# Minimum candles before the engine starts emitting signals (matches technical_engine)
_MIN_CANDLES = 50

# Maximum window passed to technical_engine per bar — prevents O(n²) growth.
# 300 candles covers EMA-200 warm-up with a safety margin.
_MAX_WINDOW = 300


# ---------------------------------------------------------------------------
# Lightweight risk-settings proxy (avoids DB round-trip inside backtest loop)
# ---------------------------------------------------------------------------

class _RiskProxy:
    """Duck-types RiskSettings for use with risk_manager.assess()."""
    def __init__(
        self,
        max_position_size_pct: float,
        max_open_positions: int,
        stop_loss_pct: float,
        take_profit_pct: float,
        max_daily_loss_pct: float = 0.05,
        max_drawdown_pct: float = 0.20,
    ) -> None:
        self.max_position_size_pct = Decimal(str(max_position_size_pct))
        self.max_open_positions    = max_open_positions
        self.stop_loss_pct         = Decimal(str(stop_loss_pct))
        self.take_profit_pct       = Decimal(str(take_profit_pct))
        self.max_daily_loss_pct    = Decimal(str(max_daily_loss_pct))
        self.max_drawdown_pct      = Decimal(str(max_drawdown_pct))


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

async def run_backtest(db: AsyncSession, run_id: UUID) -> None:
    """Main backtest runner. Loads BacktestRun, replays candles, saves results."""
    result = await db.execute(select(BacktestRun).where(BacktestRun.id == run_id))
    run: Optional[BacktestRun] = result.scalars().first()
    if run is None:
        return

    run.status     = "running"
    run.started_at = datetime.now(timezone.utc)
    run.progress_pct = 0
    await db.commit()

    try:
        params: Dict[str, Any] = run.parameters or {}

        # ── Strategy params ──────────────────────────────────────────────────
        ema_fast        = int(params.get("ema_fast", 9))
        ema_slow        = int(params.get("ema_slow", 21))
        rsi_period      = int(params.get("rsi_period", 14))
        rsi_overbought  = float(params.get("rsi_overbought", 70.0))
        rsi_oversold    = float(params.get("rsi_oversold", 30.0))
        stop_loss_pct   = float(params.get("stop_loss_pct", 0.03))
        take_profit_pct = float(params.get("take_profit_pct", 0.06))
        commission_rate = float(params.get("commission", 0.001))
        position_size_pct = float(params.get("position_size_pct", 0.05))
        use_sentiment   = bool(params.get("use_sentiment", True))
        force_pct_tp_sl = bool(params.get("force_pct_tp_sl", False))
        use_signal_exit = bool(params.get("use_signal_exit", True))

        risk_proxy = _RiskProxy(
            max_position_size_pct = position_size_pct,
            max_open_positions    = 1,   # one position at a time in backtest
            stop_loss_pct         = stop_loss_pct,
            take_profit_pct       = take_profit_pct,
        )

        # ── Fetch candles ────────────────────────────────────────────────────
        all_candles = await market_data_service.get_historical_candles(
            symbol    = run.symbol,
            timeframe = run.timeframe,
            start_date= run.start_date,
            end_date  = run.end_date,
        )

        warmup = max(ema_slow, _MIN_CANDLES, 50)

        if len(all_candles) < warmup + 10:
            run.status = "failed"
            run.error_message = (
                f"Not enough candle data — need at least {warmup + 10} candles, "
                f"got {len(all_candles)}"
            )
            await db.commit()
            return

        # ── Simulation state ──────────────────────────────────────────────────
        initial_capital = float(run.initial_capital)
        cash            = initial_capital
        equity          = initial_capital
        equity_peak     = initial_capital
        position: Optional[Dict[str, Any]] = None  # open simulated position

        # Metrics accumulators
        trade_log:    List[Dict[str, Any]] = []
        equity_curve: List[Dict[str, Any]] = []
        decisions_breakdown: Dict[str, int] = {
            "BUY": 0, "SELL": 0, "HOLD": 0, "BLOCKED": 0
        }
        tech_conf_sum    = 0.0
        final_conf_sum   = 0.0
        executed_count   = 0
        sentiment_overrides = 0
        atr_used_count   = 0

        total = len(all_candles)

        for idx in range(warmup, total):
            candle = all_candles[idx]
            close  = float(candle["close"])
            ts     = candle["timestamp"]

            # Sliding window — capped to avoid O(n²) growth.
            # technical_engine only needs the last _MAX_WINDOW bars.
            window_start = max(0, idx + 1 - _MAX_WINDOW)
            window = all_candles[window_start : idx + 1]

            # ── Phase 2: technical_engine ────────────────────────────────────
            signal = technical_engine.analyze(
                symbol         = run.symbol,
                candles        = window,
                timeframe      = run.timeframe,
                ema_fast_period= ema_fast,
                ema_slow_period= ema_slow,
                rsi_period     = rsi_period,
                rsi_overbought = rsi_overbought,
                rsi_oversold   = rsi_oversold,
            )

            # ── Phase 3: sentiment_engine (momentum proxy) ───────────────────
            if use_sentiment:
                sentiment = _momentum_sentiment(run.symbol, window)
            else:
                sentiment = _neutral_sentiment(run.symbol)

            # ── Phase 4: decision_engine ──────────────────────────────────────
            decision = decision_engine.decide(
                technical = signal,
                sentiment = sentiment,
            )
            decisions_breakdown[decision.direction] = (
                decisions_breakdown.get(decision.direction, 0) + 1
            )

            # Track sentiment overrides (technical vs final direction differ)
            if (
                signal.direction in ("BUY", "SELL")
                and decision.direction != signal.direction
            ):
                sentiment_overrides += 1

            # ── Check open position for TP/SL/signal exit ────────────────────
            if position is not None:
                closed, exit_price, exit_reason = _check_exit(
                    position, close, decision, use_signal_exit=use_signal_exit
                )
                if closed:
                    pnl, cash = _close_position(
                        position, exit_price, cash, commission_rate
                    )
                    equity = cash
                    trade_log.append({
                        "entry_time"    : position["entry_time"],
                        "exit_time"     : ts,
                        "symbol"        : run.symbol,
                        "side"          : position["side"],
                        "entry_price"   : position["entry_price"],
                        "exit_price"    : exit_price,
                        "qty"           : position["qty"],
                        "pnl"           : round(pnl, 4),
                        "exit_reason"   : exit_reason,
                        "tech_direction": position["tech_direction"],
                        "final_direction": position["final_direction"],
                        "tech_conf"     : position["tech_conf"],
                        "final_conf"    : position["final_conf"],
                        "sentiment_label": position["sentiment_label"],
                        "sizing_method" : position["sizing_method"],
                    })
                    position = None

            # ── Open new position if no position and decision is actionable ──
            if position is None and decision.is_actionable:
                invest_amount = cash * position_size_pct

                if force_pct_tp_sl:
                    # Bypass ATR entirely — use percentage-based TP/SL directly.
                    # sizing still uses position_size_pct of current cash.
                    invest = round(min(invest_amount, cash), 2)
                    invest = max(0.01, invest)
                    qty    = invest / close if close > 0 else 0.0
                    comm   = invest * commission_rate
                    total_cost = invest + comm
                    approved   = cash >= total_cost and qty > 0
                    sl = _fallback_sl(decision.direction, close, stop_loss_pct)
                    tp = _fallback_tp(decision.direction, close, take_profit_pct)
                    sizing_method = "pct_forced"
                else:
                    # Phase 5: risk_manager (ATR-based preferred, pct fallback)
                    assessment = risk_manager.assess(
                        decision             = decision,
                        technical            = signal,
                        equity               = cash,
                        open_positions_count = 0,  # always 0 — one-at-a-time
                        risk_settings        = risk_proxy,
                        invest_amount        = invest_amount,
                    )
                    approved  = assessment.approved
                    invest    = assessment.position_size_dollars if approved else 0.0
                    qty       = invest / close if close > 0 else 0.0
                    comm      = invest * commission_rate
                    total_cost = invest + comm
                    sl = assessment.stop_loss_price   or _fallback_sl(decision.direction, close, stop_loss_pct)
                    tp = assessment.take_profit_price or _fallback_tp(decision.direction, close, take_profit_pct)
                    sizing_method = assessment.sizing_method if approved else "rejected"
                    if approved and assessment.sizing_method == "atr_based":
                        atr_used_count += 1

                if approved:
                    if cash >= total_cost and qty > 0:
                        cash -= total_cost

                        position = {
                            "side"           : "long" if decision.direction == "BUY" else "short",
                            "entry_price"    : close,
                            "qty"            : qty,
                            "stop_loss"      : sl,
                            "take_profit"    : tp,
                            "entry_time"     : ts,
                            "tech_direction" : signal.direction,
                            "final_direction": decision.direction,
                            "tech_conf"      : signal.confidence,
                            "final_conf"     : decision.confidence,
                            "sentiment_label": sentiment.label,
                            "sizing_method"  : sizing_method,
                        }

                        tech_conf_sum  += signal.confidence
                        final_conf_sum += decision.confidence
                        executed_count += 1

            # ── Mark-to-market equity ─────────────────────────────────────────
            if position:
                if position["side"] == "long":
                    unrealized = (close - position["entry_price"]) * position["qty"]
                else:
                    unrealized = (position["entry_price"] - close) * position["qty"]
                equity = cash + position["entry_price"] * position["qty"] + unrealized
            else:
                equity = cash

            equity_peak = max(equity_peak, equity)
            equity_curve.append({"timestamp": ts, "equity": round(equity, 4)})

            # Progress update every ~10 %
            progress = int(((idx - warmup) / max(total - warmup, 1)) * 100)
            if progress % 10 == 0 and progress != run.progress_pct:
                run.progress_pct = progress
                await db.commit()

        # ── Force-close any remaining open position at last bar ───────────────
        if position and all_candles:
            last_close = float(all_candles[-1]["close"])
            last_ts    = all_candles[-1]["timestamp"]
            pnl, cash  = _close_position(
                position, last_close, cash, commission_rate
            )
            equity = cash
            trade_log.append({
                "entry_time"     : position["entry_time"],
                "exit_time"      : last_ts,
                "symbol"         : run.symbol,
                "side"           : position["side"],
                "entry_price"    : position["entry_price"],
                "exit_price"     : last_close,
                "qty"            : position["qty"],
                "pnl"            : round(pnl, 4),
                "exit_reason"    : "end_of_data",
                "tech_direction" : position["tech_direction"],
                "final_direction": position["final_direction"],
                "tech_conf"      : position["tech_conf"],
                "final_conf"     : position["final_conf"],
                "sentiment_label": position["sentiment_label"],
                "sizing_method"  : position["sizing_method"],
            })

        # ── Final metrics ─────────────────────────────────────────────────────
        results = _compute_metrics(
            trade_log        = trade_log,
            equity_curve     = equity_curve,
            initial_capital  = initial_capital,
            final_equity     = equity,
            decisions_breakdown = decisions_breakdown,
            tech_conf_sum    = tech_conf_sum,
            final_conf_sum   = final_conf_sum,
            executed_count   = executed_count,
            sentiment_overrides = sentiment_overrides,
            atr_used_count   = atr_used_count,
            use_sentiment    = use_sentiment,
        )

        run.results      = results
        run.equity_curve = equity_curve[-500:] if len(equity_curve) > 500 else equity_curve
        run.trade_log    = trade_log
        run.status       = "completed"
        run.progress_pct = 100
        run.completed_at = datetime.now(timezone.utc)
        await db.commit()

    except Exception as exc:
        run.status        = "failed"
        run.error_message = str(exc)
        run.completed_at  = datetime.now(timezone.utc)
        await db.commit()
        raise


# ---------------------------------------------------------------------------
# Position helpers
# ---------------------------------------------------------------------------

def _check_exit(
    position: Dict[str, Any],
    close: float,
    decision,
    use_signal_exit: bool = True,
) -> Tuple[bool, float, str]:
    """Return (should_close, exit_price, reason)."""
    side = position["side"]
    sl   = position["stop_loss"]
    tp   = position["take_profit"]

    if side == "long":
        if close <= sl:
            return True, sl, "stop_loss"
        if close >= tp:
            return True, tp, "take_profit"
        if use_signal_exit and decision.direction == "SELL":
            return True, close, "signal_exit"
    else:  # short
        if close >= sl:
            return True, sl, "stop_loss"
        if close <= tp:
            return True, tp, "take_profit"
        if use_signal_exit and decision.direction == "BUY":
            return True, close, "signal_exit"

    return False, close, ""


def _close_position(
    position: Dict[str, Any],
    exit_price: float,
    cash: float,
    commission_rate: float,
) -> Tuple[float, float]:
    """Returns (net_pnl, new_cash)."""
    qty   = position["qty"]
    entry = position["entry_price"]
    side  = position["side"]

    gross = (exit_price - entry) * qty if side == "long" else (entry - exit_price) * qty
    comm  = exit_price * qty * commission_rate
    net   = gross - comm

    # Return original investment + net PnL
    new_cash = cash + entry * qty * (1 - commission_rate) + net
    return net, new_cash


def _fallback_sl(direction: str, price: float, pct: float) -> float:
    return price * (1 - pct) if direction == "BUY" else price * (1 + pct)


def _fallback_tp(direction: str, price: float, pct: float) -> float:
    return price * (1 + pct) if direction == "BUY" else price * (1 - pct)


# ---------------------------------------------------------------------------
# Sentiment simulation (momentum proxy — no live API calls)
# ---------------------------------------------------------------------------

_POSITIVE_HEADLINES = [
    ("{sym} surges on strong earnings beat", "Revenue and EPS exceeded analyst expectations."),
    ("{sym} upgraded to Strong Buy by analysts", "Multiple firms raised price targets."),
    ("{sym} rally continues as momentum builds", "Technical breakout confirmed by high volume."),
    ("{sym} announces record revenue growth", "Sales climbed sharply driven by robust demand."),
    ("{sym} gains on bullish market sentiment", "Investor confidence boosted by macro data."),
]

_NEGATIVE_HEADLINES = [
    ("{sym} drops on disappointing earnings miss", "EPS fell short of consensus estimates."),
    ("{sym} downgraded to Sell by major firm", "Analysts cite weakening demand and margin pressure."),
    ("{sym} tumbles amid market volatility", "Risk-off sentiment weighs on growth stocks."),
    ("{sym} issues profit warning for next quarter", "Management revised guidance significantly lower."),
    ("{sym} plunges on restructuring concerns", "Layoff announcement raises operational risk."),
]

_NEUTRAL_HEADLINES = [
    ("{sym} holds steady as investors await data", "Trading range-bound ahead of key economic releases."),
    ("{sym} in line with earnings expectations", "Results met consensus; no significant catalysts."),
    ("{sym} moves with broader market trend", "Sector rotation driving mixed price action."),
]


def _momentum_sentiment(symbol: str, window: List[dict]) -> SentimentResult:
    """Derive SentimentResult from recent price momentum (no API call)."""
    now = datetime.now(timezone.utc)

    if len(window) < 6:
        return _neutral_sentiment(symbol)

    closes    = [float(c["close"]) for c in window]
    c_now     = closes[-1]
    c_5ago    = closes[-6]
    momentum  = (c_now - c_5ago) / c_5ago if c_5ago > 0 else 0.0

    if momentum > 0.015:
        templates = _POSITIVE_HEADLINES[:5]
        score     = min(0.8,  momentum * 20)
        label     = "positive"
        impact    = 55
    elif momentum > 0.005:
        templates = _POSITIVE_HEADLINES[:2]
        score     = min(0.4,  momentum * 20)
        label     = "positive"
        impact    = 30
    elif momentum < -0.015:
        templates = _NEGATIVE_HEADLINES[:5]
        score     = max(-0.8, momentum * 20)
        label     = "negative"
        impact    = 55
    elif momentum < -0.005:
        templates = _NEGATIVE_HEADLINES[:2]
        score     = max(-0.4, momentum * 20)
        label     = "negative"
        impact    = 30
    else:
        templates = _NEUTRAL_HEADLINES[:2]
        score     = 0.0
        label     = "neutral"
        impact    = 10

    items = [
        NewsItem(
            title       = t.format(sym=symbol),
            description = d.format(sym=symbol),
            source      = "simulated",
            published_at= now,
        )
        for t, d in templates
    ]

    return sentiment_engine.analyze(symbol=symbol, items=items)


def _neutral_sentiment(symbol: str) -> SentimentResult:
    """Fixed neutral sentiment used when use_sentiment=False."""
    from app.schemas.sentiment import SentimentResult
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


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

def _compute_metrics(
    trade_log: List[dict],
    equity_curve: List[dict],
    initial_capital: float,
    final_equity: float,
    decisions_breakdown: Dict[str, int],
    tech_conf_sum: float,
    final_conf_sum: float,
    executed_count: int,
    sentiment_overrides: int,
    atr_used_count: int,
    use_sentiment: bool,
) -> dict:
    total_trades = len(trade_log)
    win_trades   = sum(1 for t in trade_log if t["pnl"] > 0)
    loss_trades  = sum(1 for t in trade_log if t["pnl"] <= 0)
    win_rate     = (win_trades / total_trades * 100) if total_trades > 0 else 0.0
    net_pnl      = final_equity - initial_capital

    avg_win = (
        sum(t["pnl"] for t in trade_log if t["pnl"] > 0) / win_trades
        if win_trades > 0 else 0.0
    )
    avg_loss = (
        sum(t["pnl"] for t in trade_log if t["pnl"] <= 0) / loss_trades
        if loss_trades > 0 else 0.0
    )

    # Max drawdown
    peak   = initial_capital
    max_dd = 0.0
    for pt in equity_curve:
        e = pt["equity"]
        if e > peak:
            peak = e
        dd = (peak - e) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)

    # Sharpe ratio (annualised, simplified)
    daily_returns: List[float] = []
    prev_eq = initial_capital
    for pt in equity_curve:
        e = pt["equity"]
        if prev_eq > 0:
            daily_returns.append((e - prev_eq) / prev_eq)
        prev_eq = e

    sharpe = 0.0
    if len(daily_returns) > 1:
        mean_r   = sum(daily_returns) / len(daily_returns)
        variance = sum((r - mean_r) ** 2 for r in daily_returns) / len(daily_returns)
        std_r    = math.sqrt(variance)
        sharpe   = (mean_r / std_r * math.sqrt(252)) if std_r > 0 else 0.0

    # Profit factor
    gross_profit = sum(t["pnl"] for t in trade_log if t["pnl"] > 0)
    gross_loss   = abs(sum(t["pnl"] for t in trade_log if t["pnl"] < 0))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else 0.0

    # Exit breakdown
    exit_reasons: Dict[str, int] = {}
    for t in trade_log:
        r = t.get("exit_reason", "unknown")
        exit_reasons[r] = exit_reasons.get(r, 0) + 1

    # Expectancy per trade: (win_rate × avg_win) + (loss_rate × avg_loss)
    # avg_loss is already negative, so this gives the expected $ per trade.
    win_rate_dec = win_rate / 100.0
    expectancy   = round(win_rate_dec * avg_win + (1.0 - win_rate_dec) * avg_loss, 4)

    return {
        # Core performance
        "total_trades"         : total_trades,
        "win_trades"           : win_trades,
        "loss_trades"          : loss_trades,
        "win_rate"             : round(win_rate, 2),
        "net_pnl"              : round(net_pnl, 4),
        "net_pnl_pct"          : round((net_pnl / initial_capital) * 100, 2) if initial_capital > 0 else 0.0,
        "final_equity"         : round(final_equity, 4),
        "max_drawdown"         : round(max_dd * 100, 4),
        "sharpe_ratio"         : round(sharpe, 4),
        "profit_factor"        : round(profit_factor, 4),
        "avg_win"              : round(avg_win, 4),
        "avg_loss"             : round(avg_loss, 4),
        "expectancy_per_trade" : expectancy,
        # Engine breakdown
        "decisions_breakdown"  : decisions_breakdown,
        "exit_reasons"         : exit_reasons,
        "avg_technical_conf"   : round(tech_conf_sum / executed_count, 1) if executed_count > 0 else 0.0,
        "avg_final_conf"       : round(final_conf_sum / executed_count, 1) if executed_count > 0 else 0.0,
        "sentiment_overrides"  : sentiment_overrides,
        "atr_sl_tp_used_pct"   : round(atr_used_count / executed_count * 100, 1) if executed_count > 0 else 0.0,
        "use_sentiment"        : use_sentiment,
    }


# ---------------------------------------------------------------------------
# CRUD helpers (unchanged public API)
# ---------------------------------------------------------------------------

async def create_backtest_run(
    db: AsyncSession, user_id: UUID, params: dict
) -> BacktestRun:
    """Create BacktestRun record and launch background asyncio task."""
    from datetime import date, timedelta

    symbol    = params.get("symbol", "EURUSD")
    timeframe = params.get("timeframe", "1h")
    start_date = params.get("start_date")
    end_date   = params.get("end_date")
    initial_capital = Decimal(str(params.get("initial_capital", 10000)))

    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)
    if start_date is None:
        start_date = (datetime.now(timezone.utc) - timedelta(days=90)).date()
    if end_date is None:
        end_date = datetime.now(timezone.utc).date()

    run = BacktestRun(
        id              = uuid4(),
        user_id         = user_id,
        symbol          = symbol,
        timeframe       = timeframe,
        start_date      = start_date,
        end_date        = end_date,
        initial_capital = initial_capital,
        parameters      = params,
        status          = "queued",
        progress_pct    = 0,
        created_at      = datetime.now(timezone.utc),
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)

    run_id = run.id
    asyncio.create_task(_run_backtest_background(run_id))
    return run


async def _run_backtest_background(run_id: UUID) -> None:
    from app.db.session import get_db as get_db_context
    async for db in get_db_context():
        try:
            await run_backtest(db, run_id)
        except Exception:
            pass
        return  # consume exactly one session


async def get_backtest_runs(
    db: AsyncSession, user_id: UUID, limit: int = 20, offset: int = 0
) -> Tuple[List[BacktestRun], int]:
    count_result = await db.execute(
        select(func.count(BacktestRun.id)).where(BacktestRun.user_id == user_id)
    )
    total: int = count_result.scalar() or 0

    result = await db.execute(
        select(BacktestRun)
        .where(BacktestRun.user_id == user_id)
        .order_by(BacktestRun.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all()), total


async def get_backtest_run(
    db: AsyncSession, run_id: UUID, user_id: UUID
) -> BacktestRun:
    result = await db.execute(
        select(BacktestRun).where(
            BacktestRun.id == run_id, BacktestRun.user_id == user_id
        )
    )
    run: Optional[BacktestRun] = result.scalars().first()
    if run is None:
        raise ValueError(f"Backtest run {run_id} not found")
    return run
