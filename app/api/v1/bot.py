"""Bot control endpoints: start, stop, status, config, logs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import traceback as _traceback

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_account_mode, get_current_active_user, get_db
from app.models.bot_state import BotState
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.models.risk_settings import RiskSettings
from app.models.strategy_config import StrategyConfig
from app.models.strategy_signal import StrategySignal
from app.models.user import User
from app.schemas.strategy import BotConfigOut, BotConfigUpdate

router = APIRouter()

_DEFAULT_STRATEGY = dict(
    ema_fast=9,
    ema_slow=21,
    rsi_period=14,
    rsi_overbought=Decimal("70"),
    rsi_oversold=Decimal("30"),
    auto_trade=False,
    symbols=[],
    asset_classes=["stocks"],
    investment_amount=Decimal("100"),
    run_interval_seconds=60,
    per_symbol_max_positions=1,
    allow_buy=True,
    allow_sell=True,
    cooldown_seconds=0,
)


async def _get_or_create_bot_state(user: User, db: AsyncSession, bot_id: str, account_mode: str = "paper") -> BotState:
    result = await db.execute(
        select(BotState).where(
            BotState.user_id == user.id,
            BotState.bot_id == bot_id,
            BotState.account_mode == account_mode,
        )
    )
    state: BotState | None = result.scalars().first()
    if state is None:
        state = BotState(user_id=user.id, bot_id=bot_id, account_mode=account_mode, is_running=False)
        db.add(state)
        await db.flush()
    return state


async def _get_or_create_strategy(user: User, db: AsyncSession, bot_id: str, account_mode: str = "paper") -> StrategyConfig:
    result = await db.execute(
        select(StrategyConfig).where(
            StrategyConfig.user_id == user.id,
            StrategyConfig.bot_id  == bot_id,
            StrategyConfig.account_mode == account_mode,
        )
    )
    config: StrategyConfig | None = result.scalars().first()
    if config is None:
        config = StrategyConfig(user_id=user.id, bot_id=bot_id, account_mode=account_mode, **_DEFAULT_STRATEGY)
        db.add(config)
        await db.flush()
    return config


async def _get_or_create_risk(user: User, db: AsyncSession, bot_id: str, account_mode: str = "paper") -> RiskSettings:
    result = await db.execute(
        select(RiskSettings).where(
            RiskSettings.user_id == user.id,
            RiskSettings.bot_id  == bot_id,
            RiskSettings.account_mode == account_mode,
        )
    )
    risk: RiskSettings | None = result.scalars().first()
    if risk is None:
        risk = RiskSettings(user_id=user.id, bot_id=bot_id, account_mode=account_mode)
        db.add(risk)
        await db.flush()
    return risk


def _build_config_out(config: StrategyConfig, risk: RiskSettings) -> BotConfigOut:
    return BotConfigOut(
        ema_fast=config.ema_fast,
        ema_slow=config.ema_slow,
        rsi_period=config.rsi_period,
        rsi_overbought=config.rsi_overbought,
        rsi_oversold=config.rsi_oversold,
        symbols=list(config.symbols or []),
        asset_classes=list(config.asset_classes or ["stocks"]),
        investment_amount=config.investment_amount,
        run_interval_seconds=config.run_interval_seconds,
        per_symbol_max_positions=config.per_symbol_max_positions,
        allow_buy=config.allow_buy,
        allow_sell=config.allow_sell,
        cooldown_seconds=config.cooldown_seconds,
        stop_loss_pct=risk.stop_loss_pct,
        take_profit_pct=risk.take_profit_pct,
        max_open_positions=risk.max_open_positions,
        max_daily_loss_pct=risk.max_daily_loss_pct,
        max_position_size_pct=risk.max_position_size_pct,
    )


# ── GET /status ──────────────────────────────────────────────────────────────

@router.get("/status", summary="Get bot status")
async def get_bot_status(
    bot_id: str = Query(..., description="Bot identifier (e.g. trendmaster)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    print(f"[STATUS] enter user={current_user.id} bot_id={bot_id}", flush=True)
    try:
        return await _get_bot_status_inner(current_user, db, bot_id, account_mode)
    except Exception as exc:
        tb = _traceback.format_exc()
        print(f"[STATUS] EXCEPTION: {exc}\n{tb}", flush=True)
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


async def _get_bot_status_inner(current_user: User, db: AsyncSession, bot_id: str, account_mode: str = "paper") -> dict:
    state = await _get_or_create_bot_state(current_user, db, bot_id, account_mode)
    print(f"[STATUS] got state: bot_id={bot_id} is_running={state.is_running} cycles={state.cycles_run}", flush=True)
    config = await _get_or_create_strategy(current_user, db, bot_id, account_mode)
    print(f"[STATUS] got config: symbols={config.symbols}", flush=True)

    # Open positions count for this bot
    port_r = await db.execute(
        select(Portfolio).where(Portfolio.user_id == current_user.id, Portfolio.account_mode == account_mode)
    )
    portfolio = port_r.scalars().first()
    open_count = 0
    if portfolio:
        pos_r = await db.execute(
            select(Position).where(
                Position.portfolio_id == portfolio.id,
                Position.is_open == True,  # noqa: E712
                Position.bot_id == bot_id,
            )
        )
        open_count = len(pos_r.scalars().all())

    # Last signal
    sig_r = await db.execute(
        select(StrategySignal)
        .where(StrategySignal.user_id == current_user.id)
        .order_by(StrategySignal.triggered_at.desc())
        .limit(1)
    )
    last_signal = sig_r.scalar_one_or_none()

    # Next run estimate
    next_run_at = None
    if state.is_running and state.last_cycle_at:
        next_run_at = (
            state.last_cycle_at + timedelta(seconds=int(config.run_interval_seconds))
        ).isoformat()

    print(f"[STATUS] building return dict, last_log={state.last_log!r}", flush=True)
    return {
        "is_running": state.is_running,
        "started_at": state.started_at.isoformat() if state.started_at else None,
        "last_run_at": state.last_cycle_at.isoformat() if state.last_cycle_at else None,
        "last_cycle_at": state.last_cycle_at.isoformat() if state.last_cycle_at else None,
        "next_run_at": next_run_at,
        "cycles_run": state.cycles_run,
        "open_positions_count": open_count,
        "monitored_symbols": list(config.symbols or []),
        "last_log": state.last_log,
        "last_error": getattr(state, "last_error", None),
        "last_signal": {
            "symbol": last_signal.symbol,
            "signal_type": last_signal.signal_type,
            "triggered_at": last_signal.triggered_at.isoformat(),
            "acted_on": last_signal.acted_on,
            "confidence": float(last_signal.confidence),
        }
        if last_signal
        else None,
    }


# ── GET /config ───────────────────────────────────────────────────────────────

@router.get("/config", response_model=BotConfigOut, summary="Get bot configuration")
async def get_bot_config(
    bot_id: str = Query(..., description="Bot identifier"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> BotConfigOut:
    config = await _get_or_create_strategy(current_user, db, bot_id, account_mode)
    risk = await _get_or_create_risk(current_user, db, bot_id, account_mode)
    await db.commit()
    return _build_config_out(config, risk)


# ── PUT /config ───────────────────────────────────────────────────────────────

@router.put("/config", response_model=BotConfigOut, summary="Save bot configuration")
async def update_bot_config(
    payload: BotConfigUpdate,
    bot_id: str = Query(..., description="Bot identifier"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> BotConfigOut:
    config = await _get_or_create_strategy(current_user, db, bot_id, account_mode)
    risk = await _get_or_create_risk(current_user, db, bot_id, account_mode)

    strategy_fields = [
        "ema_fast", "ema_slow", "rsi_period", "rsi_overbought", "rsi_oversold",
        "symbols", "asset_classes", "investment_amount", "run_interval_seconds",
        "per_symbol_max_positions", "allow_buy", "allow_sell", "cooldown_seconds",
    ]
    for field in strategy_fields:
        val = getattr(payload, field)
        if val is not None:
            setattr(config, field, val)

    risk_fields = [
        "stop_loss_pct", "take_profit_pct", "max_open_positions",
        "max_daily_loss_pct", "max_position_size_pct",
    ]
    for field in risk_fields:
        val = getattr(payload, field)
        if val is not None:
            setattr(risk, field, val)

    await db.commit()
    saved_symbols = list(config.symbols or [])
    print(f"[SaveConfig] user={current_user.id} bot_id={bot_id} symbols={saved_symbols}")
    return _build_config_out(config, risk)


# ── GET /logs ─────────────────────────────────────────────────────────────────

@router.get("/logs", summary="Full bot cycle log history")
async def get_bot_logs(
    limit: int = 50,
    bot_id: str | None = Query(None, description="Filter by bot (omit for all bots)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    from app.models.bot_log import BotLog
    q = select(BotLog).where(BotLog.user_id == current_user.id)
    if bot_id:
        q = q.where(BotLog.bot_id == bot_id)
    result = await db.execute(
        q.order_by(BotLog.timestamp.desc()).limit(limit)
    )
    entries = result.scalars().all()
    return {
        "logs": [
            {
                "timestamp": e.timestamp.isoformat(),
                "message": e.message,
                "symbol": e.symbol,
            }
            for e in entries
        ]
    }


# ── POST /start ───────────────────────────────────────────────────────────────

@router.post("/start", summary="Start the auto-trading bot")
async def start_bot(
    bot_id: str = Query(..., description="Bot identifier"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    state = await _get_or_create_bot_state(current_user, db, bot_id, account_mode)
    if state.is_running:
        return {"message": "Bot is already running", "is_running": True}

    config = await _get_or_create_strategy(current_user, db, bot_id, account_mode)
    symbols = list(config.symbols or [])
    print(f"[StartBot] user={current_user.id} bot_id={bot_id} symbols={symbols}")

    if not symbols:
        raise HTTPException(
            status_code=400,
            detail="No symbols configured. Select at least one symbol and save config before starting.",
        )

    state.is_running = True
    state.started_at = datetime.now(timezone.utc)
    state.last_log = f"Bot started — monitoring {len(symbols)} symbol(s): {', '.join(symbols)}"
    if hasattr(state, "last_error"):
        state.last_error = None
    await db.commit()
    print(f"[StartBot] Bot started for user={current_user.id} bot_id={bot_id} symbols={symbols}")
    return {"message": "Bot started successfully", "is_running": True}


# ── DELETE /logs ──────────────────────────────────────────────────────────────

@router.delete("/logs", summary="Clear all bot cycle logs for the current user")
async def clear_bot_logs(
    bot_id: str | None = Query(None, description="Clear logs for specific bot (omit for all)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    from sqlalchemy import delete
    from app.models.bot_log import BotLog
    q = delete(BotLog).where(BotLog.user_id == current_user.id)
    if bot_id:
        q = q.where(BotLog.bot_id == bot_id)
    await db.execute(q)

    # Reset last_log on the relevant bot state(s)
    if bot_id:
        state_q = select(BotState).where(
            BotState.user_id == current_user.id,
            BotState.bot_id  == bot_id,
        )
    else:
        state_q = select(BotState).where(BotState.user_id == current_user.id)
    states_r = await db.execute(state_q)
    for s in states_r.scalars().all():
        s.last_log = None

    await db.commit()
    return {"cleared": True}


# ── POST /activate/trendmaster ────────────────────────────────────────────────

@router.post("/activate/trendmaster", summary="Configure and start the TrendMaster bot")
async def activate_trendmaster(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    _bot_id = "trendmaster"
    config = await _get_or_create_strategy(current_user, db, _bot_id, account_mode)
    risk   = await _get_or_create_risk(current_user, db, _bot_id, account_mode)

    config.ema_fast              = 9
    config.ema_slow              = 21
    config.rsi_period            = 14
    config.rsi_overbought        = Decimal("75")
    config.rsi_oversold          = Decimal("45")
    config.symbols               = ["EUR/USD", "GBP/USD", "USD/CHF", "AUD/USD"]
    config.asset_classes         = ["forex"]
    config.investment_amount     = Decimal("200")
    config.run_interval_seconds  = 300
    config.per_symbol_max_positions = 1
    config.allow_buy             = True
    config.allow_sell            = True
    config.cooldown_seconds      = 900

    risk.stop_loss_pct           = Decimal("0.015")
    risk.take_profit_pct         = Decimal("0.03")
    risk.max_open_positions      = 4
    risk.max_daily_loss_pct      = Decimal("0.03")
    risk.max_position_size_pct   = Decimal("0.10")

    state = await _get_or_create_bot_state(current_user, db, _bot_id, account_mode)
    state.is_running  = True
    state.started_at  = datetime.now(timezone.utc)
    state.last_log    = "TrendMaster activated — monitoring EUR/USD, GBP/USD, USD/CHF, AUD/USD"
    if hasattr(state, "last_error"):
        state.last_error = None

    await db.commit()
    return {
        "message":    "TrendMaster activated",
        "strategy":   _bot_id,
        "symbols":    list(config.symbols),
        "timeframe":  "5m",
        "is_running": True,
    }


# ── POST /activate/scalperx ───────────────────────────────────────────────────

@router.post("/activate/scalperx", summary="Configure and start the Mean Reversion bot")
async def activate_scalperx(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    _bot_id = "scalperx"
    config = await _get_or_create_strategy(current_user, db, _bot_id, account_mode)
    risk   = await _get_or_create_risk(current_user, db, _bot_id, account_mode)

    config.ema_fast              = 20
    config.ema_slow              = 5
    config.rsi_period            = 14
    config.rsi_overbought        = Decimal("70")
    config.rsi_oversold          = Decimal("30")
    config.symbols               = ["EUR/USD", "EUR/GBP", "USD/CHF", "AUD/NZD"]
    config.asset_classes         = ["forex"]
    config.investment_amount     = Decimal("200")
    config.run_interval_seconds  = 900
    config.per_symbol_max_positions = 1
    config.allow_buy             = True
    config.allow_sell            = True
    config.cooldown_seconds      = 1800

    risk.stop_loss_pct           = Decimal("0.02")
    risk.take_profit_pct         = Decimal("0.03")
    risk.max_open_positions      = 4
    risk.max_daily_loss_pct      = Decimal("0.03")
    risk.max_position_size_pct   = Decimal("0.10")

    state = await _get_or_create_bot_state(current_user, db, _bot_id, account_mode)
    state.is_running  = True
    state.started_at  = datetime.now(timezone.utc)
    state.last_log    = "Mean Reversion activated — EUR/USD, EUR/GBP, USD/CHF, AUD/NZD"
    if hasattr(state, "last_error"):
        state.last_error = None

    await db.commit()
    return {
        "message":   "Mean Reversion activated",
        "strategy":  _bot_id,
        "symbols":   list(config.symbols),
        "timeframe": "15m",
        "is_running": True,
    }


# ── POST /activate/piphunter ─────────────────────────────────────────────────

@router.post("/activate/piphunter", summary="Configure and start the Breakout bot")
async def activate_piphunter(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    _bot_id = "piphunter"
    config = await _get_or_create_strategy(current_user, db, _bot_id, account_mode)
    risk   = await _get_or_create_risk(current_user, db, _bot_id, account_mode)

    config.ema_fast              = 14
    config.ema_slow              = 50
    config.rsi_period            = 14
    config.rsi_overbought        = Decimal("70")
    config.rsi_oversold          = Decimal("30")
    config.symbols               = ["GBP/USD", "EUR/USD", "GBP/JPY", "USD/JPY"]
    config.asset_classes         = ["forex"]
    config.investment_amount     = Decimal("200")
    config.run_interval_seconds  = 900
    config.per_symbol_max_positions = 1
    config.allow_buy             = True
    config.allow_sell            = True
    config.cooldown_seconds      = 3600

    risk.stop_loss_pct           = Decimal("0.01")
    risk.take_profit_pct         = Decimal("0.03")
    risk.max_open_positions      = 3
    risk.max_daily_loss_pct      = Decimal("0.03")
    risk.max_position_size_pct   = Decimal("0.10")

    state = await _get_or_create_bot_state(current_user, db, _bot_id, account_mode)
    state.is_running  = True
    state.started_at  = datetime.now(timezone.utc)
    state.last_log    = "Breakout bot activated — GBP/USD, EUR/USD, GBP/JPY, USD/JPY"
    if hasattr(state, "last_error"):
        state.last_error = None

    await db.commit()
    return {
        "message":   "Breakout bot activated",
        "strategy":  _bot_id,
        "symbols":   list(config.symbols),
        "timeframe": "15m",
        "is_running": True,
    }


# ── POST /activate/cryptobot ─────────────────────────────────────────────────

@router.post("/activate/cryptobot", summary="Configure and start the Momentum bot")
async def activate_cryptobot(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    _bot_id = "cryptobot"
    config = await _get_or_create_strategy(current_user, db, _bot_id, account_mode)
    risk   = await _get_or_create_risk(current_user, db, _bot_id, account_mode)

    config.ema_fast              = 10
    config.ema_slow              = 100
    config.rsi_period            = 10
    config.rsi_overbought        = Decimal("75")
    config.rsi_oversold          = Decimal("50")
    config.symbols               = ["GBP/USD", "EUR/USD", "AUD/USD", "USD/JPY", "NZD/USD"]
    config.asset_classes         = ["forex"]
    config.investment_amount     = Decimal("200")
    config.run_interval_seconds  = 3600
    config.per_symbol_max_positions = 1
    config.allow_buy             = True
    config.allow_sell            = True
    config.cooldown_seconds      = 7200

    risk.stop_loss_pct           = Decimal("0.015")
    risk.take_profit_pct         = Decimal("0.06")
    risk.max_open_positions      = 2
    risk.max_daily_loss_pct      = Decimal("0.02")
    risk.max_position_size_pct   = Decimal("0.15")

    state = await _get_or_create_bot_state(current_user, db, _bot_id, account_mode)
    state.is_running  = True
    state.started_at  = datetime.now(timezone.utc)
    state.last_log    = "Momentum activated — GBP/USD, EUR/USD, AUD/USD, USD/JPY, NZD/USD"
    if hasattr(state, "last_error"):
        state.last_error = None

    await db.commit()
    return {
        "message":   "Momentum activated",
        "strategy":  _bot_id,
        "symbols":   list(config.symbols),
        "timeframe": "1h",
        "is_running": True,
    }


# ── POST /activate/safeguard ──────────────────────────────────────────────────

@router.post("/activate/safeguard", summary="Configure and start the Carry Trade bot")
async def activate_safeguard(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    _bot_id = "safeguard"
    config = await _get_or_create_strategy(current_user, db, _bot_id, account_mode)
    risk   = await _get_or_create_risk(current_user, db, _bot_id, account_mode)

    config.ema_fast              = 50
    config.ema_slow              = 200
    config.rsi_period            = 14
    config.rsi_overbought        = Decimal("70")
    config.rsi_oversold          = Decimal("35")
    config.symbols               = ["AUD/JPY", "NZD/JPY", "GBP/JPY", "USD/JPY"]
    config.asset_classes         = ["forex"]
    config.investment_amount     = Decimal("200")
    config.run_interval_seconds  = 86400
    config.per_symbol_max_positions = 1
    config.allow_buy             = True
    config.allow_sell            = False
    config.cooldown_seconds      = 86400

    risk.stop_loss_pct           = Decimal("0.03")
    risk.take_profit_pct         = Decimal("0.09")
    risk.max_open_positions      = 3
    risk.max_daily_loss_pct      = Decimal("0.025")
    risk.max_position_size_pct   = Decimal("0.08")

    state = await _get_or_create_bot_state(current_user, db, _bot_id, account_mode)
    state.is_running  = True
    state.started_at  = datetime.now(timezone.utc)
    state.last_log    = "Carry Trade bot activated — AUD/JPY, NZD/JPY, GBP/JPY, USD/JPY"
    if hasattr(state, "last_error"):
        state.last_error = None

    await db.commit()
    return {
        "message":   "Carry Trade bot activated",
        "strategy":  _bot_id,
        "symbols":   list(config.symbols),
        "timeframe": "1d",
        "is_running": True,
    }


# ── POST /activate/combo ─────────────────────────────────────────────────────

@router.post("/activate/combo", summary="Configure and start the MasterBot multi-strategy system")
async def activate_combo(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    _bot_id = "combo"
    config = await _get_or_create_strategy(current_user, db, _bot_id, account_mode)
    risk   = await _get_or_create_risk(current_user, db, _bot_id, account_mode)

    config.ema_fast              = 9
    config.ema_slow              = 200
    config.rsi_period            = 14
    config.rsi_overbought        = Decimal("70")
    config.rsi_oversold          = Decimal("30")
    config.symbols               = ["EUR/USD", "GBP/USD", "AUD/JPY", "GBP/JPY", "USD/JPY", "NZD/JPY"]
    config.asset_classes         = ["forex"]
    config.investment_amount     = Decimal("200")
    config.run_interval_seconds  = 3600
    config.per_symbol_max_positions = 1
    config.allow_buy             = True
    config.allow_sell            = True
    config.cooldown_seconds      = 3600

    risk.stop_loss_pct           = Decimal("0.015")
    risk.take_profit_pct         = Decimal("0.03")
    risk.max_open_positions      = 3
    risk.max_daily_loss_pct      = Decimal("0.03")
    risk.max_position_size_pct   = Decimal("0.10")

    state = await _get_or_create_bot_state(current_user, db, _bot_id, account_mode)
    state.is_running  = True
    state.started_at  = datetime.now(timezone.utc)
    state.last_log    = "MasterBot activated — Multi-Strategy on EUR/USD, GBP/USD, AUD/JPY, GBP/JPY, USD/JPY, NZD/JPY"
    if hasattr(state, "last_error"):
        state.last_error = None

    await db.commit()
    return {
        "message":    "MasterBot activated",
        "strategy":   _bot_id,
        "symbols":    list(config.symbols),
        "timeframe":  "1h",
        "is_running": True,
    }


# ── POST /stop ────────────────────────────────────────────────────────────────

@router.post("/stop", summary="Stop the auto-trading bot")
async def stop_bot(
    bot_id: str = Query(..., description="Bot identifier"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
    account_mode: str = Depends(get_account_mode),
) -> dict:
    state = await _get_or_create_bot_state(current_user, db, bot_id, account_mode)
    if not state.is_running:
        return {"message": "Bot is not running", "is_running": False}
    state.is_running = False
    state.last_log = f"Bot stopped after {state.cycles_run} cycles"
    await db.commit()
    return {"message": "Bot stopped successfully", "is_running": False}
