"""Bot control endpoints: start, stop, status, config, logs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import traceback as _traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
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


async def _get_or_create_bot_state(user: User, db: AsyncSession) -> BotState:
    result = await db.execute(select(BotState).where(BotState.user_id == user.id))
    state: BotState | None = result.scalars().first()
    if state is None:
        state = BotState(user_id=user.id, is_running=False)
        db.add(state)
        await db.flush()
    return state


async def _get_or_create_strategy(user: User, db: AsyncSession) -> StrategyConfig:
    result = await db.execute(select(StrategyConfig).where(StrategyConfig.user_id == user.id))
    config: StrategyConfig | None = result.scalars().first()
    if config is None:
        config = StrategyConfig(user_id=user.id, **_DEFAULT_STRATEGY)
        db.add(config)
        await db.flush()
    return config


async def _get_or_create_risk(user: User, db: AsyncSession) -> RiskSettings:
    result = await db.execute(select(RiskSettings).where(RiskSettings.user_id == user.id))
    risk: RiskSettings | None = result.scalars().first()
    if risk is None:
        risk = RiskSettings(user_id=user.id)
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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    print(f"[STATUS] enter user={current_user.id}", flush=True)
    try:
        return await _get_bot_status_inner(current_user, db)
    except Exception as exc:
        tb = _traceback.format_exc()
        print(f"[STATUS] EXCEPTION: {exc}\n{tb}", flush=True)
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")


async def _get_bot_status_inner(current_user: User, db: AsyncSession) -> dict:
    state = await _get_or_create_bot_state(current_user, db)
    print(f"[STATUS] got state: is_running={state.is_running} cycles={state.cycles_run}", flush=True)
    config = await _get_or_create_strategy(current_user, db)
    print(f"[STATUS] got config: symbols={config.symbols}", flush=True)

    # Open positions count
    port_r = await db.execute(select(Portfolio).where(Portfolio.user_id == current_user.id))
    portfolio = port_r.scalars().first()
    open_count = 0
    if portfolio:
        pos_r = await db.execute(
            select(Position).where(
                Position.portfolio_id == portfolio.id,
                Position.is_open == True,  # noqa: E712
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

    # No explicit commit needed — get_db dependency commits after the endpoint returns.
    print(f"[STATUS] building return dict, last_log={state.last_log!r}", flush=True)
    return {
        "is_running": state.is_running,
        "started_at": state.started_at.isoformat() if state.started_at else None,
        "last_run_at": state.last_cycle_at.isoformat() if state.last_cycle_at else None,
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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BotConfigOut:
    config = await _get_or_create_strategy(current_user, db)
    risk = await _get_or_create_risk(current_user, db)
    await db.commit()
    return _build_config_out(config, risk)


# ── PUT /config ───────────────────────────────────────────────────────────────

@router.put("/config", response_model=BotConfigOut, summary="Save bot configuration")
async def update_bot_config(
    payload: BotConfigUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BotConfigOut:
    config = await _get_or_create_strategy(current_user, db)
    risk = await _get_or_create_risk(current_user, db)

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
    print(f"[SaveConfig] user={current_user.id} symbols={saved_symbols}")
    return _build_config_out(config, risk)


# ── GET /logs ─────────────────────────────────────────────────────────────────

@router.get("/logs", summary="Full bot cycle log history")
async def get_bot_logs(
    limit: int = 50,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    from app.models.bot_log import BotLog
    result = await db.execute(
        select(BotLog)
        .where(BotLog.user_id == current_user.id)
        .order_by(BotLog.timestamp.desc())
        .limit(limit)
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
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    state = await _get_or_create_bot_state(current_user, db)
    if state.is_running:
        return {"message": "Bot is already running", "is_running": True}

    config = await _get_or_create_strategy(current_user, db)
    symbols = list(config.symbols or [])
    print(f"[StartBot] user={current_user.id} symbols={symbols}")

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
    print(f"[StartBot] Bot started for user={current_user.id} with symbols={symbols}")
    return {"message": "Bot started successfully", "is_running": True}


# ── DELETE /logs ──────────────────────────────────────────────────────────────

@router.delete("/logs", summary="Clear all bot cycle logs for the current user")
async def clear_bot_logs(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    from sqlalchemy import delete
    from app.models.bot_log import BotLog
    await db.execute(delete(BotLog).where(BotLog.user_id == current_user.id))
    state = await _get_or_create_bot_state(current_user, db)
    state.last_log = None
    await db.commit()
    return {"cleared": True}


# ── POST /stop ────────────────────────────────────────────────────────────────

@router.post("/stop", summary="Stop the auto-trading bot")
async def stop_bot(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    state = await _get_or_create_bot_state(current_user, db)
    if not state.is_running:
        return {"message": "Bot is not running", "is_running": False}
    state.is_running = False
    state.last_log = f"Bot stopped after {state.cycles_run} cycles"
    await db.commit()
    return {"message": "Bot stopped successfully", "is_running": False}
