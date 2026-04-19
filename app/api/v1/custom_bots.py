"""Custom bot CRUD + control endpoints."""
from __future__ import annotations

import re
import uuid as _uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_account_mode, get_current_active_user, get_db
from app.core.logger import get_logger
from app.models.bot_log import BotLog
from app.models.bot_state import BotState
from app.models.custom_bot import CustomBot
from app.models.risk_settings import RiskSettings
from app.models.strategy_config import StrategyConfig
from app.models.user import User
from app.schemas.custom_bot import (
    CustomBotCreate,
    CustomBotListItem,
    CustomBotOut,
    CustomBotUpdate,
)

log = get_logger(__name__)
router = APIRouter()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _slug(name: str) -> str:
    """Convert a name to a safe slug component."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s[:30].strip("_")


def _make_bot_id(cb_id: _uuid.UUID, name: str) -> str:
    return f"custom_{_slug(name)}_{str(cb_id)[:8]}"


async def _sync_strategy_config(
    db: AsyncSession,
    user: User,
    bot_id: str,
    account_mode: str,
    cfg: dict,
) -> None:
    """Upsert StrategyConfig for a custom bot from its config dict."""
    result = await db.execute(
        select(StrategyConfig).where(
            StrategyConfig.user_id == user.id,
            StrategyConfig.bot_id == bot_id,
            StrategyConfig.account_mode == account_mode,
        )
    )
    sc = result.scalars().first()
    if sc is None:
        sc = StrategyConfig(user_id=user.id, bot_id=bot_id, account_mode=account_mode)
        db.add(sc)

    # Map config fields → StrategyConfig columns
    asset_classes: List[str] = []
    if cfg.get("trade_stocks"):    asset_classes.append("stocks")
    if cfg.get("trade_forex"):     asset_classes.append("forex")
    if cfg.get("trade_crypto"):    asset_classes.append("crypto")
    if cfg.get("trade_commodities"): asset_classes.append("commodities")
    if not asset_classes:
        asset_classes = ["stocks"]

    sc.asset_classes          = asset_classes
    sc.symbols                = cfg.get("allowed_symbols") or []
    sc.investment_amount      = Decimal(str(cfg.get("investment_amount", 100)))
    sc.run_interval_seconds   = int(cfg.get("run_interval_seconds", 300))
    sc.per_symbol_max_positions = int(cfg.get("per_symbol_max_positions", 1))
    sc.allow_buy              = cfg.get("direction", "both") in ("both", "long_only")
    sc.allow_sell             = cfg.get("direction", "both") in ("both", "short_only")
    sc.cooldown_seconds       = int(cfg.get("cooldown_seconds", 900))
    sc.ema_fast               = int(cfg.get("ema_fast", 9))
    sc.ema_slow               = int(cfg.get("ema_slow", 21))
    sc.rsi_period             = int(cfg.get("rsi_period", 14))
    sc.rsi_overbought         = Decimal(str(cfg.get("rsi_overbought", 70)))
    sc.rsi_oversold           = Decimal(str(cfg.get("rsi_oversold", 30)))
    sc.auto_trade             = True


async def _sync_risk_settings(
    db: AsyncSession,
    user: User,
    bot_id: str,
    account_mode: str,
    cfg: dict,
) -> None:
    """Upsert RiskSettings for a custom bot from its config dict."""
    result = await db.execute(
        select(RiskSettings).where(
            RiskSettings.user_id == user.id,
            RiskSettings.bot_id == bot_id,
            RiskSettings.account_mode == account_mode,
        )
    )
    rs = result.scalars().first()
    if rs is None:
        rs = RiskSettings(user_id=user.id, bot_id=bot_id, account_mode=account_mode)
        db.add(rs)

    rs.stop_loss_pct          = Decimal(str(cfg.get("stop_loss_pct", 2.0) / 100))
    rs.take_profit_pct        = Decimal(str(cfg.get("take_profit_pct", 4.0) / 100))
    rs.max_open_positions     = int(cfg.get("max_open_positions", 5))
    rs.max_daily_loss_pct     = Decimal(str(cfg.get("daily_loss_limit_pct", 3.0) / 100))
    rs.max_position_size_pct  = Decimal(str(cfg.get("max_position_size_pct", 10.0) / 100))
    rs.max_drawdown_pct       = Decimal(str(cfg.get("max_drawdown_pct", 15.0) / 100))
    rs.trailing_stop_pct      = Decimal(str(cfg.get("trailing_stop_pct", 0.5) / 100)) if cfg.get("trailing_stop") else Decimal("0")
    rs.break_even_trigger_pct = Decimal(str(cfg.get("breakeven_trigger_pct", 1.0) / 100)) if cfg.get("breakeven") else Decimal("0")
    rs.max_consecutive_losses = int(cfg.get("stop_after_consecutive_losses", 0))


async def _get_bot_state(
    db: AsyncSession,
    user: User,
    bot_id: str,
    account_mode: str,
) -> BotState | None:
    result = await db.execute(
        select(BotState).where(
            BotState.user_id == user.id,
            BotState.bot_id == bot_id,
            BotState.account_mode == account_mode,
        )
    )
    return result.scalars().first()


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=List[CustomBotListItem], summary="List my custom bots")
async def list_custom_bots(
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> List[CustomBotListItem]:
    try:
        result = await db.execute(
            select(CustomBot)
            .where(CustomBot.user_id == current_user.id)
            .order_by(CustomBot.created_at.desc())
        )
        bots = result.scalars().all()

        items = []
        for bot in bots:
            state = await _get_bot_state(db, current_user, bot.bot_id, account_mode)
            items.append(
                CustomBotListItem(
                    id=bot.id,
                    name=bot.name,
                    bot_id=bot.bot_id,
                    color=bot.color,
                    is_enabled=bot.is_enabled,
                    is_running=state.is_running if state else False,
                    cycles_run=state.cycles_run if state else 0,
                    last_log=state.last_log if state else None,
                    created_at=bot.created_at,
                    updated_at=bot.updated_at,
                )
            )
        return items
    except Exception as exc:
        log.exception("list_custom_bots failed", user_id=str(current_user.id), error=str(exc))
        raise


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("", response_model=CustomBotOut, status_code=status.HTTP_201_CREATED, summary="Create custom bot")
async def create_custom_bot(
    payload: CustomBotCreate,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> CustomBotOut:
    log.info(
        "create_custom_bot called",
        user_id=str(current_user.id),
        name=payload.name,
        account_mode=account_mode,
    )
    try:
        # Unique name check
        existing = await db.execute(
            select(CustomBot).where(
                CustomBot.user_id == current_user.id,
                CustomBot.name == payload.name,
            )
        )
        if existing.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Ya ese bot está creado con ese nombre: '{payload.name}'",
            )

        # Create record with a deterministic bot_id
        new_id = _uuid.uuid4()
        bot_id = _make_bot_id(new_id, payload.name)
        cfg_dict = payload.config.model_dump()

        log.info("create_custom_bot: inserting row", bot_id=bot_id)

        bot = CustomBot(
            id=new_id,
            user_id=current_user.id,
            name=payload.name,
            bot_id=bot_id,
            description=cfg_dict.get("description"),
            color=cfg_dict.get("color", "#6366f1"),
            config=cfg_dict,
            is_enabled=bool(cfg_dict.get("enabled_on_save", False)),
        )
        db.add(bot)
        await db.flush()
        log.info("create_custom_bot: flush OK")

        # Sync to StrategyConfig + RiskSettings
        await _sync_strategy_config(db, current_user, bot_id, account_mode, cfg_dict)
        await _sync_risk_settings(db, current_user, bot_id, account_mode, cfg_dict)

        # If enabled_on_save → create running BotState (requires symbols)
        if cfg_dict.get("enabled_on_save") and cfg_dict.get("allowed_symbols"):
            state_r = await db.execute(
                select(BotState).where(
                    BotState.user_id == current_user.id,
                    BotState.bot_id == bot_id,
                    BotState.account_mode == account_mode,
                )
            )
            state = state_r.scalars().first()
            if state is None:
                state = BotState(user_id=current_user.id, bot_id=bot_id, account_mode=account_mode)
                db.add(state)
            state.is_running = True
            state.started_at = datetime.now(timezone.utc)
            state.last_log = f"Bot '{payload.name}' started"

        await db.commit()
        await db.refresh(bot)

        log.info("Custom bot created", user_id=str(current_user.id), bot_id=bot_id, name=payload.name)
        return CustomBotOut.model_validate(bot)

    except HTTPException:
        raise
    except Exception as exc:
        log.exception(
            "create_custom_bot FAILED",
            user_id=str(current_user.id),
            name=payload.name,
            error=str(exc),
        )
        raise


# ── Get ───────────────────────────────────────────────────────────────────────

@router.get("/{bot_uuid}", response_model=CustomBotOut, summary="Get custom bot")
async def get_custom_bot(
    bot_uuid: str,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> CustomBotOut:
    bot = await _get_or_404(bot_uuid, current_user, db)
    return CustomBotOut.model_validate(bot)


# ── Update ────────────────────────────────────────────────────────────────────

@router.put("/{bot_uuid}", response_model=CustomBotOut, summary="Update custom bot")
async def update_custom_bot(
    bot_uuid: str,
    payload: CustomBotUpdate,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> CustomBotOut:
    bot = await _get_or_404(bot_uuid, current_user, db)

    if payload.name is not None and payload.name != bot.name:
        # Check name collision
        collision = await db.execute(
            select(CustomBot).where(
                CustomBot.user_id == current_user.id,
                CustomBot.name == payload.name,
                CustomBot.id != bot.id,
            )
        )
        if collision.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Ya ese bot está creado con ese nombre: '{payload.name}'",
            )
        bot.name = payload.name

    if payload.config is not None:
        cfg_dict = payload.config.model_dump()
        bot.config = cfg_dict
        bot.description = cfg_dict.get("description") or bot.description
        bot.color = cfg_dict.get("color", bot.color)
        bot.updated_at = datetime.now(timezone.utc)
        await _sync_strategy_config(db, current_user, bot.bot_id, account_mode, cfg_dict)
        await _sync_risk_settings(db, current_user, bot.bot_id, account_mode, cfg_dict)

    await db.commit()
    await db.refresh(bot)
    return CustomBotOut.model_validate(bot)


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/{bot_uuid}", status_code=status.HTTP_200_OK, summary="Delete custom bot")
async def delete_custom_bot(
    bot_uuid: str,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> dict:
    bot = await _get_or_404(bot_uuid, current_user, db)
    bot_id = bot.bot_id

    # Stop bot if running
    state_r = await db.execute(
        select(BotState).where(
            BotState.user_id == current_user.id,
            BotState.bot_id == bot_id,
        )
    )
    for state in state_r.scalars().all():
        await db.delete(state)

    # Delete StrategyConfig
    await db.execute(
        delete(StrategyConfig).where(
            StrategyConfig.user_id == current_user.id,
            StrategyConfig.bot_id == bot_id,
        )
    )

    # Delete RiskSettings
    await db.execute(
        delete(RiskSettings).where(
            RiskSettings.user_id == current_user.id,
            RiskSettings.bot_id == bot_id,
        )
    )

    # Delete BotLogs
    await db.execute(
        delete(BotLog).where(
            BotLog.user_id == current_user.id,
            BotLog.bot_id == bot_id,
        )
    )

    # Delete CustomBot record (positions/trades/orders are kept as history
    # with their bot_id reference — they remain in the DB as orphaned history)
    await db.delete(bot)
    await db.commit()

    log.info("Custom bot deleted", user_id=str(current_user.id), bot_id=bot_id)
    return {"deleted": True, "bot_id": bot_id}


# ── Start ─────────────────────────────────────────────────────────────────────

@router.post("/{bot_uuid}/start", summary="Start custom bot")
async def start_custom_bot(
    bot_uuid: str,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> dict:
    bot = await _get_or_404(bot_uuid, current_user, db)
    cfg = bot.config or {}
    symbols = cfg.get("allowed_symbols") or []

    if not symbols:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Configure at least one symbol in 'allowed_symbols' before starting.",
        )

    state_r = await db.execute(
        select(BotState).where(
            BotState.user_id == current_user.id,
            BotState.bot_id == bot.bot_id,
            BotState.account_mode == account_mode,
        )
    )
    state = state_r.scalars().first()
    if state is None:
        state = BotState(
            user_id=current_user.id, bot_id=bot.bot_id, account_mode=account_mode
        )
        db.add(state)

    if state.is_running:
        return {"message": "Bot is already running", "is_running": True}

    state.is_running = True
    state.started_at = datetime.now(timezone.utc)
    state.last_log = f"Bot '{bot.name}' started — monitoring {len(symbols)} symbol(s)"
    state.last_error = None

    bot.is_enabled = True
    await db.commit()

    return {"message": f"Bot '{bot.name}' started", "is_running": True, "bot_id": bot.bot_id}


# ── Stop ──────────────────────────────────────────────────────────────────────

@router.post("/{bot_uuid}/stop", summary="Stop custom bot")
async def stop_custom_bot(
    bot_uuid: str,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> dict:
    bot = await _get_or_404(bot_uuid, current_user, db)

    state_r = await db.execute(
        select(BotState).where(
            BotState.user_id == current_user.id,
            BotState.bot_id == bot.bot_id,
            BotState.account_mode == account_mode,
        )
    )
    state = state_r.scalars().first()
    if state is None or not state.is_running:
        bot.is_enabled = False
        await db.commit()
        return {"message": "Bot is not running", "is_running": False}

    state.is_running = False
    state.last_log = f"Bot '{bot.name}' stopped after {state.cycles_run} cycles"
    bot.is_enabled = False
    await db.commit()

    return {"message": f"Bot '{bot.name}' stopped", "is_running": False}


# ── Status ────────────────────────────────────────────────────────────────────

@router.get("/{bot_uuid}/status", summary="Get custom bot status")
async def get_custom_bot_status(
    bot_uuid: str,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> dict:
    bot = await _get_or_404(bot_uuid, current_user, db)
    state = await _get_bot_state(db, current_user, bot.bot_id, account_mode)
    return {
        "bot_id": bot.bot_id,
        "name": bot.name,
        "is_running": state.is_running if state else False,
        "cycles_run": state.cycles_run if state else 0,
        "last_log": state.last_log if state else None,
        "last_error": state.last_error if state else None,
        "started_at": state.started_at.isoformat() if state and state.started_at else None,
        "last_cycle_at": state.last_cycle_at.isoformat() if state and state.last_cycle_at else None,
    }


# ── Duplicate ─────────────────────────────────────────────────────────────────

@router.post("/{bot_uuid}/duplicate", response_model=CustomBotOut, status_code=status.HTTP_201_CREATED, summary="Duplicate custom bot")
async def duplicate_custom_bot(
    bot_uuid: str,
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> CustomBotOut:
    source = await _get_or_404(bot_uuid, current_user, db)

    # Generate unique name
    base_name = f"{source.name} (copia)"
    candidate = base_name
    counter = 1
    while True:
        collision = await db.execute(
            select(CustomBot).where(
                CustomBot.user_id == current_user.id,
                CustomBot.name == candidate,
            )
        )
        if not collision.scalars().first():
            break
        counter += 1
        candidate = f"{base_name} {counter}"

    new_id = _uuid.uuid4()
    bot_id = _make_bot_id(new_id, candidate)
    cfg = dict(source.config or {})
    cfg["enabled_on_save"] = False

    bot = CustomBot(
        id=new_id,
        user_id=current_user.id,
        name=candidate,
        bot_id=bot_id,
        description=source.description,
        color=source.color,
        config=cfg,
        is_enabled=False,
    )
    db.add(bot)
    await db.flush()

    await _sync_strategy_config(db, current_user, bot_id, account_mode, cfg)
    await _sync_risk_settings(db, current_user, bot_id, account_mode, cfg)
    await db.commit()
    await db.refresh(bot)

    return CustomBotOut.model_validate(bot)


# ── Private helpers ───────────────────────────────────────────────────────────

async def _get_or_404(bot_uuid: str, user: User, db: AsyncSession) -> CustomBot:
    try:
        uid = _uuid.UUID(bot_uuid)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid bot ID")

    result = await db.execute(
        select(CustomBot).where(
            CustomBot.id == uid,
            CustomBot.user_id == user.id,
        )
    )
    bot = result.scalars().first()
    if bot is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Custom bot not found")
    return bot
