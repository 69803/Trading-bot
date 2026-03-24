"""Strategy configuration and signal endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.models.strategy_config import StrategyConfig
from app.models.strategy_signal import StrategySignal
from app.models.user import User
from app.schemas.strategy import (
    SignalListResponse,
    SignalOut,
    StrategyConfigOut,
    StrategyConfigUpdate,
)
from app.services import strategy_service

router = APIRouter()


@router.get(
    "/config", response_model=StrategyConfigOut, summary="Get strategy configuration"
)
async def get_config(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> StrategyConfig:
    """Return the user's strategy config, creating defaults if none exist."""
    return await strategy_service.get_strategy_config(db, user_id=current_user.id)


@router.patch(
    "/config",
    response_model=StrategyConfigOut,
    summary="Update strategy configuration",
)
@router.put(
    "/config",
    response_model=StrategyConfigOut,
    summary="Update strategy configuration (PUT)",
)
async def update_config(
    body: StrategyConfigUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> StrategyConfig:
    update_data = body.model_dump(exclude_none=True)
    return await strategy_service.update_strategy_config(
        db, user_id=current_user.id, data=update_data
    )


@router.get("/signals", response_model=SignalListResponse, summary="List recent signals")
async def list_signals(
    limit: int = Query(50, ge=1, le=500),
    symbol: str | None = Query(None),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> SignalListResponse:
    signals, _total = await strategy_service.get_signals(
        db, user_id=current_user.id, symbol=symbol, limit=limit
    )
    return SignalListResponse(signals=signals)


@router.post("/run", summary="Evaluate strategy signals now")
async def run_signals(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Evaluate strategy signals for all configured symbols immediately."""
    result = await strategy_service.evaluate_signals(db, user_id=current_user.id)
    return {"signals": result, "count": len(result)}
