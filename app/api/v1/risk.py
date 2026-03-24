"""Risk management endpoints."""

from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.models.risk_settings import RiskSettings
from app.models.user import User
from app.schemas.risk import RiskSettingsOut, RiskSettingsUpdate, RiskStatusOut
from app.services import risk_service

router = APIRouter()


@router.get("", response_model=RiskSettingsOut, summary="Get risk settings")
@router.get("/settings", response_model=RiskSettingsOut, summary="Get risk settings (alias)")
async def get_risk_settings(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> RiskSettings:
    """Return the user's risk settings, creating defaults if none exist."""
    return await risk_service.get_risk_settings(db, user_id=current_user.id)


@router.patch("", response_model=RiskSettingsOut, summary="Update risk settings")
@router.put("/settings", response_model=RiskSettingsOut, summary="Update risk settings (PUT)")
async def update_risk_settings(
    body: RiskSettingsUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> RiskSettings:
    update_data = body.model_dump(exclude_none=True)
    return await risk_service.update_risk_settings(
        db, user_id=current_user.id, data=update_data
    )


@router.get("/status", response_model=RiskStatusOut, summary="Get current risk status")
async def get_risk_status(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> RiskStatusOut:
    status_data = await risk_service.get_risk_status(db, user_id=current_user.id)
    return RiskStatusOut(
        current_drawdown_pct=Decimal(str(status_data["current_drawdown_pct"])),
        daily_pnl=Decimal(str(status_data["daily_pnl"])),
        daily_pnl_pct=Decimal(str(status_data["daily_pnl_pct"])),
        open_position_count=status_data["open_position_count"],
        trading_halted=status_data["trading_halted"],
        halt_reason=status_data["halt_reason"],
    )
