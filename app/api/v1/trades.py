"""Trade history endpoints."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.models.portfolio import Portfolio
from app.models.trade import Trade
from app.models.user import User
from app.schemas.trade import TradeListResponse, TradeOut

router = APIRouter()


async def _get_portfolio_or_404(user: User, db: AsyncSession) -> Portfolio:
    result = await db.execute(select(Portfolio).where(Portfolio.user_id == user.id))
    portfolio = result.scalars().first()
    if portfolio is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found",
        )
    return portfolio


@router.get("", response_model=TradeListResponse, summary="List executed trades")
async def list_trades(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    symbol: str | None = Query(None),
    bot_id: str | None = Query(None, description="Filter by bot (omit for all bots)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> TradeListResponse:
    portfolio = await _get_portfolio_or_404(current_user, db)

    stmt = select(Trade).where(Trade.portfolio_id == portfolio.id)
    if symbol:
        stmt = stmt.where(Trade.symbol == symbol.upper())
    if bot_id:
        stmt = stmt.where(Trade.bot_id == bot_id)

    count_result = await db.execute(
        select(func.count()).select_from(stmt.subquery())
    )
    total = count_result.scalar_one()

    stmt = (
        stmt.order_by(Trade.executed_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    trades = result.scalars().all()

    return TradeListResponse(items=trades, total=total)


@router.get("/{trade_id}", response_model=TradeOut, summary="Get a single trade")
async def get_trade(
    trade_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Trade:
    portfolio = await _get_portfolio_or_404(current_user, db)

    result = await db.execute(
        select(Trade).where(
            Trade.id == trade_id, Trade.portfolio_id == portfolio.id
        )
    )
    trade: Trade | None = result.scalar_one_or_none()
    if trade is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Trade not found"
        )
    return trade
