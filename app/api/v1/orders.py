"""Order endpoints: create, list, cancel."""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.models.order import Order
from app.models.portfolio import Portfolio
from app.models.user import User
from app.schemas.order import OrderCreate, OrderListResponse, OrderOut
from app.services import order_service

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


@router.get("", response_model=OrderListResponse, summary="List orders")
async def list_orders(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    order_status: str | None = Query(None, alias="status"),
    bot_id: str | None = Query(None, description="Filter by bot (omit for all bots)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> OrderListResponse:
    portfolio = await _get_portfolio_or_404(current_user, db)
    orders, total = await order_service.get_orders(
        db,
        portfolio_id=portfolio.id,
        status=order_status,
        limit=page_size,
        offset=(page - 1) * page_size,
        bot_id=bot_id,
    )
    return OrderListResponse(items=orders, total=total)


@router.post(
    "",
    response_model=OrderOut,
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
)
async def create_order(
    body: OrderCreate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Order:
    portfolio = await _get_portfolio_or_404(current_user, db)

    if body.order_type == "limit" and body.limit_price is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="limit_price is required for limit orders",
        )

    order = await order_service.create_order(
        db=db,
        portfolio_id=portfolio.id,
        user_id=current_user.id,
        symbol=body.symbol.upper(),
        side=body.side,
        order_type=body.order_type,
        quantity=float(body.quantity) if body.quantity is not None else None,
        investment_amount=float(body.investment_amount) if body.investment_amount is not None else None,
        limit_price=float(body.limit_price) if body.limit_price else None,
    )
    return order


@router.get("/{order_id}", response_model=OrderOut, summary="Get a single order")
async def get_order(
    order_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Order:
    portfolio = await _get_portfolio_or_404(current_user, db)

    result = await db.execute(
        select(Order).where(
            Order.id == order_id, Order.portfolio_id == portfolio.id
        )
    )
    order = result.scalar_one_or_none()
    if order is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    return order


@router.delete(
    "",
    summary="Delete all orders for the current user",
)
async def delete_all_orders(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    portfolio = await _get_portfolio_or_404(current_user, db)
    result = await db.execute(
        select(Order).where(Order.portfolio_id == portfolio.id)
    )
    orders = result.scalars().all()
    count = len(orders)
    for order in orders:
        await db.delete(order)
    await db.commit()
    return {"deleted": count}


@router.delete(
    "/{order_id}",
    response_model=OrderOut,
    summary="Cancel a pending order",
)
async def cancel_order(
    order_id: uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Order:
    try:
        order = await order_service.cancel_order(db, order_id=order_id, user_id=current_user.id)
    except ValueError as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=msg)
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=msg)
    return order
