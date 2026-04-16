"""Order endpoints: create, list, cancel."""

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.core.logger import get_logger
from app.models.order import Order
from app.models.portfolio import Portfolio
from app.models.user import User
from app.schemas.order import OrderCreate, OrderListResponse, OrderOut
from app.services import order_service

log = get_logger(__name__)
router = APIRouter()


def _order_to_out(order: Order) -> OrderOut:
    """
    Safely convert an ORM Order to OrderOut without triggering lazy loads.

    The Order.realized_pnl Python @property accesses self.trades (a lazy
    relationship). When the session is closed or the object is detached after
    commit, that access raises MissingGreenlet / DetachedInstanceError, which
    FastAPI's serialize_response turns into a ResponseValidationError (500).

    Newly-created or single-fetched orders never need realized_pnl populated
    here — the trades list endpoint already eager-loads trades when needed.
    """
    return OrderOut(
        id=order.id,
        portfolio_id=order.portfolio_id,
        symbol=order.symbol,
        side=order.side,
        order_type=order.order_type,
        investment_amount=order.investment_amount,
        quantity=order.quantity,
        filled_quantity=order.filled_quantity,
        limit_price=order.limit_price,
        avg_fill_price=order.avg_fill_price,
        status=order.status,
        rejection_reason=order.rejection_reason,
        created_at=order.created_at,
        updated_at=order.updated_at,
        bot_id=order.bot_id,
        broker_order_id=getattr(order, "broker_order_id", None),
        realized_pnl=None,  # avoids lazy-load of Order.trades
    )


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
    return OrderListResponse(items=[_order_to_out(o) for o in orders], total=total)


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

    try:
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
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )
    except Exception as exc:
        log.exception(
            "create_order unexpected error",
            symbol=body.symbol,
            side=body.side,
            error=str(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Order creation failed: {type(exc).__name__}: {exc}",
        )
    return _order_to_out(order)


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
    return _order_to_out(order)


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
    return _order_to_out(order)
