"""Portfolio endpoints: summary, positions, history, reset."""
# DEBUG_RELOAD_MARKER_v2
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import uuid as _uuid

from app.api.deps import get_current_active_user, get_db
from app.core.logger import get_logger

log = get_logger(__name__)
from app.models.portfolio import Portfolio
from app.models.portfolio_snapshot import PortfolioSnapshot
from app.models.position import Position
from app.models.user import User
from app.schemas.portfolio import (
    BalanceOut,
    PortfolioHistoryPoint,
    PortfolioOut,
    PortfolioSummary,
    PositionOut,
)
from app.services import portfolio_service
from app.services.market_data_router import market_data_router

router = APIRouter()


async def _get_portfolio_or_404(user: User, db: AsyncSession) -> Portfolio:
    result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == user.id)
    )
    portfolio = result.scalars().first()
    if portfolio is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found for current user",
        )
    return portfolio


@router.get("", response_model=PortfolioOut, summary="Get portfolio details")
async def get_portfolio(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> PortfolioOut:
    portfolio = await _get_portfolio_or_404(current_user, db)

    # Update live prices then compute unrealised PnL
    await portfolio_service.update_position_prices(db, portfolio.id)

    pos_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id, Position.is_open == True  # noqa: E712
        )
    )
    positions = pos_result.scalars().all()
    unrealized = sum(
        (
            (p.current_price - p.avg_entry_price) * p.quantity
            if p.side == "long"
            else (p.avg_entry_price - p.current_price) * p.quantity
        )
        for p in positions
        if p.current_price is not None
    )

    out = PortfolioOut.model_validate(portfolio)
    out.unrealized_pnl = Decimal(str(unrealized))
    return out


@router.get("/summary", response_model=PortfolioSummary, summary="Portfolio summary stats")
async def get_summary(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> PortfolioSummary:
    summary = await portfolio_service.get_portfolio_summary(db, user_id=current_user.id)
    return PortfolioSummary(
        balance=Decimal(str(summary["balance"])),
        equity=Decimal(str(summary["equity"])),
        pnl=Decimal(str(summary["pnl"])),
        daily_pnl=Decimal(str(summary["daily_pnl"])),
        open_positions_count=summary["open_positions_count"],
        closed_positions_count=summary["closed_positions_count"],
        win_rate=summary["win_rate"],
        bot_running=summary["bot_running"],
    )


@router.get(
    "/positions",
    response_model=List[PositionOut],
    summary="List open or all positions",
)
async def get_positions(
    open_only: bool = Query(False, description="Return only open positions when True"),
    limit: int = Query(100, ge=1, le=500),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[PositionOut]:
    portfolio = await _get_portfolio_or_404(current_user, db)

    # Refresh current prices for open positions before returning
    await portfolio_service.update_position_prices(db, portfolio.id)

    stmt = select(Position).where(Position.portfolio_id == portfolio.id)
    if open_only:
        stmt = stmt.where(Position.is_open == True)  # noqa: E712
    result = await db.execute(stmt.order_by(Position.opened_at.desc()).limit(limit))
    return result.scalars().all()


@router.get(
    "/history",
    response_model=List[PortfolioHistoryPoint],
    summary="Portfolio value history",
)
async def get_history(
    interval: str = Query("1d", description="Interval (unused, reserved for future grouping)"),
    limit: int = Query(90, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[PortfolioHistoryPoint]:
    portfolio = await _get_portfolio_or_404(current_user, db)
    raw = await portfolio_service.get_portfolio_history(db, portfolio_id=portfolio.id, limit=limit)
    return [
        PortfolioHistoryPoint(
            timestamp=datetime.fromisoformat(r["timestamp"]),
            total_value=Decimal(str(r["total_value"])),
            cash=Decimal(str(r["cash"])),
        )
        for r in raw
    ]


@router.get("/balance", response_model=BalanceOut, summary="Real-time balance snapshot")
async def get_balance(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BalanceOut:
    portfolio = await _get_portfolio_or_404(current_user, db)
    await portfolio_service.update_position_prices(db, portfolio.id)

    pos_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id, Position.is_open == True  # noqa: E712
        )
    )
    positions = pos_result.scalars().all()
    unrealized = sum(
        (
            (p.current_price - p.avg_entry_price) * p.quantity
            if p.side == "long"
            else (p.avg_entry_price - p.current_price) * p.quantity
        )
        for p in positions
        if p.current_price is not None
    )
    unrealized_dec = Decimal(str(unrealized))
    return BalanceOut(
        cash_balance=portfolio.cash_balance,
        equity=portfolio.cash_balance + unrealized_dec,
        unrealized_pnl=unrealized_dec,
        realized_pnl=portfolio.realized_pnl,
    )


@router.post("/deposit", response_model=BalanceOut, summary="Add funds to portfolio")
async def deposit(
    amount: float = Query(..., gt=0, description="Amount to deposit"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BalanceOut:
    portfolio = await _get_portfolio_or_404(current_user, db)
    portfolio.cash_balance += Decimal(str(amount))
    portfolio.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(portfolio)

    pos_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id, Position.is_open == True  # noqa: E712
        )
    )
    positions = pos_result.scalars().all()
    unrealized = sum(
        (
            (p.current_price - p.avg_entry_price) * p.quantity
            if p.side == "long"
            else (p.avg_entry_price - p.current_price) * p.quantity
        )
        for p in positions
        if p.current_price is not None
    )
    unrealized_dec = Decimal(str(unrealized))
    return BalanceOut(
        cash_balance=portfolio.cash_balance,
        equity=portfolio.cash_balance + unrealized_dec,
        unrealized_pnl=unrealized_dec,
        realized_pnl=portfolio.realized_pnl,
    )


@router.post("/withdraw", response_model=BalanceOut, summary="Remove funds from portfolio")
async def withdraw(
    amount: float = Query(..., gt=0, description="Amount to withdraw"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> BalanceOut:
    portfolio = await _get_portfolio_or_404(current_user, db)
    amount_dec = Decimal(str(amount))
    if amount_dec > portfolio.cash_balance:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Insufficient balance: have {portfolio.cash_balance}, requested {amount_dec}",
        )
    portfolio.cash_balance -= amount_dec
    portfolio.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(portfolio)

    pos_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id, Position.is_open == True  # noqa: E712
        )
    )
    positions = pos_result.scalars().all()
    unrealized = sum(
        (
            (p.current_price - p.avg_entry_price) * p.quantity
            if p.side == "long"
            else (p.avg_entry_price - p.current_price) * p.quantity
        )
        for p in positions
        if p.current_price is not None
    )
    unrealized_dec = Decimal(str(unrealized))
    return BalanceOut(
        cash_balance=portfolio.cash_balance,
        equity=portfolio.cash_balance + unrealized_dec,
        unrealized_pnl=unrealized_dec,
        realized_pnl=portfolio.realized_pnl,
    )


@router.post(
    "/positions/{position_id}/close",
    response_model=PositionOut,
    summary="Close a specific open position",
)
async def close_position(
    position_id: _uuid.UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Position:
    portfolio = await _get_portfolio_or_404(current_user, db)

    result = await db.execute(
        select(Position).where(
            Position.id == position_id,
            Position.portfolio_id == portfolio.id,
            Position.is_open == True,  # noqa: E712
        )
    )
    position: Position | None = result.scalar_one_or_none()
    if position is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Open position not found")

    # Get live close price
    try:
        close_price = Decimal(str(await market_data_router.get_current_price(position.symbol)))
    except Exception:
        close_price = position.current_price or position.avg_entry_price

    qty = position.quantity
    entry = position.avg_entry_price
    invested = position.investment_amount or (entry * qty)

    if position.side == "long":
        pnl = (close_price - entry) * qty
    else:
        pnl = (entry - close_price) * qty

    proceeds = invested + pnl  # what the user gets back

    # Credit portfolio
    portfolio.cash_balance += proceeds
    portfolio.realized_pnl += pnl
    portfolio.updated_at = datetime.now(timezone.utc)

    # Mark position closed
    position.is_open = False
    position.closed_at = datetime.now(timezone.utc)
    position.closed_price = close_price
    position.realized_pnl = pnl
    position.current_price = close_price
    position.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(position)
    return position


@router.delete(
    "/positions",
    status_code=status.HTTP_200_OK,
    summary="Delete all closed position records for the current user",
)
async def delete_all_closed_positions(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Delete every closed position for the user. Open positions are untouched.
    Adjusts portfolio.realized_pnl to stay consistent."""
    portfolio = await _get_portfolio_or_404(current_user, db)

    result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == False,  # noqa: E712
        )
    )
    positions = result.scalars().all()
    count = len(positions)

    for pos in positions:
        if pos.realized_pnl:
            portfolio.realized_pnl -= pos.realized_pnl
        await db.delete(pos)

    portfolio.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"deleted": count}


@router.delete(
    "/positions/{position_id}",
    status_code=status.HTTP_200_OK,
    summary="Delete a closed position record (cleanup only)",
)
async def delete_position(
    position_id: str,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Permanently delete a position record. Only closed positions may be deleted.
    Use POST /positions/{id}/close first if the position is still open.
    """
    log.info("DELETE position request received", position_id=position_id, user_id=str(current_user.id))
    try:
        pos_uuid = _uuid.UUID(position_id)
    except (ValueError, AttributeError):
        log.warning("DELETE position — invalid UUID", position_id=position_id)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid position id: {position_id!r}")

    portfolio = await _get_portfolio_or_404(current_user, db)

    result = await db.execute(
        select(Position).where(
            Position.id == pos_uuid,
            Position.portfolio_id == portfolio.id,
        )
    )
    position: Position | None = result.scalar_one_or_none()
    if position is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")
    if position.is_open:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete an open position. Close it first.",
        )

    # Reverse the realized_pnl contribution from the portfolio total so the
    # portfolio's realized_pnl stays consistent after cleanup.
    if position.realized_pnl:
        portfolio.realized_pnl -= position.realized_pnl
        portfolio.updated_at = datetime.now(timezone.utc)

    await db.delete(position)
    await db.commit()
    return {"deleted": True, "position_id": position_id}


@router.post("/reset", summary="Reset portfolio to a fresh state")
async def reset_portfolio(
    initial_capital: float = Query(0.0, ge=0.0, description="Starting cash balance"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Reset the user's portfolio: close all positions, clear order history,
    and set cash balance back to initial_capital.
    """
    from sqlalchemy import delete
    from app.models.trade import Trade
    from app.models.order import Order
    from app.models.bot_state import BotState

    portfolio = await _get_portfolio_or_404(current_user, db)
    pid = portfolio.id

    # Hard-delete all positions (open and closed)
    await db.execute(delete(Position).where(Position.portfolio_id == pid))

    # Hard-delete all trades and orders
    await db.execute(delete(Trade).where(Trade.portfolio_id == pid))
    await db.execute(delete(Order).where(Order.portfolio_id == pid))

    # Hard-delete all portfolio snapshots
    await db.execute(delete(PortfolioSnapshot).where(PortfolioSnapshot.portfolio_id == pid))

    # Zero out all portfolio balances
    cap = Decimal(str(initial_capital))
    portfolio.initial_capital = cap
    portfolio.cash_balance = cap
    portfolio.realized_pnl = Decimal("0")
    portfolio.updated_at = datetime.now(timezone.utc)

    # Reset bot state (last_log, cycles_run, etc.)
    bot_result = await db.execute(
        select(BotState).where(BotState.user_id == current_user.id)
    )
    bot_state = bot_result.scalars().first()
    if bot_state:
        bot_state.cycles_run = 0
        bot_state.last_log = None
        bot_state.last_error = None
        bot_state.last_signal = None
        bot_state.updated_at = datetime.now(timezone.utc)

    await db.commit()
    return {
        "message": "Portfolio hard-reset successfully",
        "initial_capital": initial_capital,
        "portfolio_id": str(portfolio.id),
    }

# RELOAD_SENTINEL_1
