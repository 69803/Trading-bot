"""Portfolio endpoints: summary, positions, history, reset."""
# DEBUG_RELOAD_MARKER_v2
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import uuid as _uuid

from app.api.deps import get_account_mode, get_current_active_user, get_db
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


def _position_to_out(p: Position) -> PositionOut:
    """
    Safely build PositionOut from scalar column values only.

    Returning raw Position ORM objects to FastAPI's serialize_response
    triggers Pydantic's from_attributes path, which in SQLAlchemy async
    can hit expired-attribute / MissingGreenlet errors via the `portfolio`
    back-ref relationship even though PositionOut doesn't declare it.
    Constructing PositionOut explicitly reads only scalar columns, which
    are guaranteed to be in the instance __dict__ after a fresh SELECT.

    realized_pnl is nullable=False in the model but older DB rows may have
    NULL if the column was added via migration without backfilling; default
    to Decimal("0") so a single bad row never breaks the whole list.
    """
    return PositionOut(
        id=p.id,
        portfolio_id=p.portfolio_id,
        symbol=p.symbol,
        side=p.side,
        investment_amount=p.investment_amount,
        quantity=p.quantity,
        avg_entry_price=p.avg_entry_price,
        current_price=p.current_price,
        stop_loss_price=p.stop_loss_price,
        take_profit_price=p.take_profit_price,
        is_open=p.is_open,
        opened_at=p.opened_at,
        closed_at=p.closed_at,
        closed_price=p.closed_price,
        realized_pnl=p.realized_pnl if p.realized_pnl is not None else Decimal("0"),
        created_at=p.created_at,
        updated_at=p.updated_at,
        bot_id=p.bot_id,
    )


async def _get_portfolio_or_404(
    user: User,
    db: AsyncSession,
    account_mode: str = "paper",
) -> Portfolio:
    result = await db.execute(
        select(Portfolio).where(
            Portfolio.user_id    == user.id,
            Portfolio.account_mode == account_mode,
        )
    )
    portfolio = result.scalars().first()
    if portfolio is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio not found for mode '{account_mode}'",
        )
    return portfolio


@router.get("", response_model=PortfolioOut, summary="Get portfolio details")
async def get_portfolio(
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> PortfolioOut:
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)

    # Update live prices then compute unrealised PnL
    await portfolio_service.update_position_prices(db, portfolio.id)
    # update_position_prices commits → expires all loaded objects; reload portfolio
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

    out = PortfolioOut.model_validate(portfolio)
    out.unrealized_pnl = Decimal(str(unrealized))
    return out


@router.get("/summary", response_model=PortfolioSummary, summary="Portfolio summary stats")
async def get_summary(
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> PortfolioSummary:
    summary = await portfolio_service.get_portfolio_summary(db, user_id=current_user.id, account_mode=account_mode)
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
    bot_id: str | None = Query(None, description="Filter by bot (omit for all bots)"),
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> List[PositionOut]:
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)

    # Refresh current prices for open positions before returning
    await portfolio_service.update_position_prices(db, portfolio.id)
    # update_position_prices may commit or rollback internally; reload portfolio
    # scalars so they are fresh regardless (consistent with get_portfolio / get_balance)
    try:
        await db.refresh(portfolio)
    except Exception:
        pass

    stmt = select(Position).where(Position.portfolio_id == portfolio.id)
    if open_only:
        stmt = stmt.where(Position.is_open == True)  # noqa: E712
    if bot_id:
        stmt = stmt.where(Position.bot_id == bot_id)
    try:
        result = await db.execute(stmt.order_by(Position.opened_at.desc()).limit(limit))
        return [_position_to_out(p) for p in result.scalars().all()]
    except Exception as exc:
        log.exception("get_positions SELECT failed", portfolio_id=str(portfolio.id), error=str(exc))
        try:
            await db.rollback()
        except Exception:
            pass
        return []


@router.get(
    "/history",
    response_model=List[PortfolioHistoryPoint],
    summary="Portfolio value history",
)
async def get_history(
    interval: str = Query("1d", description="Interval (unused, reserved for future grouping)"),
    limit: int = Query(90, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> List[PortfolioHistoryPoint]:
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)
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
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> BalanceOut:
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)
    await portfolio_service.update_position_prices(db, portfolio.id)
    # update_position_prices commits → expires all loaded objects; reload portfolio
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


@router.post("/deposit", response_model=BalanceOut, summary="Add funds to portfolio")
async def deposit(
    amount: float = Query(..., gt=0, description="Amount to deposit"),
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> BalanceOut:
    if account_mode == "live":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Live account balance cannot be modified manually. Fund your account directly via Alpaca.",
        )
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)
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
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> BalanceOut:
    if account_mode == "live":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Live account balance cannot be modified manually. Fund your account directly via Alpaca.",
        )
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)
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
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> Position:
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)

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
    return _position_to_out(position)


@router.delete(
    "/positions",
    status_code=status.HTTP_200_OK,
    summary="Delete all closed position records for the current user",
)
async def delete_all_closed_positions(
    current_user: User = Depends(get_current_active_user),
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Delete every closed position for the user. Open positions are untouched.
    Adjusts portfolio.realized_pnl to stay consistent."""
    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)

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
    account_mode: str = Depends(get_account_mode),
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

    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)

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
    account_mode: str = Depends(get_account_mode),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Reset the user's portfolio: close all positions, clear order history,
    and set cash balance back to initial_capital.
    """
    if account_mode == "live":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Live account balance cannot be modified manually. Fund your account directly via Alpaca.",
        )
    from sqlalchemy import delete
    from app.models.trade import Trade
    from app.models.order import Order
    from app.models.bot_state import BotState

    portfolio = await _get_portfolio_or_404(current_user, db, account_mode)
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
