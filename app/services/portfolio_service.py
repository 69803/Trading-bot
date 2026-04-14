"""Portfolio service: loading, summarising, and snapshotting portfolios."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bot_state import BotState
from app.models.portfolio import Portfolio
from app.models.portfolio_snapshot import PortfolioSnapshot
from app.models.position import Position
from app.models.trade import Trade
from app.services.market_data_router import market_data_router as market_data_service


async def get_portfolio(db: AsyncSession, user_id: UUID) -> Portfolio:
    """Load portfolio (with open positions) for the given user."""
    result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == user_id)
    )
    portfolio = result.scalars().first()
    if portfolio is None:
        raise ValueError(f"No portfolio found for user {user_id}")
    return portfolio


async def get_portfolio_summary(db: AsyncSession, user_id: UUID) -> dict:
    """
    Returns dashboard summary dict:
    {
        balance, equity, pnl, daily_pnl,
        open_positions_count, closed_positions_count,
        win_rate, bot_running
    }
    """
    portfolio = await get_portfolio(db, user_id)

    # Open positions
    open_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == True,  # noqa: E712
        )
    )
    open_positions: List[Position] = list(open_result.scalars().all())

    # Unrealized PnL
    unrealized_pnl = Decimal("0")
    for pos in open_positions:
        try:
            current = Decimal(str(await market_data_service.get_current_price(pos.symbol)))
        except Exception:
            current = pos.current_price or pos.avg_entry_price
        if pos.side == "long":
            unrealized_pnl += (current - pos.avg_entry_price) * pos.quantity
        else:
            unrealized_pnl += (pos.avg_entry_price - current) * pos.quantity

    cash = portfolio.cash_balance
    equity = cash + unrealized_pnl

    # Total PnL: how much the portfolio has gained/lost since inception.
    # Derived from balance math (equity - initial_capital) so it stays
    # correct even after a manual balance reset.  portfolio.realized_pnl
    # is a running counter that can drift when positions are force-closed
    # or the balance is adjusted directly — do NOT rely on it for display.
    initial = portfolio.initial_capital if portfolio.initial_capital else Decimal("0")
    total_pnl = equity - initial

    # Daily PnL: sum of CLOSED trade PnL executed today.
    # Filter realized_pnl IS NOT NULL so open (BUY) trades are excluded.
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    daily_result = await db.execute(
        select(func.sum(Trade.realized_pnl)).where(
            Trade.portfolio_id == portfolio.id,
            Trade.executed_at >= today_start,
            Trade.realized_pnl.isnot(None),
        )
    )
    daily_raw = daily_result.scalar()
    daily_pnl = Decimal(str(daily_raw)) if daily_raw is not None else Decimal("0")

    # Closed positions
    closed_result = await db.execute(
        select(func.count(Position.id)).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == False,  # noqa: E712
        )
    )
    closed_count: int = closed_result.scalar() or 0

    # Win rate: profitable closed positions / total closed positions
    win_result = await db.execute(
        select(func.count(Position.id)).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == False,  # noqa: E712
            Position.realized_pnl > Decimal("0"),
        )
    )
    win_count: int = win_result.scalar() or 0
    win_rate = (win_count / closed_count * 100) if closed_count > 0 else 0.0

    # Read real bot state
    bot_result = await db.execute(
        select(BotState).where(BotState.user_id == user_id)
    )
    bot_state: BotState | None = bot_result.scalars().first()
    bot_running = bool(bot_state and bot_state.is_running)

    return {
        "balance": float(cash),
        "equity": float(equity),
        "pnl": float(total_pnl),
        "daily_pnl": float(daily_pnl),
        "open_positions_count": len(open_positions),
        "closed_positions_count": closed_count,
        "win_rate": round(win_rate, 2),
        "bot_running": bot_running,
    }


async def get_portfolio_history(
    db: AsyncSession, portfolio_id: UUID, limit: int = 90
) -> list:
    """Load PortfolioSnapshot records ordered by timestamp asc."""
    result = await db.execute(
        select(PortfolioSnapshot)
        .where(PortfolioSnapshot.portfolio_id == portfolio_id)
        .order_by(PortfolioSnapshot.timestamp.asc())
        .limit(limit)
    )
    snapshots = result.scalars().all()
    return [
        {
            "timestamp": s.timestamp.isoformat(),
            "total_value": float(s.total_value),
            "cash": float(s.cash),
        }
        for s in snapshots
    ]


async def update_position_prices(db: AsyncSession, portfolio_id: UUID) -> None:
    """Update current_price for all open positions using market_data_service."""
    result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == True,  # noqa: E712
        )
    )
    positions: List[Position] = list(result.scalars().all())
    for pos in positions:
        try:
            price = await market_data_service.get_current_price(pos.symbol)
            pos.current_price = Decimal(str(price))
        except Exception:
            pass
    try:
        await db.commit()
    except Exception:
        await db.rollback()


async def take_portfolio_snapshot(db: AsyncSession, portfolio_id: UUID) -> None:
    """Save current portfolio value snapshot."""
    # Re-load portfolio
    result = await db.execute(
        select(Portfolio).where(Portfolio.id == portfolio_id)
    )
    portfolio: Portfolio | None = result.scalars().first()
    if portfolio is None:
        return

    # Compute equity including unrealized PnL
    open_result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio_id,
            Position.is_open == True,  # noqa: E712
        )
    )
    open_positions: List[Position] = list(open_result.scalars().all())
    unrealized = Decimal("0")
    for pos in open_positions:
        cur = pos.current_price or pos.avg_entry_price
        if pos.side == "long":
            unrealized += (cur - pos.avg_entry_price) * pos.quantity
        else:
            unrealized += (pos.avg_entry_price - cur) * pos.quantity

    total_value = portfolio.cash_balance + unrealized
    snapshot = PortfolioSnapshot(
        portfolio_id=portfolio_id,
        total_value=total_value,
        cash=portfolio.cash_balance,
        timestamp=datetime.now(timezone.utc),
    )
    db.add(snapshot)
    await db.commit()
