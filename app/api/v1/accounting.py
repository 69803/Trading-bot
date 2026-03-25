"""Accounting endpoints: P&L summary and closed-trade list filtered by date range."""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.models.user import User

router = APIRouter()


def _start_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)


def _end_of_day(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 23, 59, 59, 999999, tzinfo=timezone.utc)


@router.get("/summary", summary="Accounting summary for a date range")
async def get_accounting_summary(
    from_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Returns P&L stats and the list of closed positions for the given date range.

    Filters on `closed_at` of the Position model (paper trading positions).
    Includes only positions where `is_open = False` and `closed_at` is within
    [from_date 00:00:00 UTC, to_date 23:59:59 UTC].
    """
    # Resolve portfolio
    port_result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == current_user.id)
    )
    portfolio: Optional[Portfolio] = port_result.scalars().first()
    if portfolio is None:
        return _empty_response(from_date, to_date)

    # Fetch closed positions in range
    result = await db.execute(
        select(Position).where(
            Position.portfolio_id == portfolio.id,
            Position.is_open == False,  # noqa: E712
            Position.closed_at >= _start_of_day(from_date),
            Position.closed_at <= _end_of_day(to_date),
        ).order_by(Position.closed_at.desc())
    )
    positions: List[Position] = result.scalars().all()

    # ── Aggregate stats ───────────────────────────────────────────────────────
    total_profit = Decimal("0")
    total_loss   = Decimal("0")
    win_count    = 0
    loss_count   = 0

    trades_out = []
    for pos in positions:
        pnl = pos.realized_pnl or Decimal("0")
        invested = pos.investment_amount or (pos.avg_entry_price * pos.quantity)

        if pnl >= 0:
            total_profit += pnl
            win_count += 1
        else:
            total_loss += pnl   # negative value
            loss_count += 1

        entry = float(pos.avg_entry_price)
        close = float(pos.closed_price) if pos.closed_price else entry
        pnl_pct = round((float(pnl) / float(invested) * 100), 2) if invested else 0.0

        trades_out.append({
            "id":              str(pos.id),
            "closed_at":       pos.closed_at.isoformat() if pos.closed_at else None,
            "opened_at":       pos.opened_at.isoformat() if pos.opened_at else None,
            "symbol":          pos.symbol,
            "side":            pos.side,           # "long" | "short"
            "investment":      round(float(invested), 2),
            "entry_price":     round(entry, 5),
            "close_price":     round(close, 5),
            "realized_pnl":    round(float(pnl), 2),
            "pnl_pct":         pnl_pct,
            "result":          "win" if pnl >= 0 else "loss",
        })

    total_closed = win_count + loss_count
    net_pnl      = total_profit + total_loss   # total_loss is negative
    win_rate     = round(win_count / total_closed * 100, 1) if total_closed else 0.0

    return {
        "from_date":     from_date.isoformat(),
        "to_date":       to_date.isoformat(),
        "total_closed_trades": total_closed,
        "total_profit":  round(float(total_profit), 2),
        "total_loss":    round(float(total_loss),   2),   # negative
        "net_pnl":       round(float(net_pnl),      2),
        "win_count":     win_count,
        "loss_count":    loss_count,
        "win_rate":      win_rate,
        "trades":        trades_out,
    }


def _empty_response(from_date: date, to_date: date) -> dict:
    return {
        "from_date": from_date.isoformat(),
        "to_date":   to_date.isoformat(),
        "total_closed_trades": 0,
        "total_profit":  0.0,
        "total_loss":    0.0,
        "net_pnl":       0.0,
        "win_count":     0,
        "loss_count":    0,
        "win_rate":      0.0,
        "trades":        [],
    }
