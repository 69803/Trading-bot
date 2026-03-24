"""Risk management service."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio import Portfolio
from app.models.portfolio_snapshot import PortfolioSnapshot
from app.models.position import Position
from app.models.risk_settings import RiskSettings
from app.models.trade import Trade


async def get_risk_settings(db: AsyncSession, user_id: UUID) -> RiskSettings:
    """Load risk settings, creating defaults if not present."""
    result = await db.execute(
        select(RiskSettings).where(RiskSettings.user_id == user_id)
    )
    risk: RiskSettings | None = result.scalars().first()
    if risk is None:
        risk = RiskSettings(
            id=uuid4(),
            user_id=user_id,
            max_position_size_pct=Decimal("0.05"),
            max_daily_loss_pct=Decimal("0.02"),
            max_open_positions=10,
            stop_loss_pct=Decimal("0.03"),
            take_profit_pct=Decimal("0.06"),
            max_drawdown_pct=Decimal("0.20"),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(risk)
        await db.commit()
        await db.refresh(risk)
    return risk


async def update_risk_settings(
    db: AsyncSession, user_id: UUID, data: dict
) -> RiskSettings:
    """Update risk settings."""
    risk = await get_risk_settings(db, user_id)
    decimal_fields = {
        "max_position_size_pct",
        "max_daily_loss_pct",
        "stop_loss_pct",
        "take_profit_pct",
        "max_drawdown_pct",
    }
    int_fields = {"max_open_positions"}
    allowed = decimal_fields | int_fields

    for field, value in data.items():
        if field in allowed and value is not None:
            if field in decimal_fields:
                value = Decimal(str(value))
            elif field in int_fields:
                value = int(value)
            setattr(risk, field, value)

    risk.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(risk)
    return risk


async def get_risk_status(db: AsyncSession, user_id: UUID) -> dict:
    """
    Returns current risk exposure status dict.
    """
    halted, halt_reason = await is_trading_halted(db, user_id)
    risk = await get_risk_settings(db, user_id)

    # Load portfolio
    port_result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == user_id)
    )
    portfolio: Portfolio | None = port_result.scalars().first()
    equity = float(portfolio.cash_balance) if portfolio else 0.0

    # Daily PnL
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if portfolio:
        daily_result = await db.execute(
            select(func.sum(Trade.realized_pnl)).where(
                Trade.portfolio_id == portfolio.id,
                Trade.executed_at >= today_start,
            )
        )
        daily_raw = daily_result.scalar()
        daily_pnl = float(daily_raw) if daily_raw else 0.0
    else:
        daily_pnl = 0.0

    daily_pnl_pct = (daily_pnl / equity) * 100 if equity > 0 else 0.0

    # Current drawdown
    current_drawdown_pct = 0.0
    if portfolio:
        # Find peak equity from snapshots
        peak_result = await db.execute(
            select(func.max(PortfolioSnapshot.total_value)).where(
                PortfolioSnapshot.portfolio_id == portfolio.id
            )
        )
        peak_raw = peak_result.scalar()
        peak = float(peak_raw) if peak_raw else equity
        if peak > 0 and equity < peak:
            current_drawdown_pct = ((peak - equity) / peak) * 100

    # Open positions
    open_count = 0
    if portfolio:
        open_count_result = await db.execute(
            select(func.count(Position.id)).where(
                Position.portfolio_id == portfolio.id,
                Position.is_open == True,  # noqa: E712
            )
        )
        open_count = open_count_result.scalar() or 0

    return {
        "current_drawdown_pct": round(current_drawdown_pct, 4),
        "daily_pnl": round(daily_pnl, 4),
        "daily_pnl_pct": round(daily_pnl_pct, 4),
        "open_position_count": open_count,
        "trading_halted": halted,
        "halt_reason": halt_reason,
    }


async def is_trading_halted(
    db: AsyncSession, user_id: UUID
) -> Tuple[bool, str | None]:
    """Check if trading should be halted based on risk settings."""
    risk = await get_risk_settings(db, user_id)

    port_result = await db.execute(
        select(Portfolio).where(Portfolio.user_id == user_id)
    )
    portfolio: Portfolio | None = port_result.scalars().first()
    if portfolio is None:
        return False, None

    equity = float(portfolio.cash_balance)
    initial = float(portfolio.initial_capital)

    # Check max drawdown
    if initial > 0:
        drawdown = (initial - equity) / initial
        if drawdown >= float(risk.max_drawdown_pct):
            return True, (
                f"Max drawdown {float(risk.max_drawdown_pct):.1%} exceeded "
                f"(current {drawdown:.1%})"
            )

    # Check daily loss
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    daily_result = await db.execute(
        select(func.sum(Trade.realized_pnl)).where(
            Trade.portfolio_id == portfolio.id,
            Trade.executed_at >= today_start,
        )
    )
    daily_raw = daily_result.scalar()
    daily_pnl = float(daily_raw) if daily_raw else 0.0

    if equity > 0:
        daily_loss_pct = abs(min(daily_pnl, 0)) / equity
        if daily_loss_pct >= float(risk.max_daily_loss_pct):
            return True, (
                f"Max daily loss {float(risk.max_daily_loss_pct):.1%} exceeded "
                f"(current {daily_loss_pct:.1%})"
            )

    return False, None
