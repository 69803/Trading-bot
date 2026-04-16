"""Order service: placing, cancelling and listing orders for paper trading.

IQ Option-style investment flow:
  - User specifies investment_amount (e.g. $200)
  - quantity is derived: quantity = investment_amount / fill_price
  - Cash balance is debited by exactly investment_amount (buys)
  - On close: proceeds = quantity * close_price are credited back

Legacy flow (still supported):
  - User specifies quantity directly
  - investment_amount is inferred as quantity * fill_price
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.logger import get_logger
from app.models.order import Order
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.models.risk_settings import RiskSettings
from app.models.trade import Trade
from app.services.market_data_router import market_data_router

log = get_logger(__name__)

# Slippage represents the bid-ask spread (0.1% round-trip)
SLIPPAGE_RATE = Decimal("0.001")
# No explicit commission — cost is embedded in slippage (IQ Option style)
COMMISSION_RATE = Decimal("0")


async def create_order(
    db: AsyncSession,
    portfolio_id: UUID,
    user_id: UUID,
    symbol: str,
    side: str,
    order_type: str,
    quantity: float | None = None,
    investment_amount: float | None = None,
    limit_price: float | None = None,
    event_context: str | None = None,
    is_paper: bool = True,
    bot_id: str | None = None,
) -> Order:
    """
    Create, validate, and (for market orders) immediately fill an order.

    Priority: investment_amount > quantity.
    For market orders with investment_amount: quantity is resolved AFTER
    fetching the fill price so the user invests exactly the specified amount.
    """
    if investment_amount is None and quantity is None:
        raise ValueError("Either investment_amount or quantity must be provided")

    # ── Estimate qty for risk check ──────────────────────────────────────────
    try:
        est_price = Decimal(str(await market_data_router.get_current_price(symbol)))
    except Exception:
        est_price = Decimal("1")

    if investment_amount is not None:
        est_qty = Decimal(str(investment_amount)) / est_price
    else:
        est_qty = Decimal(str(quantity))

    # ── Risk check ───────────────────────────────────────────────────────────
    allowed, reason = await _check_risk_limits(
        db, user_id, portfolio_id,
        est_qty=float(est_qty),
        investment_amount=investment_amount,
        symbol=symbol,
        side=side,
    )
    if not allowed:
        log.warning(
            "ORDER SERVICE REJECTED (pre-flight risk check)",
            symbol=symbol, side=side, reason=reason,
            investment_amount=investment_amount,
        )
        order = Order(
            id=uuid4(),
            portfolio_id=portfolio_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            investment_amount=Decimal(str(investment_amount)) if investment_amount else None,
            quantity=est_qty,
            filled_quantity=Decimal("0"),
            limit_price=Decimal(str(limit_price)) if limit_price else None,
            status="rejected",
            rejection_reason=reason,
            bot_id=bot_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(order)
        await db.commit()
        await db.refresh(order)
        return order

    # ── Load portfolio ───────────────────────────────────────────────────────
    port_result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
    portfolio: Portfolio = port_result.scalars().first()
    if portfolio is None:
        raise ValueError(f"Portfolio {portfolio_id} not found")

    # ── Create pending order ─────────────────────────────────────────────────
    order = Order(
        id=uuid4(),
        portfolio_id=portfolio_id,
        symbol=symbol,
        side=side,
        order_type=order_type,
        investment_amount=Decimal(str(investment_amount)) if investment_amount else None,
        quantity=est_qty,
        filled_quantity=Decimal("0"),
        limit_price=Decimal(str(limit_price)) if limit_price else None,
        status="pending",
        bot_id=bot_id,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(order)
    await db.flush()

    # ── Fill market orders immediately ───────────────────────────────────────
    if order_type == "market":
        # ── Market-hours gate (manual US-equity orders only) ─────────────────
        # Bots have their own session filters (USE_TRADING_SESSIONS).
        # For manual orders: if NYSE is closed, leave the order as pending —
        # no position, no PnL, no cash debit — until the market actually opens.
        # Alpaca still receives the order and will queue it as MOO.
        if bot_id is None:
            _s = symbol.upper()
            _is_equity = (
                "/" not in _s
                and _s not in {"WTI", "BRENT", "NATGAS", "OIL", "USOIL", "UKOIL",
                               "XAUUSD", "XAGUSD", "XPTUSD"}
                and not any(
                    _s.endswith(c) and len(_s) > len(c)
                    for c in {"USDT", "USDC", "BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOGE"}
                )
            )
            if _is_equity:
                from app.services.market_hours import get_nyse_status
                _hrs = get_nyse_status()
                if not _hrs["is_open"]:
                    log.info(
                        "ORDER pending — NYSE closed, no local fill",
                        symbol=symbol, side=side,
                        session=_hrs["session"],
                        next_open=_hrs["next_open"],
                    )
                    await db.commit()
                    await db.refresh(order)
                    from app.services.alpaca_broker import submit_order_to_alpaca
                    broker_id = await submit_order_to_alpaca(
                        symbol=symbol,
                        side=side,
                        qty=float(est_qty),
                        notional=float(investment_amount) if investment_amount else None,
                        internal_order_id=str(order.id),
                        client_order_id=str(order.id),
                    )
                    if broker_id and broker_id != "unknown":
                        try:
                            order.broker_order_id = broker_id
                            await db.commit()
                        except Exception:
                            await db.rollback()
                            # Rollback expires all ORM objects — reload so
                            # _order_to_out() can access columns safely.
                            await db.refresh(order)
                    return order

        try:
            raw_price = Decimal(str(await market_data_router.get_current_price(symbol)))
        except Exception as exc:
            raise ValueError(f"Unable to fetch fill price for {symbol}: {exc}") from exc

        if side == "buy":
            fill_price = (raw_price * (Decimal("1") + SLIPPAGE_RATE)).quantize(Decimal("0.00001"))
        else:
            fill_price = (raw_price * (Decimal("1") - SLIPPAGE_RATE)).quantize(Decimal("0.00001"))

        # Resolve exact quantity and investment_amount
        if investment_amount is not None:
            invest_dec = Decimal(str(investment_amount))
            qty = invest_dec / fill_price
        else:
            qty = Decimal(str(quantity))
            invest_dec = (qty * fill_price).quantize(Decimal("0.01"))

        trade_value = (fill_price * qty).quantize(Decimal("0.01"))

        # ── Cash balance check for buys ──────────────────────────────────────
        if side == "buy":
            required = invest_dec
            if portfolio.cash_balance < required:
                order.status = "rejected"
                order.rejection_reason = (
                    f"Insufficient balance: need ${required:.2f}, "
                    f"available ${portfolio.cash_balance:.2f}"
                )
                order.updated_at = datetime.now(timezone.utc)
                log.warning(
                    "ORDER SERVICE REJECTED (insufficient cash)",
                    symbol=symbol,
                    required=float(required),
                    available=float(portfolio.cash_balance),
                )
                await db.commit()
                await db.refresh(order)
                return order

        # ── Create trade record ──────────────────────────────────────────────
        trade = Trade(
            id=uuid4(),
            order_id=order.id,
            portfolio_id=portfolio_id,
            symbol=symbol,
            side=side,
            quantity=qty,
            price=fill_price,
            commission=Decimal("0"),
            realized_pnl=None,
            bot_id=bot_id,
            executed_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )
        db.add(trade)

        # ── Update order ─────────────────────────────────────────────────────
        order.quantity = qty
        order.investment_amount = invest_dec
        order.filled_quantity = qty
        order.avg_fill_price = fill_price
        order.status = "filled"
        order.updated_at = datetime.now(timezone.utc)

        # ── Update portfolio cash (IQ Option style: always debit investment) ──
        portfolio.cash_balance -= invest_dec
        portfolio.updated_at = datetime.now(timezone.utc)

        await db.flush()

        # ── Always create a brand-new position for every trade ───────────────
        await _open_or_update_position(db, portfolio_id, trade, invest_dec, event_context, is_paper, bot_id)

        await db.commit()
        await db.refresh(order)

        # ── Alpaca Paper broker mirror (non-blocking) ─────────────────────────
        # Runs AFTER the internal commit so a failure here never rolls back the
        # trade.  Only fires for US equities when ALPACA_BROKER_ENABLED=true.
        # Wrapped in try/except so a network or auth error from Alpaca never
        # propagates as a 500 on POST /orders — the trade is already committed.
        try:
            from app.services.alpaca_broker import submit_order_to_alpaca
            await submit_order_to_alpaca(
                symbol=symbol,
                side=side,
                qty=float(qty),
                notional=float(invest_dec),
                internal_order_id=str(order.id),
            )
        except Exception as _alpaca_exc:
            log.warning(
                "Alpaca submit failed (non-blocking, order already committed)",
                symbol=symbol,
                order_id=str(order.id),
                error=str(_alpaca_exc),
            )

    return order


async def cancel_order(db: AsyncSession, order_id: UUID, user_id: UUID) -> Order:
    result = await db.execute(
        select(Order)
        .join(Portfolio, Order.portfolio_id == Portfolio.id)
        .where(Order.id == order_id, Portfolio.user_id == user_id)
    )
    order: Order | None = result.scalars().first()
    if order is None:
        raise ValueError(f"Order {order_id} not found")
    if order.status != "pending":
        raise ValueError(f"Cannot cancel order with status '{order.status}'")
    order.status = "cancelled"
    order.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(order)
    return order


async def get_orders(
    db: AsyncSession,
    portfolio_id: UUID,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    bot_id: str | None = None,
) -> Tuple[List[Order], int]:
    base_q  = select(Order).where(Order.portfolio_id == portfolio_id)
    count_q = select(func.count(Order.id)).where(Order.portfolio_id == portfolio_id)
    if status:
        base_q  = base_q.where(Order.status == status)
        count_q = count_q.where(Order.status == status)
    if bot_id:
        base_q  = base_q.where(Order.bot_id == bot_id)
        count_q = count_q.where(Order.bot_id == bot_id)
    count_result = await db.execute(count_q)
    total: int = count_result.scalar() or 0
    result = await db.execute(
        base_q
        .options(selectinload(Order.trades))
        .order_by(Order.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all()), total


# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------

async def _check_risk_limits(
    db: AsyncSession,
    user_id: UUID,
    portfolio_id: UUID,
    est_qty: float,
    investment_amount: float | None,
    symbol: str,
    side: str = "buy",
) -> Tuple[bool, str]:
    rs_result = await db.execute(select(RiskSettings).where(RiskSettings.user_id == user_id))
    risk: RiskSettings | None = rs_result.scalars().first()

    port_result = await db.execute(select(Portfolio).where(Portfolio.id == portfolio_id))
    portfolio: Portfolio | None = port_result.scalars().first()
    if portfolio is None:
        return False, "Portfolio not found"
    if risk is None:
        return True, ""

    # These checks only apply when opening a new position (buy/long)
    if side == "buy":
        open_count_result = await db.execute(
            select(func.count(Position.id)).where(
                Position.portfolio_id == portfolio_id,
                Position.is_open == True,  # noqa: E712
            )
        )
        open_count: int = open_count_result.scalar() or 0
        if open_count >= risk.max_open_positions:
            return False, f"Max open positions ({risk.max_open_positions}) reached"

        # Prevent stacking duplicate positions on the same symbol
        sym_count_result = await db.execute(
            select(func.count(Position.id)).where(
                Position.portfolio_id == portfolio_id,
                Position.symbol == symbol,
                Position.is_open == True,  # noqa: E712
            )
        )
        sym_open_count: int = sym_count_result.scalar() or 0
        if sym_open_count >= 1:
            return False, f"Position already open for {symbol} — close it before opening a new one"

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    from app.models.trade import Trade as TradeModel
    daily_pnl_result = await db.execute(
        select(func.sum(TradeModel.realized_pnl)).where(
            TradeModel.portfolio_id == portfolio_id,
            TradeModel.executed_at >= today_start,
            TradeModel.realized_pnl.isnot(None),  # exclude open BUY trades
        )
    )
    daily_pnl_raw = daily_pnl_result.scalar()
    daily_pnl = float(daily_pnl_raw) if daily_pnl_raw else 0.0
    equity = float(portfolio.cash_balance)
    if equity > 0:
        daily_loss_pct = abs(min(daily_pnl, 0)) / equity
        if daily_loss_pct >= float(risk.max_daily_loss_pct):
            return False, "Max daily loss limit reached"

    # Position-size cap is already enforced by risk_manager.assess() before
    # create_order() is called.  Re-checking it here caused float-precision
    # rejections (e.g. 20.13 / 201.30 = 0.10001 > 0.10 → rejected).
    # The only cash check needed here is that we actually have enough cash
    # to cover the investment — that is done inline in create_order() after
    # the fill price is known.
    log.info(
        "ORDER SERVICE PRE-FLIGHT OK",
        symbol=symbol,
        balance=equity,
        investment=investment_amount,
        max_allowed=round(equity * float(risk.max_position_size_pct), 2),
        max_pct=f"{float(risk.max_position_size_pct):.0%}",
    )
    return True, ""


# Legacy wrapper — kept for bots / tests that call check_risk_limits directly
async def check_risk_limits(
    db: AsyncSession,
    user_id: UUID,
    portfolio_id: UUID,
    quantity: float,
    symbol: str,
) -> Tuple[bool, str]:
    return await _check_risk_limits(
        db, user_id, portfolio_id,
        est_qty=quantity, investment_amount=None, symbol=symbol,
    )


# ---------------------------------------------------------------------------
# Position helpers
# ---------------------------------------------------------------------------

async def _open_or_update_position(
    db: AsyncSession,
    portfolio_id: UUID,
    trade: Trade,
    invest_dec: Decimal,
    event_context: str | None = None,
    is_paper: bool = True,
    bot_id: str | None = None,
) -> Position:
    """Always creates a NEW position record for every trade (IQ Option style)."""
    desired_side = "long" if trade.side == "buy" else "short"

    from app.models.portfolio import Portfolio as PortfolioModel
    port_r = await db.execute(select(PortfolioModel).where(PortfolioModel.id == portfolio_id))
    port = port_r.scalars().first()
    rs_result = await db.execute(
        select(RiskSettings).where(RiskSettings.user_id == port.user_id if port else True)
    )
    risk: RiskSettings | None = rs_result.scalars().first()
    sl_pct = float(risk.stop_loss_pct) if risk else 0.03
    tp_pct = float(risk.take_profit_pct) if risk else 0.06

    if desired_side == "long":
        stop_loss   = trade.price * Decimal(str(1 - sl_pct))
        take_profit = trade.price * Decimal(str(1 + tp_pct))
    else:
        stop_loss   = trade.price * Decimal(str(1 + sl_pct))
        take_profit = trade.price * Decimal(str(1 - tp_pct))

    new_pos = Position(
        id=uuid4(),
        portfolio_id=portfolio_id,
        symbol=trade.symbol,
        side=desired_side,
        investment_amount=invest_dec,
        quantity=trade.quantity,
        avg_entry_price=trade.price,
        current_price=trade.price,
        stop_loss_price=stop_loss.quantize(Decimal("0.00001")),
        take_profit_price=take_profit.quantize(Decimal("0.00001")),
        is_open=True,
        opened_at=trade.executed_at,
        realized_pnl=Decimal("0"),
        event_context=event_context,
        is_paper=is_paper,
        bot_id=bot_id,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(new_pos)
    await db.flush()
    return new_pos


async def _close_position(db: AsyncSession, position: Position, trade: Trade) -> float:
    if position.side == "long":
        pnl = (trade.price - position.avg_entry_price) * position.quantity
    else:
        pnl = (position.avg_entry_price - trade.price) * position.quantity

    pnl_val = float(pnl)
    position.realized_pnl = Decimal(str(pnl_val))

    port_result = await db.execute(
        select(Portfolio).where(Portfolio.id == position.portfolio_id)
    )
    portfolio: Portfolio | None = port_result.scalars().first()
    if portfolio:
        portfolio.realized_pnl += Decimal(str(pnl_val))
        portfolio.updated_at = datetime.now(timezone.utc)

    await db.flush()
    return pnl_val
