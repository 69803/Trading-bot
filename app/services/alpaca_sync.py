"""
Alpaca fill-sync service — Phase 2 of the market-hours-aware order flow.

When a manual equity order is placed while NYSE is closed, the order stays
locally as status="pending" (no trade, no position, no cash debit) while
Alpaca queues it as a day/MOO order.

This service is called by the scheduler every 2 minutes. It:
  1. Finds all local pending manual orders (bot_id IS NULL)
  2. Queries Alpaca for their current status
  3. On "filled": creates Trade + Position, debits cash, marks order filled
  4. On "canceled" / "expired": marks order cancelled locally

Idempotency guarantee
─────────────────────
  • The DB query only returns status="pending" orders.
  • After applying a fill: order.status → "filled" and committed.
  • The same order ID is never processed twice (status acts as a one-way gate).
  • APScheduler runs this job with max_instances=1 (no concurrent runs).

Fill data source
────────────────
  • Uses the real Alpaca avg_fill_price / filled_qty / filled_at — not a
    local estimate. Only falls back to local values if Alpaca omits them.

Lookup strategy (ordered by preference)
────────────────────────────────────────
  1. broker_order_id (Alpaca UUID stored when order was forwarded) → O(1) GET
  2. client_order_id (= our local order.id, set in the Alpaca payload) → list scan

Architecture note
─────────────────
  This is the polling-based Phase 2. To migrate to WebSocket streaming later:
  replace the scheduler job body with a persistent Alpaca stream listener that
  calls _apply_alpaca_fill() directly on "trade_updates" events.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.db.session import AsyncSessionFactory
from app.models.order import Order
from app.models.portfolio import Portfolio
from app.models.position import Position
from app.models.risk_settings import RiskSettings
from app.models.trade import Trade

log = get_logger(__name__)

PAPER_BASE_URL = "https://paper-api.alpaca.markets"


# ---------------------------------------------------------------------------
# Alpaca HTTP helpers (synchronous — run via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _headers() -> dict:
    return {
        "APCA-API-KEY-ID":     settings.ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": settings.ALPACA_SECRET_KEY,
        "Accept":              "application/json",
    }


def _fetch_by_broker_id(broker_order_id: str) -> Optional[dict]:
    """GET /v2/orders/{broker_order_id} — direct O(1) lookup."""
    url = f"{PAPER_BASE_URL}/v2/orders/{broker_order_id}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, headers=_headers())
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 404:
            return None
        log.warning(
            "Alpaca sync: unexpected status on order fetch",
            broker_order_id=broker_order_id,
            http_status=resp.status_code,
            body=resp.text[:200],
        )
        return None
    except Exception as exc:
        log.warning("Alpaca sync: fetch error", broker_order_id=broker_order_id, error=str(exc))
        return None


def _fetch_by_client_order_id(client_order_id: str) -> Optional[dict]:
    """
    Scan recent Alpaca orders (status=all, last 50) matching client_order_id.

    Used as fallback when broker_order_id was not stored (e.g. Alpaca was
    unreachable at submission time).
    """
    url = f"{PAPER_BASE_URL}/v2/orders"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                url,
                headers=_headers(),
                params={"status": "all", "limit": 50, "direction": "desc"},
            )
        if resp.status_code != 200:
            log.warning("Alpaca sync: order list failed", http_status=resp.status_code)
            return None
        orders = resp.json()
        if not isinstance(orders, list):
            return None
        for o in orders:
            if o.get("client_order_id") == client_order_id:
                return o
        return None
    except Exception as exc:
        log.warning("Alpaca sync: list scan error", client_order_id=client_order_id, error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Fill application
# ---------------------------------------------------------------------------

async def _apply_alpaca_fill(
    order: Order,
    alpaca_data: dict,
    db: AsyncSession,
) -> bool:
    """
    Apply a confirmed Alpaca fill to the local DB.

    Creates a Trade, opens a Position, debits cash, marks order filled.
    Returns True on success, False if skipped due to data issues.

    This mirrors the filled-path in order_service.create_order() but uses
    real broker fill data (avg_fill_price, filled_qty, filled_at).
    """
    # ── Parse fill data from Alpaca ──────────────────────────────────────────
    raw_filled_qty  = alpaca_data.get("filled_qty")  or alpaca_data.get("qty")
    raw_fill_price  = alpaca_data.get("filled_avg_price") or alpaca_data.get("limit_price")
    raw_filled_at   = alpaca_data.get("filled_at") or alpaca_data.get("updated_at")

    if not raw_fill_price or not raw_filled_qty or float(raw_filled_qty) == 0:
        log.warning(
            "Alpaca sync: fill missing price/qty — skipping",
            order_id=str(order.id),
            symbol=order.symbol,
            raw={"filled_qty": raw_filled_qty, "fill_price": raw_fill_price},
        )
        return False

    fill_price  = Decimal(str(raw_fill_price))
    filled_qty  = Decimal(str(raw_filled_qty))

    # Use stored investment_amount if we have it; otherwise derive from fill
    invest_dec = order.investment_amount if order.investment_amount else (fill_price * filled_qty).quantize(Decimal("0.01"))

    try:
        executed_at = datetime.fromisoformat(str(raw_filled_at).replace("Z", "+00:00"))
    except Exception:
        executed_at = datetime.now(timezone.utc)

    # ── Load portfolio ───────────────────────────────────────────────────────
    port_result = await db.execute(select(Portfolio).where(Portfolio.id == order.portfolio_id))
    portfolio: Portfolio | None = port_result.scalars().first()
    if portfolio is None:
        log.error("Alpaca sync: portfolio not found", order_id=str(order.id))
        return False

    # ── Cash debit / credit by side ─────────────────────────────────────────
    if order.side == "buy":
        if portfolio.cash_balance < invest_dec:
            log.warning(
                "Alpaca sync: insufficient cash at fill time — cancelling",
                order_id=str(order.id),
                symbol=order.symbol,
                need=float(invest_dec),
                have=float(portfolio.cash_balance),
            )
            order.status = "cancelled"
            order.rejection_reason = "Insufficient cash at Alpaca fill time"
            order.updated_at = datetime.now(timezone.utc)
            return False
        portfolio.cash_balance -= invest_dec
        portfolio.updated_at = datetime.now(timezone.utc)
    elif order.side == "sell":
        # Credit cash back from the fill proceeds
        proceeds = (fill_price * filled_qty).quantize(Decimal("0.01"))
        portfolio.cash_balance += proceeds
        portfolio.updated_at = datetime.now(timezone.utc)

    # ── Create Trade ─────────────────────────────────────────────────────────
    trade = Trade(
        id=uuid4(),
        order_id=order.id,
        portfolio_id=order.portfolio_id,
        symbol=order.symbol,
        side=order.side,
        quantity=filled_qty,
        price=fill_price,
        commission=Decimal("0"),
        realized_pnl=None,
        bot_id=None,        # always null for manual orders
        executed_at=executed_at,
        created_at=datetime.now(timezone.utc),
    )
    db.add(trade)

    # ── Update Order ─────────────────────────────────────────────────────────
    order.quantity          = filled_qty
    order.investment_amount = invest_dec
    order.filled_quantity   = filled_qty
    order.avg_fill_price    = fill_price
    order.status            = "filled"
    order.alpaca_status     = alpaca_data.get("status", "filled")
    order.updated_at        = datetime.now(timezone.utc)

    await db.flush()

    # ── Open or close Position ───────────────────────────────────────────────
    rs_result = await db.execute(
        select(RiskSettings)
        .where(RiskSettings.user_id == portfolio.user_id)
        .order_by(RiskSettings.created_at.asc())
    )
    risk = rs_result.scalars().first()
    sl_pct = float(risk.stop_loss_pct)   if risk else 0.03
    tp_pct = float(risk.take_profit_pct) if risk else 0.06

    if order.side == "buy":
        # Open a new long position
        stop_loss   = fill_price * Decimal(str(1 - sl_pct))
        take_profit = fill_price * Decimal(str(1 + tp_pct))
        position = Position(
            id=uuid4(),
            portfolio_id=order.portfolio_id,
            symbol=order.symbol,
            side="long",
            investment_amount=invest_dec,
            quantity=filled_qty,
            avg_entry_price=fill_price,
            current_price=fill_price,
            stop_loss_price=stop_loss.quantize(Decimal("0.00001")),
            take_profit_price=take_profit.quantize(Decimal("0.00001")),
            is_open=True,
            opened_at=executed_at,
            realized_pnl=Decimal("0"),
            is_paper=False,
            bot_id=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(position)
    else:
        # Close the matching open long position (SELL fill)
        pos_result = await db.execute(
            select(Position).where(
                Position.portfolio_id == order.portfolio_id,
                Position.symbol == order.symbol,
                Position.side == "long",
                Position.is_open == True,  # noqa: E712
            ).order_by(Position.opened_at.asc())
        )
        open_pos: Position | None = pos_result.scalars().first()
        if open_pos is not None:
            pnl = (fill_price - open_pos.avg_entry_price) * filled_qty
            open_pos.realized_pnl  = pnl.quantize(Decimal("0.00001"))
            open_pos.is_open       = False
            open_pos.closed_at     = executed_at
            open_pos.closed_price  = fill_price
            open_pos.current_price = fill_price
            open_pos.updated_at    = datetime.now(timezone.utc)
            portfolio.realized_pnl = (portfolio.realized_pnl or Decimal("0")) + pnl
            portfolio.updated_at   = datetime.now(timezone.utc)
        else:
            log.warning(
                "Alpaca sync: SELL fill — no matching open long position found",
                order_id=str(order.id),
                symbol=order.symbol,
            )

    await db.flush()

    log.info(
        "Alpaca fill synced",
        order_id=str(order.id),
        symbol=order.symbol,
        side=order.side,
        fill_price=float(fill_price),
        filled_qty=float(filled_qty),
        invest=float(invest_dec),
        broker_order_id=alpaca_data.get("id"),
    )
    return True


# ---------------------------------------------------------------------------
# Main sync entry point
# ---------------------------------------------------------------------------

async def sync_pending_alpaca_orders() -> None:
    """
    Poll Alpaca for the status of every local pending manual equity order.

    Called by the scheduler every 2 minutes. Uses its own DB session per order
    so a failure on one order never rolls back the others.

    Skips silently if:
      - ALPACA_BROKER_ENABLED is false
      - credentials are not configured
      - no pending manual orders exist
    """
    if not settings.ALPACA_BROKER_ENABLED:
        return
    if not settings.ALPACA_API_KEY or not settings.ALPACA_SECRET_KEY:
        return

    # ── Collect pending manual orders ────────────────────────────────────────
    async with AsyncSessionFactory() as db:
        result = await db.execute(
            select(Order).where(
                Order.status == "pending",
                Order.bot_id.is_(None),
            )
        )
        pending_orders = result.scalars().all()

    if not pending_orders:
        return

    log.info("Alpaca sync: checking pending orders", count=len(pending_orders))

    for order in pending_orders:
        # ── Per-order session: isolates each fill commit ──────────────────────
        async with AsyncSessionFactory() as db:
            try:
                # Re-fetch with a fresh session to get current status
                # (another sync run may have already processed this order)
                fresh_order = await db.get(Order, order.id)
                if fresh_order is None or fresh_order.status != "pending":
                    continue

                # ── Query Alpaca ──────────────────────────────────────────────
                broker_id = fresh_order.broker_order_id
                if broker_id:
                    alpaca_data = await asyncio.to_thread(_fetch_by_broker_id, broker_id)
                else:
                    # Fallback: search by client_order_id = str(order.id)
                    alpaca_data = await asyncio.to_thread(
                        _fetch_by_client_order_id, str(fresh_order.id)
                    )

                if alpaca_data is None:
                    log.info(
                        "Alpaca sync: order not found on broker",
                        order_id=str(fresh_order.id),
                        symbol=fresh_order.symbol,
                        note="may not have been submitted yet",
                    )
                    continue

                alpaca_status = alpaca_data.get("status", "")

                # Always persist the latest Alpaca status so the UI reflects it
                status_changed = fresh_order.alpaca_status != alpaca_status
                fresh_order.alpaca_status = alpaca_status
                if not fresh_order.broker_order_id and alpaca_data.get("id"):
                    fresh_order.broker_order_id = alpaca_data["id"]

                if alpaca_status == "filled":
                    success = await _apply_alpaca_fill(fresh_order, alpaca_data, db)
                    await db.commit()  # commit fill or cancellation

                elif alpaca_status in ("canceled", "expired", "rejected", "done_for_day"):
                    fresh_order.status = "cancelled"
                    fresh_order.rejection_reason = f"Broker: {alpaca_status}"
                    fresh_order.updated_at = datetime.now(timezone.utc)
                    await db.commit()
                    log.info(
                        "Alpaca sync: order cancelled/expired by broker",
                        order_id=str(fresh_order.id),
                        alpaca_status=alpaca_status,
                    )

                elif alpaca_status in ("partially_filled",):
                    # Partial fill: log but don't apply yet — wait for full fill
                    log.info(
                        "Alpaca sync: partially filled — waiting for full fill",
                        order_id=str(fresh_order.id),
                        symbol=fresh_order.symbol,
                        filled_qty=alpaca_data.get("filled_qty"),
                    )
                    await db.commit()  # persist alpaca_status + broker_order_id

                else:
                    if status_changed or fresh_order.broker_order_id:
                        log.info(
                            "Alpaca sync: order still pending",
                            order_id=str(fresh_order.id),
                            symbol=fresh_order.symbol,
                            alpaca_status=alpaca_status,
                        )
                        await db.commit()  # persist alpaca_status update

            except Exception as exc:
                log.exception(
                    "Alpaca sync: error processing order",
                    order_id=str(order.id),
                    error=str(exc),
                )
                await db.rollback()
