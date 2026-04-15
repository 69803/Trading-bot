"""
Alpaca Paper Trading Broker — mirror execution layer.

Sends a copy of every filled internal order to Alpaca Paper Trading API.
This is a NON-BLOCKING, fire-and-forget side effect: if Alpaca rejects or
is unreachable the internal trade is already committed and nothing is rolled
back.

Scope:
  - US equities ONLY (symbols without "/" and not in CRYPTO_SYMBOLS)
  - Alpaca Paper API: https://paper-api.alpaca.markets
  - Endpoint: POST /v2/orders

This module will NOT be called for:
  - Forex pairs (EUR/USD, GBP/USD, …)
  - Crypto (BTCUSDT, ETHUSDT, …)
  - Commodities (XAU/USD, WTI, …)

Audit trail:
  Every submission attempt (success or failure) is logged at INFO/WARNING
  level with structured fields readable in Render logs:

    broker=alpaca  symbol=AAPL  side=buy  qty=1.234
    notional=200.00  broker_order_id=abc123  submitted_at=...  status=accepted

  On failure:
    broker=alpaca  symbol=AAPL  side=buy  status=error  error_message=...

Usage (called only from order_service.py):
    from app.services.alpaca_broker import submit_order_to_alpaca
    await submit_order_to_alpaca(symbol="AAPL", side="buy",
                                  qty=1.23, notional=200.0,
                                  internal_order_id="uuid")
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.core.config import settings
from app.core.logger import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PAPER_BASE_URL = "https://paper-api.alpaca.markets"
ORDERS_PATH    = "/v2/orders"

# Symbols that must NOT be sent to Alpaca (they don't trade there)
_CRYPTO_SUFFIXES = {"USDT", "USDC", "BTC", "ETH", "BNB", "SOL", "ADA",
                    "XRP", "DOGE", "AVAX", "DOT", "MATIC"}

_NON_SLASH_COMMODITIES = {"WTI", "BRENT", "NATGAS", "COPPER", "OIL",
                           "USOIL", "UKOIL", "XAUUSD", "XAGUSD", "XPTUSD"}


# ---------------------------------------------------------------------------
# Eligibility check
# ---------------------------------------------------------------------------

def _is_us_equity(symbol: str) -> bool:
    """
    Return True only for plain US stock/ETF tickers.
    Anything with a "/" is forex or commodity.
    Known crypto suffixes and commodity shorthands are excluded.
    """
    s = symbol.upper()
    if "/" in s:
        return False
    if s in _NON_SLASH_COMMODITIES:
        return False
    for suffix in _CRYPTO_SUFFIXES:
        if s.endswith(suffix) and len(s) > len(suffix):
            return False
    return True


# ---------------------------------------------------------------------------
# Core submission
# ---------------------------------------------------------------------------

def _build_headers() -> dict:
    return {
        "APCA-API-KEY-ID":     settings.ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": settings.ALPACA_SECRET_KEY,
        "Content-Type":        "application/json",
        "Accept":              "application/json",
    }


def _submit_sync(
    symbol: str,
    side: str,
    qty: float,
    notional: Optional[float],
    client_order_id: Optional[str] = None,
) -> dict:
    """
    Synchronous HTTP POST to Alpaca paper API.
    Uses notional (dollar amount) when available — maps naturally to
    our investment_amount field.  Falls back to qty for legacy calls.

    client_order_id: when set, Alpaca stores it on the order so the
    fill-sync job can retrieve the order by our local UUID later.

    Returns the parsed JSON response dict.
    Raises on HTTP errors so the caller can catch and log.
    """
    if notional and notional > 0:
        payload = {
            "symbol":        symbol.upper(),
            "notional":      str(round(notional, 2)),
            "side":          side,          # "buy" | "sell"
            "type":          "market",
            "time_in_force": "day",
        }
    else:
        payload = {
            "symbol":        symbol.upper(),
            "qty":           str(round(qty, 8)),
            "side":          side,
            "type":          "market",
            "time_in_force": "day",
        }

    # Set client_order_id so we can look up this order from Alpaca later
    # (used by the fill-sync polling job). Max length: 128 chars; UUID = 36.
    if client_order_id:
        payload["client_order_id"] = client_order_id

    url = f"{PAPER_BASE_URL}{ORDERS_PATH}"
    with httpx.Client(timeout=10.0) as client:
        resp = client.post(url, headers=_build_headers(), json=payload)

    if resp.status_code in (200, 201):
        return resp.json()

    # Surface a clear error with Alpaca's own message
    try:
        detail = resp.json().get("message", resp.text)
    except Exception:
        detail = resp.text
    raise RuntimeError(
        f"Alpaca API HTTP {resp.status_code}: {detail}"
    )


# ---------------------------------------------------------------------------
# Public async interface
# ---------------------------------------------------------------------------

async def submit_order_to_alpaca(
    *,
    symbol: str,
    side: str,
    qty: float,
    notional: Optional[float] = None,
    internal_order_id: str = "",
    client_order_id: Optional[str] = None,
) -> Optional[str]:
    """
    Mirror a filled internal order to Alpaca Paper Trading.

    Returns the Alpaca broker_order_id on success, None on any error.
    NEVER raises — all exceptions are caught and logged as warnings so the
    internal order flow is never interrupted.

    Audit fields logged on every attempt:
      broker, symbol, side, qty, notional, internal_order_id,
      broker_order_id, submitted_at, status, error_message
    """
    # ── Guard: feature flag ──────────────────────────────────────────────────
    if not settings.ALPACA_BROKER_ENABLED:
        log.info(
            "ALPACA BROKER: skipped — ALPACA_BROKER_ENABLED=false",
            broker="alpaca",
            symbol=symbol,
            side=side,
            internal_order_id=internal_order_id,
        )
        return None

    # ── Guard: credentials ───────────────────────────────────────────────────
    if not settings.ALPACA_API_KEY or not settings.ALPACA_SECRET_KEY:
        log.warning(
            "ALPACA BROKER: skipped — credentials not configured",
            broker="alpaca",
            symbol=symbol,
            side=side,
            internal_order_id=internal_order_id,
        )
        return None

    # ── Guard: US equities only ──────────────────────────────────────────────
    if not _is_us_equity(symbol):
        log.info(
            "ALPACA BROKER: skipped — non-equity symbol",
            broker="alpaca",
            symbol=symbol,
            side=side,
            reason="not_us_equity",
            internal_order_id=internal_order_id,
        )
        return None

    submitted_at = datetime.now(timezone.utc).isoformat()

    try:
        data = await asyncio.to_thread(
            _submit_sync, symbol, side, qty, notional, client_order_id
        )

        broker_order_id = data.get("id", "unknown")
        alpaca_status   = data.get("status", "unknown")

        log.info(
            "ALPACA BROKER: order submitted",
            broker="alpaca",
            symbol=symbol,
            side=side,
            qty=round(qty, 6),
            notional=round(notional, 2) if notional else None,
            internal_order_id=internal_order_id,
            broker_order_id=broker_order_id,
            submitted_at=submitted_at,
            status=alpaca_status,
        )
        return broker_order_id

    except Exception as exc:
        log.warning(
            "ALPACA BROKER: submission failed — internal order NOT affected",
            broker="alpaca",
            symbol=symbol,
            side=side,
            qty=round(qty, 6),
            notional=round(notional, 2) if notional else None,
            internal_order_id=internal_order_id,
            submitted_at=submitted_at,
            status="error",
            error_message=str(exc),
        )
        return None
