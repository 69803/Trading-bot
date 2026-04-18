"""
Alpaca Broker — routes orders to paper or live Alpaca API.

Paper mode : https://paper-api.alpaca.markets  (ALPACA_API_KEY / ALPACA_SECRET_KEY)
Live mode  : https://api.alpaca.markets        (ALPACA_LIVE_API_KEY / ALPACA_LIVE_SECRET_KEY)

Scope:
  - US equities ONLY (symbols without "/" and not in CRYPTO_SYMBOLS)
  - Endpoint: POST /v2/orders

This module will NOT be called for:
  - Forex pairs (EUR/USD, GBP/USD, …)
  - Crypto (BTCUSDT, ETHUSDT, …)
  - Commodities (XAU/USD, WTI, …)

Usage (called only from order_service.py):
    from app.services.alpaca_broker import submit_order_to_alpaca
    await submit_order_to_alpaca(symbol="AAPL", side="buy",
                                  qty=1.23, notional=200.0,
                                  internal_order_id="uuid",
                                  account_mode="paper")
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

PAPER_BASE_URL   = "https://paper-api.alpaca.markets"
LIVE_BASE_URL    = "https://api.alpaca.markets"
ORDERS_PATH      = "/v2/orders"
ACCOUNT_PATH     = "/v2/account"

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

def _build_headers(account_mode: str = "paper") -> dict:
    """Return auth headers for the correct Alpaca environment."""
    if account_mode == "live":
        key    = settings.ALPACA_LIVE_API_KEY
        secret = settings.ALPACA_LIVE_SECRET_KEY
    else:
        key    = settings.ALPACA_API_KEY
        secret = settings.ALPACA_SECRET_KEY
    return {
        "APCA-API-KEY-ID":     key,
        "APCA-API-SECRET-KEY": secret,
        "Content-Type":        "application/json",
        "Accept":              "application/json",
    }


def _submit_sync(
    symbol: str,
    side: str,
    qty: float,
    notional: Optional[float],
    client_order_id: Optional[str] = None,
    account_mode: str = "paper",
) -> dict:
    """
    Synchronous HTTP POST to the correct Alpaca API endpoint.

    Routes to PAPER_BASE_URL or LIVE_BASE_URL based on account_mode.
    Uses notional (dollar amount) when available.  Falls back to qty.

    Returns the parsed JSON response dict.
    Raises on HTTP errors so the caller can catch and log.
    """
    base_url = LIVE_BASE_URL if account_mode == "live" else PAPER_BASE_URL

    if notional and notional > 0:
        payload = {
            "symbol":        symbol.upper(),
            "notional":      str(round(notional, 2)),
            "side":          side,
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

    if client_order_id:
        payload["client_order_id"] = client_order_id

    url = f"{base_url}{ORDERS_PATH}"
    with httpx.Client(timeout=10.0) as client:
        resp = client.post(url, headers=_build_headers(account_mode), json=payload)

    if resp.status_code in (200, 201):
        return resp.json()

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
    account_mode: str = "paper",
) -> Optional[dict]:
    """
    Submit an order to Alpaca (paper or live) based on account_mode.

    - account_mode='paper' → https://paper-api.alpaca.markets
    - account_mode='live'  → https://api.alpaca.markets

    Returns the full Alpaca order response dict on success, None on any error.
    NEVER raises — all exceptions are caught and logged.
    """
    # ── Guard: feature flag ──────────────────────────────────────────────────
    if not settings.ALPACA_BROKER_ENABLED:
        log.info(
            "ALPACA BROKER: skipped — ALPACA_BROKER_ENABLED=false",
            broker="alpaca", account_mode=account_mode,
            symbol=symbol, side=side,
            internal_order_id=internal_order_id,
        )
        return None

    # ── Guard: credentials (mode-specific) ───────────────────────────────────
    if account_mode == "live":
        if not settings.ALPACA_LIVE_API_KEY or not settings.ALPACA_LIVE_SECRET_KEY:
            log.warning(
                "ALPACA BROKER: skipped — LIVE credentials not configured",
                broker="alpaca", account_mode="live",
                symbol=symbol, side=side,
                internal_order_id=internal_order_id,
            )
            return None
    else:
        if not settings.ALPACA_API_KEY or not settings.ALPACA_SECRET_KEY:
            log.warning(
                "ALPACA BROKER: skipped — PAPER credentials not configured",
                broker="alpaca", account_mode="paper",
                symbol=symbol, side=side,
                internal_order_id=internal_order_id,
            )
            return None

    # ── Guard: US equities only ──────────────────────────────────────────────
    if not _is_us_equity(symbol):
        log.info(
            "ALPACA BROKER: skipped — non-equity symbol",
            broker="alpaca", account_mode=account_mode,
            symbol=symbol, side=side,
            reason="not_us_equity",
            internal_order_id=internal_order_id,
        )
        return None

    submitted_at = datetime.now(timezone.utc).isoformat()
    base_url = LIVE_BASE_URL if account_mode == "live" else PAPER_BASE_URL

    try:
        data = await asyncio.to_thread(
            _submit_sync, symbol, side, qty, notional, client_order_id, account_mode
        )

        broker_order_id = data.get("id", "unknown")
        alpaca_status   = data.get("status", "unknown")

        log.info(
            "ALPACA BROKER: order submitted",
            broker="alpaca", account_mode=account_mode,
            endpoint=base_url,
            symbol=symbol, side=side,
            qty=round(qty, 6),
            notional=round(notional, 2) if notional else None,
            internal_order_id=internal_order_id,
            broker_order_id=broker_order_id,
            submitted_at=submitted_at,
            status=alpaca_status,
        )
        return data

    except Exception as exc:
        log.warning(
            "ALPACA BROKER: submission failed — internal order NOT affected",
            broker="alpaca", account_mode=account_mode,
            endpoint=base_url,
            symbol=symbol, side=side,
            qty=round(qty, 6),
            notional=round(notional, 2) if notional else None,
            internal_order_id=internal_order_id,
            submitted_at=submitted_at,
            status="error",
            error_message=str(exc),
        )
        return None


# ---------------------------------------------------------------------------
# Account balance (balance sync — uses GET /v2/account)
# ---------------------------------------------------------------------------

def _fetch_account_sync(account_mode: str = "live") -> dict:
    """GET /v2/account — synchronous. Returns raw Alpaca account dict."""
    base_url = LIVE_BASE_URL if account_mode == "live" else PAPER_BASE_URL
    url = f"{base_url}{ACCOUNT_PATH}"
    with httpx.Client(timeout=10.0) as client:
        resp = client.get(url, headers=_build_headers(account_mode))
    if resp.status_code == 200:
        return resp.json()
    try:
        detail = resp.json().get("message", resp.text)
    except Exception:
        detail = resp.text
    raise RuntimeError(f"Alpaca /v2/account HTTP {resp.status_code}: {detail}")


async def fetch_account_balance(account_mode: str = "live") -> Optional[dict]:
    """
    Fetch real account balance from Alpaca GET /v2/account.

    Relevant fields in the response:
      cash          — settled cash available for withdrawal/trading
      buying_power  — total available buying power (may be 2x-4x cash for margin)
      equity        — total portfolio value (cash + open positions at market value)

    Returns None when credentials are missing or the request fails.
    Never raises — all exceptions are caught and logged.
    """
    if account_mode == "live":
        if not settings.ALPACA_LIVE_API_KEY or not settings.ALPACA_LIVE_SECRET_KEY:
            log.warning(
                "ALPACA: live balance fetch skipped — LIVE credentials not configured",
                account_mode="live",
            )
            return None
    else:
        if not settings.ALPACA_API_KEY or not settings.ALPACA_SECRET_KEY:
            return None

    try:
        data = await asyncio.to_thread(_fetch_account_sync, account_mode)
        log.info(
            "ALPACA: account balance fetched",
            account_mode=account_mode,
            cash=data.get("cash"),
            buying_power=data.get("buying_power"),
            equity=data.get("equity"),
        )
        return data
    except Exception as exc:
        log.warning(
            "ALPACA: account balance fetch failed",
            account_mode=account_mode,
            error=str(exc),
        )
        return None
