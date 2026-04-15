"""Market data endpoints: candles, quotes, symbols.

All requests are routed through MarketDataRouter which dispatches to the
correct provider based on asset type:
  - Stocks / ETFs → Polygon (or Alpaca / GBM depending on config)
  - Forex / Commodities → Twelve Data (or GBM fallback)
"""

from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.core.config import settings
from app.models.market_candle import MarketCandle
from app.models.user import User
from app.schemas.market_data import (
    CandleListResponse,
    CandleOut,
    QuoteListResponse,
    QuoteOut,
)
from app.services.market_data_router import market_data_router

router = APIRouter()


@router.get(
    "/symbols",
    summary="List all supported trading symbols (stocks + forex + commodities)",
)
async def get_symbols(
    current_user: User = Depends(get_current_active_user),
) -> dict:
    return {"symbols": market_data_router.get_all_symbols()}


@router.get(
    "/candles",
    response_model=CandleListResponse,
    summary="Get OHLCV candles for any symbol (stock, forex, commodity)",
)
async def get_candles(
    symbol: str = Query(..., description="Symbol e.g. AAPL, EUR/USD, XAU/USD"),
    timeframe: str = Query("1h", pattern="^(1m|5m|15m|30m|1h|4h|1d)$"),
    limit: int = Query(200, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> CandleListResponse:
    sym = symbol.upper()
    try:
        raw_candles = await market_data_router.get_candles(sym, timeframe, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    candle_list = [
        CandleOut(
            symbol=sym,
            timeframe=timeframe,
            timestamp=datetime.fromisoformat(c["timestamp"]),
            open=Decimal(str(c["open"])),
            high=Decimal(str(c["high"])),
            low=Decimal(str(c["low"])),
            close=Decimal(str(c["close"])),
            volume=int(c["volume"]),
        )
        for c in raw_candles
    ]
    return CandleListResponse(symbol=sym, timeframe=timeframe, candles=candle_list)


@router.get(
    "/candles/{symbol}",
    response_model=CandleListResponse,
    summary="Get OHLCV candles (path param variant)",
)
async def get_candles_by_path(
    symbol: str,
    timeframe: str = Query("1h", pattern="^(1m|5m|15m|30m|1h|4h|1d)$"),
    limit: int = Query(200, ge=1, le=1000),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> CandleListResponse:
    return await get_candles(
        symbol=symbol, timeframe=timeframe, limit=limit,
        current_user=current_user, db=db,
    )


@router.get(
    "/quote",
    response_model=QuoteListResponse,
    summary="Get quotes for one or more symbols (stocks, forex, commodities)",
)
async def get_quote(
    symbols: str | None = Query(
        None, description="Comma-separated symbols; omit for all"
    ),
    current_user: User = Depends(get_current_active_user),
) -> QuoteListResponse:
    requested = (
        [s.strip().upper() for s in symbols.split(",")]
        if symbols
        else market_data_router.get_all_symbols()
    )

    quotes: list[QuoteOut] = []
    for sym in requested:
        try:
            q = await market_data_router.get_quote(sym)
            quotes.append(
                QuoteOut(
                    symbol=q["symbol"],
                    price=Decimal(str(q["price"])),
                    change=Decimal(str(q["change"])),
                    change_pct=Decimal(str(q["change_pct"])),
                    bid=Decimal(str(q["bid"])),
                    ask=Decimal(str(q["ask"])),
                    timestamp=datetime.fromisoformat(q["timestamp"]),
                )
            )
        except (ValueError, KeyError):
            continue

    return QuoteListResponse(quotes=quotes)


@router.get(
    "/quotes",
    response_model=QuoteListResponse,
    summary="Alias for /quote",
)
async def get_quotes(
    symbols: str | None = Query(None),
    current_user: User = Depends(get_current_active_user),
) -> QuoteListResponse:
    return await get_quote(symbols=symbols, current_user=current_user)


@router.get(
    "/status",
    summary="Current market open/closed status",
    tags=["market"],
)
async def get_market_status(
    symbol: str | None = Query(
        None,
        description="Symbol to check — determines whether NYSE hours apply. "
                    "Forex, commodities, and crypto are always open.",
    ),
    current_user: User = Depends(get_current_active_user),
) -> dict:
    """
    Returns session status for the market that trades the given symbol.

    • US equities / ETFs  → NYSE/NASDAQ hours (Mon–Fri 09:30–16:00 ET)
    • Forex pairs         → always open  (market = "FX")
    • Crypto              → always open  (market = "Crypto")
    • Commodities         → always open  (market = "Commodities")
    """
    from app.services.market_hours import get_nyse_status

    sym = (symbol or "").upper().strip()

    # Forex: contains a "/"
    if "/" in sym:
        return {"is_open": True, "session": "regular",
                "next_open": None, "next_close": None,
                "market": "FX", "timezone": "UTC"}

    # Crypto: ends with known stablecoin/coin suffix
    _CRYPTO = {"USDT", "USDC", "BTC", "ETH", "BNB", "SOL", "ADA", "XRP", "DOGE"}
    if any(sym.endswith(s) and len(sym) > len(s) for s in _CRYPTO):
        return {"is_open": True, "session": "regular",
                "next_open": None, "next_close": None,
                "market": "Crypto", "timezone": "UTC"}

    # Commodities: known no-slash symbols
    _COMM = {"WTI", "BRENT", "NATGAS", "OIL", "USOIL", "UKOIL",
             "XAUUSD", "XAGUSD", "XPTUSD"}
    if sym in _COMM:
        return {"is_open": True, "session": "regular",
                "next_open": None, "next_close": None,
                "market": "Commodities", "timezone": "UTC"}

    # Default: US equity → NYSE session
    return get_nyse_status()


@router.get(
    "/debug",
    summary="Multi-provider diagnostics",
    tags=["market"],
)
async def get_market_debug(
    current_user: User = Depends(get_current_active_user),
) -> dict:
    """Return active providers and live sample prices."""
    stock_provider = settings.MARKET_DATA_PROVIDER.lower()
    twelvedata_loaded = bool(settings.TWELVE_DATA_API_KEY)

    samples: dict = {}
    for sym in ["AAPL", "EUR/USD", "XAU/USD"]:
        try:
            samples[sym] = await market_data_router.get_current_price(sym)
        except Exception:
            samples[sym] = None

    return {
        "stock_provider": stock_provider,
        "stock_api_key_loaded": bool(
            settings.POLYGON_API_KEY or settings.ALPACA_API_KEY
        ),
        "forex_commodity_provider": "twelvedata",
        "twelvedata_api_key_loaded": twelvedata_loaded,
        "sample_prices": samples,
    }
