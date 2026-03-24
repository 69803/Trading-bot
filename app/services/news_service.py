"""
News Service — Phase 3.

Fetches news headlines for a trading symbol from:
  1. NewsAPI (newsapi.org)        — if NEWS_API_KEY is set
  2. Alpha Vantage News           — if ALPHA_VANTAGE_KEY is set (fallback)
  3. Deterministic simulation     — always-available fallback (no API key needed)

Results are cached in-process for 15 minutes to avoid hitting rate limits
on every bot cycle.

Usage:
    from app.services.news_service import get_news
    items: List[NewsItem] = await get_news("NVDA")
    result = sentiment_engine.analyze("NVDA", items)
"""
from __future__ import annotations

import asyncio
import hashlib
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import httpx

from app.core.config import settings
from app.core.logger import get_logger
from app.schemas.sentiment import NewsItem

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# In-process cache  {symbol: (expiry_ts, List[NewsItem])}
# ---------------------------------------------------------------------------
_cache: Dict[str, Tuple[float, List[NewsItem]]] = {}
_CACHE_TTL = 900  # 15 minutes
_lock = asyncio.Lock()


async def get_news(symbol: str, max_items: int = 10) -> List[NewsItem]:
    """
    Return up to *max_items* NewsItems for *symbol*.

    Provider priority:
      1. NewsAPI  (if NEWS_API_KEY configured)
      2. Alpha Vantage News Feed (if ALPHA_VANTAGE_KEY configured)
      3. Simulated headlines (deterministic, always works)

    Results are cached for 15 minutes.
    """
    async with _lock:
        cached = _cache.get(symbol)
        if cached and time.monotonic() < cached[0]:
            log.debug("News cache hit", symbol=symbol)
            return cached[1]

    items: Optional[List[NewsItem]] = None

    if settings.NEWS_API_KEY:
        items = await _fetch_newsapi(symbol, max_items)

    if not items and settings.ALPHA_VANTAGE_KEY:
        items = await _fetch_alphavantage(symbol, max_items)

    if not items:
        items = _simulate_news(symbol, max_items)
        log.info("NEWS SOURCE = SIMULATED", symbol=symbol, count=len(items))

    # Store in cache
    async with _lock:
        _cache[symbol] = (time.monotonic() + _CACHE_TTL, items)

    return items


# ---------------------------------------------------------------------------
# Provider 1 — NewsAPI
# ---------------------------------------------------------------------------

_NEWSAPI_URL = "https://newsapi.org/v2/everything"


async def _fetch_newsapi(symbol: str, max_items: int) -> Optional[List[NewsItem]]:
    """Fetch from newsapi.org.  Returns None on any error."""
    # Use a broader query so we get results for forex/crypto too
    query = _build_query(symbol)
    params = {
        "q": query,
        "apiKey": settings.NEWS_API_KEY,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": max_items,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(_NEWSAPI_URL, params=params)
        if resp.status_code != 200:
            log.warning("NewsAPI non-200", symbol=symbol, status=resp.status_code)
            return None
        data = resp.json()
        articles = data.get("articles", [])
        if not articles:
            return None
        items: List[NewsItem] = []
        for a in articles[:max_items]:
            pub = _parse_dt(a.get("publishedAt"))
            items.append(NewsItem(
                title=a.get("title") or "No title",
                description=a.get("description"),
                source="newsapi",
                published_at=pub,
                url=a.get("url"),
            ))
        log.info("NEWS SOURCE = REAL (NewsAPI)", symbol=symbol, count=len(items), query=query)
        return items or None
    except Exception as exc:
        log.warning("NewsAPI fetch failed — falling back to simulation", symbol=symbol, error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Provider 2 — Alpha Vantage News Sentiment feed
# ---------------------------------------------------------------------------

_AV_URL = "https://www.alphavantage.co/query"


async def _fetch_alphavantage(symbol: str, max_items: int) -> Optional[List[NewsItem]]:
    """Fetch from Alpha Vantage News & Sentiment endpoint."""
    # AV uses ticker format; for forex use FROM_CURRENCY:TO_CURRENCY style
    ticker = _av_ticker(symbol)
    params = {
        "function": "NEWS_SENTIMENT",
        "tickers": ticker,
        "apikey": settings.ALPHA_VANTAGE_KEY,
        "limit": max_items,
        "sort": "LATEST",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(_AV_URL, params=params)
        if resp.status_code != 200:
            log.warning("AlphaVantage non-200", symbol=symbol, status=resp.status_code)
            return None
        data = resp.json()
        feed = data.get("feed", [])
        if not feed:
            return None
        items: List[NewsItem] = []
        for a in feed[:max_items]:
            pub = _parse_dt(a.get("time_published"))
            items.append(NewsItem(
                title=a.get("title") or "No title",
                description=a.get("summary"),
                source="alphavantage",
                published_at=pub,
                url=a.get("url"),
            ))
        log.info("AlphaVantage fetched", symbol=symbol, count=len(items))
        return items or None
    except Exception as exc:
        log.warning("AlphaVantage fetch failed", symbol=symbol, error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Provider 3 — Deterministic simulated news
# ---------------------------------------------------------------------------

# Template headlines — {symbol} is replaced with the actual symbol.
# Seeded by (symbol + date) so output is stable within a day but varies day-to-day.
_TEMPLATES = [
    # Positive
    ("{symbol} reports record quarterly earnings, beating analyst expectations",
     "The company posted earnings per share significantly above consensus estimates, driven by strong demand across all business segments."),
    ("Analysts upgrade {symbol} to Strong Buy following product launch",
     "Multiple Wall Street firms raised their price targets after the company unveiled a next-generation product line that exceeded market expectations."),
    ("{symbol} revenue growth accelerates, stock surges in after-hours trading",
     "Revenue climbed sharply year-over-year as the company continues to gain market share in its core segments."),
    ("{symbol} announces major acquisition, expanding market share",
     "The deal, valued in the billions, is expected to be immediately accretive to earnings and significantly widen the company's addressable market."),
    ("{symbol} raises dividend and approves share buyback program",
     "Management signaled strong confidence in future cash flows by increasing the dividend and authorizing a substantial repurchase plan."),
    ("Strong institutional buying detected in {symbol}",
     "Hedge funds and institutional investors have been increasing their positions in the stock, according to the latest 13-F filings."),
    ("{symbol} secures billion-dollar government contract",
     "The multi-year contract is seen as a significant revenue catalyst and validates the company's position in the sector."),
    # Negative
    ("{symbol} misses earnings estimates, revenue decline widens",
     "The company reported a disappointing quarter, with earnings per share falling short of consensus and revenue declining on weakening demand."),
    ("Analysts downgrade {symbol} amid profit warning",
     "Several analysts cut their ratings following management guidance that fell well below expectations for the upcoming fiscal year."),
    ("{symbol} faces SEC investigation over accounting practices",
     "Regulators have launched a formal inquiry into the company's financial reporting, raising concerns among investors about potential restatements."),
    ("{symbol} announces layoffs and major restructuring plan",
     "In response to slowing growth and rising costs, the company plans to cut thousands of jobs and consolidate several business units."),
    ("Lawsuit filed against {symbol} alleging patent infringement",
     "A competitor has filed suit claiming the company's flagship product infringes on key intellectual property, which could result in significant penalties."),
    ("{symbol} hit by tariff concerns as geopolitical tensions rise",
     "Escalating trade disputes threaten to increase the company's cost structure and potentially disrupt key supply chains."),
    # Neutral/mixed
    ("{symbol} reports earnings in line with expectations",
     "Results were broadly in line with analyst consensus. Management reiterated guidance but provided no additional forward-looking catalysts."),
    ("{symbol} under review as Fed signals rate hike path",
     "Higher interest rates are expected to affect valuations across the sector, with investors rotating into defensives."),
    ("Market volatility impacts {symbol} alongside broader sector",
     "The stock moved in line with sector peers as investors digested mixed macroeconomic data and uncertainty around central bank policy."),
]


def _simulate_news(symbol: str, max_items: int) -> List[NewsItem]:
    """
    Generate deterministic simulated news for *symbol*.

    Uses (symbol + today's date) as the random seed so the output is stable
    within a day but changes each day.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    seed_str = f"{symbol}-{today}"
    seed_int = int(hashlib.md5(seed_str.encode()).hexdigest(), 16) % (2**31)
    rng = random.Random(seed_int)

    shuffled = _TEMPLATES[:]
    rng.shuffle(shuffled)
    selected = shuffled[: min(max_items, len(shuffled))]

    now = datetime.now(timezone.utc)
    items: List[NewsItem] = []
    for idx, (title_tpl, desc_tpl) in enumerate(selected):
        # Spread articles over the last 24 hours
        age_hours = rng.uniform(0.5, 23.0)
        pub_time  = now - timedelta(hours=age_hours)
        items.append(NewsItem(
            title       = title_tpl.format(symbol=symbol),
            description = desc_tpl.format(symbol=symbol),
            source      = "simulated",
            published_at= pub_time,
            url         = None,
        ))
    # Sort newest first (mirrors real API behaviour)
    items.sort(key=lambda x: x.published_at or now, reverse=True)
    return items


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Rich keyword queries for major forex pairs.
# NewsAPI supports boolean: "term1" OR "term2" — each phrase in double-quotes.
# Avoid long AND chains; OR gives broader coverage and avoids zero-result queries.
_FOREX_QUERIES: Dict[str, str] = {
    "EURUSD":   '"ECB" OR "Federal Reserve" OR "EUR/USD" OR "euro dollar" OR "interest rates" OR "inflation"',
    "GBPUSD":   '"Bank of England" OR "Federal Reserve" OR "GBP/USD" OR "pound dollar" OR "interest rates"',
    "USDJPY":   '"Bank of Japan" OR "Federal Reserve" OR "USD/JPY" OR "dollar yen" OR "BOJ"',
    "USDCHF":   '"SNB" OR "Federal Reserve" OR "USD/CHF" OR "dollar franc" OR "Swiss National Bank"',
    "AUDUSD":   '"RBA" OR "Federal Reserve" OR "AUD/USD" OR "Australian dollar" OR "Reserve Bank Australia"',
    "USDCAD":   '"Bank of Canada" OR "Federal Reserve" OR "USD/CAD" OR "Canadian dollar" OR "oil prices"',
    "NZDUSD":   '"RBNZ" OR "Federal Reserve" OR "NZD/USD" OR "New Zealand dollar"',
    "XAUUSD":   '"gold price" OR "gold rally" OR "gold falls" OR "XAU/USD" OR "safe haven" OR "Fed rates"',
    "XAGUSD":   '"silver price" OR "silver rally" OR "XAG/USD" OR "silver demand"',
    "WTIUSD":   '"crude oil" OR "oil price" OR "OPEC" OR "WTI" OR "oil supply"',
    "BRENTUSD": '"Brent crude" OR "oil price" OR "OPEC" OR "Brent oil"',
}


def _build_query(symbol: str) -> str:
    """Build a NewsAPI search query string for the symbol."""
    # Check rich forex/commodity keyword map first
    s = symbol.upper().replace("/", "").replace("-", "")
    if s in _FOREX_QUERIES:
        return _FOREX_QUERIES[s]
    # Also handle slash-form keys like "EUR/USD"
    s_noslash = symbol.upper().replace("/", "")
    if s_noslash in _FOREX_QUERIES:
        return _FOREX_QUERIES[s_noslash]
    # Generic forex pair like EURUSD not in map
    if len(symbol) == 6 and symbol.isalpha():
        base, quote = symbol[:3], symbol[3:]
        return f'"{base}/{quote}" OR "{symbol}" forex'
    # Crypto like BTCUSD
    if symbol.endswith("USD") and len(symbol) > 5:
        base = symbol[:-3]
        return f'"{base}" cryptocurrency OR "{symbol}"'
    return f'"{symbol}" stock'


def _av_ticker(symbol: str) -> str:
    """Convert internal symbol to Alpha Vantage ticker format."""
    # Forex: EURUSD → FOREX:EUR
    if len(symbol) == 6 and symbol.isalpha() and not symbol.endswith("USD"):
        return f"FOREX:{symbol[:3]}"
    # Crypto: BTCUSD → CRYPTO:BTC
    if symbol.endswith("USD") and len(symbol) > 5:
        return f"CRYPTO:{symbol[:-3]}"
    return symbol


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 datetime string to an aware datetime, or return None."""
    if not value:
        return None
    try:
        # NewsAPI uses "2024-01-15T10:30:00Z"
        # AlphaVantage uses "20240115T103000"
        if "T" in value and "-" in value:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif "T" in value:
            dt = datetime.strptime(value, "%Y%m%dT%H%M%S")
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None
