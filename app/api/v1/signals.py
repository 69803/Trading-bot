"""
Signals API — Phase 9.

Endpoints
─────────
GET /signals/live
    Runs the full Phase 2-4 pipeline (technical + sentiment + decision) for
    every symbol in the user's strategy config.  Results are NOT persisted —
    this is a read-only on-demand snapshot for the frontend.

GET /signals/decisions
    Returns recent DecisionLog rows persisted by the live bot (Phase 7).
    Supports symbol filter and pagination.

GET /signals/decisions/{symbol}
    Same as above, scoped to one symbol.
"""
from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.core.config import settings
from app.core.logger import get_logger
from app.models.decision_log import DecisionLog
from app.models.strategy_config import StrategyConfig
from app.models.user import User
from app.schemas.live_signal import (
    DecisionLogOut,
    DecisionsResponse,
    LiveSignal,
    LiveSignalIndicators,
    LiveSignalsResponse,
)
from app.services import decision_engine, sentiment_engine, technical_engine
from app.services.market_data_router import market_data_router as market_data_service
from app.services.news_service import get_news

router = APIRouter()
log = get_logger(__name__)

CANDLE_LIMIT = 250


# ---------------------------------------------------------------------------
# GET /signals/live
# ---------------------------------------------------------------------------

@router.get("/live", response_model=LiveSignalsResponse, summary="Live pipeline signals for all configured symbols")
async def get_live_signals(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> LiveSignalsResponse:
    """
    Runs technical + sentiment + decision pipeline for every symbol in the
    user's strategy config and returns a snapshot.  Results are not persisted.
    """
    now = datetime.now(timezone.utc)

    # Load strategy config
    sc_result = await db.execute(
        select(StrategyConfig).where(StrategyConfig.user_id == current_user.id)
    )
    config: Optional[StrategyConfig] = sc_result.scalars().first()
    symbols: List[str] = list(config.symbols) if config and config.symbols else []

    if not symbols:
        return LiveSignalsResponse(
            signals=[], generated_at=now,
            symbols_requested=0, symbols_ok=0, symbols_failed=0,
        )

    signals: List[LiveSignal] = []
    failed = 0

    # Run all symbols concurrently
    tasks = [
        _analyze_symbol(symbol, config)
        for symbol in symbols
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for symbol, result in zip(symbols, results):
        if isinstance(result, Exception):
            log.warning("Live signal failed", symbol=symbol, error=str(result))
            failed += 1
        elif result is not None:
            signals.append(result)

    return LiveSignalsResponse(
        signals=signals,
        generated_at=now,
        symbols_requested=len(symbols),
        symbols_ok=len(signals),
        symbols_failed=failed,
    )


async def _analyze_symbol(
    symbol: str,
    config: Optional[StrategyConfig],
) -> Optional[LiveSignal]:
    """Run the full pipeline for one symbol and return a LiveSignal."""
    ema_fast      = int(config.ema_fast)       if config else 9
    ema_slow      = int(config.ema_slow)       if config else 21
    rsi_period    = int(config.rsi_period)     if config else 14
    rsi_overbought = float(config.rsi_overbought) if config else 70.0
    rsi_oversold   = float(config.rsi_oversold)   if config else 30.0

    candles = await market_data_service.get_candles(symbol, "1h", limit=CANDLE_LIMIT)
    if not candles:
        return None

    # Phase 2 — technical
    technical = technical_engine.analyze(
        symbol=symbol, candles=candles, timeframe="1h",
        ema_fast_period=ema_fast, ema_slow_period=ema_slow,
        rsi_period=rsi_period, rsi_overbought=rsi_overbought,
        rsi_oversold=rsi_oversold,
    )

    # Phase 3 — sentiment
    news_items = await get_news(symbol, max_items=10)
    _provider  = "newsapi" if settings.NEWS_API_KEY else ("alphavantage" if settings.ALPHA_VANTAGE_KEY else "simulated")
    sentiment  = sentiment_engine.analyze(symbol=symbol, items=news_items, provider=_provider)

    # Phase 4 — decision
    decision = decision_engine.decide(technical=technical, sentiment=sentiment)

    ind = technical.indicators

    return LiveSignal(
        symbol       = symbol,
        analyzed_at  = decision.decided_at,
        # Technical
        tech_direction  = technical.direction,
        tech_confidence = technical.confidence,
        tech_reasons    = technical.reasons,
        indicators      = LiveSignalIndicators(
            price          = _safe(ind.price),
            rsi            = _safe(ind.rsi),
            ema_fast       = _safe(ind.ema_fast),
            ema_slow       = _safe(ind.ema_slow),
            macd           = _safe(ind.macd),
            macd_histogram = _safe(ind.macd_histogram),
            atr            = _safe(ind.atr),
            volume_ratio   = _safe(ind.volume_ratio),
        ),
        ema_crossover   = technical.ema_crossover,
        macd_crossover  = technical.macd_crossover,
        rsi_extreme     = technical.rsi_extreme,
        candles_used    = technical.candles_used,
        # Sentiment
        sentiment_label    = sentiment.label,
        sentiment_score    = sentiment.sentiment_score,
        sentiment_impact   = sentiment.impact_score,
        sentiment_modifier = sentiment.confidence_modifier,
        sentiment_source   = sentiment.source,
        news_count         = sentiment.news_count,
        top_headlines      = sentiment.headlines[:5],
        # Decision
        direction          = decision.direction,
        final_confidence   = decision.confidence,
        override_reason    = decision.override_reason,
        decision_reasons   = decision.reasons,
        is_actionable      = decision.is_actionable,
        is_blocked         = decision.is_blocked,
    )


# ---------------------------------------------------------------------------
# GET /signals/decisions
# ---------------------------------------------------------------------------

@router.get("/decisions", response_model=DecisionsResponse, summary="Recent bot decisions from audit log")
async def get_decisions(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> DecisionsResponse:
    """Return recent DecisionLog rows for this user, newest first."""
    base = (
        select(DecisionLog)
        .where(DecisionLog.user_id == current_user.id)
    )
    count_q = (
        select(func.count())
        .where(DecisionLog.user_id == current_user.id)
    )

    if symbol:
        sym = symbol.upper()
        base    = base.where(DecisionLog.symbol == sym)
        count_q = count_q.where(DecisionLog.symbol == sym)

    total_result = await db.execute(count_q)
    total: int = total_result.scalar() or 0

    rows_result = await db.execute(
        base.order_by(DecisionLog.decided_at.desc()).offset(offset).limit(limit)
    )
    rows: List[DecisionLog] = list(rows_result.scalars().all())

    items = [
        DecisionLogOut(
            id               = str(row.id),
            symbol           = row.symbol,
            decided_at       = row.decided_at,
            direction        = row.direction,
            final_confidence = row.final_confidence,
            tech_direction   = row.tech_direction,
            tech_confidence  = row.tech_confidence,
            sentiment_label  = row.sentiment_label,
            sentiment_score  = row.sentiment_score,
            sentiment_impact = row.sentiment_impact,
            executed         = row.executed,
            rejection_reason = row.rejection_reason,
            override_reason  = row.override_reason,
            risk_stop_loss   = row.risk_stop_loss,
            risk_take_profit = row.risk_take_profit,
            risk_rr_ratio    = row.risk_rr_ratio,
            reasons          = row.reasons,
        )
        for row in rows
    ]

    return DecisionsResponse(items=items, total=total, symbol=symbol)


# ---------------------------------------------------------------------------
# GET /signals/decisions/{symbol}  (convenience alias)
# ---------------------------------------------------------------------------

@router.get("/decisions/{symbol}", response_model=DecisionsResponse, summary="Decisions for one symbol")
async def get_decisions_for_symbol(
    symbol: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> DecisionsResponse:
    return await get_decisions(
        symbol=symbol.upper(),
        limit=limit,
        offset=offset,
        current_user=current_user,
        db=db,
    )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _safe(v: float) -> float:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return 0.0
    return v
