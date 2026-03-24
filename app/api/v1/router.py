"""Aggregate API v1 router — mounts all sub-routers."""

from fastapi import APIRouter

from app.api.v1 import analytics, auth, backtest, bot, market, orders, portfolio, risk, signals, strategy, trades

api_router = APIRouter()

api_router.include_router(auth.router,      prefix="/auth",      tags=["auth"])
api_router.include_router(portfolio.router, prefix="/portfolio", tags=["portfolio"])
api_router.include_router(orders.router,    prefix="/orders",    tags=["orders"])
api_router.include_router(trades.router,    prefix="/trades",    tags=["trades"])
api_router.include_router(market.router,    prefix="/market",    tags=["market"])
api_router.include_router(strategy.router,  prefix="/strategy",  tags=["strategy"])
api_router.include_router(risk.router,      prefix="/risk",      tags=["risk"])
api_router.include_router(backtest.router,  prefix="/backtest",  tags=["backtest"])
api_router.include_router(bot.router,       prefix="/bot",       tags=["bot"])
api_router.include_router(signals.router,   prefix="/signals",   tags=["signals"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
