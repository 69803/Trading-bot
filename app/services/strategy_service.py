"""
EMA50/EMA200/RSI14 strategy signal engine.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.strategy_config import StrategyConfig
from app.models.strategy_signal import StrategySignal
from app.services.market_data_service import market_data_service
from app.utils.indicators import calculate_ema, calculate_rsi, calculate_signals


async def get_strategy_config(db: AsyncSession, user_id: UUID) -> StrategyConfig:
    """Load user's strategy config, creating defaults if not present."""
    result = await db.execute(
        select(StrategyConfig).where(StrategyConfig.user_id == user_id)
    )
    config: StrategyConfig | None = result.scalars().first()
    if config is None:
        config = StrategyConfig(
            id=uuid4(),
            user_id=user_id,
            ema_fast=50,
            ema_slow=200,
            rsi_period=14,
            rsi_overbought=Decimal("70"),
            rsi_oversold=Decimal("30"),
            auto_trade=False,
            symbols=["EURUSD", "BTCUSD"],
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db.add(config)
        await db.commit()
        await db.refresh(config)
    return config


async def update_strategy_config(
    db: AsyncSession, user_id: UUID, data: dict
) -> StrategyConfig:
    """Update strategy configuration."""
    config = await get_strategy_config(db, user_id)
    allowed_fields = {
        "ema_fast", "ema_slow", "rsi_period",
        "rsi_overbought", "rsi_oversold", "auto_trade", "symbols",
    }
    for field, value in data.items():
        if field in allowed_fields and value is not None:
            if field in ("rsi_overbought", "rsi_oversold"):
                value = Decimal(str(value))
            setattr(config, field, value)
    config.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(config)
    return config


async def evaluate_signals(db: AsyncSession, user_id: UUID) -> list[dict]:
    """
    For each symbol in user's strategy config:
    1. Get candles from market_data_service
    2. Calculate EMA fast, EMA slow, RSI
    3. Determine signal (buy/sell/hold)
    4. Save StrategySignal to DB
    5. If auto_trade=True and signal != hold, place order via order_service
    Returns list of signal dicts.
    """
    config = await get_strategy_config(db, user_id)
    symbols: list[str] = config.symbols or []
    signal_dicts: list[dict] = []

    for symbol in symbols:
        try:
            candles = await market_data_service.get_candles(symbol, "1h", limit=250)
        except Exception:
            continue

        if len(candles) < config.ema_slow + 10:
            continue

        closes = [float(c["close"]) for c in candles]

        ema_fast_series = calculate_ema(closes, int(config.ema_fast))
        ema_slow_series = calculate_ema(closes, int(config.ema_slow))
        rsi_series = calculate_rsi(closes, int(config.rsi_period))

        if not ema_fast_series or not ema_slow_series or not rsi_series:
            continue

        ema_fast_val = ema_fast_series[-1]
        ema_slow_val = ema_slow_series[-1]
        rsi_val = rsi_series[-1]
        current_price = closes[-1]

        if any(math.isnan(v) for v in (ema_fast_val, ema_slow_val, rsi_val)):
            continue

        # Determine signal
        signal_type = "hold"
        confidence = 0.5

        bullish_cross = ema_fast_val > ema_slow_val
        prev_fast = ema_fast_series[-2] if len(ema_fast_series) >= 2 else ema_fast_val
        prev_slow = ema_slow_series[-2] if len(ema_slow_series) >= 2 else ema_slow_val
        was_below = prev_fast <= prev_slow

        bearish_cross = ema_fast_val < ema_slow_val
        was_above = prev_fast >= prev_slow

        rsi_not_overbought = rsi_val < float(config.rsi_overbought)
        rsi_not_oversold = rsi_val > float(config.rsi_oversold)
        rsi_oversold = rsi_val <= float(config.rsi_oversold)
        rsi_overbought = rsi_val >= float(config.rsi_overbought)

        if (bullish_cross and was_below) or (bullish_cross and rsi_oversold):
            signal_type = "buy"
            confidence = min(0.95, 0.6 + (float(config.rsi_oversold) - rsi_val) / 100 + 0.1)
        elif (bearish_cross and was_above) or (bearish_cross and rsi_overbought):
            signal_type = "sell"
            confidence = min(0.95, 0.6 + (rsi_val - float(config.rsi_overbought)) / 100 + 0.1)
        else:
            confidence = 0.5

        # Persist signal
        db_signal = StrategySignal(
            id=uuid4(),
            user_id=user_id,
            symbol=symbol,
            signal_type=signal_type,
            ema_fast_value=Decimal(str(round(ema_fast_val, 6))),
            ema_slow_value=Decimal(str(round(ema_slow_val, 6))),
            rsi_value=Decimal(str(round(rsi_val, 4))),
            price_at_signal=Decimal(str(round(current_price, 6))),
            confidence=Decimal(str(round(confidence, 4))),
            acted_on=False,
            triggered_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )
        db.add(db_signal)
        await db.flush()

        # Auto-trade
        if config.auto_trade and signal_type != "hold":
            from app.models.portfolio import Portfolio
            from app.services.order_service import create_order

            port_result = await db.execute(
                select(Portfolio).where(Portfolio.user_id == user_id)
            )
            portfolio = port_result.scalars().first()
            if portfolio:
                # Size: 5% of equity per trade
                equity = float(portfolio.cash_balance)
                try:
                    price = await market_data_service.get_current_price(symbol)
                    qty = round((equity * 0.05) / price, 4)
                    if qty > 0:
                        order_side = "buy" if signal_type == "buy" else "sell"
                        await create_order(
                            db=db,
                            portfolio_id=portfolio.id,
                            user_id=user_id,
                            symbol=symbol,
                            side=order_side,
                            order_type="market",
                            quantity=qty,
                        )
                        db_signal.acted_on = True
                except Exception:
                    pass

        signal_dicts.append(
            {
                "id": str(db_signal.id),
                "symbol": symbol,
                "signal_type": signal_type,
                "ema_fast_value": float(ema_fast_val),
                "ema_slow_value": float(ema_slow_val),
                "rsi_value": float(rsi_val),
                "price_at_signal": current_price,
                "confidence": confidence,
                "acted_on": db_signal.acted_on,
                "triggered_at": db_signal.triggered_at.isoformat(),
            }
        )

    await db.commit()
    return signal_dicts


async def get_signals(
    db: AsyncSession,
    user_id: UUID,
    symbol: str | None = None,
    limit: int = 50,
) -> Tuple[List[StrategySignal], int]:
    """List recent signals for a user, optionally filtered by symbol."""
    base_q = select(StrategySignal).where(StrategySignal.user_id == user_id)
    count_q = select(func.count(StrategySignal.id)).where(
        StrategySignal.user_id == user_id
    )
    if symbol:
        base_q = base_q.where(StrategySignal.symbol == symbol)
        count_q = count_q.where(StrategySignal.symbol == symbol)

    count_result = await db.execute(count_q)
    total: int = count_result.scalar() or 0

    result = await db.execute(
        base_q.order_by(StrategySignal.triggered_at.desc()).limit(limit)
    )
    signals = list(result.scalars().all())
    return signals, total
