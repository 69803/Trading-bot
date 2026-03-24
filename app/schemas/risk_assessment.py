"""Pydantic schema for the risk manager output."""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


class RiskAssessment(BaseModel):
    """
    Output of the risk manager (Phase 5).

    Indicates whether a proposed trade is approved after checking all
    pre-trade risk rules, and provides the exact sizing and price levels
    the execution layer should use.

    Fields
    ──────
    approved            — False means the trade must be skipped.
    rejection_reason    — Human-readable reason when approved=False.
    position_size_dollars — Dollar amount to invest (capped by max_position_size_pct).
    stop_loss_price     — Absolute SL price level (ATR-based or pct-based).
    take_profit_price   — Absolute TP price level (ATR-based or pct-based).
    risk_per_trade_dollars — Max dollar loss if SL is hit (for audit).
    risk_reward_ratio   — TP distance / SL distance.
    sizing_method       — How SL/TP levels were calculated.
    assessed_at         — Timestamp of this assessment.
    """

    approved: bool
    rejection_reason: Optional[str] = None

    # Sizing (only meaningful when approved=True)
    position_size_dollars: float = Field(
        default=0.0,
        description="Dollar amount to invest in this trade",
    )
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None

    # Audit info
    entry_price: float = 0.0
    atr_value: float = 0.0
    risk_per_trade_dollars: float = 0.0
    risk_reward_ratio: float = 0.0
    sizing_method: Literal["atr_based", "pct_based", "fixed"] = "fixed"

    assessed_at: datetime

    @property
    def is_approved(self) -> bool:
        return self.approved
