"""add prev_evaluated_price to positions

Revision ID: a3b4c5d6e7f8
Revises: f2a3b4c5d6e7
Create Date: 2026-03-23 00:00:00.000000

Adds ``prev_evaluated_price`` (Numeric, nullable) to the ``positions`` table.

This column stores the live price that was observed at the end of the most
recent TP/SL evaluation cycle for this position.  It enables cross-detection:

  SHORT TP cross: prev_evaluated_price > take_profit AND live_price <= take_profit
  SHORT SL cross: prev_evaluated_price < stop_loss  AND live_price >= stop_loss
  LONG  TP cross: prev_evaluated_price < take_profit AND live_price >= take_profit
  LONG  SL cross: prev_evaluated_price > stop_loss  AND live_price <= stop_loss

NULL on newly-opened positions (cross detection activates from the second
evaluation cycle onward).  NULL is treated as "no previous price available"
and the snapshot + candle-wick detection continues to apply.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a3b4c5d6e7f8"
down_revision = "f2a3b4c5d6e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS prev_evaluated_price NUMERIC(18,8)")


def downgrade() -> None:
    op.drop_column("positions", "prev_evaluated_price")
