"""Add bot config fields to strategy_configs and last_error to bot_states.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-03-20 00:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "strategy_configs",
        sa.Column("asset_classes", sa.JSON(), nullable=False, server_default='["stocks"]'),
    )
    op.add_column(
        "strategy_configs",
        sa.Column(
            "investment_amount",
            sa.Numeric(precision=18, scale=8),
            nullable=False,
            server_default="100",
        ),
    )
    op.add_column(
        "strategy_configs",
        sa.Column("run_interval_seconds", sa.Integer(), nullable=False, server_default="60"),
    )
    op.add_column(
        "strategy_configs",
        sa.Column("per_symbol_max_positions", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "strategy_configs",
        sa.Column("allow_buy", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "strategy_configs",
        sa.Column("allow_sell", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "strategy_configs",
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("bot_states", sa.Column("last_error", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("bot_states", "last_error")
    op.drop_column("strategy_configs", "cooldown_seconds")
    op.drop_column("strategy_configs", "allow_sell")
    op.drop_column("strategy_configs", "allow_buy")
    op.drop_column("strategy_configs", "per_symbol_max_positions")
    op.drop_column("strategy_configs", "run_interval_seconds")
    op.drop_column("strategy_configs", "investment_amount")
    op.drop_column("strategy_configs", "asset_classes")
