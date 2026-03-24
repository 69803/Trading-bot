"""Add market_prices and historical_events tables for 10-year historical data.

Revision ID: d1e2f3a4b5c6
Revises: c3d4e5f6a7b8
Create Date: 2026-03-22 00:00:00.000000
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str                          = "d1e2f3a4b5c6"
down_revision: Union[str, None]        = "c3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on:    Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── market_prices ─────────────────────────────────────────────────────────
    op.create_table(
        "market_prices",
        sa.Column("symbol",       sa.String(20),              nullable=False),
        sa.Column("datetime_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("interval",     sa.String(10),              nullable=False),
        sa.Column("open",         sa.Float(),                 nullable=False),
        sa.Column("high",         sa.Float(),                 nullable=False),
        sa.Column("low",          sa.Float(),                 nullable=False),
        sa.Column("close",        sa.Float(),                 nullable=False),
        sa.Column("volume",       sa.Float(),                 nullable=False),
        sa.Column("source",       sa.String(50),              nullable=False),
        sa.Column("id",           sa.UUID(),                  nullable=False),
        sa.Column("created_at",   sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at",   sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "symbol", "datetime_utc", "interval",
            name="uq_market_price_symbol_dt_interval",
        ),
    )
    op.create_index("ix_market_price_symbol",    "market_prices", ["symbol"],       unique=False)
    op.create_index("ix_market_price_symbol_dt", "market_prices", ["symbol", "datetime_utc"], unique=False)

    # ── historical_events ─────────────────────────────────────────────────────
    op.create_table(
        "historical_events",
        sa.Column("event_datetime_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("country",            sa.String(10),              nullable=True),
        sa.Column("currency",           sa.String(10),              nullable=False),
        sa.Column("event_name",         sa.String(255),             nullable=False),
        sa.Column("impact",             sa.String(20),              nullable=False),
        sa.Column("actual",             sa.String(50),              nullable=True),
        sa.Column("forecast",           sa.String(50),              nullable=True),
        sa.Column("previous",           sa.String(50),              nullable=True),
        sa.Column("source",             sa.String(50),              nullable=False),
        sa.Column("id",                 sa.UUID(),                  nullable=False),
        sa.Column("created_at",         sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at",         sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "event_datetime_utc", "currency", "event_name", "source",
            name="uq_historical_event",
        ),
    )
    op.create_index("ix_historical_event_currency",    "historical_events", ["currency"],    unique=False)
    op.create_index("ix_historical_event_dt",          "historical_events", ["event_datetime_utc"], unique=False)
    op.create_index("ix_historical_event_currency_dt", "historical_events", ["currency", "event_datetime_utc"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_historical_event_currency_dt", table_name="historical_events")
    op.drop_index("ix_historical_event_dt",          table_name="historical_events")
    op.drop_index("ix_historical_event_currency",    table_name="historical_events")
    op.drop_table("historical_events")

    op.drop_index("ix_market_price_symbol_dt", table_name="market_prices")
    op.drop_index("ix_market_price_symbol",    table_name="market_prices")
    op.drop_table("market_prices")
