"""Add advanced risk management columns.

risk_settings:
  - trailing_stop_pct          (Numeric 6,4)   0 = disabled
  - break_even_trigger_pct     (Numeric 6,4)   0 = disabled
  - max_consecutive_losses     (Integer)        0 = disabled
  - max_trades_per_hour        (Integer)        0 = disabled
  - volatility_sizing_enabled  (Boolean)        default False

positions:
  - high_water_mark            (Numeric 18,8)   best price seen since open
  - trailing_stop_price        (Numeric 18,8)   current trailing stop level
  - break_even_activated       (Boolean)        True once SL moved to entry

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-03-22 12:02:00.000000
"""
import sqlalchemy as sa
from alembic import op

revision      = "b3c4d5e6f7a8"
down_revision = "a2b3c4d5e6f7"
branch_labels = None
depends_on    = None


def upgrade() -> None:
    # ── risk_settings ─────────────────────────────────────────────────────────
    op.add_column(
        "risk_settings",
        sa.Column("trailing_stop_pct", sa.Numeric(6, 4), nullable=False,
                  server_default="0.0000"),
    )
    op.add_column(
        "risk_settings",
        sa.Column("break_even_trigger_pct", sa.Numeric(6, 4), nullable=False,
                  server_default="0.0000"),
    )
    op.add_column(
        "risk_settings",
        sa.Column("max_consecutive_losses", sa.Integer(), nullable=False,
                  server_default="0"),
    )
    op.add_column(
        "risk_settings",
        sa.Column("max_trades_per_hour", sa.Integer(), nullable=False,
                  server_default="0"),
    )
    op.add_column(
        "risk_settings",
        sa.Column("volatility_sizing_enabled", sa.Boolean(), nullable=False,
                  server_default="false"),
    )

    # ── positions ─────────────────────────────────────────────────────────────
    op.add_column(
        "positions",
        sa.Column("high_water_mark", sa.Numeric(18, 8), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("trailing_stop_price", sa.Numeric(18, 8), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("break_even_activated", sa.Boolean(), nullable=False,
                  server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("positions", "break_even_activated")
    op.drop_column("positions", "trailing_stop_price")
    op.drop_column("positions", "high_water_mark")

    op.drop_column("risk_settings", "volatility_sizing_enabled")
    op.drop_column("risk_settings", "max_trades_per_hour")
    op.drop_column("risk_settings", "max_consecutive_losses")
    op.drop_column("risk_settings", "break_even_trigger_pct")
    op.drop_column("risk_settings", "trailing_stop_pct")
