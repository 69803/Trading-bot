"""Add performance_snapshots table.

Stores periodic point-in-time performance captures produced by the bot
service (at most once per hour per portfolio).  Rows accumulate over time
so the frontend can render performance trend charts without recomputing
metrics from raw positions on every request.

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-03-22 19:00:00.000000
"""
import sqlalchemy as sa
from alembic import op

revision = "c4d5e6f7a8b9"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "performance_snapshots",
        sa.Column("id",              sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("portfolio_id",    sa.dialects.postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False),
        sa.Column("captured_at",     sa.DateTime(timezone=True), nullable=False),

        # trade counts
        sa.Column("total_trades",      sa.Integer(), nullable=False, server_default="0"),
        sa.Column("open_positions",    sa.Integer(), nullable=False, server_default="0"),
        sa.Column("winning_trades",    sa.Integer(), nullable=False, server_default="0"),
        sa.Column("losing_trades",     sa.Integer(), nullable=False, server_default="0"),

        # PnL
        sa.Column("total_pnl",        sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("daily_pnl",        sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("avg_win",          sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("avg_loss",         sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("best_trade_pnl",   sa.Numeric(18, 4), nullable=False, server_default="0"),
        sa.Column("worst_trade_pnl",  sa.Numeric(18, 4), nullable=False, server_default="0"),

        # ratios
        sa.Column("win_rate",         sa.Float(), nullable=False, server_default="0"),
        sa.Column("profit_factor",    sa.Float(), nullable=False, server_default="0"),

        # streaks
        sa.Column("consecutive_wins",   sa.Integer(), nullable=False, server_default="0"),
        sa.Column("consecutive_losses", sa.Integer(), nullable=False, server_default="0"),

        # drawdown / activity
        sa.Column("max_drawdown_pct",  sa.Float(), nullable=False, server_default="0"),
        sa.Column("trades_per_day",    sa.Float(), nullable=False, server_default="0"),

        # base timestamps
        sa.Column("created_at",  sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at",  sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_perf_snap_portfolio_id",       "performance_snapshots", ["portfolio_id"])
    op.create_index("ix_perf_snap_captured_at",        "performance_snapshots", ["captured_at"])
    op.create_index("ix_perf_snap_portfolio_captured", "performance_snapshots", ["portfolio_id", "captured_at"])


def downgrade() -> None:
    op.drop_index("ix_perf_snap_portfolio_captured", table_name="performance_snapshots")
    op.drop_index("ix_perf_snap_captured_at",        table_name="performance_snapshots")
    op.drop_index("ix_perf_snap_portfolio_id",       table_name="performance_snapshots")
    op.drop_table("performance_snapshots")
