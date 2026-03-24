"""Add daily_performance_summaries table.

Revision ID: e2f3a4b5c6d7
Revises: d2e3f4a5b6c7
Create Date: 2026-03-23 00:00:00.000000
"""
import uuid
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "e2f3a4b5c6d7"
down_revision = "d2e3f4a5b6c7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "daily_performance_summaries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  onupdate=sa.func.now()),
        sa.Column("portfolio_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("portfolios.id", ondelete="CASCADE"), nullable=False),
        sa.Column("date_utc", sa.Date, nullable=False),
        sa.Column("total_trades",   sa.Integer, default=0),
        sa.Column("winning_trades", sa.Integer, default=0),
        sa.Column("losing_trades",  sa.Integer, default=0),
        sa.Column("win_rate",       sa.Float,   default=0.0),
        sa.Column("total_pnl",      sa.Numeric(18, 4), default=0),
        sa.Column("avg_pnl",        sa.Numeric(18, 4), default=0),
        sa.Column("best_symbol",    sa.String(20), nullable=True),
        sa.Column("worst_symbol",   sa.String(20), nullable=True),
        sa.Column("best_hour",      sa.Integer, nullable=True),
        sa.Column("worst_hour",     sa.Integer, nullable=True),
    )
    op.create_index(
        "ix_daily_perf_portfolio_date",
        "daily_performance_summaries",
        ["portfolio_id", "date_utc"],
    )
    op.create_unique_constraint(
        "uq_daily_perf_portfolio_date",
        "daily_performance_summaries",
        ["portfolio_id", "date_utc"],
    )


def downgrade() -> None:
    op.drop_table("daily_performance_summaries")
