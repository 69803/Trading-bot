"""Add bot_id for multi-bot architecture.

Adds bot_id column to bot_states, strategy_configs, risk_settings,
positions, orders, trades, and bot_logs.

Drops old user_id UNIQUE constraints on the first three tables and
replaces them with composite (user_id, bot_id) UNIQUE constraints.

Revision ID: h1i2j3k4l5m6
Revises: g1h2i3j4k5l6
Create Date: 2026-04-11
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "h1i2j3k4l5m6"
down_revision = "g1h2i3j4k5l6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. Add bot_id to bot_states ──────────────────────────────────────────
    op.add_column("bot_states", sa.Column("bot_id", sa.String(50), nullable=True))
    op.execute("UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.alter_column("bot_states", "bot_id", nullable=False)

    # Drop old unique constraint on user_id and add composite unique
    op.drop_constraint("bot_states_user_id_key", "bot_states", type_="unique")
    op.create_unique_constraint("uq_bot_states_user_bot", "bot_states", ["user_id", "bot_id"])

    # ── 2. Add bot_id to strategy_configs ────────────────────────────────────
    op.add_column("strategy_configs", sa.Column("bot_id", sa.String(50), nullable=True))
    op.execute("UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.alter_column("strategy_configs", "bot_id", nullable=False)

    op.drop_constraint("strategy_configs_user_id_key", "strategy_configs", type_="unique")
    op.create_unique_constraint("uq_strategy_configs_user_bot", "strategy_configs", ["user_id", "bot_id"])

    # ── 3. Add bot_id to risk_settings ────────────────────────────────────────
    op.add_column("risk_settings", sa.Column("bot_id", sa.String(50), nullable=True))
    op.execute("UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.alter_column("risk_settings", "bot_id", nullable=False)

    op.drop_constraint("risk_settings_user_id_key", "risk_settings", type_="unique")
    op.create_unique_constraint("uq_risk_settings_user_bot", "risk_settings", ["user_id", "bot_id"])

    # ── 4. Add bot_id to positions ────────────────────────────────────────────
    op.add_column("positions", sa.Column("bot_id", sa.String(50), nullable=True))
    op.create_index("ix_positions_bot_id", "positions", ["bot_id"])

    # ── 5. Add bot_id to orders ───────────────────────────────────────────────
    op.add_column("orders", sa.Column("bot_id", sa.String(50), nullable=True))
    op.create_index("ix_orders_bot_id", "orders", ["bot_id"])

    # ── 6. Add bot_id to trades ───────────────────────────────────────────────
    op.add_column("trades", sa.Column("bot_id", sa.String(20), nullable=True))
    op.create_index("ix_trades_bot_id", "trades", ["bot_id"])

    # ── 7. Add bot_id to bot_logs ─────────────────────────────────────────────
    op.add_column("bot_logs", sa.Column("bot_id", sa.String(50), nullable=True))
    op.create_index("ix_bot_logs_bot_id", "bot_logs", ["bot_id"])


def downgrade() -> None:
    # bot_logs
    op.drop_index("ix_bot_logs_bot_id", table_name="bot_logs")
    op.drop_column("bot_logs", "bot_id")

    # trades
    op.drop_index("ix_trades_bot_id", table_name="trades")
    op.drop_column("trades", "bot_id")

    # orders
    op.drop_index("ix_orders_bot_id", table_name="orders")
    op.drop_column("orders", "bot_id")

    # positions
    op.drop_index("ix_positions_bot_id", table_name="positions")
    op.drop_column("positions", "bot_id")

    # risk_settings
    op.drop_constraint("uq_risk_settings_user_bot", "risk_settings", type_="unique")
    op.create_unique_constraint("risk_settings_user_id_key", "risk_settings", ["user_id"])
    op.drop_column("risk_settings", "bot_id")

    # strategy_configs
    op.drop_constraint("uq_strategy_configs_user_bot", "strategy_configs", type_="unique")
    op.create_unique_constraint("strategy_configs_user_id_key", "strategy_configs", ["user_id"])
    op.drop_column("strategy_configs", "bot_id")

    # bot_states
    op.drop_constraint("uq_bot_states_user_bot", "bot_states", type_="unique")
    op.create_unique_constraint("bot_states_user_id_key", "bot_states", ["user_id"])
    op.drop_column("bot_states", "bot_id")
