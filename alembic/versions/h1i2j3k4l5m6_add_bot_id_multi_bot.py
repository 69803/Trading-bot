"""Add bot_id for multi-bot architecture.

Adds bot_id column to bot_states, strategy_configs, risk_settings,
positions, orders, trades, and bot_logs.

Drops old user_id UNIQUE constraints on the first three tables and
replaces them with composite (user_id, bot_id) UNIQUE constraints.

Uses IF EXISTS and dynamic PL/pgSQL to handle any constraint-name
variation that may exist in the production database.

Revision ID: h1i2j3k4l5m6
Revises: g1h2i3j4k5l6
Create Date: 2026-04-11
"""
from alembic import op
import sqlalchemy as sa

revision = "h1i2j3k4l5m6"
down_revision = "g1h2i3j4k5l6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. bot_states ────────────────────────────────────────────────────────
    op.add_column("bot_states", sa.Column("bot_id", sa.String(50), nullable=True))
    op.execute("UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.alter_column("bot_states", "bot_id", nullable=False)

    # Drop any unique constraint/index on user_id alone (name may vary).
    # bot_states was created with BOTH a UniqueConstraint AND a unique index.
    op.execute("""
        DO $$ DECLARE r RECORD; BEGIN
            FOR r IN
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'bot_states'
                  AND indexdef LIKE '%UNIQUE%'
                  AND indexdef LIKE '%user_id%'
                  AND indexname NOT LIKE '%uq_bot_states_user_bot%'
            LOOP
                EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(r.indexname);
            END LOOP;
            FOR r IN
                SELECT conname FROM pg_constraint
                WHERE conrelid = 'bot_states'::regclass
                  AND contype = 'u'
                  AND conname NOT LIKE '%uq_bot_states_user_bot%'
            LOOP
                EXECUTE 'ALTER TABLE bot_states DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
            END LOOP;
        END $$;
    """)
    op.create_unique_constraint(
        "uq_bot_states_user_bot", "bot_states", ["user_id", "bot_id"]
    )

    # ── 2. strategy_configs ───────────────────────────────────────────────────
    op.add_column("strategy_configs", sa.Column("bot_id", sa.String(50), nullable=True))
    op.execute("UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.alter_column("strategy_configs", "bot_id", nullable=False)

    op.execute("""
        DO $$ DECLARE r RECORD; BEGIN
            FOR r IN
                SELECT conname FROM pg_constraint
                WHERE conrelid = 'strategy_configs'::regclass
                  AND contype = 'u'
                  AND conname NOT LIKE '%uq_strategy_configs_user_bot%'
            LOOP
                EXECUTE 'ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
            END LOOP;
        END $$;
    """)
    op.create_unique_constraint(
        "uq_strategy_configs_user_bot", "strategy_configs", ["user_id", "bot_id"]
    )

    # ── 3. risk_settings ──────────────────────────────────────────────────────
    op.add_column("risk_settings", sa.Column("bot_id", sa.String(50), nullable=True))
    op.execute("UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.alter_column("risk_settings", "bot_id", nullable=False)

    op.execute("""
        DO $$ DECLARE r RECORD; BEGIN
            FOR r IN
                SELECT conname FROM pg_constraint
                WHERE conrelid = 'risk_settings'::regclass
                  AND contype = 'u'
                  AND conname NOT LIKE '%uq_risk_settings_user_bot%'
            LOOP
                EXECUTE 'ALTER TABLE risk_settings DROP CONSTRAINT IF EXISTS ' || quote_ident(r.conname);
            END LOOP;
        END $$;
    """)
    op.create_unique_constraint(
        "uq_risk_settings_user_bot", "risk_settings", ["user_id", "bot_id"]
    )

    # ── 4. positions ──────────────────────────────────────────────────────────
    op.add_column("positions", sa.Column("bot_id", sa.String(50), nullable=True))
    op.create_index("ix_positions_bot_id", "positions", ["bot_id"])

    # ── 5. orders ─────────────────────────────────────────────────────────────
    op.add_column("orders", sa.Column("bot_id", sa.String(50), nullable=True))
    op.create_index("ix_orders_bot_id", "orders", ["bot_id"])

    # ── 6. trades ─────────────────────────────────────────────────────────────
    op.add_column("trades", sa.Column("bot_id", sa.String(20), nullable=True))
    op.create_index("ix_trades_bot_id", "trades", ["bot_id"])

    # ── 7. bot_logs ───────────────────────────────────────────────────────────
    op.add_column("bot_logs", sa.Column("bot_id", sa.String(50), nullable=True))
    op.create_index("ix_bot_logs_bot_id", "bot_logs", ["bot_id"])


def downgrade() -> None:
    op.drop_index("ix_bot_logs_bot_id", table_name="bot_logs")
    op.drop_column("bot_logs", "bot_id")

    op.drop_index("ix_trades_bot_id", table_name="trades")
    op.drop_column("trades", "bot_id")

    op.drop_index("ix_orders_bot_id", table_name="orders")
    op.drop_column("orders", "bot_id")

    op.drop_index("ix_positions_bot_id", table_name="positions")
    op.drop_column("positions", "bot_id")

    op.drop_constraint("uq_risk_settings_user_bot", "risk_settings", type_="unique")
    op.execute("ALTER TABLE risk_settings ADD CONSTRAINT risk_settings_user_id_key UNIQUE (user_id)")
    op.drop_column("risk_settings", "bot_id")

    op.drop_constraint("uq_strategy_configs_user_bot", "strategy_configs", type_="unique")
    op.execute("ALTER TABLE strategy_configs ADD CONSTRAINT strategy_configs_user_id_key UNIQUE (user_id)")
    op.drop_column("strategy_configs", "bot_id")

    op.drop_constraint("uq_bot_states_user_bot", "bot_states", type_="unique")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_bot_states_user_id ON bot_states (user_id)")
    op.drop_column("bot_states", "bot_id")
