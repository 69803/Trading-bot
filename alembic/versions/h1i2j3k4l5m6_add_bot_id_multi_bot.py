"""Add bot_id for multi-bot architecture.

Adds bot_id column to bot_states, strategy_configs, risk_settings,
positions, orders, trades, and bot_logs.

Replaces the user_id-only unique constraints with composite
(user_id, bot_id) unique indexes.

No PL/pgSQL — plain SQL only, fully compatible with Supabase's
PgBouncer transaction-mode pooler (port 6543).

All statements use IF NOT EXISTS / IF EXISTS so the migration is safe
to re-run after a partial failure.

Revision ID: h1i2j3k4l5m6
Revises: g1h2i3j4k5l6
Create Date: 2026-04-11
"""
from alembic import op

revision = "h1i2j3k4l5m6"
down_revision = "g1h2i3j4k5l6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. bot_states ────────────────────────────────────────────────────────
    # Constraint names (created by migration a1b2c3d4e5f6):
    #   UniqueConstraint → bot_states_user_id_key
    #   create_index(unique=True) → ix_bot_states_user_id
    op.execute("ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("ALTER TABLE bot_states ALTER COLUMN bot_id SET NOT NULL")
    op.execute("DROP INDEX IF EXISTS ix_bot_states_user_id")
    op.execute("ALTER TABLE bot_states DROP CONSTRAINT IF EXISTS bot_states_user_id_key")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_bot_states_user_bot"
        " ON bot_states (user_id, bot_id)"
    )

    # ── 2. strategy_configs ───────────────────────────────────────────────────
    # Constraint name (created by migration 710bdfb02681):
    #   UniqueConstraint → strategy_configs_user_id_key
    op.execute("ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("ALTER TABLE strategy_configs ALTER COLUMN bot_id SET NOT NULL")
    op.execute("ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS strategy_configs_user_id_key")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_configs_user_bot"
        " ON strategy_configs (user_id, bot_id)"
    )

    # ── 3. risk_settings ──────────────────────────────────────────────────────
    # Constraint name (created by migration 710bdfb02681):
    #   UniqueConstraint → risk_settings_user_id_key
    op.execute("ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("ALTER TABLE risk_settings ALTER COLUMN bot_id SET NOT NULL")
    op.execute("ALTER TABLE risk_settings DROP CONSTRAINT IF EXISTS risk_settings_user_id_key")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_settings_user_bot"
        " ON risk_settings (user_id, bot_id)"
    )

    # ── 4. positions ──────────────────────────────────────────────────────────
    op.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_positions_bot_id ON positions (bot_id)")

    # ── 5. orders ─────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_orders_bot_id ON orders (bot_id)")

    # ── 6. trades ─────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS bot_id VARCHAR(20)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_trades_bot_id ON trades (bot_id)")

    # ── 7. bot_logs ───────────────────────────────────────────────────────────
    op.execute("ALTER TABLE bot_logs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_bot_logs_bot_id ON bot_logs (bot_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_bot_logs_bot_id")
    op.execute("ALTER TABLE bot_logs DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS ix_trades_bot_id")
    op.execute("ALTER TABLE trades DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS ix_orders_bot_id")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS ix_positions_bot_id")
    op.execute("ALTER TABLE positions DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS uq_risk_settings_user_bot")
    op.execute("ALTER TABLE risk_settings DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS uq_strategy_configs_user_bot")
    op.execute("ALTER TABLE strategy_configs DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS uq_bot_states_user_bot")
    op.execute("ALTER TABLE bot_states DROP COLUMN IF EXISTS bot_id")
