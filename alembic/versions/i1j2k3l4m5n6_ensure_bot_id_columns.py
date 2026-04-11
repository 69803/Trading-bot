"""Ensure bot_id columns exist (safety-net migration).

No-op if h1i2j3k4l5m6 ran successfully. Recovers the case where
alembic_version was recorded as h1i2j3k4l5m6 but the schema changes
did not land (e.g. rolled-back transaction).

No PL/pgSQL — plain SQL only, fully compatible with Supabase's
PgBouncer transaction-mode pooler (port 6543).

Revision ID: i1j2k3l4m5n6
Revises: h1i2j3k4l5m6
Create Date: 2026-04-11
"""
from alembic import op

revision = "i1j2k3l4m5n6"
down_revision = "h1i2j3k4l5m6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── bot_states ────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("ALTER TABLE bot_states ALTER COLUMN bot_id SET NOT NULL")
    op.execute("DROP INDEX IF EXISTS ix_bot_states_user_id")
    op.execute("ALTER TABLE bot_states DROP CONSTRAINT IF EXISTS bot_states_user_id_key")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_bot_states_user_bot"
        " ON bot_states (user_id, bot_id)"
    )

    # ── strategy_configs ──────────────────────────────────────────────────────
    op.execute("ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("ALTER TABLE strategy_configs ALTER COLUMN bot_id SET NOT NULL")
    op.execute("ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS strategy_configs_user_id_key")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_configs_user_bot"
        " ON strategy_configs (user_id, bot_id)"
    )

    # ── risk_settings ─────────────────────────────────────────────────────────
    op.execute("ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("ALTER TABLE risk_settings ALTER COLUMN bot_id SET NOT NULL")
    op.execute("ALTER TABLE risk_settings DROP CONSTRAINT IF EXISTS risk_settings_user_id_key")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_settings_user_bot"
        " ON risk_settings (user_id, bot_id)"
    )

    # ── positions ─────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE positions ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_positions_bot_id ON positions (bot_id)")

    # ── orders ────────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE orders ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_orders_bot_id ON orders (bot_id)")

    # ── trades ────────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS bot_id VARCHAR(20)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_trades_bot_id ON trades (bot_id)")

    # ── bot_logs ──────────────────────────────────────────────────────────────
    op.execute("ALTER TABLE bot_logs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_bot_logs_bot_id ON bot_logs (bot_id)")


def downgrade() -> None:
    # Safety-net is a no-op on downgrade; h1i2j3k4l5m6 handles removal.
    pass
