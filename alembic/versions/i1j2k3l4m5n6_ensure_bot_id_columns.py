"""Ensure bot_id columns exist (safety-net migration).

This migration is a no-op if h1i2j3k4l5m6 ran successfully.
It exists to recover the case where alembic_version was recorded as
h1i2j3k4l5m6 but the schema changes did not actually land (e.g. due to
an aborted transaction whose version row somehow survived).

All statements use IF NOT EXISTS / IF EXISTS so running this on an
already-correct schema is completely safe.

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
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE bot_states ALTER COLUMN bot_id SET NOT NULL;
        EXCEPTION WHEN others THEN NULL; END $$
    """)
    op.execute("""
        DO $$ DECLARE r RECORD; BEGIN
            FOR r IN
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'bot_states'
                  AND indexdef ILIKE '%unique%'
                  AND indexdef ILIKE '%user_id%'
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
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'bot_states'::regclass
                  AND conname = 'uq_bot_states_user_bot'
            ) THEN
                ALTER TABLE bot_states
                    ADD CONSTRAINT uq_bot_states_user_bot UNIQUE (user_id, bot_id);
            END IF;
        END $$
    """)

    # ── strategy_configs ──────────────────────────────────────────────────────
    op.execute("ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE strategy_configs ALTER COLUMN bot_id SET NOT NULL;
        EXCEPTION WHEN others THEN NULL; END $$
    """)
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
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'strategy_configs'::regclass
                  AND conname = 'uq_strategy_configs_user_bot'
            ) THEN
                ALTER TABLE strategy_configs
                    ADD CONSTRAINT uq_strategy_configs_user_bot UNIQUE (user_id, bot_id);
            END IF;
        END $$
    """)

    # ── risk_settings ─────────────────────────────────────────────────────────
    op.execute("ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)")
    op.execute("UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL")
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE risk_settings ALTER COLUMN bot_id SET NOT NULL;
        EXCEPTION WHEN others THEN NULL; END $$
    """)
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
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'risk_settings'::regclass
                  AND conname = 'uq_risk_settings_user_bot'
            ) THEN
                ALTER TABLE risk_settings
                    ADD CONSTRAINT uq_risk_settings_user_bot UNIQUE (user_id, bot_id);
            END IF;
        END $$
    """)

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
    # This safety-net migration is intentionally a no-op on downgrade;
    # h1i2j3k4l5m6's downgrade handles the actual schema removal.
    pass
