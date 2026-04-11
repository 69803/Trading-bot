"""Add bot_id for multi-bot architecture.

Adds bot_id column to bot_states, strategy_configs, risk_settings,
positions, orders, trades, and bot_logs.

Drops old user_id UNIQUE constraints on the first three tables and
replaces them with composite (user_id, bot_id) UNIQUE constraints.

Fully idempotent: uses IF NOT EXISTS / IF EXISTS everywhere so the
migration can be re-run safely after a partial failure.

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
    op.execute("""
        ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50);
    """)
    op.execute("""
        UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL;
    """)
    op.execute("""
        ALTER TABLE bot_states ALTER COLUMN bot_id SET NOT NULL;
    """)
    # Drop ALL unique constraints/indexes on user_id alone, then add composite
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
        END $$;
    """)

    # ── 2. strategy_configs ───────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50);
    """)
    op.execute("""
        UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL;
    """)
    op.execute("""
        ALTER TABLE strategy_configs ALTER COLUMN bot_id SET NOT NULL;
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
        END $$;
    """)

    # ── 3. risk_settings ──────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50);
    """)
    op.execute("""
        UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL;
    """)
    op.execute("""
        ALTER TABLE risk_settings ALTER COLUMN bot_id SET NOT NULL;
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
        END $$;
    """)

    # ── 4. positions ──────────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE positions ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_positions_bot_id ON positions (bot_id);
    """)

    # ── 5. orders ─────────────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_orders_bot_id ON orders (bot_id);
    """)

    # ── 6. trades ─────────────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE trades ADD COLUMN IF NOT EXISTS bot_id VARCHAR(20);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_trades_bot_id ON trades (bot_id);
    """)

    # ── 7. bot_logs ───────────────────────────────────────────────────────────
    op.execute("""
        ALTER TABLE bot_logs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_bot_logs_bot_id ON bot_logs (bot_id);
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_bot_logs_bot_id")
    op.execute("ALTER TABLE bot_logs DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS ix_trades_bot_id")
    op.execute("ALTER TABLE trades DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS ix_orders_bot_id")
    op.execute("ALTER TABLE orders DROP COLUMN IF EXISTS bot_id")

    op.execute("DROP INDEX IF EXISTS ix_positions_bot_id")
    op.execute("ALTER TABLE positions DROP COLUMN IF EXISTS bot_id")

    op.execute("""
        DO $$ BEGIN
            ALTER TABLE risk_settings DROP CONSTRAINT IF EXISTS uq_risk_settings_user_bot;
        EXCEPTION WHEN undefined_object THEN NULL; END $$;
    """)
    op.execute("ALTER TABLE risk_settings DROP COLUMN IF EXISTS bot_id")

    op.execute("""
        DO $$ BEGIN
            ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS uq_strategy_configs_user_bot;
        EXCEPTION WHEN undefined_object THEN NULL; END $$;
    """)
    op.execute("ALTER TABLE strategy_configs DROP COLUMN IF EXISTS bot_id")

    op.execute("""
        DO $$ BEGIN
            ALTER TABLE bot_states DROP CONSTRAINT IF EXISTS uq_bot_states_user_bot;
        EXCEPTION WHEN undefined_object THEN NULL; END $$;
    """)
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_bot_states_user_id ON bot_states (user_id)")
    op.execute("ALTER TABLE bot_states DROP COLUMN IF EXISTS bot_id")
