"""
Smart migration runner.

After Alembic upgrade finishes, _ensure_bot_id_columns() runs unconditionally
using one auto-commit transaction per statement.  This guarantees the bot_id
columns exist even if the Alembic migration was recorded as applied without
the DDL actually landing (a known failure mode with PgBouncer transaction mode
+ psycopg3 single-transaction DDL batches).
"""
import os
import sys

from alembic.config import Config
from alembic import command
from sqlalchemy import create_engine, inspect, text


def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("migrate.py: No DATABASE_URL — skipping migrations")
        return

    # psycopg3 sync driver; prepare_threshold=0 required for PgBouncer
    # transaction-mode pooler (Supabase port 6543).
    sync_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    sync_url = sync_url.replace("postgresql+psycopg_async://", "postgresql+psycopg://")

    engine = create_engine(sync_url, connect_args={"prepare_threshold": 0})
    cfg = Config("alembic.ini")

    try:
        with engine.connect() as conn:
            inspector = inspect(conn)
            has_alembic_version = inspector.has_table("alembic_version")
            has_market_candles = inspector.has_table("market_candles")
    finally:
        engine.dispose()

    if not has_alembic_version and has_market_candles:
        print("migrate.py: existing schema without alembic_version — stamping to head")
        command.stamp(cfg, "head")
    elif not has_alembic_version:
        print("migrate.py: fresh database — running all migrations")
    else:
        print("migrate.py: alembic_version found — applying pending migrations")

    command.upgrade(cfg, "head")

    # Safety net: apply bot_id columns with one transaction per statement so
    # a single failure never aborts the rest.
    _ensure_bot_id_columns(sync_url)

    # Safety net: create custom_bots table if Alembic migration didn't land.
    _ensure_custom_bots_table(sync_url)

    print("migrate.py: done")


# ---------------------------------------------------------------------------
# Each statement gets its own engine.begin() — independent auto-commit.
# A PgBouncer transaction-mode rollback in one statement never poisons the
# others.
# ---------------------------------------------------------------------------
_BOT_ID_STATEMENTS = [
    # ── bot_states ────────────────────────────────────────────────────────
    "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
    "DROP INDEX IF EXISTS ix_bot_states_user_id",
    "ALTER TABLE bot_states DROP CONSTRAINT IF EXISTS bot_states_user_id_key",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_bot_states_user_bot ON bot_states (user_id, bot_id)",
    # ── strategy_configs ──────────────────────────────────────────────────
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
    "ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS strategy_configs_user_id_key",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_configs_user_bot ON strategy_configs (user_id, bot_id)",
    # ── risk_settings ─────────────────────────────────────────────────────
    "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
    "ALTER TABLE risk_settings DROP CONSTRAINT IF EXISTS risk_settings_user_id_key",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_settings_user_bot ON risk_settings (user_id, bot_id)",
    # ── other tables ──────────────────────────────────────────────────────
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "CREATE INDEX IF NOT EXISTS ix_positions_bot_id ON positions (bot_id)",
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "CREATE INDEX IF NOT EXISTS ix_orders_bot_id ON orders (bot_id)",
    "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bot_id VARCHAR(20)",
    "CREATE INDEX IF NOT EXISTS ix_trades_bot_id ON trades (bot_id)",
    "ALTER TABLE bot_logs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "CREATE INDEX IF NOT EXISTS ix_bot_logs_bot_id ON bot_logs (bot_id)",
]


def _ensure_bot_id_columns(sync_url: str) -> None:
    """Run each DDL statement in its own transaction (engine.begin auto-commits).

    One-transaction-per-statement means a PgBouncer rollback in one step
    never aborts the remaining steps.
    """
    engine = create_engine(sync_url, connect_args={"prepare_threshold": 0})
    errors = []
    try:
        for stmt in _BOT_ID_STATEMENTS:
            try:
                with engine.begin() as conn:
                    conn.execute(text(stmt))
                print(f"migrate.py: OK  {stmt[:80]}")
            except Exception as exc:
                print(f"migrate.py: WARN {stmt[:80]} — {exc}")
                errors.append((stmt, exc))

        # Verify the three critical columns actually exist.
        critical = [
            ("bot_states", "bot_id"),
            ("strategy_configs", "bot_id"),
            ("risk_settings", "bot_id"),
        ]
        with engine.connect() as conn:
            for table, col in critical:
                row = conn.execute(text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = :t AND column_name = :c"
                ), {"t": table, "c": col}).fetchone()
                if row:
                    print(f"migrate.py: VERIFIED {table}.{col} EXISTS")
                else:
                    print(f"migrate.py: FATAL {table}.{col} MISSING after ensure — aborting")
                    sys.exit(1)

    finally:
        engine.dispose()

    if errors:
        print(f"migrate.py: {len(errors)} non-fatal warning(s) during _ensure_bot_id_columns")


_CUSTOM_BOTS_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS custom_bots (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        name        VARCHAR(100) NOT NULL,
        bot_id      VARCHAR(80) NOT NULL,
        description TEXT,
        color       VARCHAR(20) NOT NULL DEFAULT '#6366f1',
        config      JSONB NOT NULL DEFAULT '{}',
        is_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT uq_custom_bots_user_name UNIQUE (user_id, name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_custom_bots_user_id ON custom_bots (user_id)",
    "CREATE INDEX IF NOT EXISTS ix_custom_bots_bot_id  ON custom_bots (bot_id)",
]


def _ensure_custom_bots_table(sync_url: str) -> None:
    engine = create_engine(sync_url, connect_args={"prepare_threshold": 0})
    try:
        for stmt in _CUSTOM_BOTS_STATEMENTS:
            try:
                with engine.begin() as conn:
                    conn.execute(text(stmt))
                print(f"migrate.py: OK  {stmt.strip()[:80]}")
            except Exception as exc:
                print(f"migrate.py: WARN custom_bots — {exc}")

        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'custom_bots'"
            )).fetchone()
            if row:
                print("migrate.py: VERIFIED custom_bots EXISTS")
            else:
                print("migrate.py: FATAL custom_bots MISSING — aborting")
                sys.exit(1)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
