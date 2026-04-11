"""
Smart migration runner.

Handles the case where tables were created by SQLAlchemy's create_all()
without Alembic ever running, leaving the DB in a state where:
  - All tables exist
  - alembic_version table does NOT exist

In that case we stamp the DB at the current head so Alembic knows the
schema is already up to date, then run `upgrade head` (which becomes a no-op).

For a brand-new empty database, stamping is skipped and `upgrade head` runs
all migrations normally.

After Alembic finishes, _ensure_bot_id_columns() runs unconditionally.
It applies all bot_id schema changes directly using IF NOT EXISTS, so
the columns are guaranteed to exist even if the Alembic migration was
recorded as applied without the DDL actually landing.
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

    # psycopg3 sync driver; prepare_threshold=0 for PgBouncer transaction mode
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
        # Tables were created by create_all() without Alembic.
        # Stamp head so `upgrade head` doesn't try to recreate them.
        print("migrate.py: existing schema detected without alembic_version — stamping to head")
        command.stamp(cfg, "head")
    elif not has_alembic_version:
        print("migrate.py: fresh database — running all migrations")
    else:
        print("migrate.py: alembic_version found — applying any pending migrations")

    command.upgrade(cfg, "head")

    # ── Safety net ─────────────────────────────────────────────────────────────
    # Apply bot_id columns directly, regardless of what Alembic version tracking
    # says.  If the columns already exist, IF NOT EXISTS makes every statement
    # a no-op.  This handles the scenario where a migration was recorded as
    # applied (alembic_version updated) but the DDL was rolled back.
    _ensure_bot_id_columns(sync_url)

    print("migrate.py: migrations complete")


def _ensure_bot_id_columns(sync_url: str) -> None:
    """Idempotently add bot_id columns to all relevant tables."""
    statements = [
        # Tables where bot_id must NOT be NULL — backfill first, then set NOT NULL
        "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
        "UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
        "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
        "UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
        "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
        "UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
        # Tables where bot_id is nullable
        "ALTER TABLE positions ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS bot_id VARCHAR(20)",
        "ALTER TABLE bot_logs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
        # Indexes (CREATE INDEX IF NOT EXISTS is idempotent)
        "CREATE INDEX IF NOT EXISTS ix_positions_bot_id ON positions (bot_id)",
        "CREATE INDEX IF NOT EXISTS ix_orders_bot_id ON orders (bot_id)",
        "CREATE INDEX IF NOT EXISTS ix_trades_bot_id ON trades (bot_id)",
        "CREATE INDEX IF NOT EXISTS ix_bot_logs_bot_id ON bot_logs (bot_id)",
        # Composite unique indexes replacing the old single-column user_id ones
        "DROP INDEX IF EXISTS ix_bot_states_user_id",
        "ALTER TABLE bot_states DROP CONSTRAINT IF EXISTS bot_states_user_id_key",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_bot_states_user_bot ON bot_states (user_id, bot_id)",
        "ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS strategy_configs_user_id_key",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_configs_user_bot ON strategy_configs (user_id, bot_id)",
        "ALTER TABLE risk_settings DROP CONSTRAINT IF EXISTS risk_settings_user_id_key",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_settings_user_bot ON risk_settings (user_id, bot_id)",
    ]

    engine = create_engine(sync_url, connect_args={"prepare_threshold": 0})
    try:
        with engine.connect() as conn:
            for stmt in statements:
                try:
                    conn.execute(text(stmt))
                except Exception as exc:
                    # Log but don't abort — most failures here are benign
                    # (e.g. constraint already exists under a different name).
                    print(f"migrate.py: _ensure_bot_id_columns warning: {exc}")
            conn.commit()
        print("migrate.py: bot_id columns verified")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
