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
"""
import os
import sys

from alembic.config import Config
from alembic import command
from sqlalchemy import create_engine, inspect


def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("migrate.py: No DATABASE_URL — skipping migrations")
        return

    # psycopg3 sync driver works for both async and sync SQLAlchemy.
    # Replace the asyncpg variant just in case it slipped through.
    sync_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")

    engine = create_engine(sync_url)
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
    print("migrate.py: migrations complete")


if __name__ == "__main__":
    main()
