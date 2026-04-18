"""Alembic migration environment.

Uses psycopg3 in SYNC mode for migrations — avoids greenlet/async cleanup
issues that arise when using the async driver with NullPool and Supabase's
PgBouncer transaction-mode pooler (port 6543).

prepare_threshold=0 disables psycopg3 prepared statements, which are not
supported by PgBouncer transaction mode.
"""

import os
from logging.config import fileConfig
from dotenv import load_dotenv

load_dotenv()

from alembic import context
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import Connection

# ---------------------------------------------------------------------------
# Alembic Config object (access to alembic.ini values)
# ---------------------------------------------------------------------------
config = context.config

# Inject the DATABASE_URL from the environment (overrides the blank value in
# alembic.ini so we never hard-code credentials in version control).
database_url = os.environ.get("DATABASE_URL")
if database_url:
    # Ensure we use the psycopg3 sync driver (not asyncpg).
    database_url = database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
    # Strip the async variant prefix if present.
    database_url = database_url.replace("postgresql+psycopg_async://", "postgresql+psycopg://")
    config.set_main_option("sqlalchemy.url", database_url)

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ---------------------------------------------------------------------------
# Import Base so all models are registered on its metadata
# ---------------------------------------------------------------------------
from app.db.base import Base  # noqa: E402
# Import all models so Alembic can detect them
import app.models.user  # noqa: F401, E402
import app.models.portfolio  # noqa: F401, E402
import app.models.position  # noqa: F401, E402
import app.models.order  # noqa: F401, E402
import app.models.trade  # noqa: F401, E402
import app.models.market_candle  # noqa: F401, E402
import app.models.strategy_config  # noqa: F401, E402
import app.models.strategy_signal  # noqa: F401, E402
import app.models.risk_settings  # noqa: F401, E402
import app.models.backtest_run  # noqa: F401, E402
import app.models.portfolio_snapshot  # noqa: F401, E402
import app.models.bot_state  # noqa: F401, E402
import app.models.refresh_token  # noqa: F401, E402
import app.models.decision_log  # noqa: F401, E402
import app.models.market_price  # noqa: F401, E402
import app.models.historical_event  # noqa: F401, E402
import app.models.performance_snapshot  # noqa: F401, E402
import app.models.daily_performance_summary  # noqa: F401, E402
import app.models.bot_log    # noqa: F401, E402
import app.models.custom_bot  # noqa: F401, E402

target_metadata = Base.metadata


# ---------------------------------------------------------------------------
# Offline migrations (generate SQL without connecting to the DB)
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Online migrations — sync psycopg3, no async/greenlet complexity
# ---------------------------------------------------------------------------
def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations using psycopg3 sync driver.

    Sync is preferred over async for Alembic: simpler lifecycle, no greenlet
    cleanup issues, and fully compatible with PgBouncer transaction mode when
    prepare_threshold=0 is set.
    """
    url = config.get_main_option("sqlalchemy.url")
    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
        connect_args={"prepare_threshold": 0},
    )
    with connectable.connect() as connection:
        do_run_migrations(connection)
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
