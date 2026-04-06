"""Alembic async migration environment."""

import asyncio
import os
from logging.config import fileConfig
from dotenv import load_dotenv

load_dotenv()

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# ---------------------------------------------------------------------------
# Alembic Config object (access to alembic.ini values)
# ---------------------------------------------------------------------------
config = context.config

# Inject the DATABASE_URL from the environment (overrides the blank value in
# alembic.ini so we never hard-code credentials in version control).
database_url = os.environ.get("DATABASE_URL")
if database_url:
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
import app.models.bot_log  # noqa: F401, E402

target_metadata = Base.metadata


# ---------------------------------------------------------------------------
# Offline migrations (generate SQL without connecting to the DB)
# ---------------------------------------------------------------------------
def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine, though an
    Engine is acceptable here as well.  By skipping the Engine creation we
    don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the script output.
    """
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
# Online migrations (connect to DB and run migrations)
# ---------------------------------------------------------------------------
def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create an async engine and run migrations within an async context."""
    # PgBouncer (Supabase transaction pooler) does not support prepared
    # statements — disable the asyncpg statement cache for pooler hosts.
    _url = config.get_main_option("sqlalchemy.url") or ""
    from urllib.parse import urlparse as _urlparse
    _host = _urlparse(_url).hostname or ""
    _is_pooler = "pooler.supabase.com" in _host
    _connect_args = {"statement_cache_size": 0} if _is_pooler else {}
    _exec_opts = {"compiled_cache": None} if _is_pooler else {}

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=_connect_args,
        execution_options=_exec_opts,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode using asyncio."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
