import os
from collections.abc import AsyncGenerator
from typing import Optional, TypeAlias

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import (
    AsyncSession as _AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings

# ---------------------------------------------------------------------------
# Engine & session factory — only created when DATABASE_URL env var is set
# ---------------------------------------------------------------------------

_DB_URL: Optional[str] = os.environ.get("DATABASE_URL")

# Normalise driver: replace asyncpg with psycopg (psycopg3).
# psycopg3 handles PgBouncer/Supabase pooler correctly without needing
# statement_cache_size hacks.
if _DB_URL:
    _DB_URL = _DB_URL.replace("postgresql+asyncpg://", "postgresql+psycopg://")

if _DB_URL:
    engine = create_async_engine(
        _DB_URL,
        echo=settings.ENVIRONMENT != "production",
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        connect_args={"prepare_threshold": None},  # disable psycopg3 prepared stmts — incompatible with Render/PgBouncer transaction-mode pooler
    )
    AsyncSessionFactory: Optional[async_sessionmaker[_AsyncSession]] = async_sessionmaker(
        bind=engine,
        class_=_AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )
else:
    engine = None  # type: ignore[assignment]
    AsyncSessionFactory = None

# Public type alias used across the codebase
AsyncSession: TypeAlias = _AsyncSession


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[_AsyncSession, None]:
    """Yield an async database session for use as a FastAPI dependency.

    Raises 503 if no DATABASE_URL is configured.
    Commits the transaction on successful completion and rolls back on any
    unhandled exception, then always closes the session.
    """
    if AsyncSessionFactory is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not configured",
        )
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
