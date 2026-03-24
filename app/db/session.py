from collections.abc import AsyncGenerator
from typing import TypeAlias

from sqlalchemy.ext.asyncio import (
    AsyncSession as _AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import settings

# ---------------------------------------------------------------------------
# Engine & session factory
# ---------------------------------------------------------------------------

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.ENVIRONMENT != "production",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

AsyncSessionFactory: async_sessionmaker[_AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=_AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

# Public type alias used across the codebase
AsyncSession: TypeAlias = _AsyncSession


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[_AsyncSession, None]:
    """Yield an async database session for use as a FastAPI dependency.

    Commits the transaction on successful completion and rolls back on any
    unhandled exception, then always closes the session.
    """
    async with AsyncSessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
