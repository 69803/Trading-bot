import os
import ssl
from collections.abc import AsyncGenerator
from typing import Optional, TypeAlias
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

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

# asyncpg does not accept `sslmode` as a URL query param (libpq-only).
# Strip it out and, if SSL was requested or the host is Supabase, pass
# ssl=True via connect_args instead.
_connect_args: dict = {}
if _DB_URL and _DB_URL.startswith("postgresql+asyncpg"):
    _parsed = urlparse(_DB_URL)
    _qs = parse_qs(_parsed.query, keep_blank_values=True)
    _ssl_requested = "sslmode" in _qs
    _qs.pop("sslmode", None)
    _DB_URL = urlunparse(_parsed._replace(query=urlencode(_qs, doseq=True)))
    if _ssl_requested or (_parsed.hostname and "supabase" in _parsed.hostname):
        _ssl_ctx = ssl.create_default_context()
        _connect_args["ssl"] = _ssl_ctx

if _DB_URL:
    engine = create_async_engine(
        _DB_URL,
        echo=settings.ENVIRONMENT != "production",
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        connect_args=_connect_args,
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
