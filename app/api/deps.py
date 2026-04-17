"""FastAPI dependency injection: DB session, current user, role guards."""

from typing import Literal
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import verify_token
from app.db.session import get_db
from app.models.user import User

AccountMode = Literal["paper", "live"]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Decode the JWT bearer token and load the corresponding User from the DB.

    Raises:
        HTTPException 401: If the token is invalid, expired, or the user
                           referenced in ``sub`` no longer exists.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    payload = verify_token(token)  # raises 401 on its own if invalid
    user_id: str | None = payload.get("sub")
    if user_id is None:
        raise credentials_exception

    try:
        uid = UUID(user_id)
    except (ValueError, AttributeError):
        raise credentials_exception

    result = await db.execute(select(User).where(User.id == uid))
    user: User | None = result.scalar_one_or_none()
    if user is None:
        raise credentials_exception

    return user


async def get_current_active_user(
    user: User = Depends(get_current_user),
) -> User:
    """Guard: ensure the authenticated user has an active account.

    Raises:
        HTTPException 403: If the user's ``is_active`` flag is False.
    """
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account",
        )
    return user


async def get_account_mode(
    x_account_mode: str = Header(default="paper", alias="X-Account-Mode"),
) -> AccountMode:
    """
    Read the trading account mode from the X-Account-Mode request header.

    Returns 'paper' (default) or 'live'.  Any unrecognised value is silently
    downgraded to 'paper' — never accidentally operate on real money.
    """
    return "live" if x_account_mode == "live" else "paper"


async def get_admin_user(
    user: User = Depends(get_current_active_user),
) -> User:
    """Guard: ensure the authenticated user is an administrator.

    Raises:
        HTTPException 403: If the user's ``is_admin`` flag is False.
    """
    if not user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return user
