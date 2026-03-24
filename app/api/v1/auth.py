"""Authentication endpoints: register, login, refresh, logout, /me."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user, get_db
from app.core.security import (
    create_access_token,
    generate_refresh_token,
    hash_password,
    hash_refresh_token,
    refresh_token_expiry,
    verify_password,
)
from app.models.refresh_token import RefreshToken
from app.models.user import User
from app.schemas.auth import (
    FullTokenResponse,
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
    UserOut,
)

router = APIRouter()


async def _authenticate_user(email: str, password: str, db: AsyncSession) -> User:
    result = await db.execute(select(User).where(User.email == email))
    user: User | None = result.scalar_one_or_none()
    if user is None or not verify_password(password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account",
        )
    return user


async def _issue_tokens(user: User, db: AsyncSession) -> FullTokenResponse:
    """Create access + refresh token pair, persist refresh token hash."""
    access = create_access_token(data={"sub": str(user.id)})
    raw_refresh = generate_refresh_token()
    token_record = RefreshToken(
        user_id=user.id,
        token_hash=hash_refresh_token(raw_refresh),
        expires_at=refresh_token_expiry(),
        revoked=False,
    )
    db.add(token_record)
    await db.flush()
    return FullTokenResponse(access_token=access, refresh_token=raw_refresh)


@router.post(
    "/register",
    response_model=FullTokenResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user account",
)
async def register(
    body: RegisterRequest,
    db: AsyncSession = Depends(get_db),
) -> FullTokenResponse:
    """Create a new user, portfolio, and default settings, then return tokens."""
    existing = await db.execute(select(User).where(User.email == body.email))
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
        )
    user = User(
        email=body.email,
        hashed_password=hash_password(body.password),
        is_active=True,
        is_admin=False,
    )
    db.add(user)
    await db.flush()

    # Bootstrap default records for the new user
    from app.db.init_db import _bootstrap_user
    await _bootstrap_user(db, user)

    return await _issue_tokens(user, db)


@router.post(
    "/login",
    response_model=FullTokenResponse,
    summary="Obtain JWT tokens (OAuth2 form)",
)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: AsyncSession = Depends(get_db),
) -> FullTokenResponse:
    user = await _authenticate_user(form_data.username, form_data.password, db)
    return await _issue_tokens(user, db)


@router.post(
    "/login/json",
    response_model=FullTokenResponse,
    summary="Obtain JWT tokens (JSON body)",
)
async def login_json(
    body: LoginRequest,
    db: AsyncSession = Depends(get_db),
) -> FullTokenResponse:
    user = await _authenticate_user(body.email, body.password, db)
    return await _issue_tokens(user, db)


@router.post(
    "/refresh",
    response_model=FullTokenResponse,
    summary="Exchange a refresh token for a new token pair",
)
async def refresh(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_db),
) -> FullTokenResponse:
    token_hash = hash_refresh_token(body.refresh_token)
    now = datetime.now(timezone.utc)

    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.token_hash == token_hash,
            RefreshToken.revoked == False,  # noqa: E712
            RefreshToken.expires_at > now,
        )
    )
    record: RefreshToken | None = result.scalar_one_or_none()
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
        )

    # Rotate: revoke old token
    record.revoked = True
    await db.flush()

    # Load user
    user_result = await db.execute(select(User).where(User.id == record.user_id))
    user: User | None = user_result.scalar_one_or_none()
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
        )

    return await _issue_tokens(user, db)


@router.post("/logout", summary="Revoke the current refresh token")
async def logout(
    body: RefreshRequest,
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Revoke a refresh token. Access tokens remain valid until expiry."""
    token_hash = hash_refresh_token(body.refresh_token)
    result = await db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    )
    record: RefreshToken | None = result.scalar_one_or_none()
    if record:
        record.revoked = True
    return {"message": "Logged out successfully"}


@router.get("/me", response_model=UserOut, summary="Get current user profile")
async def me(current_user: User = Depends(get_current_active_user)) -> User:
    return current_user
