import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException, status
from jose import JWTError, jwt
from passlib.context import CryptContext

from app.core.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create a signed JWT access token.

    Args:
        data: Payload dict to encode. Should include a ``sub`` key.
        expires_delta: Optional expiry offset. Defaults to settings value.

    Returns:
        Encoded JWT string.
    """
    to_encode = data.copy()
    if expires_delta is not None:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire})
    encoded_jwt: str = jwt.encode(
        to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM
    )
    return encoded_jwt


def verify_token(token: str) -> dict[str, Any]:
    """Decode and validate a JWT token.

    Args:
        token: The raw JWT string.

    Returns:
        The decoded payload dict.

    Raises:
        HTTPException 401: If the token is invalid or expired.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload: dict[str, Any] = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )
        sub: str | None = payload.get("sub")
        if sub is None:
            raise credentials_exception
        return payload
    except JWTError:
        raise credentials_exception


def hash_password(password: str) -> str:
    """Hash a plain-text password using bcrypt.

    Args:
        password: Plain-text password string.

    Returns:
        Bcrypt hashed password string.
    """
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plain-text password against a bcrypt hash."""
    return pwd_context.verify(plain, hashed)


def generate_refresh_token() -> str:
    """Generate a cryptographically secure random refresh token string."""
    return secrets.token_urlsafe(64)


def hash_refresh_token(token: str) -> str:
    """Return a SHA-256 hex digest of the refresh token for safe DB storage."""
    return hashlib.sha256(token.encode()).hexdigest()


def refresh_token_expiry() -> datetime:
    """Return the expiry datetime for a new refresh token."""
    return datetime.now(timezone.utc) + timedelta(
        days=settings.REFRESH_TOKEN_EXPIRE_DAYS
    )
