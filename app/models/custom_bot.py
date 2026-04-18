"""CustomBot: user-defined automated trading bots."""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy import Boolean, ForeignKey, JSON, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from app.models.user import User


class CustomBot(Base):
    __tablename__ = "custom_bots"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_custom_bots_user_name"),
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    # Human-readable name (unique per user)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    # Internal bot_id used in BotState / StrategyConfig / RiskSettings
    # Format: "custom_<uuid_hex_8_chars>"
    bot_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    color: Mapped[str] = mapped_column(String(20), nullable=False, default="#6366f1")
    # Full configuration stored as JSON
    config: Mapped[Any] = mapped_column(JSON, nullable=False, default=dict)
    # Whether the bot is currently enabled (mirrors BotState.is_running)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    def __repr__(self) -> str:
        return f"<CustomBot user={self.user_id} name={self.name!r} bot_id={self.bot_id}>"
