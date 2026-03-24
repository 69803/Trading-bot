import logging
import sys

import structlog
from structlog.types import Processor

from app.core.config import settings


def _configure_structlog() -> None:
    """Configure structlog once at import time based on the current environment."""

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.ENVIRONMENT == "production":
        # JSON output — suitable for log aggregators (Loki, CloudWatch, etc.)
        renderer: Processor = structlog.processors.JSONRenderer()
    else:
        # Human-readable coloured output for development
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    # Avoid adding duplicate handlers when modules are reloaded in dev
    if not root_logger.handlers:
        root_logger.addHandler(handler)

    log_level = logging.DEBUG if settings.ENVIRONMENT != "production" else logging.INFO
    root_logger.setLevel(log_level)


_configure_structlog()


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structlog bound logger for the given name.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A bound structlog logger instance.
    """
    return structlog.get_logger(name)
