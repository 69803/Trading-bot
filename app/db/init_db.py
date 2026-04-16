"""Database seeding: creates the admin user and all default records on first run."""

from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.logger import get_logger
from app.core.security import hash_password
from app.models.bot_log import BotLog  # noqa: F401 — ensures table is registered
from app.models.bot_state import BotState
from app.models.portfolio import Portfolio
from app.models.risk_settings import RiskSettings
from app.models.strategy_config import StrategyConfig
from app.models.user import User

log = get_logger(__name__)

_ADMIN_EMAIL = settings.ADMIN_EMAIL
_ADMIN_PASSWORD = settings.ADMIN_PASSWORD


async def _bootstrap_user(session: AsyncSession, user: User) -> None:
    """Create default StrategyConfig, RiskSettings, Portfolio, and BotState
    for a newly created user. Safe to call in a flush-before-commit context."""

    # Strategy config
    sc_result = await session.execute(
        select(StrategyConfig).where(StrategyConfig.user_id == user.id)
    )
    if sc_result.scalars().first() is None:
        session.add(StrategyConfig(
            user_id=user.id,
            ema_fast=50,
            ema_slow=200,
            rsi_period=14,
            rsi_overbought=Decimal("70.0"),
            rsi_oversold=Decimal("30.0"),
            auto_trade=False,
            symbols=["EURUSD", "BTCUSD"],
        ))

    # Risk settings
    rs_result = await session.execute(
        select(RiskSettings).where(RiskSettings.user_id == user.id)
    )
    if rs_result.scalars().first() is None:
        session.add(RiskSettings(
            user_id=user.id,
            max_position_size_pct=Decimal("0.05"),
            max_daily_loss_pct=Decimal("0.02"),
            max_open_positions=10,
            stop_loss_pct=Decimal("0.03"),
            take_profit_pct=Decimal("0.06"),
            max_drawdown_pct=Decimal("0.20"),
        ))

    # Portfolio
    port_result = await session.execute(
        select(Portfolio).where(Portfolio.user_id == user.id)
    )
    if port_result.scalars().first() is None:
        session.add(Portfolio(
            user_id=user.id,
            initial_capital=Decimal(str(settings.INITIAL_BALANCE)),
            cash_balance=Decimal(str(settings.INITIAL_BALANCE)),
            realized_pnl=Decimal("0.0"),
        ))

    # Bot state
    bot_result = await session.execute(
        select(BotState).where(BotState.user_id == user.id)
    )
    if bot_result.scalars().first() is None:
        session.add(BotState(user_id=user.id, is_running=False))

    await session.flush()


async def init_db(session: AsyncSession) -> None:
    """Seed the database with initial data. Safe to call on every startup."""
    result = await session.execute(select(User).where(User.email == _ADMIN_EMAIL))
    admin: User | None = result.scalar_one_or_none()

    if admin is None:
        log.info("Creating admin user", email=_ADMIN_EMAIL)
        admin = User(
            email=_ADMIN_EMAIL,
            hashed_password=hash_password(_ADMIN_PASSWORD),
            is_active=True,
            is_admin=True,
        )
        session.add(admin)
        await session.flush()
    else:
        log.info("Admin user already exists — skipping creation", email=_ADMIN_EMAIL)

    await _bootstrap_user(session, admin)
    await session.flush()

    # One-time patch: remove BTCUSD from active strategy config for admin user.
    # Idempotent — only fires while BTCUSD is still present in symbols.
    sc_patch = await session.execute(
        select(StrategyConfig).where(StrategyConfig.user_id == admin.id)
    )
    sc = sc_patch.scalars().first()
    if sc is not None and "BTCUSD" in (sc.symbols or []):
        sc.symbols = ["EURUSD"]
        log.info("Patched admin symbols → ['EURUSD']", user_id=str(admin.id))

    log.info("Database seed complete")


# ---------------------------------------------------------------------------
# Schema safety net
# ---------------------------------------------------------------------------

_BOT_ID_DDL = [
    # ── Add bot_id columns ────────────────────────────────────────────────────
    "ALTER TABLE bot_states       ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "UPDATE bot_states SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "UPDATE strategy_configs SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
    "ALTER TABLE risk_settings    ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "UPDATE risk_settings SET bot_id = 'trendmaster' WHERE bot_id IS NULL",
    "ALTER TABLE positions  ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "ALTER TABLE orders     ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    "ALTER TABLE trades     ADD COLUMN IF NOT EXISTS bot_id VARCHAR(20)",
    "ALTER TABLE bot_logs   ADD COLUMN IF NOT EXISTS bot_id VARCHAR(50)",
    # ── Drop old single-column unique constraints (they prevent multi-bot) ────
    # Without this, inserting a 2nd bot row (scalperx, piphunter…) for the
    # same user violates the user_id-only unique constraint → 500 on activate.
    "ALTER TABLE bot_states       DROP CONSTRAINT IF EXISTS bot_states_user_id_key",
    "DROP INDEX                       IF EXISTS ix_bot_states_user_id",
    "ALTER TABLE strategy_configs DROP CONSTRAINT IF EXISTS strategy_configs_user_id_key",
    "ALTER TABLE risk_settings    DROP CONSTRAINT IF EXISTS risk_settings_user_id_key",
    # ── Create composite unique indexes (allow one row per user+bot pair) ─────
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_bot_states_user_bot       ON bot_states       (user_id, bot_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_strategy_configs_user_bot ON strategy_configs (user_id, bot_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_risk_settings_user_bot    ON risk_settings    (user_id, bot_id)",

    # ── positions: columns added after the initial schema ─────────────────────
    # These were added incrementally to the ORM model but never backfilled
    # to the Render DB via a formal migration.  Without them every SELECT on
    # the positions table fails with UndefinedColumn → 500 on all position
    # endpoints.  All statements use IF NOT EXISTS so they are idempotent.
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS investment_amount   NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS stop_loss_price     NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_price   NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS closed_at           TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS closed_price        NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_pnl        NUMERIC(18,8) NOT NULL DEFAULT 0",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS is_paper            BOOLEAN",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS event_context       VARCHAR(40)",
    # TP/SL cross-detection state (added for trailing-stop / break-even logic)
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS prev_evaluated_price NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS high_water_mark      NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS trailing_stop_price  NUMERIC(18,8)",
    "ALTER TABLE positions ADD COLUMN IF NOT EXISTS break_even_activated BOOLEAN NOT NULL DEFAULT FALSE",

    # ── orders: columns added after the initial schema ────────────────────────
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS investment_amount NUMERIC(18,8)",
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS avg_fill_price    NUMERIC(18,8)",
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS rejection_reason  TEXT",
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS filled_quantity   NUMERIC(18,8) NOT NULL DEFAULT 0",
    # broker_order_id: Alpaca paper-trading UUID stored when a pending order is
    # forwarded so the fill-sync job can poll status directly without scanning.
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS broker_order_id   VARCHAR(100)",
    # alpaca_status / submitted_at: track Alpaca order lifecycle for the
    # Alpaca-first manual trading flow (ALPACA_BROKER_ENABLED=true).
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS alpaca_status     VARCHAR(30)",
    "ALTER TABLE orders ADD COLUMN IF NOT EXISTS submitted_at      TIMESTAMP WITH TIME ZONE",

    # ── risk_settings: advanced risk-management columns (professional upgrade) ─
    # Added to the ORM model after the initial schema.  Without them every
    # SELECT on risk_settings (called by _check_risk_limits on every order)
    # fails with UndefinedColumn → 500 on POST /orders.
    "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS trailing_stop_pct        NUMERIC(6,4)  NOT NULL DEFAULT 0.0000",
    "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS break_even_trigger_pct   NUMERIC(6,4)  NOT NULL DEFAULT 0.0000",
    "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS max_consecutive_losses   INTEGER       NOT NULL DEFAULT 0",
    "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS max_trades_per_hour      INTEGER       NOT NULL DEFAULT 0",
    "ALTER TABLE risk_settings ADD COLUMN IF NOT EXISTS volatility_sizing_enabled BOOLEAN      NOT NULL DEFAULT FALSE",

    # ── strategy_configs: new columns added in professional upgrade ───────────
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS asset_classes          JSON",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS investment_amount      NUMERIC(18,8) NOT NULL DEFAULT 100",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS run_interval_seconds   INTEGER       NOT NULL DEFAULT 60",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS per_symbol_max_positions INTEGER     NOT NULL DEFAULT 1",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS allow_buy              BOOLEAN       NOT NULL DEFAULT TRUE",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS allow_sell             BOOLEAN       NOT NULL DEFAULT TRUE",
    "ALTER TABLE strategy_configs ADD COLUMN IF NOT EXISTS cooldown_seconds       INTEGER       NOT NULL DEFAULT 0",

    # ── bot_states: columns that may be missing on older schemas ─────────────
    "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS started_at    TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS last_cycle_at TIMESTAMP WITH TIME ZONE",
    "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS cycles_run    INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS last_log      TEXT",
    "ALTER TABLE bot_states ADD COLUMN IF NOT EXISTS last_error    TEXT",
]


async def ensure_bot_id_columns() -> None:
    """Idempotently add bot_id columns before any ORM queries touch them.

    Uses the app's async session factory so it runs through the exact same
    psycopg3 async driver as the rest of the application — no separate sync
    engine, no greenlet issues, no PgBouncer transaction-mode surprises.

    Each statement gets its own session (independent commit) so a single
    failure never aborts the rest.
    """
    from app.db.session import AsyncSessionFactory

    for stmt in _BOT_ID_DDL:
        try:
            async with AsyncSessionFactory() as s:
                await s.execute(text(stmt))
                await s.commit()
        except Exception as exc:
            log.warning("ensure_bot_id_columns: skipped", stmt=stmt.strip()[:70], error=str(exc))
