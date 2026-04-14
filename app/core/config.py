from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+asyncpg://trader:trader123@localhost:5432/tradingdb"
    SECRET_KEY: str = "dev-secret-key-change-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 43200  # 30 days — never expires mid-session
    ENVIRONMENT: str = "development"
    INITIAL_BALANCE: float = 10000.0
    ALGORITHM: str = "HS256"
    ADMIN_EMAIL: str = "admin@trading.local"
    ADMIN_PASSWORD: str = "admin1234"
    REFRESH_TOKEN_EXPIRE_DAYS: int = 30

    # Stock provider: "gbm" (default), "alpaca", or "polygon"
    MARKET_DATA_PROVIDER: str = "gbm"
    ALPACA_API_KEY: str = ""
    ALPACA_SECRET_KEY: str = ""
    POLYGON_API_KEY: str = ""

    # Broker mirror: when True, filled orders for US equities are also sent
    # to Alpaca Paper Trading API (paper-api.alpaca.markets) as a side effect.
    # The internal simulation always runs regardless of this flag.
    # Set to True in Render once ALPACA_API_KEY and ALPACA_SECRET_KEY are set.
    ALPACA_BROKER_ENABLED: bool = False

    # Forex + Commodities provider (Twelve Data)
    TWELVE_DATA_API_KEY: str = ""

    # News providers (optional — system works without them via fallback)
    NEWS_API_KEY: str = ""          # newsapi.org  — free: 100 req/day
    ALPHA_VANTAGE_KEY: str = ""     # alphavantage.co — free: 25 req/day

    # Frontend URL for CORS (set to actual Render URL in production)
    FRONTEND_URL: str = ""

    # Historical data providers
    TRADING_ECONOMICS_API_KEY: str = ""   # tradingeconomics.com — for historical events

    # Historical data defaults (ISO date strings: YYYY-MM-DD)
    HISTORICAL_DEFAULT_START_DATE: str = "2015-01-01"   # ~10 years back
    HISTORICAL_DEFAULT_END_DATE:   str = ""              # empty = today

    # ── Historical performance guardrails ────────────────────────────────────
    # Set HISTORICAL_GUARDRAIL_ENABLED=false in .env to disable entirely.
    HISTORICAL_GUARDRAIL_ENABLED: bool = True

    # Minimum closed-trade sample required before a rule can fire.
    # Below this threshold the rule is skipped (not enough evidence).
    GUARDRAIL_MIN_TRADES_SYMBOL: int = 10   # trades per symbol
    GUARDRAIL_MIN_TRADES_HOUR:   int = 5    # trades per UTC hour
    GUARDRAIL_MIN_TRADES_EVENT_CTX: int = 8  # trades per event context

    # Rule 1 — Symbol block: BOTH conditions must be true to block.
    # Win rate threshold: below this is considered poor performance.
    GUARDRAIL_SYMBOL_MIN_WIN_RATE: float = 0.35
    # PnL threshold: symbol must also be net-negative by at least this amount.
    # Using both prevents blocking low-win-rate / high-reward strategies.
    GUARDRAIL_SYMBOL_MAX_NEGATIVE_PNL: float = -100.0

    # Rule 2 — Hour reduce: trading during historically poor UTC hours
    # reduces position size (not a full block).
    GUARDRAIL_HOUR_MIN_WIN_RATE: float = 0.35

    # Rule 3 — Event context escalation: if the "reduced_size_due_to_event"
    # context has consistently poor historical performance, escalate REDUCE → BLOCK.
    GUARDRAIL_EVENT_CTX_MIN_WIN_RATE: float = 0.40

    # ── Expert Forex Mode filters (PASO 7) ───────────────────────────────────
    # Master switch — set EXPERT_FILTERS_ENABLED=false in .env to disable all.
    EXPERT_FILTERS_ENABLED: bool = True

    # Filter A — Trading sessions
    # Set USE_TRADING_SESSIONS=false to operate 24/7 (useful for testing).
    USE_TRADING_SESSIONS: bool = False

    EXPERT_SESSION_LONDON_START: int = 7
    EXPERT_SESSION_LONDON_END:   int = 11
    EXPERT_SESSION_NY_START:     int = 13
    EXPERT_SESSION_NY_END:       int = 17

    # Filter B — Minimum ATR gate: skip if market is too quiet.
    # 0.0003 ≈ 3 pips for major FX pairs.
    EXPERT_ATR_MIN: float = 0.0003

    # Filter C — EMA200 trend alignment period.
    EXPERT_TREND_EMA_PERIOD: int = 200

    # Filter D — Minimum confirming signal conditions out of 3
    # (EMA alignment, RSI momentum, MACD histogram direction).
    EXPERT_MIN_SIGNAL_CONDITIONS: int = 1  # TESTING: lowered from 2

    # Filter E — Anti-overtrading
    EXPERT_MAX_TRADES_PER_DAY: int  = 5   # new positions opened today (UTC)
    EXPERT_COOLDOWN_MINUTES:   int  = 30  # portfolio-level gap between any two trades

    # Filter F — Post-event delay after a high-impact DB event.
    EXPERT_POST_EVENT_DELAY_MINUTES: int = 15

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
