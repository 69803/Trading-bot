"""
Sentiment Analysis Engine — Phase 3.

Converts a list of news articles into a structured SentimentResult by:
  1. Scoring each headline + description with a financial lexicon.
  2. Detecting high-impact keywords (earnings, merger, lawsuit, …).
  3. Aggregating scores with recency weighting.
  4. Computing an impact_score that reflects how market-moving the news is.

No external ML library is required — uses a curated financial lexicon
implemented as pure Python sets/dicts. This keeps startup fast and the
container small while still producing accurate directional signals.

Lexicon design
──────────────
Words are scored in three layers:
  Layer 1 – Base polarity     (+1 positive / -1 negative)
  Layer 2 – Intensity modifier (×1.5 for very strong words like "crash", "soar")
  Layer 3 – Negation handling  ("not beat" → flips sign for that phrase)

Impact keywords trigger a separate counter that raises impact_score
independently of sentiment direction.  A news item can be neutral in tone
but still highly impactful (e.g., "Company announces major restructuring").
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import List, Literal, Optional

from app.core.logger import get_logger
from app.schemas.sentiment import NewsItem, SentimentResult

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Financial sentiment lexicon
# ---------------------------------------------------------------------------

# Positive signals — words/phrases associated with bullish price action
_POSITIVE: dict[str, float] = {
    # Earnings & revenue
    "beat": 1.0, "beats": 1.0, "exceed": 1.0, "exceeded": 1.0,
    "record": 0.8, "record high": 1.2, "all-time high": 1.2,
    "revenue growth": 1.2, "profit": 0.8, "profits": 0.8,
    "earnings beat": 1.5, "record earnings": 1.5,
    # Analyst actions
    "upgrade": 1.2, "upgraded": 1.2, "outperform": 1.0,
    "buy rating": 1.3, "strong buy": 1.5, "overweight": 0.9,
    "price target raised": 1.4, "target raised": 1.2,
    # Market movement
    "surge": 1.2, "surges": 1.2, "soar": 1.3, "soars": 1.3,
    "rally": 1.0, "rallies": 1.0, "jump": 0.9, "jumps": 0.9,
    "rise": 0.7, "rises": 0.7, "climb": 0.8, "climbs": 0.8,
    "gain": 0.7, "gains": 0.7, "advance": 0.7, "advances": 0.7,
    "recover": 0.8, "recovery": 0.9,
    # Growth & outlook
    "growth": 0.8, "expansion": 0.9, "opportunity": 0.6,
    "innovation": 0.7, "breakthrough": 1.1, "launch": 0.6,
    "strong": 0.7, "robust": 0.8, "positive": 0.6,
    "optimistic": 0.8, "confidence": 0.7, "bullish": 1.0,
    # Corporate actions
    "dividend": 0.8, "buyback": 0.9, "share repurchase": 1.0,
    "acquisition": 0.7, "merger": 0.7, "partnership": 0.6,
    "deal": 0.5, "contract": 0.6, "approved": 0.7,
    # Demand & supply
    "demand": 0.6, "sales growth": 1.0, "market share": 0.7,
    "momentum": 0.7, "outpaced": 0.8,
    # Forex-specific bullish
    "dovish": 0.7, "easing": 0.6, "rate cut": 0.9, "stimulus": 0.8,
    "surplus": 0.6, "strong gdp": 1.0, "employment growth": 0.8,
}

# Negative signals — words/phrases associated with bearish price action
_NEGATIVE: dict[str, float] = {
    # Earnings & revenue
    "miss": 1.0, "misses": 1.0, "shortfall": 1.0, "below expectations": 1.2,
    "disappointing": 1.0, "disappoints": 1.0, "weaker than expected": 1.2,
    "revenue decline": 1.2, "loss": 0.8, "losses": 0.8,
    "earnings miss": 1.5, "profit warning": 1.5,
    # Analyst actions
    "downgrade": 1.2, "downgraded": 1.2, "underperform": 1.0,
    "sell rating": 1.3, "strong sell": 1.5, "underweight": 0.9,
    "price target cut": 1.4, "target lowered": 1.2,
    # Market movement
    "crash": 1.5, "crashes": 1.5, "plunge": 1.3, "plunges": 1.3,
    "tumble": 1.1, "tumbles": 1.1, "drop": 0.8, "drops": 0.8,
    "fall": 0.7, "falls": 0.7, "decline": 0.8, "declines": 0.8,
    "sink": 0.9, "sinks": 0.9, "slide": 0.8, "slides": 0.8,
    # Risk & concern
    "risk": 0.5, "risks": 0.5, "concern": 0.6, "concerns": 0.6,
    "uncertainty": 0.7, "volatile": 0.6, "volatility": 0.6,
    "warning": 0.9, "warns": 0.9, "alert": 0.6,
    "bearish": 1.0, "negative": 0.6, "pessimistic": 0.8,
    # Financial distress
    "debt": 0.5, "default": 1.3, "bankrupt": 1.5, "bankruptcy": 1.5,
    "insolvency": 1.4, "writedown": 1.1, "impairment": 0.9,
    "restructuring": 0.8, "layoff": 1.0, "layoffs": 1.0,
    "downsizing": 0.9, "job cuts": 1.0,
    # Legal & regulatory
    "lawsuit": 1.1, "sued": 1.0, "fraud": 1.3, "investigation": 1.0,
    "probe": 0.9, "penalty": 1.0, "fine": 0.8, "sec charges": 1.4,
    "recall": 1.0, "ban": 1.0, "blocked": 0.8, "rejected": 0.8,
    # Macro / external
    "recession": 1.2, "inflation": 0.5, "rate hike": 0.8,
    "tariff": 0.7, "sanction": 0.9, "geopolitical": 0.6,
    # Forex-specific bearish
    "hawkish": 0.7, "tightening": 0.6, "slowdown": 0.8,
    "contraction": 0.9, "stagflation": 1.2, "devaluation": 1.0,
    "trade war": 1.0, "deficit": 0.6, "debt ceiling": 0.8,
}

# High-impact keywords — raise impact_score regardless of polarity direction
_IMPACT_KEYWORDS: set[str] = {
    # Scale
    "billion", "trillion", "billion-dollar",
    # Significance
    "record", "historic", "unprecedented", "major", "significant",
    "massive", "huge", "enormous",
    # Corporate events
    "acquisition", "merger", "ipo", "spin-off", "spinoff",
    "bankruptcy", "default", "restructuring",
    "earnings", "revenue", "guidance", "forecast",
    # Regulatory
    "fda", "sec", "ftc", "doj", "antitrust", "regulation",
    "approval", "approved", "rejected", "ban",
    # Market events
    "crash", "correction", "bear market", "bull market",
    "rate hike", "rate cut", "fed", "central bank",
    # Leadership
    "ceo", "cfo", "cto", "chairman", "founder", "resign", "fired",
    # Forex / macro specific
    "ecb", "federal reserve", "fomc", "boe", "bank of japan", "boj",
    "interest rate decision", "inflation report", "jobs report", "nonfarm",
    "gdp", "cpi", "pmi", "unemployment", "payrolls",
    "hawkish", "dovish", "quantitative easing", "quantitative tightening",
    "geopolitical crisis", "war", "sanctions", "trade war", "debt ceiling",
}

# Negation words — if found near a sentiment word, flip its score
_NEGATORS: set[str] = {
    "not", "no", "never", "without", "despite", "fails to",
    "unable to", "missed", "didn't", "doesn't", "won't",
}

# Intensity amplifiers — multiply score of the next sentiment word
_AMPLIFIERS: dict[str, float] = {
    "very": 1.3, "highly": 1.3, "extremely": 1.5, "significantly": 1.3,
    "sharply": 1.4, "dramatically": 1.4, "substantially": 1.3,
    "slightly": 0.6, "modestly": 0.7, "marginally": 0.6,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze(
    symbol: str,
    items: List[NewsItem],
    *,
    provider: Literal["newsapi", "alphavantage", "simulated"] = "simulated",
    recency_half_life_hours: float = 12.0,
) -> SentimentResult:
    """
    Analyze a list of NewsItems and return an aggregated SentimentResult.

    Args:
        symbol:                  Trading symbol (e.g. "NVDA").
        items:                   Articles from news_service.get_news().
        recency_half_life_hours: Articles older than this lose half their
                                 weight in the aggregate score.

    Returns:
        SentimentResult with sentiment_score, impact_score, label and detail.
    """
    now = datetime.now(timezone.utc)

    if not items:
        log.debug("No news items — returning neutral sentiment", symbol=symbol)
        return _neutral_result(symbol, now)

    scored_items: List[NewsItem] = []
    weighted_score_sum = 0.0
    weight_total       = 0.0
    impact_hits        = 0

    for article in items:
        text = _build_text(article)

        article_score, found_impact = _score_text(text)
        impact_hits += len(found_impact)

        label = _label(article_score)
        article.score          = round(article_score, 4)
        article.label          = label
        article.impact_keywords = found_impact
        scored_items.append(article)

        # Recency weight: w = 0.5^(age_hours / half_life)
        age_hours = _age_hours(article.published_at, now)
        weight    = math.pow(0.5, age_hours / recency_half_life_hours)

        weighted_score_sum += article_score * weight
        weight_total       += weight

        log.debug(
            "Article scored",
            symbol=symbol, score=article_score, label=label,
            impact=found_impact, title=article.title[:60],
        )

    # Aggregate sentiment score (weighted average, clamped to [-1, +1])
    agg_score = weighted_score_sum / weight_total if weight_total > 0 else 0.0
    agg_score = max(-1.0, min(1.0, agg_score))

    # Impact score: 0–100
    # - Base: number of articles (more coverage = more impact)
    # - Boost: impact keyword hits
    # - Boost: extreme sentiment magnitude
    base_impact    = min(40, len(items) * 8)        # 5 articles → 40 pts
    keyword_impact = min(40, impact_hits * 5)        # 8 keywords → 40 pts
    magnitude_boost = int(abs(agg_score) * 20)       # max 20 pts for extreme score
    impact_score   = min(100, base_impact + keyword_impact + magnitude_boost)

    result = SentimentResult(
        symbol          = symbol,
        sentiment_score = round(agg_score, 4),
        impact_score    = impact_score,
        label           = _label(agg_score),
        news_count      = len(items),
        headlines       = [a.title for a in scored_items],
        items           = scored_items,
        analyzed_at     = now,
        source          = provider,
    )

    log.info(
        "Sentiment analysis complete",
        symbol=symbol,
        score=result.sentiment_score,
        impact=result.impact_score,
        label=result.label,
        articles=result.news_count,
        modifier=result.confidence_modifier,
    )
    return result


def _score_text(text: str) -> tuple[float, list[str]]:
    """
    Score a block of text with the financial lexicon.

    Returns (score [-1,+1], list_of_impact_keywords_found).
    """
    text_lower = text.lower()
    tokens     = re.findall(r"[a-zA-Z/'-]+", text_lower)

    raw_score      = 0.0
    term_count     = 0
    impact_found:  list[str] = []

    # ── Check multi-word phrases first (up to 4 tokens) ─────────────────────
    for length in (4, 3, 2):
        for i in range(len(tokens) - length + 1):
            phrase = " ".join(tokens[i: i + length])
            if phrase in _POSITIVE:
                weight   = _amplifier_weight(tokens, i)
                negate   = _is_negated(tokens, i)
                polarity = _POSITIVE[phrase] * weight * (-1 if negate else 1)
                raw_score  += polarity
                term_count += 1
            elif phrase in _NEGATIVE:
                weight   = _amplifier_weight(tokens, i)
                negate   = _is_negated(tokens, i)
                polarity = -_NEGATIVE[phrase] * weight * (-1 if negate else 1)
                raw_score  += polarity
                term_count += 1
            # Check impact keywords (polarity-independent)
            if phrase in _IMPACT_KEYWORDS and phrase not in impact_found:
                impact_found.append(phrase)

    # ── Single-word pass ─────────────────────────────────────────────────────
    for i, token in enumerate(tokens):
        if token in _POSITIVE:
            weight   = _amplifier_weight(tokens, i)
            negate   = _is_negated(tokens, i)
            polarity = _POSITIVE[token] * weight * (-1 if negate else 1)
            raw_score  += polarity
            term_count += 1
        elif token in _NEGATIVE:
            weight   = _amplifier_weight(tokens, i)
            negate   = _is_negated(tokens, i)
            polarity = -_NEGATIVE[token] * weight * (-1 if negate else 1)
            raw_score  += polarity
            term_count += 1
        if token in _IMPACT_KEYWORDS and token not in impact_found:
            impact_found.append(token)

    # Normalise: divide by term count (avoid long articles dominating)
    if term_count == 0:
        return 0.0, impact_found

    normalised = raw_score / (term_count + 1)   # +1 avoids single-term spikes
    clamped    = max(-1.0, min(1.0, normalised))
    return float(clamped), impact_found


def _amplifier_weight(tokens: list[str], pos: int) -> float:
    """Return intensity multiplier based on the word immediately before pos."""
    if pos > 0 and tokens[pos - 1] in _AMPLIFIERS:
        return _AMPLIFIERS[tokens[pos - 1]]
    return 1.0


def _is_negated(tokens: list[str], pos: int, window: int = 3) -> bool:
    """Return True if a negator appears in the window before pos."""
    start = max(0, pos - window)
    return any(tokens[i] in _NEGATORS for i in range(start, pos))


def _build_text(article: NewsItem) -> str:
    """Combine title and description into one text block for scoring."""
    parts = [article.title]
    if article.description:
        parts.append(article.description)
    return " ".join(parts)


def _age_hours(published_at: Optional[datetime], now: datetime) -> float:
    """Return age of an article in hours; defaults to 6 h if timestamp missing."""
    if published_at is None:
        return 6.0
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    delta = now - published_at
    return max(0.0, delta.total_seconds() / 3600)


def _label(score: float) -> Literal["positive", "negative", "neutral"]:
    if score > 0.10:
        return "positive"
    if score < -0.10:
        return "negative"
    return "neutral"


def _neutral_result(symbol: str, now: datetime) -> SentimentResult:
    return SentimentResult(
        symbol          = symbol,
        sentiment_score = 0.0,
        impact_score    = 0,
        label           = "neutral",
        news_count      = 0,
        headlines       = [],
        items           = [],
        analyzed_at     = now,
        source          = "simulated",
    )
