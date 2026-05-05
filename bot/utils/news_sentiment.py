"""
News sentiment scorer using Alpaca News API + VADER.
Used as a pre-filter for AI Research strategy — avoids wasting Claude API credits
on stocks with strongly negative sentiment.
No external API key needed — uses existing Alpaca credentials.
"""
import logging
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

_sentiment_cache: dict = {}  # {symbol: {"score": float, "updated": datetime}}
CACHE_TTL_MINUTES = 60

def get_sentiment_score(symbol: str, alpaca_key: str, alpaca_secret: str) -> float:
    """
    Returns a sentiment score for a symbol: -1.0 (very bearish) to +1.0 (very bullish).
    0.0 = neutral or no data.
    Cached for 60 minutes.
    """
    now = datetime.now(timezone.utc)
    cached = _sentiment_cache.get(symbol)
    if cached and (now - cached["updated"]).seconds < CACHE_TTL_MINUTES * 60:
        return cached["score"]

    try:
        # Fetch recent news from Alpaca News API
        since = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
        headers = {
            "APCA-API-KEY-ID": alpaca_key,
            "APCA-API-SECRET-KEY": alpaca_secret,
        }
        resp = requests.get(
            "https://data.alpaca.markets/v1beta1/news",
            headers=headers,
            params={"symbols": symbol, "limit": 10, "start": since},
            timeout=8
        )
        if resp.status_code != 200:
            return 0.0

        articles = resp.json().get("news", [])
        if not articles:
            _sentiment_cache[symbol] = {"score": 0.0, "updated": now}
            return 0.0

        # Score with VADER
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            analyzer = SentimentIntensityAnalyzer()
            scores = []
            for article in articles:
                text = f"{article.get('headline', '')}. {article.get('summary', '')}"
                score = analyzer.polarity_scores(text)["compound"]  # -1 to +1
                scores.append(score)
            avg_score = sum(scores) / len(scores) if scores else 0.0
        except ImportError:
            # VADER not installed — fall back to 0 (neutral)
            avg_score = 0.0

        _sentiment_cache[symbol] = {"score": avg_score, "updated": now}
        logger.debug(f"[Sentiment] {symbol}: {avg_score:+.2f} ({len(articles)} articles)")
        return avg_score

    except Exception as e:
        logger.debug(f"[Sentiment] Error for {symbol}: {e}")
        return 0.0

def is_sentiment_bullish(symbol: str, alpaca_key: str, alpaca_secret: str, min_score: float = -0.2) -> bool:
    """
    Returns True if sentiment is not strongly negative.
    Default: block only if score < -0.2 (clearly bearish news).
    This is intentionally lenient — we only filter out bad news, not require good news.
    """
    score = get_sentiment_score(symbol, alpaca_key, alpaca_secret)
    return score >= min_score
