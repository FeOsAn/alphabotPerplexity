"""
Regime-aware position sizing multipliers.
Strategies call get_multiplier(strategy_name) to scale their position sizes.
"""
from utils.regime_detector import get_regime

REGIME_WEIGHTS = {
    "BULL_STRONG": {
        "momentum":     1.5,
        "breakout":     1.5,
        "earnings_nlp": 1.2,
        "ts_momentum":  1.0,
        "ai_research":  1.2,
        "event_driven": 1.2,
        "mean_reversion": 0.7,
        "short_hedge":  0.0,
    },
    "BULL_NORMAL": {
        "momentum":     1.0,
        "breakout":     1.0,
        "earnings_nlp": 1.0,
        "ts_momentum":  1.0,
        "ai_research":  1.0,
        "event_driven": 1.0,
        "mean_reversion": 1.0,
        "short_hedge":  0.0,
    },
    "CHOPPY": {
        "momentum":     0.5,
        "breakout":     0.5,
        "earnings_nlp": 0.8,
        "ts_momentum":  1.0,
        "ai_research":  0.8,
        "event_driven": 0.8,
        "mean_reversion": 1.5,
        "short_hedge":  0.5,
    },
    "BEAR_MILD": {
        "momentum":     0.3,
        "breakout":     0.3,
        "earnings_nlp": 0.7,
        "ts_momentum":  1.2,
        "ai_research":  0.5,
        "event_driven": 0.7,
        "mean_reversion": 1.2,
        "short_hedge":  1.0,
    },
    "BEAR_STRONG": {
        "momentum":     0.0,
        "breakout":     0.0,
        "earnings_nlp": 0.5,
        "ts_momentum":  1.5,
        "ai_research":  0.0,
        "event_driven": 0.5,
        "mean_reversion": 1.0,
        "short_hedge":  1.5,
    },
}

DEFAULT_MULTIPLIER = 1.0


def get_multiplier(strategy: str) -> float:
    """
    Return position size multiplier for the given strategy based on current regime.
    """
    try:
        regime, confidence = get_regime()
        weights = REGIME_WEIGHTS.get(regime, {})
        multiplier = weights.get(strategy, DEFAULT_MULTIPLIER)
        if confidence < 0.6:
            multiplier = 1.0 + (multiplier - 1.0) * confidence
        return max(0.0, multiplier)
    except Exception:
        return DEFAULT_MULTIPLIER
