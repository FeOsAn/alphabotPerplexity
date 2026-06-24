"""
Regime-aware position sizing multipliers.
Strategies call get_multiplier(strategy_name) to scale their position sizes.
"""
from utils.regime_detector import get_regime

# Note: earnings_nlp was deprecated in v90 (dead/orphaned strategy, never
# dispatched, superseded by earnings_prediction) — its weights and TP config
# were removed.
REGIME_WEIGHTS = {
    "BULL_STRONG": {
        # ── Existing strategies ──────────────────────────────────────────────
        "momentum":       1.5,
        "breakout":       1.5,
        "ts_momentum":    1.0,
        "ai_research":    1.2,
        "event_driven":   1.2,
        "vwap_reclaim":   1.2,
        "mean_reversion": 0.7,
        "short_hedge":    0.0,
        "sector_rotation":1.2,
        "spy_dip":        1.2,
        "trend_following":1.2,
        # ── v83 new strategies ───────────────────────────────────────────────
        "trend_pullback": 1.5,   # B+G: bull = full authority, long only
        "multi_tf_rsi":   1.2,   # I:   bull = long only, solid edge
        "52wh_vol":       1.0,   # v87: 52wk-high breakout — bull only at full weight
    },
    "BULL_NORMAL": {
        "momentum":       1.0,
        "breakout":       1.0,
        "ts_momentum":    1.0,
        "ai_research":    1.0,
        "event_driven":   1.0,
        "vwap_reclaim":   1.0,
        "mean_reversion": 1.0,
        "short_hedge":    0.0,
        "sector_rotation":1.0,
        "spy_dip":        1.0,
        "trend_following":1.0,
        "trend_pullback": 1.2,
        "multi_tf_rsi":   1.0,
        "52wh_vol":       1.0,   # v87: bull = full weight
    },
    "CHOPPY": {
        # Trend-following strategies: OFF in chop (proven to bleed)
        "momentum":       0.0,   # was 0.5 — now fully blocked
        "breakout":       0.0,   # was 0.5 — now fully blocked
        "trend_following":0.0,   # new block
        "sector_rotation":0.0,   # new block
        "spy_dip":        0.0,   # new block
        "trend_pullback": 0.0,   # B bleeds -0.42%/trade in chop — blocked
        # Regime-agnostic / counter-trend: keep running
        "ts_momentum":    1.0,
        "ai_research":    0.8,
        "event_driven":   0.8,
        "vwap_reclaim":   0.8,
        "mean_reversion": 1.5,
        "short_hedge":    0.5,
        "multi_tf_rsi":   0.8,   # I: positive in chop (+0.49%), run at 0.8×
        "52wh_vol":       0.5,   # v87: chop = half weight (fewer clean breakouts)
    },
    "BEAR_MILD": {
        "momentum":       0.0,   # was 0.3 — no longs in bear
        "breakout":       0.0,   # was 0.3 — no longs in bear
        "trend_following":0.5,   # short-side only, internally gated
        "sector_rotation":0.0,
        "spy_dip":        0.0,
        "ts_momentum":    1.2,
        "ai_research":    0.5,
        "event_driven":   0.7,
        "vwap_reclaim":   0.0,   # intraday long gap fills — off in bear
        "mean_reversion": 1.2,
        "short_hedge":    1.0,
        "trend_pullback": 0.8,   # short-side entries only, internally gated
        "multi_tf_rsi":   1.3,   # best regime: +0.91% avg/trade, short-heavy
        "52wh_vol":       0.0,   # v87: no longs in bear
    },
    "BEAR_STRONG": {
        "momentum":       0.0,
        "breakout":       0.0,
        "trend_following":0.0,
        "sector_rotation":0.0,
        "spy_dip":        0.0,
        "ts_momentum":    1.5,
        "ai_research":    0.0,
        "event_driven":   0.5,
        "vwap_reclaim":   0.0,
        "mean_reversion": 1.0,
        "short_hedge":    1.5,
        "trend_pullback": 0.6,   # short-side only, conservative
        "multi_tf_rsi":   1.5,   # maximum authority in strong bear
        "52wh_vol":       0.0,   # v87: no longs in bear
    },
    # v95 — composite-score "transition" regime (score 40–70). Momentum/trend
    # strategies are fully blocked; defensive/counter-trend run at 0.75×;
    # dual_momentum runs full (its own cross-asset filter handles it). This map
    # is selected by get_multiplier() when the composite regime is "transition",
    # independent of the HMM 5-state vocabulary.
    "TRANSITION": {
        # momentum / trend / breakout — blocked
        "momentum":        0.0,
        "breakout":        0.0,
        "trend_following": 0.0,
        "sector_rotation": 0.0,
        "spy_dip":         0.75,  # dip-buy is counter-trend / defensive
        "trend_pullback":  0.0,
        "52wh_vol":        0.0,
        "cs_momentum":     0.0,
        "quality_momentum":0.0,
        "ts_momentum":     0.0,
        "gap_scanner":     0.0,
        "earnings_drift":  0.0,
        "conviction_long": 0.0,
        # defensive / counter-trend — reduced
        "mean_reversion":  0.75,
        "vwap_reclaim":    0.75,
        "short_hedge":     0.75,
        "vix_reversal":    0.75,
        "multi_tf_rsi":    0.75,
        "ai_research":     0.75,
        "event_driven":    0.75,
        # cross-asset — its own filter handles it
        "dual_momentum":   1.0,
    },
}

# v85 — regime compatibility per strategy. Drives check_regime_exits(): when the
# market regime flips, any open position whose opening_strategy is NOT compatible
# with the new regime is closed at market. Uses the 3-tier regime vocabulary
# ("bull"/"chop"/"bear") emitted by main.get_market_regime().
# v95: "transition" added to defensive / counter-trend / regime-agnostic
# strategies so their existing positions are HELD and managed (not force-swept)
# when the composite regime is confirmed as "transition". Momentum/trend sleeves
# deliberately omit "transition" → their longs are swept on a confirmed
# transition (the trend has weakened across 3 confirming closes).
STRATEGY_REGIME_COMPAT = {
    "momentum":         ["bull", "chop"],
    "breakout":         ["bull", "chop"],
    "trend_pullback":   ["bull", "chop"],
    "squeeze_screener": ["bull", "chop"],
    "short_hedge":      ["bear", "chop", "transition"],
    "mean_reversion":   ["bull", "chop", "bear", "transition"],
    "multi_tf_rsi":     ["bull", "chop", "bear", "transition"],
    "vwap_reclaim":     ["bull", "chop", "bear", "transition"],
    "vix_reversal":     ["bear", "chop", "transition"],
    "spy_dip":          ["bull", "chop", "transition"],
    "earnings_drift":   ["bull", "chop", "bear", "transition"],
    "sector_rotation":  ["bull", "chop"],
    "pairs_trading":    ["bull", "chop", "bear", "transition"],
    "trend_following":  ["bull", "chop"],
    "52wh_vol":         ["bull", "chop"],
    "conviction_long":  ["bull", "chop"],
    # v90: previously missing — these open longs but were never swept on a
    # regime flip. earnings_prediction has no regime gate (dispatched in all
    # regimes); ai_research carries nonzero weight through bear_mild.
    "earnings_prediction": ["bull", "chop", "bear", "transition"],
    "ai_research":         ["bull", "chop", "bear", "transition"],
    # v91: deep-backtest momentum sleeves.
    "cs_momentum":      ["bull"],                  # pure 12-1 momentum, bull only
    "quality_momentum": ["bull", "chop"],          # quality tilt survives chop
    "dual_momentum":    ["bull", "chop", "bear", "transition"],  # rotates to GLD, all regimes
}

DEFAULT_MULTIPLIER = 1.0
# v95 — strategies not listed in the TRANSITION map default to defensive sizing
# (treated as 0.75× rather than full) so an unknown strategy can't size up during
# a fragile regime transition.
TRANSITION_DEFAULT_MULTIPLIER = 0.75


def get_multiplier(strategy: str) -> float:
    """
    Return position size multiplier for the given strategy based on current regime.

    v95: when the composite-score regime is "transition", use the dedicated
    TRANSITION weight map (momentum blocked, defensive at 0.75×, dual_momentum
    full), scaled by the state-machine transition_confidence. Otherwise fall back
    to the HMM 5-state vocabulary.
    """
    try:
        from utils.regime_detector import get_regime_context
        ctx = get_regime_context(strategy)
        if ctx.get("regime") == "transition":
            mult = REGIME_WEIGHTS["TRANSITION"].get(strategy, TRANSITION_DEFAULT_MULTIPLIER)
            mult *= ctx.get("transition_confidence", 1.0)
            return max(0.0, mult)
    except Exception:
        pass
    try:
        regime, confidence = get_regime()
        weights = REGIME_WEIGHTS.get(regime, {})
        multiplier = weights.get(strategy, DEFAULT_MULTIPLIER)
        if confidence < 0.6:
            multiplier = 1.0 + (multiplier - 1.0) * confidence
        return max(0.0, multiplier)
    except Exception:
        return DEFAULT_MULTIPLIER
