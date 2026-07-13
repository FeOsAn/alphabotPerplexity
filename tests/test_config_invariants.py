"""
Config / wiring invariants. Catches the "strategy added but not wired" class:
donchian_trend silently capped at the 0.15 default ceiling, missing regime
weights, missing tp_engine entries. Imports only config + pure-python modules
(no alpaca / pandas_ta needed).
"""
import re
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))

import config  # noqa: E402


def test_caps_are_mutually_consistent():
    assert 0 < config.MIN_CASH_RESERVE_PCT < 0.5
    assert 0.5 <= config.MAX_PORTFOLIO_EXPOSURE <= 1.0
    assert config.MIN_CASH_RESERVE_PCT + config.MAX_PORTFOLIO_EXPOSURE <= 1.001, \
        "cash floor + exposure cap must not overcommit equity"
    assert 0 < config.INDEX_ETF_CLUSTER_CAP < config.MAX_PORTFOLIO_EXPOSURE
    assert 0 < config.VOL_TARGET_ANNUAL < 0.5
    assert 0 < config.VOL_SCALAR_FLOOR <= 1.0
    assert 0 <= config.CIRCUIT_BREAKER_DAILY_LOSS_PCT < 0.2


def _dispatched_strategies():
    """Strategy names actually run by main.py (via `X.run` references)."""
    src = (ROOT / "bot" / "main.py").read_text()
    return set(re.findall(r"(\w+)\.run\b", src)) & {
        "mean_reversion", "trend_following", "spy_dip", "vix_reversal",
        "gap_scanner", "earnings_drift", "sector_rotation", "momentum",
        "breakout", "short_hedge", "pairs_trading", "insider_buying",
        "options_flow", "squeeze_screener", "trend_pullback", "multi_tf_rsi",
        "conviction_long", "cs_momentum", "quality_momentum", "dual_momentum",
        "donchian_trend", "crypto_trend", "gold_trend", "ts_momentum",
        "earnings_prediction", "ai_research", "vwap_reclaim", "event_driven",
    }


def test_every_dispatched_strategy_is_in_regime_compat():
    """v85 regime exits skip unknown strategies (safe) but every LIVE strategy
    should be classified deliberately, not by accident."""
    src = (ROOT / "bot" / "utils" / "regime_weights.py").read_text()
    compat_block = src[src.index("STRATEGY_REGIME_COMPAT"):]
    missing = []
    # fifty_two_wh dispatches as module fifty_two_wh but compat key is 52wh_vol
    aliases = {"fifty_two_wh": "52wh_vol"}
    for s in _dispatched_strategies():
        key = aliases.get(s, s)
        # event_driven/news pipeline strategies are managed inside their modules
        if key in ("event_driven",):
            continue
        if f'"{key}"' not in compat_block:
            missing.append(key)
    assert not missing, f"strategies dispatched but absent from STRATEGY_REGIME_COMPAT: {missing}"


def test_new_sleeves_have_explicit_capital_ceilings():
    """The donchian lesson: falling to the 0.15 default silently halves a
    sleeve. Every sleeve whose design size exceeds the default must have an
    explicit ceiling."""
    for strat in ("donchian_trend", "crypto_trend", "conviction_long",
                  "cs_momentum", "quality_momentum", "dual_momentum", "gold_trend"):
        assert strat in config.STRATEGY_CAPITAL_LIMITS, \
            f"{strat} missing explicit STRATEGY_CAPITAL_LIMITS entry (would fall to default)"


def test_no_tp_strategies_are_marked_in_tp_engine():
    """Strategies whose exits are self-managed must be (0,0) in tp_engine so the
    bracket engine places a plain protective stop instead of a bogus TP."""
    src = (ROOT / "bot" / "strategies" / "tp_engine.py").read_text()
    for strat in ("donchian_trend", "crypto_trend", "gold_trend",
                  "sector_rotation", "pairs_trading", "short_hedge"):
        m = re.search(r'"%s":\s*\(([\d.]+),\s*([\d.]+)\)' % strat, src)
        assert m, f"{strat} missing from tp_engine._TP_MULTIPLES"
        assert float(m.group(1)) == 0.0 and float(m.group(2)) == 0.0, \
            f"{strat} must be (0.0, 0.0) — its exits are self-managed"
