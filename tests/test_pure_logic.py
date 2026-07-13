"""
Pure-logic unit tests for the risk plumbing (no network, no alpaca).
"""
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))


def test_market_filter_scalars():
    import utils.market_filter as mf
    # fail-safe: no data -> full cap
    mf._cache, mf._cache_ts = None, 0.0
    assert mf.effective_exposure_cap(0.90) == 0.90
    # calm: above 200DMA, low vol -> full cap
    mf._cache = {"below_ma200": False, "realised_vol": 0.10, "detail": "t"}
    mf._cache_ts = 9e18
    assert abs(mf.effective_exposure_cap(0.90) - 0.90) < 1e-9
    # choppy: vol 0.30 -> scalar = VOL_TARGET/0.30 (floored)
    from config import VOL_TARGET_ANNUAL, VOL_SCALAR_FLOOR
    mf._cache = {"below_ma200": False, "realised_vol": 0.30, "detail": "t"}
    expect = max(VOL_TARGET_ANNUAL / 0.30, VOL_SCALAR_FLOOR)
    assert abs(mf.effective_exposure_cap(0.90) - 0.90 * expect) < 1e-9
    # crisis: below 200DMA + huge vol -> min(floor, ma_mult)
    mf._cache = {"below_ma200": True, "realised_vol": 0.60, "detail": "t"}
    from config import REGIME_DERISK_EXPOSURE_MULT
    expect = min(max(VOL_TARGET_ANNUAL / 0.60, VOL_SCALAR_FLOOR), REGIME_DERISK_EXPOSURE_MULT)
    assert abs(mf.effective_exposure_cap(0.90) - 0.90 * expect) < 1e-9
    mf._cache, mf._cache_ts = None, 0.0  # reset


def test_watchdog_stop_price_long_and_short():
    from utils.stop_watchdog import _target_stop_price, _TIERS
    # long: with tiers disabled, floor = entry * (1 - base_stop)
    px = _target_stop_price(entry=100.0, current=110.0, is_short=False, base_stop=0.06)
    if not _TIERS:
        assert px == 94.0
    assert px < 110.0  # a long's sell-stop must sit below market
    # short: buy-stop above entry
    px_s = _target_stop_price(entry=100.0, current=95.0, is_short=True, base_stop=0.06)
    assert px_s == 106.0 and px_s > 95.0


def test_ratchet_disabled_means_base_stop_only():
    """v100.6: with _ATR_RATCHET_TIERS = [], the ratchet must return the base
    stop (or an existing tighter stop) and never invent a floor."""
    import strategies.trade_management as tm
    assert tm._ATR_RATCHET_TIERS == [], \
        "trailing tiers were re-enabled without going through a backtest PR"
    tm._ratchet_stops.pop("TEST", None)
    out = tm._get_ratchet_stop("TEST", current_pnl_pct=0.15, base_stop_pct=0.06)
    assert out == -0.06, f"with tiers off, a +15% winner must keep its -6% base stop, got {out}"


def test_cooldown_hours_config():
    from config import SYMBOL_COOLDOWN_HOURS
    assert SYMBOL_COOLDOWN_HOURS >= 24, "cooldown below 24h re-enables stop-out churn"
