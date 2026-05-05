"""
Adaptive market filter system.
Assesses market conditions every hour and sets dynamic signal thresholds.
Strategies import get_thresholds() to get the current regime's parameters.
"""
import logging
import gc
from datetime import datetime, timezone, timedelta
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Regime thresholds ────────────────────────────────────────────────────────
REGIME_THRESHOLDS = {
    "BULL_STRONG": {
        "momentum_rsi_max": 82,
        "momentum_score_min": 0.0,
        "breakout_vol_min": 1.1,
        "breakout_proximity": 0.90,
        "breakout_rsi_max": 80,
        "mr_rsi_oversold": 35,
        "tf_adx_min": 22,
        "max_new_positions_per_cycle": 3,
    },
    "BULL_NORMAL": {
        "momentum_rsi_max": 78,
        "momentum_score_min": 0.0,
        "breakout_vol_min": 1.2,
        "breakout_proximity": 0.92,
        "breakout_rsi_max": 78,
        "mr_rsi_oversold": 32,
        "tf_adx_min": 25,
        "max_new_positions_per_cycle": 2,
    },
    "BEAR_MILD": {
        "momentum_rsi_max": 65,
        "momentum_score_min": 0.02,
        "breakout_vol_min": 1.5,
        "breakout_proximity": 0.95,
        "breakout_rsi_max": 70,
        "mr_rsi_oversold": 28,
        "tf_adx_min": 28,
        "max_new_positions_per_cycle": 1,
    },
    "BEAR_STRONG": {
        "momentum_rsi_max": 55,
        "momentum_score_min": 0.05,
        "breakout_vol_min": 2.0,
        "breakout_proximity": 0.97,
        "breakout_rsi_max": 60,
        "mr_rsi_oversold": 25,
        "tf_adx_min": 30,
        "max_new_positions_per_cycle": 0,
    },
}

# ── Cache (refresh hourly) ───────────────────────────────────────────────────
_cache: dict = {}
CACHE_TTL_MINUTES = 60


def _assess_regime() -> tuple[str, dict]:
    """Fetch market data and classify the current regime. Returns (regime_name, details_dict)."""
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1y", interval="1d")
        gc.collect()

        if hist.empty or len(hist) < 200:
            logger.warning("[AdaptiveFilters] Not enough SPY history — defaulting to BULL_NORMAL")
            return "BULL_NORMAL", {}

        close = hist["Close"]
        price = float(close.iloc[-1])
        ma50 = float(close.tail(50).mean())
        ma200 = float(close.tail(200).mean())
        mom_20d = (price - float(close.iloc[-21])) / float(close.iloc[-21]) if len(close) >= 21 else 0.0

        # VIX via VIXY (ETF proxy — always available)
        try:
            vixy = yf.Ticker("VIXY")
            vixy_hist = vixy.history(period="5d", interval="1d")
            gc.collect()
            vixy_price = float(vixy_hist["Close"].iloc[-1]) if not vixy_hist.empty else 20.0
            vix_proxy = vixy_price
        except Exception:
            vix_proxy = 20.0

        details = {
            "spy_price": round(price, 2),
            "ma50": round(ma50, 2),
            "ma200": round(ma200, 2),
            "above_ma50": price > ma50,
            "above_ma200": price > ma200,
            "vix_proxy": round(vix_proxy, 2),
            "mom_20d_pct": round(mom_20d * 100, 2),
        }

        # Count bearish signals
        bearish_signals = sum([
            price < ma50,
            price < ma200,
            vix_proxy > 25,
            mom_20d < 0,
        ])

        # Classify based on signal count + severity
        if price < ma200 or vix_proxy > 35 or mom_20d < -0.03:
            regime = "BEAR_STRONG"  # any single extreme trigger
        elif bearish_signals >= 2:
            regime = "BEAR_MILD"    # need at least 2 signals to flip bear
        elif price > ma50 and price > ma200 and vix_proxy < 20 and mom_20d > 0.02:
            regime = "BULL_STRONG"
        else:
            regime = "BULL_NORMAL"

        logger.info(
            f"[AdaptiveFilters] Regime={regime} (bearish_signals={bearish_signals}/4) | "
            f"SPY=${price:.2f} MA50=${ma50:.2f} MA200=${ma200:.2f} | "
            f"VIX~{vix_proxy:.1f} | Mom20d={mom_20d:+.1%}"
        )
        return regime, details

    except Exception as e:
        logger.warning(f"[AdaptiveFilters] Assessment failed: {e} — defaulting to BULL_NORMAL")
        return "BULL_NORMAL", {}


def get_thresholds() -> dict:
    """
    Return current regime thresholds. Refreshes every 60 minutes.
    Always safe to call — returns BULL_NORMAL defaults on any failure.
    """
    global _cache
    now = datetime.now(timezone.utc)

    if _cache and (now - _cache.get("updated_at", now - timedelta(hours=2))).seconds < CACHE_TTL_MINUTES * 60:
        return _cache["thresholds"]

    regime, details = _assess_regime()
    thresholds = REGIME_THRESHOLDS.get(regime, REGIME_THRESHOLDS["BULL_NORMAL"]).copy()
    thresholds["regime"] = regime
    thresholds["details"] = details

    _cache = {
        "regime": regime,
        "thresholds": thresholds,
        "updated_at": now,
        "details": details,
    }
    return thresholds


def get_regime() -> str:
    """Convenience — just returns the regime name string."""
    return get_thresholds().get("regime", "BULL_NORMAL")
