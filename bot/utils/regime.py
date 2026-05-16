"""
Market regime filter.
Returns True if market is in a BULL regime (safe to open new longs).
Returns False if SPY is below its 50-day SMA → defense mode, no new entries.

Also exposes current_regime() — the unified regime string entry point
(QW10). When the HMM regime_detector is confident, that result is used;
otherwise falls back to the SPY-MA50 bull/bear classification.
"""
import logging
import yfinance as yf
import gc
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_regime_cache: dict = {}  # {"date": str, "bull": bool}


def is_bull_market() -> bool:
    """Return True if SPY is above its 50-day SMA (bull regime)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _regime_cache.get("date") == today:
        return _regime_cache["bull"]
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="3mo", interval="1d")
        gc.collect()
        if hist.empty or len(hist) < 50:
            logger.warning("[Regime] Not enough SPY data — assuming bull")
            return True
        sma50 = hist["Close"].rolling(50).mean().iloc[-1]
        price = hist["Close"].iloc[-1]
        bull = price > sma50
        _regime_cache["date"] = today
        _regime_cache["bull"] = bull
        logger.info(f"[Regime] SPY={price:.2f} SMA50={sma50:.2f} → {'BULL' if bull else 'BEAR'}")
        return bull
    except Exception as e:
        logger.warning(f"[Regime] Error checking regime: {e} — assuming bull")
        return True


def current_regime() -> str:
    """
    Unified regime string (QW10).
    Returns one of: BULL_STRONG, BULL_NORMAL, CHOPPY, BEAR_MILD, BEAR_STRONG.
    Falls back to bull/bear from is_bull_market() if the HMM detector
    isn't confident.
    """
    try:
        from utils.regime_detector import get_regime as _hmm_get_regime
        regime, conf = _hmm_get_regime()
        if regime and conf >= 0.5:
            return regime
    except Exception:
        pass
    return "BULL_NORMAL" if is_bull_market() else "BEAR_MILD"
