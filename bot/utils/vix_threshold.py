"""
vix_threshold.py — Dynamic parameter adjustment based on VIX regime.
v80: vol_ratio threshold varies with VIX level.
"""
import logging
import yfinance as yf

logger = logging.getLogger("alphabot.vix_threshold")

_vix_cache: dict = {"value": None, "date": None}


def get_vix() -> float:
    """Fetch current VIX level. Cached per day."""
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _vix_cache["date"] == today and _vix_cache["value"] is not None:
        return _vix_cache["value"]
    try:
        df = yf.download("^VIX", period="2d", interval="1d", progress=False, auto_adjust=True)
        if df is not None and not df.empty:
            vix = float(df["Close"].dropna().iloc[-1])
            _vix_cache["value"] = vix
            _vix_cache["date"] = today
            logger.debug(f"[VIX] Current VIX={vix:.1f}")
            return vix
    except Exception as e:
        logger.debug(f"[VIX] fetch failed: {e}")
    return 20.0  # fallback to neutral


def get_vol_ratio_threshold() -> float:
    """Return the appropriate vol_ratio entry threshold based on current VIX."""
    try:
        from config import VOL_RATIO_LOW_VIX, VOL_RATIO_MID_VIX, VOL_RATIO_HIGH_VIX, MIN_VOL_RATIO
        vix = get_vix()
        if vix < 20:
            threshold = VOL_RATIO_LOW_VIX
        elif vix <= 30:
            threshold = VOL_RATIO_MID_VIX
        else:
            threshold = VOL_RATIO_HIGH_VIX
        logger.debug(f"[VIX] VIX={vix:.1f} → vol_ratio threshold={threshold}")
        return threshold
    except Exception:
        return 1.5  # safe fallback
