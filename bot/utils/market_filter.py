"""
Market regime + volatility filter for the drawdown-control overlay.
------------------------------------------------------------------
Two exposure scalars, both driven by one cached SPY fetch (1h TTL, fail-safe):

1. Vol-target (primary DD lever). Scale exposure by TARGET_VOL / realised_vol so
   the book runs near a target volatility; when markets get choppy, exposure is
   automatically cut. Backtest (backtests/dd_reduction.py, 2015-2026): a 12% vol
   target roughly HALVED max drawdown (-32.6% -> -13.6%) and the 2022-flip
   drawdown (-21.8% -> -12.9%), lifted Sharpe 1.16 -> 1.34 and nearly doubled
   Calmar (0.70 -> 1.22), while keeping ~17% CAGR (well above SPY). Best DD/return
   tradeoff tested.

2. 200DMA trend gate (secondary). Below SPY's 200DMA, apply an additional cut.
   Vol-targeting largely subsumes it, but it's a cheap trend backstop.

The effective exposure cap = base_cap * min(vol_scalar, ma200_mult). Scalars are
capped at 1.0 (no leverage) and floored (VOL_SCALAR_FLOOR) so the book never goes
fully flat on a data blip. Live realisation is NO forced selling: the cap gates
NEW entries; existing positions run off via normal exits, so exposure drifts to
the target. Fails safe to the full cap on any data error.
"""
from __future__ import annotations
import time
import logging
import numpy as np

logger = logging.getLogger("alphabot.market_filter")

_CACHE_TTL = 3600.0     # 1 hour
_cache: dict | None = None
_cache_ts: float = 0.0


def _defaults():
    try:
        from config import (REGIME_DERISK_EXPOSURE_MULT, VOL_TARGET_ANNUAL,
                            VOL_SCALAR_FLOOR)
        return REGIME_DERISK_EXPOSURE_MULT, VOL_TARGET_ANNUAL, VOL_SCALAR_FLOOR
    except Exception:
        return 0.60, 0.12, 0.30


def _compute() -> dict:
    """One SPY fetch → 200DMA state + 20d realised vol. Cached 1h."""
    import yfinance as yf
    hist = yf.Ticker("SPY").history(period="320d", interval="1d")
    close = hist["Close"].dropna()
    if len(close) < 200:
        raise ValueError(f"only {len(close)} SPY closes, need 200")
    ma200 = float(close.tail(200).mean())
    spy_now = float(close.iloc[-1])
    rets = close.pct_change().dropna()
    realised_vol = float(rets.tail(20).std() * np.sqrt(252))
    return {
        "below_ma200": spy_now < ma200,
        "realised_vol": realised_vol,
        "detail": f"SPY={spy_now:.2f} MA200={ma200:.2f} rvol={realised_vol*100:.0f}%",
    }


def _state() -> dict | None:
    global _cache, _cache_ts
    now = time.time()
    if _cache is not None and (now - _cache_ts) < _CACHE_TTL:
        return _cache
    try:
        _cache, _cache_ts = _compute(), now
        return _cache
    except Exception as e:
        logger.debug(f"[MarketFilter] SPY fetch failed ({e}) — assuming full exposure")
        return None


def spy_below_ma200() -> bool:
    st = _state()
    return bool(st["below_ma200"]) if st else False


def vol_target_scalar() -> float:
    """TARGET_VOL / realised_vol, capped [floor, 1.0]. 1.0 on data error."""
    st = _state()
    if not st or st["realised_vol"] <= 0:
        return 1.0
    _, target, floor = _defaults()
    return float(np.clip(target / st["realised_vol"], floor, 1.0))


def effective_exposure_cap(base_cap: float) -> float:
    """base_cap * min(vol-target scalar, 200DMA mult). Full cap on data error."""
    st = _state()
    if not st:
        return base_cap
    mult, target, floor = _defaults()
    vt = float(np.clip(target / st["realised_vol"], floor, 1.0)) if st["realised_vol"] > 0 else 1.0
    ma = mult if st["below_ma200"] else 1.0
    scalar = min(vt, ma)
    if scalar < 0.999:
        logger.info(f"[MarketFilter] exposure scalar {scalar:.2f} "
                    f"(voltgt {vt:.2f}, ma200 {ma:.2f}) — {st['detail']}")
    return base_cap * scalar
