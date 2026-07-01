"""
Market regime filter for the partial cash-defense overlay.
--------------------------------------------------------
Backtest (backtests/regime_overlay.py, 2015-2026): de-risking part of the long
book to CASH when SPY < 200-day MA is the best return / regime-flip-loss
tradeoff. A 60/40 blend (60% of the book stays long, 40% goes to cash below the
200DMA) kept ~17% 5-yr CAGR while cutting the 2022-flip drawdown from -21.8% to
-18.4% and lifting Sharpe 1.08->1.20. Rotating to GLD instead of cash made 2022
*worse* (-29.6%) — GLD sold off with equities — so this uses cash, not GLD.

Live realisation (no forced selling): when SPY < 200DMA we scale the hard
portfolio exposure cap down by REGIME_DERISK_EXPOSURE_MULT (0.60), i.e. from 80%
to ~48%. New long entries are blocked past the lower cap; existing positions run
off via the normal exit/regime logic, so exposure drifts toward the reduced cap.

Kept intentionally DECOUPLED from regime_scorer (which downloads only 90d and is
tuned) — this needs 200 days. 1-hour TTL cache; fails safe to "not below" (full
exposure = current behaviour) on any data error, so a yfinance glitch never
silently de-risks the book.
"""
from __future__ import annotations
import time
import logging

logger = logging.getLogger("alphabot.market_filter")

_CACHE_TTL = 3600.0     # 1 hour — the 200DMA moves slowly
_cache_val: bool | None = None
_cache_ts: float = 0.0
_last_detail: str = ""


def _compute_spy_below_ma200() -> bool:
    import yfinance as yf
    hist = yf.Ticker("SPY").history(period="320d", interval="1d")
    close = hist["Close"].dropna()
    if len(close) < 200:
        raise ValueError(f"only {len(close)} SPY closes, need 200")
    ma200 = float(close.tail(200).mean())
    spy_now = float(close.iloc[-1])
    global _last_detail
    _last_detail = f"SPY={spy_now:.2f} MA200={ma200:.2f} ({(spy_now/ma200-1)*100:+.1f}%)"
    return spy_now < ma200


def spy_below_ma200() -> bool:
    """True if SPY closed below its 200-day MA (cached 1h). Fail-safe: False."""
    global _cache_val, _cache_ts
    now = time.time()
    if _cache_val is not None and (now - _cache_ts) < _CACHE_TTL:
        return _cache_val
    try:
        val = _compute_spy_below_ma200()
        _cache_val, _cache_ts = val, now
        if val:
            logger.info(f"[MarketFilter] SPY below 200DMA — cash-defense ON ({_last_detail})")
        return val
    except Exception as e:
        logger.debug(f"[MarketFilter] SPY/200DMA check failed ({e}) — assuming full exposure")
        return False


def effective_exposure_cap(base_cap: float) -> float:
    """Scale the portfolio exposure cap down when SPY is below its 200DMA."""
    try:
        from config import REGIME_DERISK_EXPOSURE_MULT
    except Exception:
        REGIME_DERISK_EXPOSURE_MULT = 0.60
    if spy_below_ma200():
        return base_cap * REGIME_DERISK_EXPOSURE_MULT
    return base_cap
