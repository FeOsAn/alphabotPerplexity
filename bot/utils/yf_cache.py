"""
Per-cycle yfinance memoize.
The bot's main loop fires every 5 minutes; many strategies hit the same
tickers in sequence. This cache stores history() results for ~4.5 min
so subsequent strategies in the same cycle reuse the fetch.
"""
import time
import yfinance as yf

_cache: dict = {}
_cache_ts: dict = {}
CACHE_TTL = 270  # 4.5 minutes (cycle is 5 min — clear at top of next cycle)


def get_history(symbol: str, period: str = "1d", interval: str = "1m"):
    """Cached yf.Ticker(symbol).history(period=..., interval=...)."""
    key = (symbol, period, interval)
    now = time.time()
    if key in _cache and now - _cache_ts[key] < CACHE_TTL:
        return _cache[key]
    data = yf.Ticker(symbol).history(period=period, interval=interval)
    _cache[key] = data
    _cache_ts[key] = now
    return data


def clear_cycle_cache():
    """Drop everything — called at the start of each main loop cycle."""
    _cache.clear()
    _cache_ts.clear()
