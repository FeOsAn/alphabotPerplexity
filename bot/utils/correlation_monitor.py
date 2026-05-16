"""
Portfolio Correlation Monitor.
Before entering a new position, checks its correlation with existing holdings.
Blocks entries that would push portfolio-level correlation above threshold.
Runs correlation on 60-day daily returns.

Fails CLOSED on missing data — we'd rather skip a candidate than pile in
correlated longs when yfinance is rate-limited.
"""
import gc
import logging
import time

logger = logging.getLogger(__name__)

CORRELATION_THRESHOLD = 0.70
LOOKBACK_DAYS = 60

# Per-cycle cache of existing-position returns (M11) — TTL 4 minutes
_returns_cache: dict = {"ts": 0.0, "data": {}}
_CACHE_TTL = 240  # seconds


def _get_returns(symbol: str) -> list:
    """Get last 60 days of daily returns for symbol."""
    import yfinance as yf
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3mo", interval="1d", auto_adjust=True)
        if hist is None or len(hist) < 20:
            return []
        closes = hist["Close"].tolist()
        returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1] != 0]
        return returns[-LOOKBACK_DAYS:]
    except Exception:
        return []
    finally:
        gc.collect()


def _pearson_correlation(a: list, b: list) -> float:
    """Compute Pearson correlation between two return series."""
    n = min(len(a), len(b))
    if n < 10:
        return 0.0
    a, b = a[-n:], b[-n:]
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    num = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))
    den_a = (sum((x - mean_a) ** 2 for x in a)) ** 0.5
    den_b = (sum((x - mean_b) ** 2 for x in b)) ** 0.5
    if den_a == 0 or den_b == 0:
        return 0.0
    return num / (den_a * den_b)


def _existing_long_symbols(broker) -> list:
    """Extract long-only existing position symbols, robust to broker API style."""
    try:
        if hasattr(broker, "get_positions"):
            positions = broker.get_positions()
            return [p["symbol"] for p in positions if float(p.get("qty", 0)) > 0]
        if hasattr(broker, "get_all_positions"):
            positions = broker.get_all_positions()
            return [p.symbol for p in positions if float(getattr(p, "qty", 0)) > 0]
        if hasattr(broker, "trading"):
            positions = broker.trading.get_all_positions()
            return [p.symbol for p in positions if float(getattr(p, "qty", 0)) > 0]
    except Exception as e:
        logger.debug(f"[CorrMonitor] Could not list positions: {e}")
    return []


def _existing_returns(existing_symbols: list) -> dict:
    """
    Fetch returns for each existing-position symbol, using a per-cycle cache
    so candidate-loop scans don't re-fetch the same matrix every time.
    """
    now = time.time()
    if now - _returns_cache["ts"] < _CACHE_TTL and _returns_cache["data"]:
        # Use cached, intersected with current symbol set
        cached = {s: r for s, r in _returns_cache["data"].items() if s in existing_symbols}
        # If all symbols are present in cache, return as-is
        if set(cached.keys()) >= set(existing_symbols):
            return cached

    from concurrent.futures import ThreadPoolExecutor, as_completed
    out = {}
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_get_returns, sym): sym for sym in existing_symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    r = future.result()
                    if r:
                        out[sym] = r
                except Exception:
                    pass
    except Exception as e:
        logger.debug(f"[CorrMonitor] Executor error: {e}")

    _returns_cache["ts"] = now
    _returns_cache["data"] = out
    return out


def is_entry_allowed(new_symbol: str, broker) -> tuple:
    """
    Check if entering new_symbol would breach correlation threshold.
    Returns (allowed: bool, reason: str)

    H8: fail CLOSED on missing data — skip the entry rather than pile in
    correlated longs while yfinance is rate-limited.
    """
    try:
        existing_symbols = _existing_long_symbols(broker)
        if not existing_symbols:
            return True, "No existing long positions"

        # Skip self-correlation
        existing_symbols = [s for s in existing_symbols if s != new_symbol]
        if not existing_symbols:
            return True, "Only existing position is this symbol"

        new_returns = _get_returns(new_symbol)
        if not new_returns:
            # H8: fail CLOSED — was previously "allowing entry"
            return False, "Could not fetch returns — skipping entry to be safe"

        corr_results = {}
        existing_returns = _existing_returns(existing_symbols)
        for sym, r in existing_returns.items():
            try:
                corr_results[sym] = _pearson_correlation(new_returns, r)
            except Exception:
                pass

        if not corr_results:
            return False, "Could not compute correlations — skipping entry to be safe"

        max_corr_sym = max(corr_results, key=corr_results.get)
        max_corr = corr_results[max_corr_sym]

        if max_corr > CORRELATION_THRESHOLD:
            return False, f"Correlation {max_corr:.2f} with {max_corr_sym} exceeds threshold {CORRELATION_THRESHOLD}"

        logger.debug(f"[CorrMonitor] {new_symbol} max correlation: {max_corr:.2f} with {max_corr_sym} — OK")
        return True, f"Max correlation {max_corr:.2f} with {max_corr_sym} — within threshold"

    except Exception as e:
        logger.error(f"[CorrMonitor] Error checking {new_symbol}: {e}")
        return False, f"Error — skipping entry: {e}"
