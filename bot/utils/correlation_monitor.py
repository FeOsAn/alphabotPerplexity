"""
Portfolio Correlation Monitor.
Before entering a new position, checks its correlation with existing holdings.
Blocks entries that would push portfolio-level correlation above threshold.
Runs correlation on 60-day daily returns.
"""
import gc
import logging

logger = logging.getLogger(__name__)

CORRELATION_THRESHOLD = 0.70
LOOKBACK_DAYS = 60


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
        # Fallback: raw alpaca-style get_all_positions
        if hasattr(broker, "get_all_positions"):
            positions = broker.get_all_positions()
            return [p.symbol for p in positions if float(getattr(p, "qty", 0)) > 0]
        if hasattr(broker, "trading"):
            positions = broker.trading.get_all_positions()
            return [p.symbol for p in positions if float(getattr(p, "qty", 0)) > 0]
    except Exception as e:
        logger.debug(f"[CorrMonitor] Could not list positions: {e}")
    return []


def is_entry_allowed(new_symbol: str, broker) -> tuple:
    """
    Check if entering new_symbol would breach correlation threshold.
    Returns (allowed: bool, reason: str)
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
            return True, "Could not fetch returns — allowing entry"

        from concurrent.futures import ThreadPoolExecutor, as_completed
        corr_results = {}
        try:
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(_get_returns, sym): sym for sym in existing_symbols}
                for future in as_completed(futures):
                    sym = futures[future]
                    try:
                        existing_returns = future.result()
                        if existing_returns:
                            corr = _pearson_correlation(new_returns, existing_returns)
                            corr_results[sym] = corr
                    except Exception:
                        pass
        except Exception as e:
            logger.debug(f"[CorrMonitor] Executor error: {e}")
            return True, "Executor error — allowing entry"

        if not corr_results:
            return True, "Could not compute correlations — allowing entry"

        max_corr_sym = max(corr_results, key=corr_results.get)
        max_corr = corr_results[max_corr_sym]

        if max_corr > CORRELATION_THRESHOLD:
            return False, f"Correlation {max_corr:.2f} with {max_corr_sym} exceeds threshold {CORRELATION_THRESHOLD}"

        logger.debug(f"[CorrMonitor] {new_symbol} max correlation: {max_corr:.2f} with {max_corr_sym} — OK")
        return True, f"Max correlation {max_corr:.2f} with {max_corr_sym} — within threshold"

    except Exception as e:
        logger.error(f"[CorrMonitor] Error checking {new_symbol}: {e}")
        return True, f"Error — allowing entry: {e}"
