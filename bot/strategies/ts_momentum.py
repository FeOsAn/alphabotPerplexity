"""
Time-Series Momentum Strategy — AlphaBot
Applies 12-month trend following across macro ETFs and sector ETFs.
Long if 12-month return > 0, short (or flat) if < 0.
Rebalances monthly. Near-zero correlation to equity strategies.
Documented to generate positive returns in 2008 and 2022 bear markets.
"""
import logging
import gc
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# Universe — liquid macro ETFs and sector ETFs
TS_UNIVERSE = [
    # Broad market
    "SPY",   # S&P 500
    "QQQ",   # Nasdaq
    "IWM",   # Russell 2000
    "EFA",   # International developed
    "EEM",   # Emerging markets
    # Fixed income
    "TLT",   # 20Y Treasury
    "IEF",   # 7-10Y Treasury
    "HYG",   # High yield corporate
    "LQD",   # Investment grade corporate
    # Commodities
    "GLD",   # Gold
    "SLV",   # Silver
    "USO",   # Oil
    "DBC",   # Broad commodities
    # Sectors
    "XLF",   # Financials
    "XLE",   # Energy
    "XLK",   # Technology
    "XLV",   # Healthcare
    "XLI",   # Industrials
    "XLP",   # Consumer staples
    "XLY",   # Consumer discretionary
    "XLRE",  # Real estate
    "XLU",   # Utilities
    "XLB",   # Materials
    # Volatility
    "UVXY",  # VIX proxy (short signal)
]

LOOKBACK_MONTHS = 12      # 12-month return signal
SKIP_MONTH = 1            # skip most recent month (reversal effect)
MAX_POSITION_PCT = 0.04   # 4% per ETF
MAX_POSITIONS = 8         # max 8 concurrent TS momentum positions

_last_rebalance: str = ""   # "YYYY-MM" — rebalance once per month
_ts_positions: dict = {}    # symbol -> side
_state_restored: bool = False


def _restore_state(broker):
    """Rebuild _ts_positions from broker on startup."""
    global _state_restored
    if _state_restored:
        return
    _state_restored = True
    try:
        positions = broker.get_positions()
        for pos in positions:
            sym = pos["symbol"]
            tag = pos.get("strategy", "") or ""
            if ("ts_momentum" in tag.lower() or "tsmomentum" in tag.lower()) and sym not in _ts_positions:
                _ts_positions[sym] = "long"
                logger.info(f"[TSMomentum] Restored position: {sym}")
    except Exception as e:
        logger.warning(f"[TSMomentum] State restore failed: {e}")


def _get_12m_return(symbol: str) -> float:
    """Return 12-month price return, skipping most recent month."""
    import yfinance as yf
    try:
        ticker = yf.Ticker(symbol)
        # Get 13 months of data
        hist = ticker.history(period="13mo", interval="1mo", auto_adjust=True)
        if hist is None or len(hist) < 13:
            return 0.0
        # Skip most recent month, use return from month[-13] to month[-2]
        price_start = float(hist["Close"].iloc[-13])
        price_end = float(hist["Close"].iloc[-2])
        if price_start <= 0:
            return 0.0
        return (price_end - price_start) / price_start
    except Exception as e:
        logger.debug(f"[TSMomentum] Return calc error {symbol}: {e}")
        return 0.0
    finally:
        gc.collect()


def run(broker, db_conn=None):
    """
    Monthly rebalance: score each ETF by 12-month return.
    Go long top 8 positive-return ETFs, exit negative ones.
    """
    global _last_rebalance

    _restore_state(broker)

    now = datetime.now(timezone.utc)
    month_key = now.strftime("%Y-%m")

    # Only rebalance once per month (first trading day of month OR first run)
    if _last_rebalance == month_key:
        return

    # Only run on weekdays during market hours
    if now.weekday() >= 5:
        return

    # Only rebalance when market is open — avoid submitting orders pre/post market
    try:
        if not broker.is_market_open():
            logger.info("[TSMomentum] Market closed — skipping rebalance")
            return
    except Exception:
        pass

    logger.info("[TSMomentum] Monthly rebalance triggered")
    _last_rebalance = month_key

    try:
        account = broker.get_account()
        equity = float(account.get("equity") or account.get("portfolio_value") or 100000.0)
        positions = broker.get_positions()
        existing_positions = {p["symbol"]: p for p in positions}
    except Exception as e:
        logger.error(f"[TSMomentum] Account fetch failed: {e}")
        return

    # Score all symbols in parallel
    from concurrent.futures import ThreadPoolExecutor, as_completed
    scores = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_get_12m_return, sym): sym for sym in TS_UNIVERSE}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                score = future.result()
                scores[sym] = score
            except Exception:
                scores[sym] = 0.0

    # Rank — only long positive-return ETFs, top MAX_POSITIONS
    positive = sorted(
        [(sym, ret) for sym, ret in scores.items() if ret > 0],
        key=lambda x: x[1], reverse=True
    )[:MAX_POSITIONS]

    target_longs = {sym for sym, _ in positive}

    logger.info(f"[TSMomentum] Target longs: {target_longs}")
    logger.info(f"[TSMomentum] Scores: { {s: f'{r:.2%}' for s, r in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]} }")

    # Exit positions no longer in target
    for sym, pos in list(_ts_positions.items()):
        if sym not in target_longs:
            if sym not in existing_positions:
                logger.info(f"[TSMomentum] {sym} not in current positions — removing from tracker")
                del _ts_positions[sym]
                continue
            try:
                broker.submit_order(
                    symbol=sym, qty=abs(int(float(existing_positions[sym]["qty"]))),
                    side="sell", type="market", time_in_force="day"
                )
                logger.info(f"[TSMomentum] Exit {sym} (no longer in top {MAX_POSITIONS})")
                del _ts_positions[sym]
            except Exception as e:
                logger.error(f"[TSMomentum] Exit error {sym}: {e}")

    # Enter new positions
    for sym, ret in positive:
        if sym in _ts_positions:
            continue  # already held
        if sym in existing_positions:
            continue  # held by another strategy
        try:
            import yfinance as yf
            price = getattr(yf.Ticker(sym).fast_info, "last_price", None)
            if not price or price <= 0:
                continue
            trade_value = equity * MAX_POSITION_PCT
            qty = int(trade_value / price)
            if qty < 1:
                continue
            broker.submit_order(
                symbol=sym, qty=qty, side="buy",
                type="market", time_in_force="day"
            )
            _ts_positions[sym] = "long"
            logger.info(f"[TSMomentum] BUY {qty} {sym} @ ~${price:.2f} (12m return {ret:.2%})")
        except Exception as e:
            logger.error(f"[TSMomentum] Entry error {sym}: {e}")
        finally:
            gc.collect()
