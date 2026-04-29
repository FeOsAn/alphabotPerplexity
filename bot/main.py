"""
AlphaBot — Main entry point
Multi-factor algorithmic trading bot for Alpaca Markets
Runs 24/7 on Railway. Handles all 6 strategies + API server.
"""
import os
import sys
import time
import threading
import subprocess

# Load .env if present (local dev). Railway injects env vars directly.
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass
import logging
import schedule
import yfinance as yf
from datetime import datetime, time as dtime
import pytz

# Resolve base directory robustly (works in Docker, Railway, local)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Build log handlers — always log to stdout (Railway captures this)
_log_handlers = [logging.StreamHandler(sys.stdout)]
try:
    _LOG_PATH = os.path.join(_BASE_DIR, 'alphabot.log')
    _log_handlers.append(logging.FileHandler(_LOG_PATH))
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=_log_handlers,
)
logger = logging.getLogger("alphabot.main")

from config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY,
    MARKET_OPEN_BUFFER_MIN, MARKET_CLOSE_BUFFER_MIN, CHECK_INTERVAL_MIN,
    STOP_LOSS_PCT
)
from broker import AlpacaBroker
from db import init_db, get_connection, log_snapshot, log_trade

# Import strategies
sys.path.insert(0, os.path.dirname(__file__))
from strategies import momentum, mean_reversion, trend_following, ai_research, earnings_drift, sector_rotation

EASTERN = pytz.timezone("America/New_York")


def is_trading_window() -> bool:
    """Check if we're within the safe trading window (not too close to open/close)."""
    now_et = datetime.now(EASTERN)
    open_total  = 9 * 60 + 30 + MARKET_OPEN_BUFFER_MIN
    close_total = 16 * 60 - MARKET_CLOSE_BUFFER_MIN
    market_open  = dtime(open_total  // 60, open_total  % 60)
    market_close = dtime(close_total // 60, close_total % 60)
    current_time = now_et.time()
    return market_open <= current_time <= market_close


def get_spy_price() -> float:
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return 0.0


def get_market_regime() -> str:
    """
    Market regime filter — checks if SPY is above its 50-day MA.
    Returns 'bull' (safe to trade) or 'bear' (reduce exposure).
    """
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="3mo")
        if len(hist) >= 50:
            price = hist["Close"].iloc[-1]
            ma50 = hist["Close"].tail(50).mean()
            regime = "bull" if price > ma50 else "bear"
            logger.info(f"Market regime: {regime.upper()} (SPY ${price:.2f} vs MA50 ${ma50:.2f})")
            return regime
    except Exception as e:
        logger.warning(f"Could not determine market regime: {e}")
    return "bull"  # Default to bull if unavailable


def enforce_global_stops(broker: AlpacaBroker, db_conn):
    """
    Enforce stop losses on ALL positions regardless of strategy tag.
    Catches positions tagged 'unknown' or any strategy that missed a stop.
    Uses tighter 5% stop loss for better capital protection.
    """
    TIGHT_STOP = 0.05  # 5% stop — tighter than default 7%
    positions = broker.get_positions()
    for pos in positions:
        if pos["unrealized_pnl_pct"] <= -TIGHT_STOP * 100:
            sym = pos["symbol"]
            strategy = pos.get("strategy", "unknown")
            logger.info(
                f"[GLOBAL STOP] {sym} ({strategy}) @ {pos['unrealized_pnl_pct']:.1f}% "
                f"— closing position"
            )
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "global_stop_loss"})
            except Exception as e:
                logger.error(f"[GLOBAL STOP] Failed to close {sym}: {e}")


def run_all_strategies(broker: AlpacaBroker, db_conn):
    """Execute all strategies in sequence with regime filter."""
    if not broker.is_market_open():
        logger.info("Market is closed — skipping strategy run")
        return

    if not is_trading_window():
        logger.info("Outside trading window — skipping (too close to open/close)")
        return

    logger.info("==========================================")
    logger.info(f"Running all strategies — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("==========================================")

    # Always enforce stops first — catches all positions including 'unknown'
    enforce_global_stops(broker, db_conn)

    # Check market regime — in bear market, skip new entries but still manage exits
    regime = get_market_regime()

    if regime == "bear":
        logger.info("BEAR MARKET regime — skipping new entries, managing exits only")
        # Still run strategies but they will only exit, not enter
        # (each strategy checks regime internally via the broker's positions)
        try:
            mean_reversion.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Mean reversion error: {e}", exc_info=True)
        try:
            trend_following.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Trend following error: {e}", exc_info=True)
        logger.info("Strategy run complete (bear market — exits only)")
        return

    # Bull market — run all strategies
    try:
        momentum.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Momentum error: {e}", exc_info=True)
    try:
        mean_reversion.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Mean reversion error: {e}", exc_info=True)
    try:
        trend_following.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Trend following error: {e}", exc_info=True)
    try:
        earnings_drift.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Earnings drift error: {e}", exc_info=True)
    try:
        sector_rotation.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Sector rotation error: {e}", exc_info=True)

    logger.info("Strategy run complete")


def run_ai_research(broker: AlpacaBroker, db_conn):
    """Run AI research strategy — once daily at market open."""
    if not broker.is_market_open():
        return
    if not is_trading_window():
        return
    # Skip AI research in bear market
    regime = get_market_regime()
    if regime == "bear":
        logger.info("AI Research skipped — bear market regime")
        return
    logger.info("==========================================")
    logger.info("Running AI Research Strategy")
    logger.info("==========================================")
    try:
        ai_research.run(broker, db_conn)
    except Exception as e:
        logger.error(f"AI Research strategy error: {e}", exc_info=True)


def take_snapshot(broker: AlpacaBroker, db_conn):
    """Record portfolio snapshot for performance tracking."""
    try:
        acct = broker.get_account()
        spy_price = get_spy_price()
        log_snapshot(
            db_conn,
            portfolio_value=acct["portfolio_value"],
            cash=acct["cash"],
            equity=acct["equity"],
            pnl_today=acct["pnl_today"],
            spy_price=spy_price,
        )
        logger.info(
            f"Snapshot: Portfolio ${acct['portfolio_value']:,.2f} | "
            f"P&L Today: ${acct['pnl_today']:+,.2f} ({acct['pnl_today_pct']:+.2f}%) | "
            f"SPY: ${spy_price:.2f}"
        )
    except Exception as e:
        logger.error(f"Snapshot error: {e}", exc_info=True)


def start_api_server():
    """Start the FastAPI server as a background process."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logger.info("Starting API server on port 8000...")
    subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "api.server:app", "--host", "0.0.0.0", "--port", "8000"],
        cwd=base_dir,
    )
    logger.info("API server started")


def main():
    logger.info("=== AlphaBot Starting ===")

    start_api_server()
    time.sleep(2)

    if ALPACA_API_KEY == "YOUR_API_KEY_HERE":
        logger.error("API keys not configured!")
        sys.exit(1)

    init_db()
    db_conn = get_connection()

    broker = AlpacaBroker()
    acct = broker.get_account()
    logger.info(f"Connected | Portfolio: ${acct['portfolio_value']:,.2f} | Cash: ${acct['cash']:,.2f}")

    schedule.every(CHECK_INTERVAL_MIN).minutes.do(run_all_strategies, broker, db_conn)
    schedule.every(60).minutes.do(take_snapshot, broker, db_conn)
    schedule.every().day.at("16:05").do(take_snapshot, broker, db_conn)
    schedule.every().day.at("09:45").do(run_ai_research, broker, db_conn)

    take_snapshot(broker, db_conn)
    run_all_strategies(broker, db_conn)

    logger.info(f"Bot running — checking signals every {CHECK_INTERVAL_MIN} minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
