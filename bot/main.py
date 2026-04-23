"""
AlphaBot — Main entry point
Multi-factor algorithmic trading bot for Alpaca Markets
Runs 24/7 on Railway. Handles all 4 strategies.
"""
import os
import sys
import time

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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/user/workspace/alphabot/alphabot.log"),
    ]
)
logger = logging.getLogger("alphabot.main")

from config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY,
    MARKET_OPEN_BUFFER_MIN, MARKET_CLOSE_BUFFER_MIN, CHECK_INTERVAL_MIN
)
from broker import AlpacaBroker
from db import init_db, get_connection, log_snapshot

# Import strategies
sys.path.insert(0, os.path.dirname(__file__))
from strategies import momentum, mean_reversion, trend_following, ai_research

EASTERN = pytz.timezone("America/New_York")


def is_trading_window() -> bool:
    """Check if we're within the safe trading window (not too close to open/close)."""
    now_et = datetime.now(EASTERN)
    market_open = dtime(9, 30 + MARKET_OPEN_BUFFER_MIN)
    market_close = dtime(16, 0 - MARKET_CLOSE_BUFFER_MIN)
    current_time = now_et.time()
    return market_open <= current_time <= market_close


def get_spy_price() -> float:
    try:
        spy = yf.Ticker("SPY")
        hist = spy.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return 0.0


def run_all_strategies(broker: AlpacaBroker, db_conn):
    """Execute all three strategies in sequence."""
    if not broker.is_market_open():
        logger.info("Market is closed — skipping strategy run")
        return

    if not is_trading_window():
        logger.info("Outside trading window — skipping (too close to open/close)")
        return

    logger.info("==========================================")
    logger.info(f"Running all strategies — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("==========================================")

    try:
        # Strategy 1: Momentum (monthly rebalance)
        momentum.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Momentum strategy error: {e}", exc_info=True)

    try:
        # Strategy 2: Mean Reversion (intraday signals)
        mean_reversion.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Mean reversion strategy error: {e}", exc_info=True)

    try:
        # Strategy 3: Trend Following (daily signals)
        trend_following.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Trend following strategy error: {e}", exc_info=True)

    logger.info("Strategy run complete")


def run_ai_research(broker: AlpacaBroker, db_conn):
    """Run AI research strategy — once daily at market open."""
    if not broker.is_market_open():
        return
    if not is_trading_window():
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


def main():
    logger.info("=== AlphaBot Starting ===")

    if ALPACA_API_KEY == "YOUR_API_KEY_HERE":
        logger.error("API keys not configured! Set ALPACA_API_KEY and ALPACA_SECRET_KEY env vars.")
        sys.exit(1)

    # Initialize database
    init_db()
    db_conn = get_connection()

    # Initialize broker
    broker = AlpacaBroker()
    acct = broker.get_account()
    logger.info(
        f"Connected | Portfolio: ${acct['portfolio_value']:,.2f} | Cash: ${acct['cash']:,.2f}"
    )

    # Schedule tasks
    schedule.every(CHECK_INTERVAL_MIN).minutes.do(run_all_strategies, broker, db_conn)
    schedule.every(60).minutes.do(take_snapshot, broker, db_conn)
    schedule.every().day.at("16:05").do(take_snapshot, broker, db_conn)       # EOD snapshot
    # AI Research: runs once daily at 9:45 AM ET (15 min after open)
    schedule.every().day.at("09:45").do(run_ai_research, broker, db_conn)

    # Run immediately on startup
    take_snapshot(broker, db_conn)
    run_all_strategies(broker, db_conn)

    logger.info(f"Bot running — checking signals every {CHECK_INTERVAL_MIN} minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
