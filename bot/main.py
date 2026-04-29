"""
AlphaBot — Main entry point
Multi-factor algorithmic trading bot for Alpaca Markets
Runs 24/7 on Railway. Handles all strategies + API server.
"""
import os
import sys
import time
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
from strategies import mean_reversion, trend_following, ai_research, earnings_drift, sector_rotation
from strategies import spy_dip

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


def is_ai_research_window() -> bool:
    """
    AI Research runs once daily — between 9:45 and 10:30 AM ET.
    Uses ET time directly (not UTC) to avoid Railway timezone issues.
    schedule.every().day.at() uses server time (UTC on Railway),
    so instead we check ET time manually on each 5-min cycle.
    """
    now_et = datetime.now(EASTERN)
    t = now_et.time()
    return dtime(9, 45) <= t <= dtime(10, 30)


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
    """
    TIGHT_STOP = 0.05  # 5% stop
    positions = broker.get_positions()
    logger.info(f"[GLOBAL STOP] Checking {len(positions)} position(s)...")
    for pos in positions:
        pnl_pct = pos["unrealized_pnl_pct"]
        sym = pos["symbol"]
        strategy = pos.get("strategy", "unknown")
        logger.info(f"  {sym} ({strategy}): P&L={pnl_pct:+.1f}%")
        if pnl_pct <= -TIGHT_STOP * 100:
            logger.info(
                f"[GLOBAL STOP] CUTTING {sym} ({strategy}) @ {pnl_pct:.1f}% — stop triggered"
            )
            try:
                broker.close_position(sym, strategy)
                log_trade(db_conn, strategy, sym, "sell_stop",
                          pos["qty"], pos["current_price"], pos["unrealized_pnl"],
                          metadata={"reason": "global_stop_loss"})
            except Exception as e:
                logger.error(f"[GLOBAL STOP] Failed to close {sym}: {e}")


# Track whether AI research has fired today
_ai_research_fired_date: str = ""


def run_all_strategies(broker: AlpacaBroker, db_conn):
    """Execute all strategies in sequence with regime filter."""
    global _ai_research_fired_date

    if not broker.is_market_open():
        logger.info("Market is closed — skipping strategy run")
        return

    if not is_trading_window():
        logger.info("Outside trading window — skipping (too close to open/close)")
        return

    logger.info("==========================================")
    logger.info(f"Running all strategies — {datetime.now(EASTERN).strftime('%Y-%m-%d %H:%M:%S ET')}")
    logger.info("==========================================")

    # Always enforce stops first — catches ALL positions including 'unknown' tagged ones
    enforce_global_stops(broker, db_conn)

    # Check market regime
    regime = get_market_regime()

    if regime == "bear":
        logger.info("BEAR MARKET regime — managing exits only, no new entries")
        try:
            mean_reversion.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Mean reversion error: {e}", exc_info=True)
        try:
            trend_following.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Trend following error: {e}", exc_info=True)
        try:
            spy_dip.run(broker, db_conn)
        except Exception as e:
            logger.error(f"SPY dip error: {e}", exc_info=True)
        logger.info("Strategy run complete (bear market — exits only)")
        return

    # Bull market — run all strategies
    try:
        mean_reversion.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Mean reversion error: {e}", exc_info=True)
    try:
        trend_following.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Trend following error: {e}", exc_info=True)
    try:
        spy_dip.run(broker, db_conn)
    except Exception as e:
        logger.error(f"SPY dip error: {e}", exc_info=True)
    try:
        earnings_drift.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Earnings drift error: {e}", exc_info=True)
    try:
        sector_rotation.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Sector rotation error: {e}", exc_info=True)

    # AI Research: fires once daily in the 9:45–10:30 AM ET window
    # Checked here (not via schedule.at()) to avoid Railway UTC timezone issues
    today_str = datetime.now(EASTERN).strftime("%Y-%m-%d")
    if is_ai_research_window() and _ai_research_fired_date != today_str:
        logger.info("==========================================")
        logger.info("Running AI Research Strategy (daily window)")
        logger.info("==========================================")
        try:
            ai_research.run(broker, db_conn)
            _ai_research_fired_date = today_str
            logger.info(f"AI Research fired for {today_str} — will not re-run today")
        except Exception as e:
            logger.error(f"AI Research strategy error: {e}", exc_info=True)
    elif not is_ai_research_window():
        now_et = datetime.now(EASTERN)
        logger.info(f"AI Research: outside window (ET={now_et.strftime('%H:%M')}, window=09:45–10:30)")
    elif _ai_research_fired_date == today_str:
        logger.info(f"AI Research: already fired today ({today_str})")

    logger.info("Strategy run complete")


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

    # Log current ET time so we can verify timezone handling
    now_et = datetime.now(EASTERN)
    logger.info(f"Current time: {now_et.strftime('%Y-%m-%d %H:%M:%S ET')} (UTC offset {now_et.strftime('%z')})")
    logger.info(f"AI Research window: 09:45–10:30 ET | Currently: {'IN WINDOW' if is_ai_research_window() else 'outside window'}")

    # Schedules — snapshot only (strategies run on 5-min cycle, AI research checked inline)
    schedule.every(CHECK_INTERVAL_MIN).minutes.do(run_all_strategies, broker, db_conn)
    schedule.every(60).minutes.do(take_snapshot, broker, db_conn)
    schedule.every().day.at("21:05").do(take_snapshot, broker, db_conn)  # 21:05 UTC = 16:05 ET (after close)

    take_snapshot(broker, db_conn)
    run_all_strategies(broker, db_conn)

    logger.info(f"Bot running — checking signals every {CHECK_INTERVAL_MIN} minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
