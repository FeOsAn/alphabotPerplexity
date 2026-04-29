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
import gc
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
)
from broker import AlpacaBroker, restore_tags_from_db
from db import init_db, get_connection, log_snapshot

# Import strategies
sys.path.insert(0, os.path.dirname(__file__))
from strategies import mean_reversion, trend_following, ai_research
from strategies import earnings_drift, sector_rotation, spy_dip
from strategies import vix_reversal, gap_scanner
from strategies.trade_management import run_global_trade_management

EASTERN = pytz.timezone("America/New_York")


def is_trading_window() -> bool:
    """Check if we're within the safe trading window (not too close to open/close)."""
    now_et = datetime.now(EASTERN)
    open_total  = 9 * 60 + 30 + MARKET_OPEN_BUFFER_MIN
    close_total = 16 * 60 - MARKET_CLOSE_BUFFER_MIN
    market_open  = dtime(open_total  // 60, open_total  % 60)
    market_close = dtime(close_total // 60, close_total % 60)
    return market_open <= now_et.time() <= market_close


def is_premarket_window() -> bool:
    """8:00–9:25 AM ET — gap scanner pre-market window."""
    now_et = datetime.now(EASTERN)
    return dtime(8, 0) <= now_et.time() <= dtime(9, 25)


def is_ai_research_window() -> bool:
    """9:45–10:30 AM ET — AI research fires once daily in this window."""
    now_et = datetime.now(EASTERN)
    return dtime(9, 45) <= now_et.time() <= dtime(10, 30)


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
    Market regime filter — SPY above 50-day MA = bull, below = bear.
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
    return "bull"


# Track AI research daily fire
_ai_research_fired_date: str = ""


def run_all_strategies(broker: AlpacaBroker, db_conn):
    """Execute all strategies in sequence."""
    global _ai_research_fired_date

    now_et = datetime.now(EASTERN)
    market_open = broker.is_market_open()

    # ── Pre-market window: gap scanner only ───────────────────────────────────
    if is_premarket_window() and not market_open:
        logger.info(f"Pre-market window ({now_et.strftime('%H:%M ET')}) — running gap scanner")
        try:
            gap_scanner.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Gap scanner error: {e}", exc_info=True)
        return

    # ── Market closed ─────────────────────────────────────────────────────────
    if not market_open:
        logger.info("Market is closed — skipping strategy run")
        return

    if not is_trading_window():
        logger.info("Outside trading window — skipping (too close to open/close)")
        return

    logger.info("==========================================")
    logger.info(f"Running all strategies — {now_et.strftime('%Y-%m-%d %H:%M:%S ET')}")
    logger.info("==========================================")

    # ── Trade management: trailing stops + partial takes on ALL positions ─────
    run_global_trade_management(broker, db_conn)

    # ── Market regime check ───────────────────────────────────────────────────
    regime = get_market_regime()

    if regime == "bear":
        logger.info("BEAR MARKET regime — managing exits only, no new entries")
        for fn, name in [
            (mean_reversion.run,  "Mean reversion"),
            (trend_following.run, "Trend following"),
            (spy_dip.run,         "SPY dip"),
            (vix_reversal.run,    "VIX reversal"),   # VIX reversal still runs in bear — it's designed for it
            (gap_scanner.run,     "Gap scanner"),     # exits only
        ]:
            try:
                fn(broker, db_conn)
            except Exception as e:
                logger.error(f"{name} error: {e}", exc_info=True)
        logger.info("Strategy run complete (bear market — exits only)")
        return

    # ── Bull market — run all strategies ─────────────────────────────────────
    strategies = [
        (mean_reversion.run,  "Mean reversion"),
        (trend_following.run, "Trend following"),
        (spy_dip.run,         "SPY dip"),
        (vix_reversal.run,    "VIX reversal"),
        (gap_scanner.run,     "Gap scanner"),
        (earnings_drift.run,  "Earnings drift"),
        (sector_rotation.run, "Sector rotation"),
    ]

    for fn, name in strategies:
        try:
            fn(broker, db_conn)
        except Exception as e:
            logger.error(f"{name} error: {e}", exc_info=True)
        finally:
            gc.collect()  # Free memory between each strategy to stay under Railway 512MB

    # ── AI Research: once daily 9:45–10:30 ET ────────────────────────────────
    today_str = now_et.strftime("%Y-%m-%d")
    if is_ai_research_window() and _ai_research_fired_date != today_str:
        logger.info("==========================================")
        logger.info("Running AI Research Strategy (daily window)")
        logger.info("==========================================")
        try:
            ai_research.run(broker, db_conn)
            _ai_research_fired_date = today_str
            logger.info(f"AI Research fired for {today_str}")
        except Exception as e:
            logger.error(f"AI Research error: {e}", exc_info=True)
    elif not is_ai_research_window():
        logger.info(f"AI Research: outside window (ET={now_et.strftime('%H:%M')}, window=09:45–10:30)")
    else:
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


def start_health_server():
    """
    Bind a minimal HTTP health check server on $PORT (Railway requires this).
    Uses only Python built-ins — zero extra RAM vs uvicorn's ~150MB.
    Returns 200 OK on GET / so Railway's health check passes.
    """
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler

    port = int(os.environ.get("PORT", 8080))

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        def log_message(self, *args):
            pass  # suppress access logs

    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"Health check server started on port {port}")


def main():
    logger.info("=== AlphaBot Starting ===")

    start_health_server()  # Must bind to $PORT or Railway kills the container

    if ALPACA_API_KEY == "YOUR_API_KEY_HERE":
        logger.error("API keys not configured!")
        sys.exit(1)

    init_db()
    db_conn = get_connection()

    broker = AlpacaBroker()
    acct = broker.get_account()
    logger.info(f"Connected | Portfolio: ${acct['portfolio_value']:,.2f} | Cash: ${acct['cash']:,.2f}")

    # Restore strategy tags from DB so positions aren't labelled 'unknown' after restart
    restored = restore_tags_from_db(db_conn)
    logger.info(f"Restored {restored} strategy tag(s) from trade history")

    now_et = datetime.now(EASTERN)
    logger.info(f"Time: {now_et.strftime('%Y-%m-%d %H:%M:%S ET')} | "
                f"Pre-market: {is_premarket_window()} | "
                f"AI window: {is_ai_research_window()} | "
                f"Trading: {is_trading_window()}")

    # Schedules
    schedule.every(CHECK_INTERVAL_MIN).minutes.do(run_all_strategies, broker, db_conn)
    schedule.every(60).minutes.do(take_snapshot, broker, db_conn)
    schedule.every().day.at("21:05").do(take_snapshot, broker, db_conn)  # 21:05 UTC = 16:05 ET

    take_snapshot(broker, db_conn)

    # Delay first strategy run by CHECK_INTERVAL_MIN — let imports settle
    # and avoid OOM on boot when all modules are freshly loaded in RAM.
    logger.info(f"Bot running — first strategy run in {CHECK_INTERVAL_MIN} minutes")

    logger.info(f"Bot running — checking every {CHECK_INTERVAL_MIN} minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
