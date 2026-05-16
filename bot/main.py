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

# Suppress yFinance 404 noise — ETFs have no fundamentals, that's expected
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

from config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY,
    MARKET_OPEN_BUFFER_MIN, MARKET_CLOSE_BUFFER_MIN, CHECK_INTERVAL_MIN,
)
from broker import AlpacaBroker, restore_tags_from_db, retag_all_positions, check_pyramid_adds
from db import init_db, get_connection, log_snapshot

# Import strategies
sys.path.insert(0, os.path.dirname(__file__))
from strategies import mean_reversion, trend_following, ai_research
from strategies import earnings_drift, sector_rotation, spy_dip
from strategies import vix_reversal, gap_scanner
from strategies import momentum, breakout, short_hedge
from strategies import pairs_trading
from strategies.trade_management import run_global_trade_management
from reporting.weekly_report import generate_weekly_report
from utils import news_scanner
from strategies import event_driven
from strategies import earnings_nlp
from strategies import ts_momentum
from utils import regime_detector

EASTERN = pytz.timezone("America/New_York")
LONDON  = pytz.timezone("Europe/London")  # user's timezone (BST/GMT)

def now_str() -> str:
    """Always log both ET and London time to avoid timezone confusion."""
    et     = datetime.now(EASTERN)
    london = datetime.now(LONDON)
    return f"{et.strftime('%H:%M ET')} / {london.strftime('%H:%M London')}"

def market_closes_in() -> str:
    """Human-readable time until market close."""
    now_et = datetime.now(EASTERN)
    close  = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    diff   = close - now_et
    if diff.total_seconds() <= 0:
        return "CLOSED"
    h, m = divmod(int(diff.total_seconds()) // 60, 60)
    return f"{h}h{m:02d}m remaining"

# Track last weekly P&L report fire date — module-level so it persists across cycles
_last_report_date: str = ""


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



# Track last risk-metrics fire date (logged once daily after 10 AM ET)
_last_metrics_date: str = ""

# ── Daily P&L recap / gap-protection / circuit-breaker state ─────────────────
_recap_sent_date: str = ""
_gap_protection_run_date: str = ""
_circuit_breaker_active: bool = False
_circuit_breaker_reset_date: str = ""


def send_daily_recap(broker: AlpacaBroker):
    """Send daily P&L summary to ntfy after market close."""
    import requests as _requests
    try:
        account = broker.trading.get_account()
        equity = float(account.equity)
        prev_close = float(account.last_equity)
        day_pnl = equity - prev_close
        day_pct = (day_pnl / prev_close) * 100 if prev_close else 0
        cash_pct = (float(account.cash) / equity) * 100 if equity else 0

        positions = broker.trading.get_all_positions()
        pos_lines = []
        for p in positions[:5]:  # top 5
            pnl = float(p.unrealized_plpc) * 100
            pos_lines.append(f"{p.symbol} {pnl:+.1f}%")

        emoji = "📈" if day_pnl >= 0 else "📉"
        body = (
            f"Portfolio: ${equity:,.0f} ({day_pnl:+,.0f}, {day_pct:+.2f}%) {emoji}\n"
            f"Cash: {cash_pct:.0f}% idle\n"
            f"Positions: {', '.join(pos_lines) if pos_lines else 'None'}\n"
            f"Go hit your goals 💪"
        )

        _requests.post(
            "https://ntfy.sh/perplexitybotnr1foa_goat",
            data=body.encode("utf-8"),
            headers={
                "Title": f"AlphaBot Daily — {day_pct:+.2f}%",
                "Priority": "default",
                "Tags": "chart_with_upwards_trend",
            },
            timeout=10,
        )
        logger.info(f"[Recap] ntfy sent — P&L {day_pnl:+,.0f} ({day_pct:+.2f}%)")
    except Exception as e:
        logger.error(f"[Recap] ntfy failed: {e}")


def run_gap_protection(broker: AlpacaBroker):
    """Pre-market gap protection: close positions down >4% from prev close."""
    GAP_THRESHOLD = 0.04
    try:
        positions = broker.trading.get_all_positions()
        for pos in positions:
            sym = pos.symbol
            if sym in ("SDS", "SQQQ", "SPXS", "SOXS", "UVXY"):
                continue
            try:
                qty = float(pos.qty)
                if qty <= 0:
                    continue  # skip shorts
                ticker = yf.Ticker(sym)
                fi = ticker.fast_info
                current = getattr(fi, "last_price", None) or getattr(fi, "regular_market_price", None)
                prev_close = getattr(fi, "previous_close", None)
                if not current or not prev_close:
                    continue
                gap_pct = (current - prev_close) / prev_close
                if gap_pct <= -GAP_THRESHOLD:
                    logger.warning(
                        f"[GapProtect] {sym} gapped down {gap_pct:.1%} pre-market "
                        f"(prev_close=${prev_close:.2f} → now=${current:.2f}) — closing position"
                    )
                    from alpaca.trading.requests import MarketOrderRequest
                    from alpaca.trading.enums import OrderSide, TimeInForce
                    req = MarketOrderRequest(
                        symbol=sym, qty=int(abs(qty)),
                        side=OrderSide.SELL, time_in_force=TimeInForce.DAY,
                    )
                    broker.trading.submit_order(req)
            except Exception as e:
                logger.error(f"[GapProtect] Error checking {sym}: {e}")
    except Exception as e:
        logger.error(f"[GapProtect] Failed: {e}")


def check_circuit_breaker(broker: AlpacaBroker):
    """
    Sets _circuit_breaker_active if portfolio down >3% on the day.
    Auto-resets if portfolio recovers to better than -2%.
    """
    global _circuit_breaker_active
    try:
        account = broker.trading.get_account()
        equity = float(account.equity)
        last_equity = float(account.last_equity)
        if last_equity <= 0:
            return
        day_pct = (equity - last_equity) / last_equity
        if day_pct <= -0.03 and not _circuit_breaker_active:
            _circuit_breaker_active = True
            logger.error(
                f"[CircuitBreaker] TRIGGERED — portfolio down {day_pct:.2%} today. "
                f"Halting new entries."
            )
        elif day_pct > -0.02 and _circuit_breaker_active:
            _circuit_breaker_active = False
            logger.info(
                f"[CircuitBreaker] RESET — portfolio recovered to {day_pct:.2%}, "
                f"resuming new entries."
            )
    except Exception as e:
        logger.warning(f"[CircuitBreaker] Check failed: {e}")


def run_all_strategies(broker: AlpacaBroker, db_conn):
    """Execute all strategies in sequence."""
    global _last_report_date, _last_metrics_date
    global _recap_sent_date, _gap_protection_run_date
    global _circuit_breaker_active, _circuit_breaker_reset_date

    now_utc = datetime.utcnow()
    today_utc = now_utc.strftime("%Y-%m-%d")
    weekday = now_utc.weekday()

    # ── Reset circuit breaker at start of each new trading day (13:30 UTC) ────
    if (now_utc.hour == 13 and now_utc.minute >= 30 and weekday < 5
            and _circuit_breaker_reset_date != today_utc):
        if _circuit_breaker_active:
            logger.info("[CircuitBreaker] New trading day — resetting active flag")
        _circuit_breaker_active = False
        _circuit_breaker_reset_date = today_utc

    # ── Pre-market gap protection: 13:00–13:10 UTC (8:00–8:10 AM ET), weekdays ─
    if (now_utc.hour == 13 and now_utc.minute < 10 and weekday < 5
            and _gap_protection_run_date != today_utc):
        try:
            run_gap_protection(broker)
        except Exception as e:
            logger.error(f"[GapProtect] Outer failure: {e}")
        _gap_protection_run_date = today_utc

    # ── Daily P&L recap: 20:30 UTC (16:30 ET / 21:30 BST), weekdays only ─────
    # Fires after market close, so checked here BEFORE any early-return paths.
    if (now_utc.hour == 20 and now_utc.minute >= 30 and weekday < 5
            and _recap_sent_date != today_utc):
        try:
            send_daily_recap(broker)
        except Exception as e:
            logger.error(f"[Recap] Outer failure: {e}")
        _recap_sent_date = today_utc

    # ── Circuit breaker check (3% daily drawdown halts new entries) ──────────
    try:
        check_circuit_breaker(broker)
    except Exception as e:
        logger.warning(f"[CircuitBreaker] Outer failure: {e}")

    if _circuit_breaker_active:
        logger.info("[CircuitBreaker] ACTIVE — skipping new entries, running exits only")
        try:
            from strategies.trade_management import run_global_trade_management
            run_global_trade_management(broker, db_conn)
        except Exception as e:
            logger.error(f"[CircuitBreaker] Exits-only trade_management failed: {e}", exc_info=True)
        return

    try:
        from utils.adaptive_filters import get_thresholds
        t = get_thresholds()
        logger.info(
            f"[Regime] {t['regime']} — thresholds active: "
            f"RSI_max={t['momentum_rsi_max']}, BRK_vol={t['breakout_vol_min']}x, "
            f"MR_oversold={t['mr_rsi_oversold']}"
        )
    except Exception as e:
        logger.warning(f"[Regime] Could not assess: {e}")

    now_et = datetime.now(EASTERN)
    market_open = broker.is_market_open()

    # ── Weekly P&L report: Monday 9:30–10:00 ET, once per day ────────────────
    if now_et.weekday() == 0 and 9 <= now_et.hour <= 10:
        today_str = now_et.strftime("%Y-%m-%d")
        if _last_report_date != today_str:
            try:
                generate_weekly_report(broker)
                _last_report_date = today_str
            except Exception as e:
                logger.error(f"Weekly report error: {e}", exc_info=True)

    # ── Daily risk metrics: log once per day after 10 AM ET ──────────────────
    if now_et.hour >= 10:
        today_str = now_et.strftime("%Y-%m-%d")
        if _last_metrics_date != today_str:
            try:
                from utils.risk_metrics import compute_metrics
                from config import ALPACA_BASE_URL
                compute_metrics(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL)
                _last_metrics_date = today_str
            except Exception as e:
                logger.warning(f"[RiskMetrics] Daily log error: {e}")

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
    logger.info(f"Running all strategies — {now_str()} | Market: {market_closes_in()}")
    logger.info("==========================================")

    # ── Auto-tag all positions so labels are always current ──────────────────
    positions = broker.get_positions()
    retag_all_positions(positions)

    # ── Trade management: trailing stops + partial takes on ALL positions ─────
    run_global_trade_management(broker, db_conn)

    # ── Pyramid entry: check tracked positions for +2% / +4% add triggers ────
    try:
        check_pyramid_adds(broker)
    except Exception as e:
        logger.error(f"Pyramid add check error: {e}", exc_info=True)

    # ── Circuit breaker: halt new entries on >5% daily drawdown ──────────────
    from utils.circuit_breaker import check_and_update as check_circuit_breaker
    try:
        account = broker.get_account()
        portfolio_value = float(account["portfolio_value"])
        if check_circuit_breaker(portfolio_value):
            logger.warning("[Main] Circuit breaker active — skipping strategy cycle (exits already ran via trade_management)")
            return
    except Exception as e:
        logger.warning(f"[Main] Could not check circuit breaker: {e}")

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
            (short_hedge.run,     "Short hedge"),     # inverse ETFs — gated by adaptive regime
            (pairs_trading.run,   "Pairs trading"),   # market-neutral — runs in any regime
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
        (momentum.run,        "Momentum"),       # internally checks _should_rebalance() for weekly cadence
        (breakout.run,        "Breakout"),
        (short_hedge.run,     "Short hedge"),    # inverse ETFs — internally gated by adaptive regime
        (pairs_trading.run,   "Pairs trading"),  # market-neutral — long/short pairs via cointegration
    ]

    for fn, name in strategies:
        try:
            fn(broker, db_conn)
        except Exception as e:
            logger.error(f"{name} error: {e}", exc_info=True)
        finally:
            gc.collect()

    # ── Event-driven: consume news_scanner EVENT_QUEUE ───────────────────────
    try:
        event_driven.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Event-driven error: {e}", exc_info=True)

    # ── Earnings NLP: Claude-driven PEAD trades on earnings events ──────────
    try:
        earnings_nlp.run(broker, db_conn)
    except Exception as e:
        logger.error(f"Earnings NLP error: {e}", exc_info=True)

    # ── TS Momentum: monthly rebalance, macro/sector ETF trend-following ────
    try:
        ts_momentum.run(broker, db_conn)
    except Exception as e:
        logger.error(f"TS Momentum error: {e}", exc_info=True)

    # ── AI Research: self-manages window (9:45–15:30 ET) + daily fire internally ───────
    # Exit checks run every cycle. New research fires once daily inside the strategy.
    try:
        ai_research.run(broker, db_conn)
    except Exception as e:
        logger.error(f"AI Research error: {e}", exc_info=True)

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
    Uses only Python built-ins.
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

    class ReusableHTTPServer(HTTPServer):
        allow_reuse_address = True

    for attempt in range(5):
        try:
            server = ReusableHTTPServer(("0.0.0.0", port), HealthHandler)
            t = threading.Thread(target=server.serve_forever, daemon=True)
            t.start()
            logger.info(f"Health check server started on port {port} (attempt {attempt+1})")
            break
        except OSError as e:
            logger.warning(f"Health server port {port} busy (attempt {attempt+1}/5): {e} — retrying in 2s")
            time.sleep(2)
    else:
        logger.error(f"Health server could not bind to port {port} after 5 attempts — continuing anyway")


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

    # Force immediate recalibration on startup — don't use stale cached thresholds
    from utils.adaptive_filters import force_recalibrate, get_thresholds
    force_recalibrate()
    t = get_thresholds()
    logger.info(
        f"[Startup] Calibrated | regime={t.get('regime')} | "
        f"RSI_max={t.get('momentum_rsi_max'):.1f} "
        f"score_min={t.get('momentum_score_min'):+.3f} "
        f"vol_min={t.get('breakout_vol_min'):.2f} "
        f"max_pos={t.get('max_new_positions_per_cycle')}"
    )

    now_et = datetime.now(EASTERN)
    logger.info(f"Time: {now_str()} | "
                f"Pre-market: {is_premarket_window()} | "
                f"Trading: {is_trading_window()}")

    # Start real-time news scanner (background thread)
    news_scanner.start(
        os.environ.get("ALPACA_API_KEY", ""),
        os.environ.get("ALPACA_SECRET_KEY", "")
    )
    logger.info("News scanner started — event-driven strategy armed")

    regime_detector.start()
    logger.info("Regime detector started (background thread, 30-min updates)")

    # Schedules
    schedule.every(CHECK_INTERVAL_MIN).minutes.do(run_all_strategies, broker, db_conn)
    schedule.every(60).minutes.do(take_snapshot, broker, db_conn)
    schedule.every().day.at("21:05").do(take_snapshot, broker, db_conn)  # 21:05 UTC = 16:05 ET

    take_snapshot(broker, db_conn)

    # Delay first strategy run by CHECK_INTERVAL_MIN — let imports settle.
    logger.info(f"Bot running — first strategy run in {CHECK_INTERVAL_MIN} minutes")

    logger.info(f"Bot running — checking every {CHECK_INTERVAL_MIN} minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
