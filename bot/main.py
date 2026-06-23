"""
AlphaBot — Main entry point  (v44)
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
import threading
import yfinance as yf
from datetime import datetime, time as dtime, timezone
import pytz

VERSION = "v87"

# --- Liveness / re-entrancy state (Fix 8 + Fix 9) ------------------------------
# Updated at the top of every run_all_strategies(). Health endpoint serves 503
# when this gets stale (>15min) so Railway can detect a dead trading loop.
_last_cycle_ts: float = 0.0
# Re-entrancy guard: if a cycle is still running when the next 5-min tick fires,
# skip the new one rather than running two concurrent copies (duplicate orders,
# DB race conditions).
_cycle_lock = threading.Lock()

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

# Keep yFinance warnings visible — real data outages must surface in logs.
logging.getLogger("yfinance").setLevel(logging.WARNING)

from config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY,
    MARKET_OPEN_BUFFER_MIN, MARKET_CLOSE_BUFFER_MIN, CHECK_INTERVAL_MIN,
    MIN_CASH_RESERVE_PCT,
)
from broker import AlpacaBroker, restore_tags_from_db, retag_all_positions, check_pyramid_adds
from db import init_db, get_connection, log_snapshot, get_state, set_state

# Import strategies
sys.path.insert(0, os.path.dirname(__file__))
from strategies import mean_reversion, trend_following, ai_research
from strategies import earnings_drift, sector_rotation, spy_dip
from strategies import vix_reversal, gap_scanner
from strategies import trend_pullback, multi_tf_rsi  # v83
from strategies import fifty_two_wh  # v87 — 52WH-Vol breakout
from strategies import momentum, breakout, short_hedge
from strategies import pairs_trading
from strategies import insider_buying, options_flow, squeeze_screener
from strategies.trade_management import (
    run_global_trade_management,
    restore_trade_management_state,
    run_trade_management,
    apply_earnings_stop_tightening,
    check_overnight_exit,
    migrate_missing_brackets,
)
from reporting.weekly_report import generate_weekly_report
from utils import news_scanner
from strategies import event_driven
from strategies import earnings_prediction
from strategies import ts_momentum
from strategies import vwap_reclaim
from utils import regime_detector
from utils import earnings_calendar
from utils import ofi_monitor
from utils import yf_cache
from utils import notify

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


# v83: cache regime once per market day — re-evaluated at open and if VIX spikes >15%
_regime_cache_date: str = ""
_regime_cache_value: str = "bull"
_regime_cache_vix: float = 0.0


def get_market_regime() -> str:
    """
    v83: 3-tier market regime — 'bull', 'chop', or 'bear'.
    Evaluated once per trading day (cached). Re-evaluated mid-session only if
    VIX moves >15% from the morning reading (regime shock detection).
    Delegates to HMM regime_detector when confident (>=0.5), falls back to
    SPY-MA50 + VIX heuristic otherwise.
    """
    global _regime_cache_date, _regime_cache_value, _regime_cache_vix

    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Check for intraday VIX shock (regime can change mid-day on crisis events)
    try:
        _vix_now = float(yf.Ticker("^VIX").fast_info.last_price)
        vix_shock = (_regime_cache_vix > 0 and
                     abs(_vix_now - _regime_cache_vix) / _regime_cache_vix > 0.15)
    except Exception:
        _vix_now = _regime_cache_vix
        vix_shock = False

    # Return cache if same day and no VIX shock
    if _regime_cache_date == today and not vix_shock:
        return _regime_cache_value

    regime = "bull"  # safe default
    try:
        from utils.regime_detector import get_regime as _rd_get_regime
        hmm_regime, confidence = _rd_get_regime()
        if confidence >= 0.5 and hmm_regime:
            if hmm_regime in ("BEAR_STRONG", "BEAR_MILD"):
                regime = "bear"
            elif hmm_regime == "CHOPPY":
                regime = "chop"
            else:  # BULL_STRONG, BULL_NORMAL
                regime = "bull"
            logger.info(f"[Regime] HMM={hmm_regime} conf={confidence:.2f} → {regime.upper()}")
        else:
            raise ValueError(f"HMM confidence too low: {confidence:.2f}")
    except Exception:
        # Fallback: SPY-MA50 + VIX heuristic
        try:
            spy = yf.Ticker("SPY")
            hist = spy.history(period="3mo")
            if len(hist) >= 50:
                price = float(hist["Close"].iloc[-1])
                ma50  = float(hist["Close"].tail(50).mean())
                ma20  = float(hist["Close"].tail(20).mean())
                vix   = _vix_now if _vix_now > 0 else 20.0
                if price < ma50 and vix > 25:
                    regime = "bear"
                elif price < ma20 and vix > 22:
                    regime = "chop"
                else:
                    regime = "bull"
                logger.info(
                    f"[Regime] Fallback SPY=${price:.2f} MA50=${ma50:.2f} "
                    f"VIX={vix:.1f} → {regime.upper()}"
                )
        except Exception as e:
            logger.warning(f"[Regime] Could not determine regime: {e} — defaulting to bull")
            regime = "bull"

    _regime_cache_date  = today
    _regime_cache_value = regime
    _regime_cache_vix   = _vix_now if _vix_now > 0 else _regime_cache_vix
    return regime



# Track last risk-metrics fire date (logged once daily after 10 AM ET)
_last_metrics_date: str = ""

# v85 — track last regime-exit sweep date (runs once daily after the open)
_last_regime_exit_date: str = ""

# ── Daily P&L recap / gap-protection / circuit-breaker state ─────────────────
_recap_sent_date: str = ""
_gap_protection_run_date: str = ""
_circuit_breaker_active: bool = False
_circuit_breaker_reset_date: str = ""
_last_snapshot_ts: float = 0.0  # epoch seconds — guards back-to-back snapshots


def _persist_cb_state(db_conn):
    """Persist circuit breaker globals to the bot_state table."""
    try:
        set_state(db_conn, "circuit_breaker_active", "1" if _circuit_breaker_active else "0")
        set_state(db_conn, "circuit_breaker_reset_date", _circuit_breaker_reset_date or "")
    except Exception as e:
        logger.warning(f"[CircuitBreaker] Persist failed: {e}")


def _persist_daily_state(db_conn):
    """Persist daily one-shot dates (recap, gap protection, metrics, report)."""
    try:
        set_state(db_conn, "recap_sent_date",          _recap_sent_date or "")
        set_state(db_conn, "gap_protection_run_date",  _gap_protection_run_date or "")
        set_state(db_conn, "last_report_date",         _last_report_date or "")
        set_state(db_conn, "last_metrics_date",        _last_metrics_date or "")
        set_state(db_conn, "last_regime_exit_date",     _last_regime_exit_date or "")
    except Exception as e:
        logger.warning(f"[State] Persist daily dates failed: {e}")


def _restore_state(db_conn):
    """Restore persisted state from bot_state on startup."""
    global _circuit_breaker_active, _circuit_breaker_reset_date
    global _recap_sent_date, _gap_protection_run_date
    global _last_report_date, _last_metrics_date, _last_regime_exit_date
    try:
        cb = get_state(db_conn, "circuit_breaker_active")
        if cb is not None:
            _circuit_breaker_active = (cb == "1")
        cb_reset = get_state(db_conn, "circuit_breaker_reset_date")
        if cb_reset:
            _circuit_breaker_reset_date = cb_reset

        recap = get_state(db_conn, "recap_sent_date")
        if recap:
            _recap_sent_date = recap
        gp = get_state(db_conn, "gap_protection_run_date")
        if gp:
            _gap_protection_run_date = gp
        rep = get_state(db_conn, "last_report_date")
        if rep:
            _last_report_date = rep
        met = get_state(db_conn, "last_metrics_date")
        if met:
            _last_metrics_date = met
        rex = get_state(db_conn, "last_regime_exit_date")
        if rex:
            _last_regime_exit_date = rex

        logger.info(
            f"[State] Restored: CB={_circuit_breaker_active} (reset={_circuit_breaker_reset_date}) | "
            f"recap={_recap_sent_date} gap={_gap_protection_run_date} "
            f"report={_last_report_date} metrics={_last_metrics_date}"
        )
    except Exception as e:
        logger.warning(f"[State] Restore failed: {e}")


def send_daily_recap(broker: AlpacaBroker) -> bool:
    """Send daily P&L summary to ntfy after market close."""
    try:
        account = broker.get_account()
        equity = float(account["equity"])
        prev_close = equity - float(account["pnl_today"])   # last_equity = equity - pnl_today
        day_pnl = equity - prev_close
        day_pct = (day_pnl / prev_close) * 100 if prev_close else 0
        cash_pct = (float(account["cash"]) / equity) * 100 if equity else 0

        positions = broker.get_positions()
        pos_lines = []
        for pos in positions[:5]:  # top 5
            pnl = pos["unrealized_pnl_pct"]
            pos_lines.append(f"{pos['symbol']} {pnl:+.1f}%")

        emoji = "📈" if day_pnl >= 0 else "📉"
        body = (
            f"Portfolio: ${equity:,.0f} ({day_pnl:+,.0f}, {day_pct:+.2f}%) {emoji}\n"
            f"Cash: {cash_pct:.0f}% idle\n"
            f"Positions: {', '.join(pos_lines) if pos_lines else 'None'}\n"
            f"Go hit your goals 💪"
        )

        ok = notify.send(
            title=f"AlphaBot Daily — {day_pct:+.2f}%",
            body=body,
            priority="default",
            tags="chart_with_upwards_trend",
        )
        if ok:
            logger.info(f"[Recap] ntfy sent — P&L {day_pnl:+,.0f} ({day_pct:+.2f}%)")
        else:
            logger.warning("[Recap] ntfy send returned non-200")
        return ok
    except Exception as e:
        logger.error(f"[Recap] ntfy failed: {e}")
        return False


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


def check_circuit_breaker(broker: AlpacaBroker, db_conn=None):
    """
    Sets _circuit_breaker_active if portfolio down >3% on the day.
    Auto-resets if portfolio recovers to better than -2%.
    Persists changes to the bot_state table so they survive restarts.
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
            if db_conn is not None:
                _persist_cb_state(db_conn)
            try:
                notify.emergency("🚨 Circuit Breaker", f"Daily drawdown {day_pct:.1%} — entries halted", key="circuit_breaker")
            except Exception as e:
                logger.warning(f"[CircuitBreaker] ntfy failed: {e}")
        elif day_pct > -0.02 and _circuit_breaker_active:
            _circuit_breaker_active = False
            logger.info(
                f"[CircuitBreaker] RESET — portfolio recovered to {day_pct:.2%}, "
                f"resuming new entries."
            )
            if db_conn is not None:
                _persist_cb_state(db_conn)
            try:
                notify.emergency("🚨 Circuit Breaker", f"Daily drawdown reset (now {day_pct:.1%}) — entries resumed", key="circuit_breaker")
            except Exception as e:
                logger.warning(f"[CircuitBreaker] ntfy reset failed: {e}")
    except Exception as e:
        logger.warning(f"[CircuitBreaker] Check failed: {e}")


def run_all_strategies(broker: AlpacaBroker, db_conn):
    """Execute all strategies in sequence."""
    global _last_report_date, _last_metrics_date
    global _recap_sent_date, _gap_protection_run_date
    global _circuit_breaker_active, _circuit_breaker_reset_date
    global _last_cycle_ts

    # Fix 9: Re-entrancy guard — if a prior cycle is still running (e.g. a
    # strategy hung past the 5-minute tick), skip this tick rather than
    # spawning a concurrent run that could double-submit orders.
    if not _cycle_lock.acquire(blocking=False):
        logger.warning("[Main] Previous cycle still running, skipping this tick")
        return

    try:
        # Fix 8: liveness heartbeat for the health endpoint
        _last_cycle_ts = time.time()

        # Clear per-cycle yfinance memoize at the top of every run
        try:
            yf_cache.clear_cycle_cache()
        except Exception:
            pass

        # v79: sync exchange-filled orders (stops/TPs that filled without bot knowing)
        try:
            from strategies.position_lifecycle import sync_exchange_fills
            sync_exchange_fills(broker, db_conn)
        except Exception as _e:
            logger.debug(f"[Main] GTC fill sync error: {_e}")

        # v79: stale position re-evaluator
        try:
            from strategies.position_lifecycle import check_stale_positions
            check_stale_positions(broker, db_conn)
        except Exception as _e:
            logger.debug(f"[Main] Stale position check error: {_e}")

        now_utc = datetime.now(timezone.utc)
        today_utc = now_utc.strftime("%Y-%m-%d")
        weekday = now_utc.weekday()

        # ── Reset circuit breaker at start of each new trading day (after 13:30 UTC) ──
        # Fires on the first cycle of the day at/after 13:30 UTC even if the exact
        # 13:30-13:59 window was missed (e.g. bot restart at 14:00).
        if (weekday < 5
                and _circuit_breaker_reset_date != today_utc
                and (now_utc.hour > 13 or (now_utc.hour == 13 and now_utc.minute >= 30))):
            if _circuit_breaker_active:
                logger.info("[CircuitBreaker] New trading day — resetting active flag")
            _circuit_breaker_active = False
            _circuit_breaker_reset_date = today_utc
            _persist_cb_state(db_conn)

        # ── Pre-market gap protection: 13:00–13:10 UTC (8:00–8:10 AM ET), weekdays ─
        if (now_utc.hour == 13 and now_utc.minute < 10 and weekday < 5
                and _gap_protection_run_date != today_utc):
            try:
                run_gap_protection(broker)
            except Exception as e:
                logger.error(f"[GapProtect] Outer failure: {e}")
            _gap_protection_run_date = today_utc
            _persist_daily_state(db_conn)

        # ── Daily P&L recap: 20:00–22:59 UTC (9 PM–midnight BST), weekdays only ─
        # Fires after market close (4 PM ET = 9 PM BST = 20:00 UTC).
        if (now_utc.hour in (20, 21, 22)
                and weekday < 5 and _recap_sent_date != today_utc):
            try:
                success = send_daily_recap(broker)
                if success:
                    _recap_sent_date = today_utc
                    _persist_daily_state(db_conn)
                else:
                    logger.warning("[Recap] Send failed — will retry next cycle")
            except Exception as e:
                logger.error(f"[Recap] Outer failure: {e}")

        # ── Account health check — halt on real margin call / cash floor ─────────
        try:
            acc = broker.get_account()
            live_cash = float(acc["cash"])
            live_pv   = float(acc["portfolio_value"])
            equity    = float(acc.get("equity") or 0.0)
            maint     = float(acc.get("maintenance_margin") or 0.0)
            if maint > 0:
                ratio = equity / maint
                logger.info(
                    f"[MarginCheck] equity=${equity:,.0f} vs maintenance=${maint:,.0f} — ratio={ratio:.2f}x"
                )
                if equity < maint * 1.1:
                    msg = (
                        f"🚨 MARGIN CALL RISK: equity=${equity:,.0f} vs maintenance=${maint:,.0f} "
                        f"(ratio={ratio:.2f}x, threshold=1.10x). Halting all entries this cycle."
                    )
                    logger.critical(msg)
                    from utils.notify import emergency as _notify_emergency
                    _notify_emergency("🚨 Margin Call Risk", msg, key="margin_call_risk", priority="urgent")
                    run_trade_management(broker, db_conn)  # still run exits
                    return
            # v86 (C4) — short proceeds inflate raw cash; measure reserve against
            # equity-adjusted available cash (cash minus short market value).
            short_mv = float(acc.get("short_market_value") or 0.0)
            avail_ratio = (live_cash - short_mv) / equity if equity > 0 else 1.0
            if equity > 0 and avail_ratio < MIN_CASH_RESERVE_PCT:
                msg = f"Cash floor: ${live_cash - short_mv:,.0f} ({avail_ratio:.1%} of equity). Halting entries."
                logger.warning(msg)  # silent — bot handles this itself
                run_trade_management(broker, db_conn)
                return
        except Exception as e:
            logger.warning(f"[HealthCheck] Failed: {e}")

        # ── Circuit breaker check (3% daily drawdown halts new entries) ──────────
        try:
            check_circuit_breaker(broker, db_conn)
        except Exception as e:
            logger.warning(f"[CircuitBreaker] Outer failure: {e}")

        if _circuit_breaker_active:
            logger.info("[CircuitBreaker] ACTIVE — skipping new entries, running exits only")
            try:
                run_trade_management(broker, db_conn)
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
                    _persist_daily_state(db_conn)
                except Exception as e:
                    logger.error(f"Weekly report error: {e}", exc_info=True)

        # ── Daily risk metrics: log once per day after 10 AM ET ──────────────────
        if now_et.hour >= 10:
            today_str = now_et.strftime("%Y-%m-%d")
            if _last_metrics_date != today_str:
                try:
                    from utils.risk_metrics import compute_metrics
                    from config import ALPACA_BASE_URL
                    compute_metrics(ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL, db_conn=db_conn)
                    _last_metrics_date = today_str
                    _persist_daily_state(db_conn)
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

        # ── Update OFI watched symbols based on current positions ────────────────
        try:
            ofi_monitor.update_watched(broker)
        except Exception as e:
            logger.debug(f"[OFI] update_watched failed: {e}")

        # ── Trade management: v44 — earnings tightening + post-earnings review
        # run BEFORE the normal ratchet/trailing-stop/partial-take logic.
        run_trade_management(broker, db_conn)

        # ── Pyramid entry: check tracked positions for +2% / +4% add triggers ────
        try:
            check_pyramid_adds(broker)
        except Exception as e:
            logger.error(f"Pyramid add check error: {e}", exc_info=True)

        # ── Market regime check (v83: 3-tier) ────────────────────────────────────
        regime = get_market_regime()
        logger.info(f"[Regime] Current regime: {regime.upper()}")

        # v85 — persist the active 3-tier regime so record_entry() can tag each
        # new position with the regime it was opened under.
        try:
            from db import set_state as _set_state
            _set_state(db_conn, "current_regime", regime)
        except Exception as e:
            logger.debug(f"[Regime] could not persist current_regime: {e}")

        # v85 — regime-change exits: close positions whose opening_strategy is no
        # longer compatible with the current regime. Runs once per day on the
        # first strategy pass after the regime is (re)evaluated at the open.
        global _last_regime_exit_date
        _today_str = now_et.strftime("%Y-%m-%d")
        if _last_regime_exit_date != _today_str:
            try:
                from strategies.regime_exit import check_regime_exits
                exited = check_regime_exits(broker, regime)
                logger.info(f"[REGIME EXIT] {len(exited)} position(s) closed: {exited}")
                _last_regime_exit_date = _today_str
                _persist_daily_state(db_conn)
            except Exception as e:
                logger.error(f"[REGIME EXIT] check_regime_exits failed: {e}", exc_info=True)

        if regime == "bear":
            logger.info("BEAR regime — running bear-safe strategies")
            _strategy_list = [
                (mean_reversion.run,    "Mean reversion"),   # 1.2×
                (trend_following.run,   "Trend following"),  # short-side only, internally gated
                (vix_reversal.run,      "VIX reversal"),     # designed for bear
                (gap_scanner.run,       "Gap scanner"),      # exits only
                (short_hedge.run,       "Short hedge"),      # inverse ETFs
                (pairs_trading.run,     "Pairs trading"),    # market-neutral
                (earnings_drift.run,    "Earnings drift"),   # catalyst-driven, 0.5×
                (trend_pullback.run,    "Trend pullback"),   # short-side entries, 0.8×
                (multi_tf_rsi.run,      "Multi-TF RSI"),     # strongest in bear, 1.3×
            ]

        elif regime == "chop":
            logger.info("CHOP regime — running regime-agnostic strategies only")
            _strategy_list = [
                (mean_reversion.run,    "Mean reversion"),   # 1.5× in chop — primary
                (pairs_trading.run,     "Pairs trading"),    # market-neutral
                (vix_reversal.run,      "VIX reversal"),     # VIX spikes common in chop
                (earnings_drift.run,    "Earnings drift"),   # catalyst-driven, 0.8×
                (short_hedge.run,       "Short hedge"),      # 0.5×
                (gap_scanner.run,       "Gap scanner"),      # exits only
                (multi_tf_rsi.run,      "Multi-TF RSI"),     # proven positive in chop, 0.8×
                (fifty_two_wh.run,      "52WH-Vol"),         # v87: 0.5× in chop
                # Below handle exits only — self-skip new entries via regime_weight=0.0
                (momentum.run,          "Momentum"),
                (breakout.run,          "Breakout"),
                (trend_following.run,   "Trend following"),
                (sector_rotation.run,   "Sector rotation"),
                (spy_dip.run,           "SPY dip"),
            ]

        else:  # bull
            logger.info("BULL regime — running all strategies")
            _strategy_list = [
                (mean_reversion.run,    "Mean reversion"),
                (trend_following.run,   "Trend following"),
                (spy_dip.run,           "SPY dip"),
                (vix_reversal.run,      "VIX reversal"),
                (gap_scanner.run,       "Gap scanner"),
                (earnings_drift.run,    "Earnings drift"),
                (sector_rotation.run,   "Sector rotation"),
                (momentum.run,          "Momentum"),
                (breakout.run,          "Breakout"),
                (short_hedge.run,       "Short hedge"),
                (pairs_trading.run,     "Pairs trading"),
                (insider_buying.run,    "Insider buying"),
                (options_flow.run,      "Options flow"),
                (squeeze_screener.run,  "Squeeze screener"),
                (trend_pullback.run,    "Trend pullback"),   # v83: 1.5×
                (multi_tf_rsi.run,      "Multi-TF RSI"),     # v83: 1.2×
                (fifty_two_wh.run,      "52WH-Vol"),         # v87: 1.0× in bull
            ]

        for fn, name in _strategy_list:
            try:
                fn(broker, db_conn)
            except Exception as e:
                logger.error(f"{name} error: {e}", exc_info=True)

        # Single gc.collect() after the whole loop — was per-iteration before (CPU burn)
        gc.collect()

        # ── Event-driven: consume news_scanner EVENT_QUEUE ───────────────────────
        try:
            event_driven.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Event-driven error: {e}", exc_info=True)

        # ── Earnings Prediction (v49): 5-signal pre-earnings LONG entries ───────
        try:
            earnings_prediction.run(broker, db_conn)
        except Exception as e:
            logger.error(f"Earnings prediction error: {e}", exc_info=True)

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

        # ── VWAP Reclaim: intraday gap-down reclaim (9:35–10:30 AM ET entries) ───
        try:
            vwap_reclaim.run(broker, db_conn)
        except Exception as e:
            logger.error(f"VWAP Reclaim error: {e}", exc_info=True)

        logger.info("Strategy run complete")
    finally:
        _cycle_lock.release()


def take_snapshot(broker: AlpacaBroker, db_conn):
    """Record portfolio snapshot for performance tracking."""
    global _last_snapshot_ts
    # Dedup: skip if we snapshotted in the last 30 minutes (QW9)
    now_ts = time.time()
    if now_ts - _last_snapshot_ts < 30 * 60:
        logger.debug("[Snapshot] Skipping — last snapshot < 30min ago")
        return
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
        _last_snapshot_ts = now_ts
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
    Returns 200 OK on GET / when the trading loop is fresh (<15 min since last
    cycle), 503 otherwise so Railway can detect a dead loop instead of treating
    a zombie process as healthy.
    """
    from http.server import HTTPServer, BaseHTTPRequestHandler

    port = int(os.environ.get("PORT", 8080))
    STALE_AFTER_SEC = 15 * 60  # 15 minutes — > 2x CHECK_INTERVAL_MIN cycle

    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            # Before the first cycle has run, _last_cycle_ts is 0.0 — treat as
            # healthy during the startup grace window so Railway doesn't kill
            # the container before strategies have had a chance to fire.
            if _last_cycle_ts == 0.0 or (time.time() - _last_cycle_ts) < STALE_AFTER_SEC:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
            else:
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b"trading loop stalled")
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


def _scheduled_recap(broker: AlpacaBroker, db_conn):
    """Schedule-driven recap wrapper — fires at 20:35 UTC via schedule lib."""
    global _recap_sent_date
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _recap_sent_date == today_utc:
        return
    if datetime.now(timezone.utc).weekday() >= 5:
        return
    try:
        if send_daily_recap(broker):
            _recap_sent_date = today_utc
            _persist_daily_state(db_conn)
    except Exception as e:
        logger.error(f"[Recap] Scheduled fire error: {e}")


def _bracket_heartbeat(broker: AlpacaBroker):
    """
    Periodic safety net: re-run migrate_missing_brackets() during market hours
    so a silently-failed bracket or a stuck GTC order can't leave a position
    unprotected for days. Polls every 30 min; self-gates to Mon–Fri 9:30–16:00 ET.
    """
    now = datetime.now(EASTERN)
    if now.weekday() >= 5:
        return
    if not (dtime(9, 30) <= now.time() <= dtime(16, 0)):
        return
    logger.info("[BRACKET HEARTBEAT] Running migrate_missing_brackets() periodic check")
    try:
        n = migrate_missing_brackets(broker)
        if n:
            logger.info(f"[BRACKET HEARTBEAT] placed {n} OCO bracket(s)")
    except Exception as e:
        logger.error(f"[BRACKET HEARTBEAT] migrate_missing_brackets failed: {e}", exc_info=True)


def main():
    from utils.clock import log_timestamp, now_et, market_open_et, minutes_to_close
    logger.info(f"=== AlphaBot Starting ({VERSION}) ===")
    logger.info(f"[Clock] {log_timestamp()} | Market open: {market_open_et()} | Mins to close: {minutes_to_close()}")

    start_health_server()  # Must bind to $PORT or Railway kills the container

    if ALPACA_API_KEY == "YOUR_API_KEY_HERE":
        logger.error("API keys not configured!")
        sys.exit(1)

    init_db()
    db_conn = get_connection()

    broker = AlpacaBroker()
    # Startup connectivity check — fail fast if Alpaca credentials are bad
    # so Railway sees a hard exit instead of a zombie process passing /health.
    try:
        acct = broker.get_account()
    except Exception as e:
        logger.error(f"[Startup] Alpaca connectivity check failed: {e}", exc_info=True)
        sys.exit(1)
    logger.info(f"Connected | Portfolio: ${acct['portfolio_value']:,.2f} | Cash: ${acct['cash']:,.2f}")

    # Restore strategy tags from DB so positions aren't labelled 'unknown' after restart
    restored = restore_tags_from_db(db_conn)
    logger.info(f"Restored {restored} strategy tag(s) from trade history")

    # Restore circuit breaker + daily one-shot dates from bot_state (H2 + M22)
    _restore_state(db_conn)

    # Hydrate trailing-stop / ratchet / partial-taken state from DB (H4)
    try:
        restore_trade_management_state(broker, db_conn)
    except Exception as e:
        logger.warning(f"[Startup] trade_management restore failed: {e}")

    # v82: scan all open positions and place any missing OCO brackets
    # Runs every restart — idempotent if brackets are already live.
    try:
        n = migrate_missing_brackets(broker)
        if n:
            logger.info(f"[Startup] migrate_missing_brackets: placed {n} OCO bracket(s)")
    except Exception as e:
        logger.warning(f"[Startup] migrate_missing_brackets failed: {e}")

    logger.info("[Startup] NOW flagged for manual post-earnings review")

    # v44 one-time: apply earnings stop tightening on startup for any position
    # that would qualify — catches positions already open before this feature existed
    # (e.g. PANW: earnings today, RSI 95, +18% — locks the stop immediately).
    try:
        startup_positions = broker.get_positions()
        apply_earnings_stop_tightening(startup_positions, broker, db_conn)
    except Exception as e:
        logger.warning(f"[Startup] earnings stop tightening failed: {e}")

    # Hydrate pairs_trading._active_pairs from open positions (M23)
    try:
        from strategies.pairs_trading import restore_active_pairs
        restore_active_pairs(broker, db_conn)
    except Exception as e:
        logger.debug(f"[Startup] pairs restore failed: {e}")

    # Sanity check — fail loudly at startup if a critical import is missing (QW12)
    try:
        from utils.news_sentiment import is_sentiment_bullish
        assert callable(is_sentiment_bullish), "news_sentiment.is_sentiment_bullish missing"
    except Exception as e:
        logger.error(f"[Startup] is_sentiment_bullish import failed: {e}")

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

    try:
        earnings_calendar.start()
    except Exception as e:
        logger.error(f"Earnings calendar start failed: {e}")

    try:
        ofi_monitor.start(broker)
    except Exception as e:
        logger.error(f"OFI monitor start failed: {e}")

    # Schedules
    schedule.every(CHECK_INTERVAL_MIN).minutes.do(run_all_strategies, broker, db_conn)
    schedule.every(60).minutes.do(take_snapshot, broker, db_conn)
    schedule.every().day.at("21:05").do(take_snapshot, broker, db_conn)  # 21:05 UTC = 16:05 ET
    # Dedicated recap fire — survives edge cases where the polling window misses (M17)
    schedule.every().day.at("20:35").do(_scheduled_recap, broker, db_conn)
    # v75 FIX 1 — overnight loser sweep. Polls every minute; the function
    # itself enforces the 20:15–20:29 BST window via now_london().
    schedule.every(1).minutes.do(check_overnight_exit, broker, db_conn)
    # v84.1 — bracket-order heartbeat. Re-run migrate_missing_brackets() every
    # 30 min; _bracket_heartbeat() self-gates to Mon–Fri 9:30–16:00 ET.
    schedule.every(30).minutes.do(_bracket_heartbeat, broker)

    take_snapshot(broker, db_conn)

    # Delay first strategy run by CHECK_INTERVAL_MIN — let imports settle.
    logger.info(f"Bot running — first strategy run in {CHECK_INTERVAL_MIN} minutes")

    logger.info(f"Bot running — checking every {CHECK_INTERVAL_MIN} minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
