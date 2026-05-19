"""
Earnings Calendar.

Two roles:
1. has_upcoming_earnings(symbol) — legacy 2-day blackout check (used by 4 strategies).
2. Pre-loader (start/refresh/get_upcoming) — every Sunday at 20:00 UTC (and on startup),
   fetches the next 14 days of earnings dates for the full earnings_nlp universe.
   Stored in UPCOMING_EARNINGS dict, used as a fast path by earnings_nlp.
"""
import gc
import logging
import threading
import time
import yfinance as yf
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ── Shared calendar state ──────────────────────────────────────────────────────
UPCOMING_EARNINGS: dict = {}   # {symbol: [datetime, ...]}
_calendar_lock = threading.Lock()
_last_refresh: str = ""
_running = False
_thread = None


# ── Legacy blackout check (unchanged behaviour) ────────────────────────────────
def has_upcoming_earnings(symbol: str, days_ahead: int = 2) -> bool:
    """Return True if symbol has earnings within the next `days_ahead` calendar days."""
    try:
        ticker = yf.Ticker(symbol)
        cal = ticker.calendar
        gc.collect()
        if cal is None:
            return False
        # cal may be a DataFrame or a dict, depending on yfinance version
        if hasattr(cal, "empty") and cal.empty:
            return False
        dates = []
        if hasattr(cal, "columns") and "Earnings Date" in getattr(cal, "columns", []):
            dates = cal["Earnings Date"].dropna().tolist()
        elif hasattr(cal, "index") and "Earnings Date" in getattr(cal, "index", []):
            val = cal.loc["Earnings Date"]
            dates = val.tolist() if hasattr(val, "tolist") else [val]
        elif isinstance(cal, dict):
            ed = cal.get("Earnings Date", [])
            dates = ed if isinstance(ed, list) else ([ed] if ed else [])
        else:
            return False
        now = datetime.now(timezone.utc).date()
        cutoff = now + timedelta(days=days_ahead)
        for d in dates:
            try:
                earn_date = d.date() if hasattr(d, "date") else d
                if now <= earn_date <= cutoff:
                    logger.info(f"[Blackout] {symbol} has earnings on {earn_date} — blackout active")
                    return True
            except Exception:
                continue
        return False
    except Exception as e:
        logger.debug(f"[Blackout] Could not check earnings for {symbol}: {e}")
        return False


# ── Pre-loader ─────────────────────────────────────────────────────────────────
def _get_universe():
    """Import EARNINGS_UNIVERSE from earnings_nlp lazily to avoid circular imports."""
    try:
        from strategies.earnings_nlp import EARNINGS_UNIVERSE
        return list(EARNINGS_UNIVERSE)
    except Exception:
        return []


def _fetch_calendar_for_symbol(sym: str, horizon_days: int = 14) -> list:
    """Fetch upcoming earnings dates for one symbol."""
    results = []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=horizon_days)
    try:
        ticker = yf.Ticker(sym)
        cal = ticker.calendar
        if cal is None:
            return []
        earnings_dates = []
        if isinstance(cal, dict):
            ed = cal.get("Earnings Date", [])
            if ed:
                earnings_dates = ed if isinstance(ed, list) else [ed]
        elif hasattr(cal, "loc"):
            try:
                ed = cal.loc["Earnings Date"].values
                earnings_dates = list(ed)
            except Exception:
                pass
        for ed in earnings_dates:
            try:
                if hasattr(ed, "to_pydatetime"):
                    ed = ed.to_pydatetime()
                if isinstance(ed, str):
                    ed = datetime.fromisoformat(ed)
                if hasattr(ed, "date") and not isinstance(ed, datetime):
                    # plain date object — convert to datetime at midnight UTC
                    ed = datetime(ed.year, ed.month, ed.day, tzinfo=timezone.utc)
                if isinstance(ed, datetime) and ed.tzinfo is None:
                    ed = ed.replace(tzinfo=timezone.utc)
                if isinstance(ed, datetime):
                    ed_utc = ed.astimezone(timezone.utc)
                    if now <= ed_utc <= cutoff:
                        results.append(ed_utc)
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"[EarningsCalendar] Error fetching {sym}: {e}")
    finally:
        gc.collect()
    return results


def refresh():
    """Fetch 14-day earnings calendar for all symbols in parallel."""
    global _last_refresh
    # Set timestamp FIRST to prevent concurrent worker runs treating us as stale
    _last_refresh = datetime.now(timezone.utc).isoformat()
    universe = _get_universe()
    if not universe:
        logger.warning("[EarningsCalendar] Empty universe — skipping refresh")
        return

    logger.info(f"[EarningsCalendar] Refreshing calendar for {len(universe)} symbols...")
    new_calendar = {}

    try:
        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = {executor.submit(_fetch_calendar_for_symbol, sym): sym for sym in universe}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    dates = future.result()
                    if dates:
                        new_calendar[sym] = dates
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"[EarningsCalendar] Refresh executor error: {e}")
        return

    with _calendar_lock:
        UPCOMING_EARNINGS.clear()
        UPCOMING_EARNINGS.update(new_calendar)

    _last_refresh = datetime.now(timezone.utc).isoformat()
    count = sum(len(v) for v in new_calendar.values())
    logger.info(f"[EarningsCalendar] Refresh complete — {len(new_calendar)} symbols, {count} upcoming events")


def get_upcoming(days_ahead: int = 14) -> dict:
    """Return copy of upcoming earnings within days_ahead."""
    with _calendar_lock:
        cutoff = datetime.now(timezone.utc) + timedelta(days=days_ahead)
        return {
            sym: [d for d in dates if d <= cutoff]
            for sym, dates in UPCOMING_EARNINGS.items()
            if any(d <= cutoff for d in dates)
        }


def _worker():
    """Background thread: refresh on startup, then every Sunday at 20:00 UTC."""
    try:
        refresh()
    except Exception as e:
        logger.error(f"[EarningsCalendar] Initial refresh failed: {e}")

    while _running:
        try:
            now = datetime.now(timezone.utc)
            is_sunday_evening = (now.weekday() == 6 and now.hour == 20 and now.minute < 5)
            last_dt = None
            if _last_refresh:
                try:
                    last_dt = datetime.fromisoformat(_last_refresh)
                except Exception:
                    last_dt = None
            stale = (last_dt is None or (now - last_dt).total_seconds() > 86400)
            if is_sunday_evening or stale:
                refresh()
        except Exception as e:
            logger.error(f"[EarningsCalendar] Worker error: {e}")
        time.sleep(300)


def start():
    global _running, _thread
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_worker, daemon=True, name="earnings_calendar")
    _thread.start()
    logger.info("[EarningsCalendar] Started (14-day forward calendar, Sunday refresh)")


def stop():
    global _running
    _running = False


# ── Next-earnings lookup (v44) ────────────────────────────────────────────────
# Used by trade_management.apply_earnings_stop_tightening to compute
# days_to_earnings for any open position. 24-hour module-level cache so we
# don't slam yfinance every cycle.
_next_earnings_cache: dict = {}   # symbol -> (date|None, cached_at_epoch)
_NEXT_EARNINGS_TTL = 86400        # 24 hours


def get_next_earnings_date(symbol: str):
    """Return the next upcoming earnings date (datetime.date) or None.

    Lookup order:
      1. In-memory 24h cache.
      2. Pre-loaded UPCOMING_EARNINGS (refreshed weekly).
      3. yf.Ticker(symbol).calendar (best-effort, swallows all errors).
    """
    from datetime import date as _date
    now_ts = time.time()
    cached = _next_earnings_cache.get(symbol)
    if cached is not None and (now_ts - cached[1]) < _NEXT_EARNINGS_TTL:
        return cached[0]

    today = datetime.now(timezone.utc).date()
    next_dt = None

    # 2. Pre-loaded calendar
    try:
        with _calendar_lock:
            dates = UPCOMING_EARNINGS.get(symbol, [])
        upcoming = []
        for d in dates:
            try:
                dd = d.date() if hasattr(d, "date") else d
                if isinstance(dd, _date) and dd >= today:
                    upcoming.append(dd)
            except Exception:
                continue
        if upcoming:
            next_dt = min(upcoming)
    except Exception:
        pass

    # 3. yfinance fallback
    if next_dt is None:
        try:
            ticker = yf.Ticker(symbol)
            cal = ticker.calendar
            gc.collect()
            raw_dates = []
            if cal is None:
                pass
            elif isinstance(cal, dict):
                ed = cal.get("Earnings Date", [])
                if ed:
                    raw_dates = ed if isinstance(ed, list) else [ed]
            elif hasattr(cal, "empty") and not cal.empty:
                if hasattr(cal, "columns") and "Earnings Date" in getattr(cal, "columns", []):
                    raw_dates = cal["Earnings Date"].dropna().tolist()
                elif hasattr(cal, "index") and "Earnings Date" in getattr(cal, "index", []):
                    val = cal.loc["Earnings Date"]
                    raw_dates = val.tolist() if hasattr(val, "tolist") else [val]
            upcoming = []
            for d in raw_dates:
                try:
                    dd = d.date() if hasattr(d, "date") else d
                    if isinstance(dd, _date) and dd >= today:
                        upcoming.append(dd)
                except Exception:
                    continue
            if upcoming:
                next_dt = min(upcoming)
        except Exception as e:
            logger.debug(f"[EarningsCalendar] get_next_earnings_date {symbol}: {e}")

    _next_earnings_cache[symbol] = (next_dt, now_ts)
    return next_dt
