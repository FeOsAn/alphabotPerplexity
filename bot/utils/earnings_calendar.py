"""
Earnings blackout: returns True if a symbol has earnings within the next 2 calendar days.
Uses the existing earnings cache from earnings_drift if available, otherwise fetches fresh.
"""
import logging
import yfinance as yf
import gc
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def has_upcoming_earnings(symbol: str, days_ahead: int = 2) -> bool:
    """Return True if symbol has earnings within the next `days_ahead` calendar days."""
    try:
        ticker = yf.Ticker(symbol)
        cal = ticker.calendar
        gc.collect()
        if cal is None or cal.empty:
            return False
        # calendar has columns like 'Earnings Date'
        if "Earnings Date" in cal.columns:
            dates = cal["Earnings Date"].dropna()
        elif hasattr(cal, "index") and "Earnings Date" in cal.index:
            dates = [cal.loc["Earnings Date"]]
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
