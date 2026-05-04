"""
Time-of-day filters for entry signals.
Blocks entries in the first 15 min (9:30–9:45 ET) and last 15 min (3:45–4:00 ET).
These windows have the widest spreads and most noise.
"""
import logging
from datetime import datetime, time
import pytz

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")

def is_entry_allowed() -> bool:
    """Return True if current time is in a safe entry window."""
    now_et = datetime.now(ET).time()
    market_open = time(9, 30)
    safe_start = time(9, 45)
    safe_end = time(15, 45)
    market_close = time(16, 0)

    if now_et < market_open or now_et >= market_close:
        logger.debug("[TimeFilter] Market closed")
        return False  # outside market hours

    if market_open <= now_et < safe_start:
        logger.info("[TimeFilter] Blocked — first 15 min (open noise)")
        return False

    if now_et >= safe_end:
        logger.info("[TimeFilter] Blocked — last 15 min (close noise)")
        return False

    return True
