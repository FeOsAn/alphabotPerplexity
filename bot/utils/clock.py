"""
clock.py — Single source of truth for all time in AlphaBot.

NEVER use datetime.now() or datetime.utcnow() anywhere in the codebase.
Always use these helpers instead.

Market facts:
  - NYSE/NASDAQ open:  09:30 ET  = 14:30 UTC (13:30 UTC during US DST)
  - NYSE/NASDAQ close: 16:00 ET  = 21:00 UTC (20:00 UTC during US DST)
  - US market hours are in America/New_York (handles DST automatically)
  - Bot operator is in London (Europe/London = BST = UTC+1 in summer)

Rule: ALL internal timestamps use UTC. Display to logs in ET + UTC.
"""

from datetime import datetime, timezone
import pytz

_ET = pytz.timezone("America/New_York")
_LONDON = pytz.timezone("Europe/London")


def now_utc() -> datetime:
    """Current time as timezone-aware UTC datetime. Use this everywhere."""
    return datetime.now(timezone.utc)


def now_et() -> datetime:
    """Current time in US Eastern (handles EDT/EST automatically)."""
    return datetime.now(_ET)


def now_london() -> datetime:
    """Current time in London (handles BST/GMT automatically)."""
    return datetime.now(_LONDON)


def today_utc() -> str:
    """Today's date as UTC string 'YYYY-MM-DD'."""
    return now_utc().strftime("%Y-%m-%d")


def market_open_et() -> bool:
    """True if NYSE is currently open (09:30–16:00 ET, Mon–Fri)."""
    et = now_et()
    if et.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    open_time  = et.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_time = et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return open_time <= et < close_time


def minutes_to_close() -> int:
    """Minutes until market close. Negative if already closed."""
    et = now_et()
    close_time = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return int((close_time - et).total_seconds() / 60)


def log_timestamp() -> str:
    """Human-readable timestamp for logs: '2026-05-20 21:00 UTC | 17:00 ET | 22:00 BST'"""
    utc = now_utc()
    et  = utc.astimezone(_ET)
    lon = utc.astimezone(_LONDON)
    return (
        f"{utc.strftime('%Y-%m-%d %H:%M')} UTC | "
        f"{et.strftime('%H:%M')} ET | "
        f"{lon.strftime('%H:%M')} BST"
    )
