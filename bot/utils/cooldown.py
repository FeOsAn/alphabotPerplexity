"""
Re-entry cooldown tracker.
After a stop-loss fires, blocks re-entry for 24 hours.
In-memory — resets on restart (acceptable, Railway restarts are intentional).
"""
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

_cooldowns: dict[str, datetime] = {}  # symbol -> allowed_reentry_after
COOLDOWN_HOURS = 24


def set_cooldown(symbol: str) -> None:
    """Call when a stop-loss fires on a symbol."""
    until = datetime.now(timezone.utc) + timedelta(hours=COOLDOWN_HOURS)
    _cooldowns[symbol] = until
    logger.info(f"[Cooldown] {symbol} blocked for {COOLDOWN_HOURS}h (until {until.strftime('%H:%M UTC')})")


def is_on_cooldown(symbol: str) -> bool:
    """Returns True if symbol is still in cooldown period."""
    until = _cooldowns.get(symbol)
    if until is None:
        return False
    if datetime.now(timezone.utc) >= until:
        del _cooldowns[symbol]
        logger.debug(f"[Cooldown] {symbol} cooldown expired")
        return False
    return True


def get_all_cooldowns() -> dict[str, str]:
    """Returns all active cooldowns as {symbol: time_remaining_str} for logging."""
    now = datetime.now(timezone.utc)
    result = {}
    for sym, until in list(_cooldowns.items()):
        if until > now:
            remaining = until - now
            hours = remaining.seconds // 3600
            mins = (remaining.seconds % 3600) // 60
            result[sym] = f"{hours}h{mins}m"
        else:
            del _cooldowns[sym]
    return result
