"""
Re-entry cooldown tracker.
After a stop-loss fires, blocks re-entry for COOLDOWN_HOURS.

2h is a balance: long enough to avoid immediately buying back into a broken
name on the next 5-minute cycle, short enough to allow valid re-entries
later the same session in a bull market.
"""
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

_cooldowns: dict[str, datetime] = {}  # symbol -> allowed_reentry_after
COOLDOWN_HOURS = 2  # 2h post-stop cooldown


def set_cooldown(symbol: str) -> None:
    """Mark symbol as on cooldown for COOLDOWN_HOURS."""
    if COOLDOWN_HOURS <= 0:
        return
    until = datetime.now(timezone.utc) + timedelta(hours=COOLDOWN_HOURS)
    _cooldowns[symbol] = until
    logger.info(f"[Cooldown] {symbol} on cooldown until {until.isoformat()} ({COOLDOWN_HOURS}h)")


def is_on_cooldown(symbol: str) -> bool:
    """True if symbol is still inside its cooldown window."""
    if COOLDOWN_HOURS <= 0:
        return False
    until = _cooldowns.get(symbol)
    if until is None:
        return False
    if datetime.now(timezone.utc) >= until:
        _cooldowns.pop(symbol, None)
        return False
    return True


def get_all_cooldowns() -> dict[str, str]:
    """Returns all active cooldowns as {symbol: time_remaining_str} for logging."""
    now = datetime.now(timezone.utc)
    result = {}
    for sym, until in list(_cooldowns.items()):
        if until > now:
            remaining = until - now
            total = int(remaining.total_seconds())
            hours = total // 3600
            mins = (total % 3600) // 60
            result[sym] = f"{hours}h{mins}m"
        else:
            del _cooldowns[sym]
    return result
