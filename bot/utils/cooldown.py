"""
Re-entry cooldown tracker.
After a stop-loss fires, blocks re-entry for COOLDOWN_HOURS.

v71: Persisted to DB so container restarts no longer wipe cooldowns.
Bumped from 2h → 6h: ensures a symbol cannot be re-entered more than
once per half-session.
"""
import logging
from datetime import datetime, timedelta
from utils.clock import now_utc

logger = logging.getLogger(__name__)

_cooldowns: dict[str, datetime] = {}  # symbol -> allowed_reentry_after
COOLDOWN_HOURS = 6  # 6h post-stop cooldown (v71)


def _db_key(symbol: str) -> str:
    return f"cooldown_{symbol}"


def _get_db_conn():
    """Best-effort DB conn fetch. Returns None on failure (cooldown still works in-process)."""
    try:
        from db import get_connection
        return get_connection()
    except Exception as e:
        logger.debug(f"[Cooldown] DB conn unavailable: {e}")
        return None


def set_cooldown(symbol: str) -> None:
    """Mark symbol as on cooldown for COOLDOWN_HOURS. Persists to DB."""
    if COOLDOWN_HOURS <= 0:
        return
    until = now_utc() + timedelta(hours=COOLDOWN_HOURS)
    _cooldowns[symbol] = until
    # Persist to DB
    conn = _get_db_conn()
    if conn is not None:
        try:
            from db import set_state
            set_state(conn, _db_key(symbol), until.isoformat())
        except Exception as e:
            logger.debug(f"[Cooldown] DB persist failed for {symbol}: {e}")
    logger.info(f"[Cooldown] {symbol} on cooldown until {until.isoformat()} ({COOLDOWN_HOURS}h)")


def is_on_cooldown(symbol: str) -> bool:
    """True if symbol is still inside its cooldown window. Checks DB if not in-process."""
    if COOLDOWN_HOURS <= 0:
        return False
    # Fast path: in-process dict
    until = _cooldowns.get(symbol)
    if until is not None:
        if now_utc() >= until:
            _cooldowns.pop(symbol, None)
            # Expired in-process — also clear DB
            conn = _get_db_conn()
            if conn is not None:
                try:
                    from db import del_state
                    del_state(conn, _db_key(symbol))
                except Exception:
                    pass
            return False
        return True
    # Slow path: check DB (survives restart)
    conn = _get_db_conn()
    if conn is None:
        return False
    try:
        from db import get_state, del_state
        val = get_state(conn, _db_key(symbol))
        if not val:
            return False
        try:
            db_until = datetime.fromisoformat(val)
        except Exception:
            del_state(conn, _db_key(symbol))
            return False
        if now_utc() >= db_until:
            del_state(conn, _db_key(symbol))
            return False
        # Repopulate in-process cache
        _cooldowns[symbol] = db_until
        return True
    except Exception as e:
        logger.debug(f"[Cooldown] DB check failed for {symbol}: {e}")
        return False


def clear_cooldown(symbol: str) -> None:
    """Clear cooldown for a symbol from both in-process dict and DB."""
    _cooldowns.pop(symbol, None)
    conn = _get_db_conn()
    if conn is not None:
        try:
            from db import del_state
            del_state(conn, _db_key(symbol))
        except Exception as e:
            logger.debug(f"[Cooldown] DB clear failed for {symbol}: {e}")


def get_all_cooldowns() -> dict[str, str]:
    """Returns all active cooldowns as {symbol: time_remaining_str} for logging."""
    now = now_utc()
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
