"""
Cross-strategy re-entry cooldown lock.

After a stop-loss OR take-profit fires on a symbol, blocks re-entry by ANY
strategy for SYMBOL_COOLDOWN_HOURS.

v94: the lock is now GLOBAL (cross-strategy) and durable, backed by the
dedicated `symbol_cooldown` DB table rather than per-process state or
per-strategy bot_state keys. Previously the cooldown was keyed only by symbol
in an in-process dict and a bot_state row; the central entry gate never
consulted it, so a different strategy could re-enter a symbol minutes after a
stop-out (the MRVL/NKE rapid-fire loop). Now `is_on_cooldown` is the single
source of truth and `_entry_blocked` calls it on every entry path.

The `strategy` argument accepted by `is_on_cooldown` / `set_cooldown` is kept
for call-site compatibility but is intentionally IGNORED — the lock spans all
strategies by design.
"""
import logging
from datetime import datetime
from utils.clock import now_utc

logger = logging.getLogger(__name__)

# v94: bumped 6h → 48h and made global. Kept the name COOLDOWN_HOURS for any
# external readers; the authoritative value comes from config.
try:
    from config import SYMBOL_COOLDOWN_HOURS as COOLDOWN_HOURS
except Exception:
    COOLDOWN_HOURS = 48

# In-process cache: symbol -> allowed_reentry_after. A fast path in front of the
# DB; the DB row is always authoritative.
_cooldowns: dict[str, datetime] = {}


def _get_db_conn():
    """Best-effort DB conn fetch. Returns None on failure."""
    try:
        from db import get_connection
        return get_connection()
    except Exception as e:
        logger.debug(f"[Cooldown] DB conn unavailable: {e}")
        return None


def set_cooldown(symbol: str, strategy: str | None = None,
                 hours: float | None = None, reason: str | None = None) -> None:
    """Lock `symbol` from re-entry by ANY strategy for `hours` (default
    COOLDOWN_HOURS). `strategy` is accepted for compatibility but ignored — the
    lock is global."""
    h = COOLDOWN_HOURS if hours is None else hours
    if h <= 0:
        return
    from datetime import timedelta
    until = now_utc() + timedelta(hours=h)
    _cooldowns[symbol] = until
    conn = _get_db_conn()
    if conn is not None:
        try:
            from db import set_symbol_cooldown
            set_symbol_cooldown(conn, symbol, h, reason or (strategy or "stop_or_tp"))
        finally:
            try:
                conn.close()
            except Exception:
                pass
    logger.info(
        f"[Cooldown] {symbol} LOCKED (cross-strategy) until {until.isoformat()} "
        f"({h}h, reason={reason or strategy or 'stop_or_tp'})"
    )


def is_on_cooldown(symbol: str, strategy: str | None = None) -> bool:
    """True if `symbol` is still inside its cross-strategy cooldown window.
    `strategy` is accepted for compatibility but ignored — the lock is global."""
    if COOLDOWN_HOURS <= 0:
        return False
    # Fast path: in-process cache
    until = _cooldowns.get(symbol)
    if until is not None:
        if now_utc() < until:
            return True
        _cooldowns.pop(symbol, None)
    # Authoritative path: DB
    conn = _get_db_conn()
    if conn is None:
        return False
    try:
        from db import is_symbol_locked
        locked = is_symbol_locked(conn, symbol)
        if not locked:
            _cooldowns.pop(symbol, None)
        return locked
    finally:
        try:
            conn.close()
        except Exception:
            pass


def clear_cooldown(symbol: str) -> None:
    """Clear a symbol's cooldown from both the in-process cache and the DB."""
    _cooldowns.pop(symbol, None)
    conn = _get_db_conn()
    if conn is not None:
        try:
            from db import clear_symbol_cooldown
            clear_symbol_cooldown(conn, symbol)
        finally:
            try:
                conn.close()
            except Exception:
                pass


def get_all_cooldowns() -> dict[str, str]:
    """Return all active cooldowns as {symbol: time_remaining_str} for logging."""
    conn = _get_db_conn()
    rows: dict[str, str] = {}
    if conn is not None:
        try:
            from db import get_active_symbol_cooldowns
            rows = get_active_symbol_cooldowns(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    now = now_utc()
    result = {}
    for sym, until_iso in rows.items():
        try:
            until = datetime.fromisoformat(until_iso)
        except Exception:
            continue
        if until <= now:
            continue
        total = int((until - now).total_seconds())
        result[sym] = f"{total // 3600}h{(total % 3600) // 60}m"
    return result
