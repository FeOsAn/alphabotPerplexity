"""
Portfolio-level drawdown circuit breaker.
If portfolio drops >5% in a single day, halts all new entries for 48 hours.
State is stored in-memory (resets on redeploy — acceptable, Railway redeploys are intentional).
"""
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

_halt_until: datetime | None = None
_last_portfolio_value: float | None = None
_last_check_date: str | None = None

def check_and_update(current_portfolio_value: float) -> bool:
    """
    Call once per cycle with current portfolio value.
    Returns True if trading is HALTED (drawdown triggered).
    Returns False if trading is OK.
    """
    global _halt_until, _last_portfolio_value, _last_check_date

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    # Check if still in halt window
    if _halt_until and now < _halt_until:
        remaining = (_halt_until - now).seconds // 3600
        logger.warning(f"[CircuitBreaker] HALTED — {remaining}h remaining")
        return True

    # Reset halt if window expired
    if _halt_until and now >= _halt_until:
        logger.info("[CircuitBreaker] Halt window expired — resuming trading")
        _halt_until = None

    # First run — just record baseline
    if _last_portfolio_value is None or _last_check_date != today:
        _last_portfolio_value = current_portfolio_value
        _last_check_date = today
        return False

    # Check daily drawdown
    daily_change = (current_portfolio_value - _last_portfolio_value) / _last_portfolio_value
    if daily_change < -0.05:
        _halt_until = now + timedelta(hours=48)
        logger.error(
            f"[CircuitBreaker] TRIGGERED — portfolio down {daily_change:.1%} today "
            f"(${_last_portfolio_value:,.0f} → ${current_portfolio_value:,.0f}). "
            f"Halting new entries for 48h until {_halt_until.strftime('%Y-%m-%d %H:%M UTC')}"
        )
        return True

    return False
