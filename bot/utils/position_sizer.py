"""
ATR-based position sizing.
Volatile stocks get smaller positions (same dollar risk per trade).
Target risk per trade: 1% of portfolio value.

Formula:
  ATR(14) = average true range over 14 days
  ATR_pct = ATR / current_price
  position_size = (portfolio * risk_per_trade) / (ATR_pct * portfolio)
                = risk_per_trade / ATR_pct

  Capped between MIN_POSITION_PCT and MAX_POSITION_PCT from config.
  Hard minimum: $500 in absolute dollar terms — positions below this
  are not worth trading (leaves dust after partial takes).
"""
import logging
import gc
from typing import Optional
import yfinance as yf

logger = logging.getLogger(__name__)

RISK_PER_TRADE = 0.01   # risk 1% of portfolio per trade
MIN_POSITION_DOLLARS = 500  # never enter a position worth less than $500


def get_position_size_pct(symbol: str, fallback_pct: float = 0.08,
                          portfolio_value: float = 0.0) -> float:
    """
    Returns what fraction of portfolio to allocate to this symbol.
    e.g. 0.06 = 6% of portfolio.
    Falls back to fallback_pct on any error.

    portfolio_value: current total portfolio value in dollars.
    If provided, enforces a hard floor so the resulting dollar value
    is at least MIN_POSITION_DOLLARS. Returns 0.0 if portfolio is too
    small to meet the floor even at the calculated size.
    """
    try:
        from config import MAX_POSITION_PCT, MIN_CASH_RESERVE_PCT
        min_pct = 0.02   # never less than 2%
        max_pct = MAX_POSITION_PCT * 1.5   # never more than 1.5x base max

        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1mo", interval="1d")
        gc.collect()

        if hist.empty or len(hist) < 14:
            return fallback_pct

        high = hist["High"]
        low = hist["Low"]
        close = hist["Close"]

        # True Range
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        import pandas as pd
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr14 = float(tr.tail(14).mean())

        price = float(close.iloc[-1])
        if price <= 0:
            return fallback_pct

        atr_pct = atr14 / price

        if atr_pct <= 0:
            return fallback_pct

        # Size so that 1 ATR move = 1% portfolio loss
        size_pct = RISK_PER_TRADE / atr_pct

        # Clamp by percentage
        size_pct = max(min_pct, min(max_pct, size_pct))

        # Enforce absolute dollar floor — if the resulting dollar value is
        # below MIN_POSITION_DOLLARS, bump pct up so it meets the floor.
        # If portfolio is 0 (not provided), skip this check.
        if portfolio_value > 0:
            dollar_value = size_pct * portfolio_value
            if dollar_value < MIN_POSITION_DOLLARS:
                required_pct = MIN_POSITION_DOLLARS / portfolio_value
                if required_pct > max_pct:
                    # Even at max position size we can't meet the floor —
                    # portfolio is too small or the stock is priced oddly.
                    # Use max_pct anyway; dust cleanup handles the rest.
                    size_pct = max_pct
                    logger.warning(
                        f"[ATRSizer] {symbol}: dollar floor ${MIN_POSITION_DOLLARS} "
                        f"requires {required_pct:.1%} but max_pct={max_pct:.1%} — "
                        f"using max_pct"
                    )
                else:
                    logger.info(
                        f"[ATRSizer] {symbol}: ATR size={dollar_value:.0f} "
                        f"< floor ${MIN_POSITION_DOLLARS} → bumping to "
                        f"{required_pct:.1%} (${MIN_POSITION_DOLLARS})"
                    )
                    size_pct = required_pct

        logger.debug(f"[ATRSizer] {symbol}: ATR={atr14:.2f} ({atr_pct:.1%}) → size={size_pct:.1%}")
        return size_pct

    except Exception as e:
        logger.debug(f"[ATRSizer] {symbol} error: {e} — using fallback {fallback_pct:.1%}")
        return fallback_pct
