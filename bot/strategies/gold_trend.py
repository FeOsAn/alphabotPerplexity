"""
Strategy: Gold Trend (GLD 200DMA long-flat)
-------------------------------------------
Diversifier sleeve, not a return engine. GLD standalone is only Sharpe 0.61 —
but its correlation to the equity book is 0.02, and the sleeve-weight grid
(backtests/x5_sleeves.py) shows a 10% gold sleeve LIFTS the whole book:
45/30/15/10 engine/donchian/crypto/gold = Sharpe 1.81 (vs 1.70 without gold),
2022-flip DD -11.1% (better), CAGR 21.8%. Sized for correlation, not for CAGR.

Rules: long GLD (10% of equity) while GLD > its 200DMA; flat below. The trend
exit is the manager; the exchange stop from the shared bracket flow is a
catastrophe backstop only.

Overlap guard: dual_momentum and the pre-transition hedge can also hold GLD.
Alpaca positions are per-symbol, so this sleeve only enters when NO GLD position
exists, and only exits GLD that IT opened (tracked via db state key).
"""
import logging
import yfinance as yf
from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal, get_state, set_state

logger = logging.getLogger("alphabot.gold_trend")
STRATEGY_NAME = "gold_trend"

ALLOCATION_PCT = 0.10
MA_DAYS = 200
_OWNED_KEY = "gold_trend_owns_gld"   # "1" when this sleeve opened the GLD position


def _gld_above_ma200():
    try:
        hist = yf.Ticker("GLD").history(period="320d", interval="1d")
        close = hist["Close"].dropna()
        if len(close) < MA_DAYS:
            return None
        return bool(float(close.iloc[-1]) > float(close.tail(MA_DAYS).mean()))
    except Exception as e:
        logger.debug(f"[GoldTrend] data error: {e}")
        return None


def run(broker: AlpacaBroker, db_conn):
    above = _gld_above_ma200()
    if above is None:
        return  # fail-safe: no data, no action

    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[GoldTrend] positions fetch failed: {e}")
        return
    gld_pos = next((p for p in positions if p["symbol"] == "GLD"), None)
    owns = False
    try:
        owns = get_state(db_conn, _OWNED_KEY) == "1"
    except Exception:
        pass

    # ── Exit: trend broke AND we own the position ────────────────────────────
    if gld_pos and owns and not above:
        logger.info(f"[GoldTrend] EXIT GLD — closed below {MA_DAYS}DMA")
        try:
            broker.close_position("GLD", STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, "GLD", "sell_trend",
                      gld_pos.get("qty", 0), gld_pos.get("current_price", 0),
                      gld_pos.get("unrealized_pnl", 0))
            set_state(db_conn, _OWNED_KEY, "0")
        except Exception as e:
            logger.error(f"[GoldTrend] close failed: {e}")
        return

    # Position closed elsewhere (stop / another sleeve)? clear ownership.
    if owns and not gld_pos:
        try:
            set_state(db_conn, _OWNED_KEY, "0")
        except Exception:
            pass

    # ── Entry: uptrend, and NO existing GLD position (overlap guard) ─────────
    if above and not gld_pos:
        try:
            if broker._daily_loss_tripped():
                logger.info("[GoldTrend] circuit breaker active — no entry")
                return
        except Exception:
            pass
        try:
            account = broker.get_account()
            equity = float(account["portfolio_value"])
            cash = float(account["cash"])
        except Exception as e:
            logger.warning(f"[GoldTrend] account fetch failed: {e}")
            return
        notional = round(equity * ALLOCATION_PCT, 2)
        if cash - notional < equity * MIN_CASH_RESERVE_PCT:
            logger.info("[GoldTrend] cash floor — skip entry")
            return
        try:
            order = broker.market_buy("GLD", notional, STRATEGY_NAME)
            if order:
                tag_symbol("GLD", STRATEGY_NAME)
                set_state(db_conn, _OWNED_KEY, "1")
                log_signal(db_conn, STRATEGY_NAME, "GLD", "buy", 1.0,
                           {"notional": notional, "gate": f">{MA_DAYS}DMA"})
                logger.info(f"[GoldTrend] ENTER GLD ${notional:,.0f} (above {MA_DAYS}DMA)")
        except Exception as e:
            logger.error(f"[GoldTrend] entry failed: {e}")
