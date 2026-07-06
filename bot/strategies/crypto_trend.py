"""
Strategy: Crypto Trend (BTC/ETH 200DMA long-flat)
-------------------------------------------------
The book's one genuinely uncorrelated return stream. Hold BTC and ETH while each
trades above its own 200-day MA; go flat (to cash) when it closes below. No
shorting, no leverage, fixed sleeve size.

Backtest (backtests/crypto_sleeve.py, 2015-2026, weekend moves compounded onto
the equity calendar):
  - correlation to the equity return engine: 0.03 (bonds/commodities failed this;
    crypto is the diversifier that actually diversifies)
  - +10% sleeve: book CAGR 16.2% -> 19.7%, Sharpe 1.33 -> 1.61, 2022-flip DD
    BETTER (-14.3% -> -13.7%) because the 200DMA gate exited crypto in Nov 2021
  - improves ALL sub-periods (1.59 / 1.48 / 1.92 vs 1.23 / 1.12 / 1.72)
Forward-return caveat: historical crypto CAGR won't repeat; the 0.03 correlation
and the trend gate are the load-bearing properties, so the sleeve is sized small
(5% + 5%) and the ceiling self-policed.

Implementation notes (deliberately self-contained):
  - Orders go straight through broker.trading (Alpaca crypto: symbol "BTC/USD",
    notional orders, GTC). The equity bracket/positions_state plumbing is NOT
    used — crypto doesn't support the OCO stop flow; the strategy manages its
    own exit (trend break) every cycle, 24/7-safe.
  - Honors the daily-loss circuit breaker and the cash floor before entering.
  - Alpaca reports the position symbol as "BTCUSD"/"ETHUSD" (no slash).
"""
import logging
import yfinance as yf
from broker import AlpacaBroker, tag_symbol
from config import MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.crypto_trend")
STRATEGY_NAME = "crypto_trend"

SLEEVE = [  # (order symbol, position symbol, yahoo symbol, allocation)
    ("BTC/USD", "BTCUSD", "BTC-USD", 0.05),
    ("ETH/USD", "ETHUSD", "ETH-USD", 0.05),
]
MA_DAYS = 200
SLEEVE_CAP = 0.12          # self-policed ceiling for the whole crypto sleeve


def _above_ma200(yahoo_sym: str):
    """True/False for close > 200DMA; None on data failure (fail-safe: no action)."""
    try:
        hist = yf.Ticker(yahoo_sym).history(period="320d", interval="1d")
        close = hist["Close"].dropna()
        if len(close) < MA_DAYS:
            return None
        return bool(float(close.iloc[-1]) > float(close.tail(MA_DAYS).mean()))
    except Exception as e:
        logger.debug(f"[CryptoTrend] {yahoo_sym} data error: {e}")
        return None


def run(broker: AlpacaBroker, db_conn):
    """Trend check once per cycle: exit on break below 200DMA, enter on above."""
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[CryptoTrend] positions fetch failed: {e}")
        return
    pos_map = {p["symbol"]: p for p in positions}

    try:
        account = broker.get_account()
        equity = float(account["portfolio_value"])
        cash = float(account["cash"])
    except Exception as e:
        logger.warning(f"[CryptoTrend] account fetch failed: {e}")
        return

    sleeve_mv = sum(float(pos_map[ps]["market_value"]) for _, ps, _, _ in SLEEVE
                    if ps in pos_map)

    for order_sym, pos_sym, yahoo_sym, alloc in SLEEVE:
        above = _above_ma200(yahoo_sym)
        if above is None:
            continue  # fail-safe: no data, no action
        held = pos_sym in pos_map

        # ── Exit: trend broke ────────────────────────────────────────────────
        if held and not above:
            p = pos_map[pos_sym]
            logger.info(f"[CryptoTrend] EXIT {pos_sym} — closed below {MA_DAYS}DMA")
            try:
                broker.trading.close_position(pos_sym)
                log_trade(db_conn, STRATEGY_NAME, pos_sym, "sell_trend",
                          p.get("qty", 0), p.get("current_price", 0),
                          p.get("unrealized_pnl", 0))
            except Exception as e:
                logger.error(f"[CryptoTrend] close failed {pos_sym}: {e}")
            continue

        # ── Entry: in uptrend, not held ──────────────────────────────────────
        if above and not held:
            # Honor the global daily-loss circuit breaker (we bypass market_buy,
            # so check it explicitly).
            try:
                if broker._daily_loss_tripped():
                    logger.info(f"[CryptoTrend] circuit breaker active — no entry {order_sym}")
                    continue
            except Exception:
                pass
            notional = round(equity * alloc, 2)
            if sleeve_mv + notional > equity * SLEEVE_CAP:
                logger.info(f"[CryptoTrend] sleeve cap {SLEEVE_CAP:.0%} reached — skip {order_sym}")
                continue
            if cash - notional < equity * MIN_CASH_RESERVE_PCT:
                logger.info(f"[CryptoTrend] cash floor — skip {order_sym}")
                continue
            try:
                from alpaca.trading.requests import MarketOrderRequest
                from alpaca.trading.enums import OrderSide, TimeInForce
                req = MarketOrderRequest(symbol=order_sym, notional=notional,
                                         side=OrderSide.BUY,
                                         time_in_force=TimeInForce.GTC)
                order = broker.trading.submit_order(req)
                tag_symbol(pos_sym, STRATEGY_NAME)
                log_signal(db_conn, STRATEGY_NAME, pos_sym, "buy", 1.0,
                           {"notional": notional, "gate": f">{MA_DAYS}DMA"})
                log_trade(db_conn, STRATEGY_NAME, pos_sym, "buy", 0, 0, 0,
                          metadata={"notional": notional})
                logger.info(f"[CryptoTrend] ENTER {order_sym} ${notional:,.0f} "
                            f"(above {MA_DAYS}DMA) — order {order.id}")
                cash -= notional
                sleeve_mv += notional
            except Exception as e:
                logger.error(f"[CryptoTrend] entry failed {order_sym}: {e}")
