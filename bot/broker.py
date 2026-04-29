"""
Broker interface — wraps Alpaca API
"""
import logging
from typing import Optional
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta
import pandas as pd

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL

logger = logging.getLogger("alphabot.broker")


class AlpacaBroker:
    def __init__(self):
        paper = "paper" in ALPACA_BASE_URL
        self.trading = TradingClient(
            ALPACA_API_KEY,
            ALPACA_SECRET_KEY,
            paper=paper
        )
        self.data = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)
        logger.info(f"Connected to Alpaca ({'paper' if paper else 'live'} trading)")

    # ------------------------------------------------------------------ account
    def get_account(self) -> dict:
        acct = self.trading.get_account()
        return {
            "portfolio_value": float(acct.portfolio_value),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
            "equity": float(acct.equity),
            "pnl_today": float(acct.equity) - float(acct.last_equity),
            "pnl_today_pct": (float(acct.equity) - float(acct.last_equity)) / float(acct.last_equity) * 100 if float(acct.last_equity) else 0,
        }

    # ---------------------------------------------------------------- positions
    def get_positions(self) -> list[dict]:
        positions = self.trading.get_all_positions()
        result = []
        for p in positions:
            # Detect options by checking for P/C in the symbol (e.g. SPY250117C00500000)
            sym = p.symbol
            is_option = len(sym) > 10 and any(c in sym[4:] for c in ['C', 'P']) and sym[-8:].isdigit()
            result.append({
                "symbol": sym,
                "qty": float(p.qty),
                "avg_entry": float(p.avg_entry_price),
                "current_price": float(p.current_price),
                "market_value": float(p.market_value),
                "unrealized_pnl": float(p.unrealized_pl),
                "unrealized_pnl_pct": float(p.unrealized_plpc) * 100,
                "side": p.side.value,
                "strategy": _infer_strategy(sym),
                "asset_class": "option" if is_option else "equity",
            })
        return result

    def get_position(self, symbol: str) -> Optional[dict]:
        try:
            p = self.trading.get_open_position(symbol)
            return {
                "symbol": p.symbol,
                "qty": float(p.qty),
                "avg_entry": float(p.avg_entry_price),
                "current_price": float(p.current_price),
                "market_value": float(p.market_value),
                "unrealized_pnl": float(p.unrealized_pl),
                "unrealized_pnl_pct": float(p.unrealized_plpc) * 100,
            }
        except Exception:
            return None

    # ------------------------------------------------------------------ orders
    def market_buy(self, symbol: str, notional: float, strategy: str = "manual") -> dict:
        """Buy $notional worth of symbol."""
        req = MarketOrderRequest(
            symbol=symbol,
            notional=round(notional, 2),
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        )
        order = self.trading.submit_order(req)
        logger.info(f"[{strategy}] BUY ${notional:.0f} {symbol} — order {order.id}")
        return {"id": str(order.id), "symbol": symbol, "side": "buy", "notional": notional, "strategy": strategy}

    def market_sell(self, symbol: str, qty: float, strategy: str = "manual") -> dict:
        """Sell qty shares of symbol."""
        req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        order = self.trading.submit_order(req)
        logger.info(f"[{strategy}] SELL {qty} {symbol} — order {order.id}")
        return {"id": str(order.id), "symbol": symbol, "side": "sell", "qty": qty, "strategy": strategy}

    def close_position(self, symbol: str, strategy: str = "manual") -> Optional[dict]:
        try:
            order = self.trading.close_position(symbol)
            logger.info(f"[{strategy}] CLOSE position {symbol}")
            return {"id": str(order.id), "symbol": symbol, "side": "sell", "strategy": strategy}
        except Exception as e:
            logger.warning(f"Could not close {symbol}: {e}")
            return None

    def get_orders(self, status: str = "open") -> list[dict]:
        req = GetOrdersRequest(status=QueryOrderStatus.OPEN if status == "open" else QueryOrderStatus.ALL)
        orders = self.trading.get_orders(req)
        return [{
            "id": str(o.id),
            "symbol": o.symbol,
            "side": o.side.value,
            "qty": float(o.qty or 0),
            "notional": float(o.notional or 0),
            "status": o.status.value,
            "created_at": str(o.created_at),
        } for o in orders]

    # -------------------------------------------------------------------- data
    def get_bars(self, symbols: list[str], days: int = 300) -> dict[str, pd.DataFrame]:
        end = datetime.now()
        start = end - timedelta(days=days + 50)  # buffer for weekends/holidays
        req = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            feed="iex",  # paper trading accounts only have IEX feed access
        )
        bars = self.data.get_stock_bars(req).df
        result = {}
        if isinstance(bars.index, pd.MultiIndex):
            for sym in symbols:
                try:
                    sym_bars = bars.loc[sym].copy()
                    sym_bars.index = pd.to_datetime(sym_bars.index)
                    result[sym] = sym_bars.tail(days)
                except KeyError:
                    pass
        return result

    def is_market_open(self) -> bool:
        clock = self.trading.get_clock()
        return clock.is_open


# ------------------------------------------------------------------ helpers
_STRATEGY_TAGS: dict[str, str] = {}  # populated by strategies at runtime


def tag_symbol(symbol: str, strategy: str):
    _STRATEGY_TAGS[symbol] = strategy


def restore_tags_from_db(db_conn) -> int:
    """
    On bot restart, re-populate strategy tags from the trades DB.
    Finds the most recent 'buy' trade for each currently-open symbol
    and restores the tag so positions aren't labelled 'unknown'.
    Called once at startup from main.py.
    """
    try:
        rows = db_conn.execute("""
            SELECT symbol, strategy
            FROM trades
            WHERE side = 'buy'
            GROUP BY symbol
            HAVING MAX(created_at)
            ORDER BY created_at DESC
        """).fetchall()
        count = 0
        for row in rows:
            sym, strat = row["symbol"], row["strategy"]
            if sym not in _STRATEGY_TAGS and strat and strat != "unknown":
                _STRATEGY_TAGS[sym] = strat
                count += 1
        return count
    except Exception as e:
        import logging
        logging.getLogger("alphabot.broker").warning(f"Could not restore tags from DB: {e}")
        return 0


def _infer_strategy(symbol: str) -> str:
    return _STRATEGY_TAGS.get(symbol, "unknown")
