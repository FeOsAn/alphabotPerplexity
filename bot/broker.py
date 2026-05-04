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


# ---------------------------------------------------------- auto-tagger
# Rules for inferring strategy from live position data.
# Runs every cycle so tags are always current, surviving restarts + DB wipes.

_SECTOR_ETFS = {"XLE", "XLK", "XLRE", "XLV", "XLF", "XLI", "XLB", "XLC", "XLY", "XLP", "XLU"}
_SPY_DIP_SYMS = {"SPY", "QQQ"}


def retag_all_positions(positions: list[dict]):
    """
    Infer and update strategy tags for every open position on every cycle.
    Priority order:
      1. Sector ETF → sector_rotation
      2. SPY/QQQ → spy_dip
      3. Already correctly tagged (non-unknown) → keep existing tag
      4. Unknown → try to infer from P&L profile:
           - Large loss + oversold profile → mean_reversion
           - Positive momentum → trend_following
           - Otherwise → keep unknown
    Logs any tag that changes so you can see reassignments in Railway logs.
    """
    logger = __import__("logging").getLogger("alphabot.broker")
    for pos in positions:
        sym = pos["symbol"]
        current_tag = _STRATEGY_TAGS.get(sym, "unknown")
        new_tag = current_tag

        if sym in _SECTOR_ETFS:
            new_tag = "sector_rotation"
        elif sym in _SPY_DIP_SYMS:
            new_tag = "spy_dip"
        elif current_tag == "unknown":
            # Best-effort inference for legacy orphan positions
            pnl = pos.get("unrealized_pnl_pct", 0)
            if pnl <= -1.0:
                # Losing position — likely mean reversion entry that hasn't reverted yet
                new_tag = "mean_reversion"
            else:
                # Flat or winning — likely trend following
                new_tag = "trend_following"

        if new_tag != current_tag:
            logger.info(f"[RETAG] {sym}: {current_tag} → {new_tag}")
            _STRATEGY_TAGS[sym] = new_tag
        elif new_tag != "unknown":
            # Ensure it's in the dict even if unchanged
            _STRATEGY_TAGS[sym] = new_tag


# ---------------------------------------------------------------- sector correlation
_SECTOR_MAP = {
    # Energy
    "XLE": "energy", "XOM": "energy", "CVX": "energy", "COP": "energy", "EOG": "energy", "SLB": "energy",
    # Technology
    "XLK": "tech", "AAPL": "tech", "MSFT": "tech", "NVDA": "tech", "AVGO": "tech", "AMD": "tech",
    "CRM": "tech", "NOW": "tech", "PANW": "tech", "ADBE": "tech", "ORCL": "tech",
    # Financials
    "XLF": "financials", "JPM": "financials", "BAC": "financials", "GS": "financials",
    "MS": "financials", "V": "financials", "MA": "financials",
    # Healthcare
    "XLV": "healthcare", "JNJ": "healthcare", "LLY": "healthcare", "ABBV": "healthcare",
    "MRK": "healthcare", "ISRG": "healthcare",
    # Industrials
    "XLI": "industrials", "CAT": "industrials", "HON": "industrials", "GE": "industrials",
    "RTX": "industrials", "LMT": "industrials",
    # Consumer
    "XLY": "consumer", "AMZN": "consumer", "TSLA": "consumer", "HD": "consumer", "MCD": "consumer",
    # Communication
    "XLC": "comms", "META": "comms", "GOOGL": "comms", "NFLX": "comms",
    # Real estate
    "XLRE": "realestate",
    # Materials
    "XLB": "materials",
    # Utilities
    "XLU": "utilities",
}

_SECTOR_LIMIT = 2  # max positions per sector before blocking a new entry


def is_correlated_position(symbol: str, existing_positions: list[dict]) -> bool:
    """
    Returns True if adding *symbol* would create over-concentrated sector exposure.

    Blocks entry when the candidate symbol is in the same sector as an existing
    position AND there are already _SECTOR_LIMIT (2) positions in that sector.
    Sector ETFs (XLE, XLK, etc.) count as 1 toward the sector limit like any
    other holding.

    Examples
    --------
    Holding XLE + XOM (2 energy) → a 3rd energy name is blocked → returns True.
    Holding XLE alone (1 energy) → XOM would be a 2nd → returns False.
    Symbol not in _SECTOR_MAP → unknown sector, never blocked → returns False.
    """
    _logger = __import__("logging").getLogger("alphabot.broker")

    new_sector = _SECTOR_MAP.get(symbol)
    if new_sector is None:
        # Sector unknown — allow through (no data to block on)
        return False

    # Count how many existing positions share the same sector
    sector_count = sum(
        1 for p in existing_positions
        if _SECTOR_MAP.get(p["symbol"]) == new_sector
    )

    if sector_count >= _SECTOR_LIMIT:
        _logger.info(
            f"[CORR] {symbol} ({new_sector}) blocked — already {sector_count} "
            f"{new_sector} positions open (limit {_SECTOR_LIMIT})"
        )
        return True

    return False


# ---------------------------------------------------------------- pyramid entry
# Pyramid entry state: {symbol: {"initial_price": float, "notional": float, "add1_done": bool, "add2_done": bool}}
_pyramid_state: dict = {}


def place_pyramid_order(symbol: str, full_notional: float, current_price: float, broker_instance) -> None:
    """
    Pyramid entry: buy 50% now, plan to add 25% at +2%, 25% at +4%.
    Call this instead of a single full-size order for new entries.
    The remaining adds are checked in check_pyramid_adds().
    """
    import logging
    logger = logging.getLogger(__name__)

    initial_notional = full_notional * 0.50
    qty = max(1, int(initial_notional / current_price))

    try:
        broker_instance.submit_order(
            symbol=symbol,
            qty=qty,
            side="buy",
            type="market",
            time_in_force="day"
        )
        _pyramid_state[symbol] = {
            "initial_price": current_price,
            "full_notional": full_notional,
            "add1_done": False,
            "add2_done": False,
        }
        logger.info(f"[Pyramid] {symbol} initial entry {qty} shares @ ~${current_price:.2f} (50% = ${initial_notional:,.0f})")
    except Exception as e:
        logger.error(f"[Pyramid] Failed initial entry {symbol}: {e}")


def check_pyramid_adds(broker_instance) -> None:
    """
    Check all tracked pyramid positions and add if +2% or +4% targets hit.
    Call this once per cycle from main.py.
    """
    import logging, yfinance as yf, gc
    logger = logging.getLogger(__name__)

    for symbol, state in list(_pyramid_state.items()):
        if state["add1_done"] and state["add2_done"]:
            del _pyramid_state[symbol]
            continue
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="1d", interval="1m")
            gc.collect()
            if hist.empty:
                continue
            current_price = hist["Close"].iloc[-1]
            pct_gain = (current_price - state["initial_price"]) / state["initial_price"]

            if not state["add1_done"] and pct_gain >= 0.02:
                qty = max(1, int(state["full_notional"] * 0.25 / current_price))
                broker_instance.submit_order(symbol=symbol, qty=qty, side="buy", type="market", time_in_force="day")
                _pyramid_state[symbol]["add1_done"] = True
                logger.info(f"[Pyramid] {symbol} add1 {qty} shares @ ${current_price:.2f} (+{pct_gain:.1%}, 25% add)")

            if not state["add2_done"] and pct_gain >= 0.04:
                qty = max(1, int(state["full_notional"] * 0.25 / current_price))
                broker_instance.submit_order(symbol=symbol, qty=qty, side="buy", type="market", time_in_force="day")
                _pyramid_state[symbol]["add2_done"] = True
                logger.info(f"[Pyramid] {symbol} add2 {qty} shares @ ${current_price:.2f} (+{pct_gain:.1%}, final 25% add)")
        except Exception as e:
            logger.warning(f"[Pyramid] Error checking adds for {symbol}: {e}")
