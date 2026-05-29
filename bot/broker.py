"""
Broker interface — wraps Alpaca API
"""
import logging
import re
from typing import Optional
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from utils.clock import now_utc as _now_utc
from datetime import datetime, timedelta
import pandas as pd

from config import (
    ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_BASE_URL,
    MAX_TOTAL_POSITIONS, MAX_GROSS_EXPOSURE_PCT, STRATEGY_CAPITAL_LIMITS,
)
import time as _time

# OCC option symbol: ROOT (1-5 letters) + YYMMDD + C|P + 8-digit strike price.
# e.g. SPY250117C00500000
_OPTION_SYMBOL_RE = re.compile(r"^[A-Z]{1,5}\d{6}[CP]\d{8}$")

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
        # 30-second cache of open orders to avoid hammering the API on every
        # market_buy/market_sell dedup check.
        self._open_orders_cache: list[dict] = []
        self._open_orders_cache_ts: float = 0.0
        logger.info(f"Connected to Alpaca ({'paper' if paper else 'live'} trading)")

    # ------------------------------------------------------------------ account
    def get_account(self) -> dict:
        acct = self.trading.get_account()
        return {
            "portfolio_value": float(acct.portfolio_value),
            "cash": float(acct.cash),
            "buying_power": float(acct.buying_power),
            "equity": float(acct.equity),
            "maintenance_margin": float(getattr(acct, "maintenance_margin", 0) or 0),
            "initial_margin": float(getattr(acct, "initial_margin", 0) or 0),
            "pnl_today": float(acct.equity) - float(acct.last_equity),
            "pnl_today_pct": (float(acct.equity) - float(acct.last_equity)) / float(acct.last_equity) * 100 if float(acct.last_equity) else 0,
        }

    # ---------------------------------------------------------------- positions
    def get_positions(self) -> list[dict]:
        positions = self.trading.get_all_positions()
        result = []
        for p in positions:
            # Detect options via strict OCC format (e.g. SPY250117C00500000)
            sym = p.symbol
            is_option = bool(_OPTION_SYMBOL_RE.match(sym))
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

    # ---------------------------------------------------------- safety gates
    def _open_orders_cached(self) -> list[dict]:
        """Return open orders with a 30s cache so order checks don't spam the API."""
        now = _time.time()
        if now - self._open_orders_cache_ts < 30 and self._open_orders_cache_ts > 0:
            return self._open_orders_cache
        try:
            self._open_orders_cache = self.get_orders(status="open")
            self._open_orders_cache_ts = now
        except Exception as e:
            logger.warning(f"[Broker] get_orders failed (dedup check): {e}")
            self._open_orders_cache = []
            self._open_orders_cache_ts = now
        return self._open_orders_cache

    def _has_pending_order(self, symbol: str, side: str) -> bool:
        """True if an open order already exists for symbol+side (case-insensitive)."""
        side_l = side.lower()
        for o in self._open_orders_cached():
            if o.get("symbol") == symbol and (o.get("side") or "").lower() == side_l:
                return True
        return False

    def _gross_exposure_pct(self, positions: Optional[list[dict]] = None,
                            equity: Optional[float] = None) -> float:
        """Return (|long MV| + |short MV|) / equity. 0.0 on any error."""
        try:
            if positions is None:
                positions = self.get_positions()
            if equity is None:
                equity = float(self.get_account().get("equity") or 0.0)
            if equity <= 0:
                return 0.0
            gross = sum(abs(float(p.get("market_value", 0.0))) for p in positions)
            return gross / equity
        except Exception as e:
            logger.warning(f"[Broker] gross_exposure calc failed: {e}")
            return 0.0

    def _strategy_capital_pct(self, strategy: str,
                              positions: Optional[list[dict]] = None,
                              equity: Optional[float] = None) -> float:
        """Return absolute MV held by this strategy as a fraction of equity."""
        try:
            if positions is None:
                positions = self.get_positions()
            if equity is None:
                equity = float(self.get_account().get("equity") or 0.0)
            if equity <= 0:
                return 0.0
            strat_mv = sum(
                abs(float(p.get("market_value", 0.0))) for p in positions
                if p.get("strategy") == strategy
            )
            return strat_mv / equity
        except Exception as e:
            logger.warning(f"[Broker] strategy_capital calc failed: {e}")
            return 0.0

    def _entry_blocked(self, symbol: str, side: str, strategy: str,
                       notional: float) -> bool:
        """
        Centralised pre-order gate. Returns True if the order should be blocked.
        Enforces:
          - symbol blacklist (v75 — FIX 3, BUY only)
          - MAX_TOTAL_POSITIONS (only for NEW symbol buys)
          - per-strategy capital ceiling
          - portfolio gross exposure cap
          - open-order deduplication (same symbol + side already pending)
        """
        # --- Symbol blacklist (BUY side only) -----------------------------
        if side.lower() == "buy":
            try:
                from utils.symbol_performance import get_blacklisted_symbols
                from db import get_connection as _bl_get_conn
                bl_conn = _bl_get_conn()
                try:
                    if symbol in get_blacklisted_symbols(bl_conn):
                        logger.info(
                            f"[Broker] {symbol} is blacklisted, skipping buy"
                        )
                        return True
                finally:
                    try:
                        bl_conn.close()
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(f"[Broker] blacklist check failed for {symbol}: {e}")

        # --- Order dedup --------------------------------------------------
        if self._has_pending_order(symbol, side):
            logger.info(
                f"[Broker] Duplicate order skipped: {symbol} {side} already pending"
            )
            return True

        try:
            positions = self.get_positions()
        except Exception as e:
            logger.warning(f"[Broker] get_positions failed in gate: {e}")
            positions = []
        try:
            equity = float(self.get_account().get("equity") or 0.0)
        except Exception:
            equity = 0.0

        # --- MAX_TOTAL_POSITIONS (only enforce for new-symbol buys) -------
        if side.lower() == "buy":
            held_syms = {p["symbol"] for p in positions}
            if symbol not in held_syms and len(positions) >= MAX_TOTAL_POSITIONS:
                logger.info(
                    f"[Broker] MAX_TOTAL_POSITIONS={MAX_TOTAL_POSITIONS} reached, "
                    f"skipping buy for {symbol}"
                )
                return True

        # --- Per-strategy capital ceiling --------------------------------
        if equity > 0 and notional and strategy and strategy != "manual":
            limit = STRATEGY_CAPITAL_LIMITS.get(
                strategy, STRATEGY_CAPITAL_LIMITS["default"]
            )
            current = self._strategy_capital_pct(strategy, positions, equity)
            projected = current + (float(notional) / equity)
            if current >= limit:
                logger.info(
                    f"[{strategy}] Capital ceiling hit ({current:.1%} ≥ {limit:.0%}), "
                    f"skipping entry for {symbol}"
                )
                return True
            if projected > limit:
                logger.info(
                    f"[{strategy}] Capital ceiling would be breached "
                    f"(projected {projected:.1%} > {limit:.0%}), skipping {symbol}"
                )
                return True

        # --- Gross exposure cap ------------------------------------------
        if equity > 0 and notional:
            gross = self._gross_exposure_pct(positions, equity)
            projected_gross = gross + (float(notional) / equity)
            if projected_gross > MAX_GROSS_EXPOSURE_PCT:
                logger.warning(
                    f"[Broker] Gross exposure cap would be exceeded "
                    f"({projected_gross:.1%} > {MAX_GROSS_EXPOSURE_PCT:.0%}), "
                    f"skipping {side} {symbol}"
                )
                return True

        return False

    # ------------------------------------------------------------------ orders
    def market_buy(self, symbol: str, notional: float, strategy: str = "manual",
                   tp_target_override: Optional[float] = None,
                   stop_override: Optional[float] = None) -> Optional[dict]:
        """Buy $notional worth of symbol. Returns None if blocked by safety gates.

        v74: After a successful submit, calls `record_entry` to persist a
        positions_state row so trade_management can drive dollar-based stops
        and TPs off durable state.
        """
        if self._entry_blocked(symbol, "buy", strategy, notional):
            return None
        req = MarketOrderRequest(
            symbol=symbol,
            notional=round(notional, 2),
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        )
        order = self.trading.submit_order(req)
        # Invalidate dedup cache so subsequent calls see this new open order
        self._open_orders_cache_ts = 0.0
        logger.info(f"[{strategy}] BUY ${notional:.0f} {symbol} — order {order.id}")

        # v74 — record positions_state row for this entry.
        try:
            est_price = _estimate_fill_price(order, symbol)
            if est_price > 0:
                qty = float(notional) / est_price
                _record_entry_safe(
                    symbol=symbol, side="long", qty=qty,
                    entry_price=est_price, strategy=strategy,
                    tp_target_override=tp_target_override,
                    stop_override=stop_override,
                )
        except Exception as e:
            logger.debug(f"[Broker] record_entry skipped for {symbol}: {e}")

        return {"id": str(order.id), "symbol": symbol, "side": "buy", "notional": notional, "strategy": strategy}

    def market_sell(self, symbol: str, qty: float, strategy: str = "manual") -> Optional[dict]:
        """Sell qty shares of symbol. Dedup only — sells don't add exposure."""
        if self._has_pending_order(symbol, "sell"):
            logger.info(f"[Broker] Duplicate order skipped: {symbol} sell already pending")
            return None
        req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        order = self.trading.submit_order(req)
        self._open_orders_cache_ts = 0.0
        logger.info(f"[{strategy}] SELL {qty} {symbol} — order {order.id}")
        return {"id": str(order.id), "symbol": symbol, "side": "sell", "qty": qty, "strategy": strategy}

    def submit_order(self, symbol: str, qty, side: str, type: str = "market",
                     time_in_force: str = "day", strategy_tag: Optional[str] = None,
                     tp_target_override: Optional[float] = None,
                     stop_override: Optional[float] = None):
        """
        Unified order submission wrapper used by event_driven, earnings_nlp, ts_momentum, vwap_reclaim.
        Translates keyword-arg call to the correct alpaca-py request objects.

        v73: BUY-side orders now go through the same entry gates as market_buy
        (cooldown, entry-window, MAX_TOTAL_POSITIONS). Closes the back door
        that let EP/event_driven/ts_momentum/vwap_reclaim bypass every safety
        check by calling submit_order directly.
        """
        try:
            qty = abs(float(qty))
            if qty < 0.001:
                logger.warning(f"[Broker] submit_order: qty {qty} too small for {symbol}, skipping")
                return None
            order_side = OrderSide.BUY if side.lower() in ("buy", "long", "buy_to_cover") else OrderSide.SELL
            tif = TimeInForce.DAY if time_in_force.lower() == "day" else TimeInForce.GTC
            normalized_side = "buy" if order_side == OrderSide.BUY else "sell"

            # v73 — BUY-side entry gates (cooldown + entry window + position cap)
            if normalized_side == "buy":
                from utils.cooldown import is_on_cooldown
                from utils.market_hours import is_entry_allowed
                if is_on_cooldown(symbol):
                    logger.info(f"[Broker] submit_order blocked: {symbol} on cooldown")
                    return None
                if not is_entry_allowed():
                    logger.info(f"[Broker] submit_order blocked: outside entry window ({symbol})")
                    return None
                # v75 — symbol blacklist check
                try:
                    from utils.symbol_performance import get_blacklisted_symbols
                    from db import get_connection as _bl_get_conn
                    bl_conn = _bl_get_conn()
                    try:
                        if symbol in get_blacklisted_symbols(bl_conn):
                            logger.info(
                                f"[Broker] submit_order blocked: {symbol} is blacklisted"
                            )
                            return None
                    finally:
                        try:
                            bl_conn.close()
                        except Exception:
                            pass
                except Exception as e:
                    logger.debug(f"[Broker] blacklist check failed for {symbol}: {e}")
                try:
                    current_positions = self.get_positions()
                    if len(current_positions) >= MAX_TOTAL_POSITIONS:
                        logger.info(
                            f"[Broker] submit_order blocked: MAX_TOTAL_POSITIONS={MAX_TOTAL_POSITIONS} reached ({symbol})"
                        )
                        return None
                except Exception as e:
                    logger.debug(f"[Broker] submit_order position-count check failed: {e}")

            # Dedup guard (open same-symbol/side order)
            if self._has_pending_order(symbol, normalized_side):
                logger.info(
                    f"[Broker] Duplicate order skipped: {symbol} {normalized_side} already pending"
                )
                return None
            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=tif,
            )
            result = self.trading.submit_order(req)
            self._open_orders_cache_ts = 0.0
            logger.info(f"[Broker] submit_order: {side} {qty} {symbol} → {result.id if result else 'no result'}")

            # v74 — record positions_state row for this entry (BUY long, SELL short opens).
            try:
                est_price = _estimate_fill_price(result, symbol)
                if est_price > 0:
                    entry_side = "long" if order_side == OrderSide.BUY else "short"
                    _record_entry_safe(
                        symbol=symbol, side=entry_side, qty=float(qty),
                        entry_price=est_price, strategy=strategy_tag or "unknown",
                        tp_target_override=tp_target_override,
                        stop_override=stop_override,
                    )
            except Exception as e:
                logger.debug(f"[Broker] record_entry skipped for {symbol}: {e}")

            return result
        except Exception as e:
            logger.error(f"[Broker] submit_order failed for {symbol}: {e}")
            return None

    def close_position(self, symbol: str, strategy: str = "manual") -> Optional[dict]:
        try:
            order = self.trading.close_position(symbol)
            logger.info(f"[{strategy}] CLOSE position {symbol}")
            # v74 — drop the positions_state row alongside the close so trade_management
            # callers don't all need to remember to clean up.
            try:
                from db import get_connection, delete_position_state
                conn = get_connection()
                try:
                    delete_position_state(conn, symbol)
                finally:
                    conn.close()
            except Exception as e:
                logger.debug(f"[Broker] close_position state-cleanup skipped for {symbol}: {e}")
            try:
                from strategies.trade_management import clear_symbol as _tm_clear
                _tm_clear(symbol)
            except Exception:
                pass
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
        end = _now_utc()
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

    def get_live_cash(self) -> tuple[float, float]:
        """Fetch real-time cash and portfolio_value from Alpaca. Use after every order."""
        try:
            acc = self.get_account()
            return float(acc["cash"]), float(acc["portfolio_value"])
        except Exception as e:
            logger.warning(f"[broker] get_live_cash failed: {e}")
            return 0.0, 0.0


# ------------------------------------------------------------------ helpers
def _estimate_fill_price(order, symbol: str) -> float:
    """Best-effort fill-price estimate for newly-submitted market orders.

    Market orders typically return 'accepted' before fills land — read
    filled_avg_price if Alpaca already populated it; otherwise fall back to
    a yfinance fast_info lookup. Returns 0.0 if no price can be obtained.
    """
    try:
        price = float(getattr(order, "filled_avg_price", 0) or 0)
        if price > 0:
            return price
    except Exception:
        pass
    try:
        import yfinance as _yf
        fi = _yf.Ticker(symbol).fast_info
        for attr in ("last_price", "regular_market_price"):
            v = getattr(fi, attr, None)
            if v and float(v) > 0:
                return float(v)
    except Exception:
        pass
    return 0.0


def _record_entry_safe(**kwargs) -> None:
    """Lazy-import record_entry to avoid circular imports at module load."""
    try:
        from db import get_connection
        from utils.entry_state import record_entry
        conn = get_connection()
        try:
            record_entry(conn, None, **kwargs)
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[Broker] _record_entry_safe failed: {e}")


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
            SELECT symbol, strategy FROM trades t1
            WHERE side = 'buy'
              AND created_at = (SELECT MAX(created_at) FROM trades t2
                                WHERE t2.symbol = t1.symbol AND t2.side = 'buy')
            GROUP BY symbol
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
    Uses fast_info.last_price — much cheaper than fetching a full 1m bar series.
    """
    import logging, yfinance as yf
    logger = logging.getLogger(__name__)

    # v71: respect open/close blackout windows — no pyramid adds outside safe entry window
    try:
        from utils.market_hours import is_entry_allowed
        if not is_entry_allowed():
            logger.debug("[Pyramid] Outside safe entry window — skipping all adds this cycle")
            return
    except Exception:
        pass

    from utils.cooldown import is_on_cooldown

    for symbol, state in list(_pyramid_state.items()):
        if state["add1_done"] and state["add2_done"]:
            del _pyramid_state[symbol]
            continue
        # v71: don't add to a position currently on cooldown
        if is_on_cooldown(symbol):
            logger.debug(f"[Pyramid] {symbol} on cooldown — skipping add")
            continue
        try:
            current_price = None
            try:
                fi = yf.Ticker(symbol).fast_info
                current_price = getattr(fi, "last_price", None) or getattr(fi, "regular_market_price", None)
            except Exception:
                current_price = None
            if not current_price or current_price <= 0:
                continue
            current_price = float(current_price)
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


