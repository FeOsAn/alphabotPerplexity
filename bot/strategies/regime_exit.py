"""
v85 — regime-change position exit.

When the market regime flips (bull/chop/bear), positions opened under a strategy
that is no longer compatible with the new regime are closed at market. The
opening strategy is read from the durable positions_state DB row (written at
entry by record_entry); the live position list comes from the Alpaca API, which
is the source of truth for what is actually open.
"""
import logging
import time
from datetime import datetime, timezone

from utils.regime_weights import STRATEGY_REGIME_COMPAT
from db import get_connection, get_position_state, delete_position_state

logger = logging.getLogger("alphabot.regime_exit")

_CANCEL_SETTLE_SECONDS = 3


def _today_utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _entry_is_today(state: dict) -> bool:
    """True if the position was opened today (UTC) — grace period guard.

    positions_state.entry_time is an ISO timestamp written by write_position_state.
    """
    entry_time = (state or {}).get("entry_time")
    if not entry_time:
        return False
    try:
        return str(entry_time)[:10] == _today_utc_date()
    except Exception:
        return False


def _cancel_open_orders_for_symbol(broker, symbol: str) -> int:
    """Cancel ALL open orders for one symbol. Returns count cancelled."""
    cancelled = 0
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        open_orders = broker.trading.get_orders(
            GetOrdersRequest(status=QueryOrderStatus.OPEN, symbols=[symbol])
        )
        for o in (open_orders or []):
            try:
                broker.trading.cancel_order_by_id(str(o.id))
                cancelled += 1
            except Exception as e:
                logger.debug(f"[REGIME EXIT] {symbol}: could not cancel {o.id}: {e}")
    except Exception as e:
        logger.warning(f"[REGIME EXIT] {symbol}: order cancel sweep failed: {e}")
    return cancelled


def _close_short(broker, symbol: str, qty: float) -> bool:
    """Buy-to-cover a short at market, bypassing buy-side entry gates."""
    try:
        from alpaca.trading.requests import MarketOrderRequest
        from alpaca.trading.enums import OrderSide, TimeInForce
        req = MarketOrderRequest(
            symbol=symbol,
            qty=abs(int(qty)),
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
        )
        order = broker.trading.submit_order(req)
        broker._open_orders_cache_ts = 0.0
        logger.info(f"[REGIME EXIT] BUY-TO-COVER {abs(int(qty))} {symbol} — order {order.id}")
        return True
    except Exception as e:
        logger.warning(f"[REGIME EXIT] {symbol}: buy-to-cover failed: {e}")
        return False


def check_regime_exits(broker, current_regime: str) -> list:
    """
    For each open position, check if its opening_strategy is compatible
    with current_regime. If not, close at market.
    Returns list of exited symbols.
    """
    exited: list = []
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[REGIME EXIT] could not fetch live positions: {e}")
        return exited

    conn = get_connection()
    try:
        for pos in positions:
            symbol = pos.get("symbol")
            if not symbol:
                continue
            # Options aren't covered by the strategy/regime compat map.
            if pos.get("asset_class") == "option":
                continue

            state = get_position_state(conn, symbol)
            if not state:
                # No DB row → can't determine opening strategy. Don't blindly close.
                logger.debug(f"[REGIME EXIT] {symbol}: no positions_state row — skipping")
                continue

            strat = state.get("opening_strategy") or state.get("strategy")
            if not strat:
                logger.debug(f"[REGIME EXIT] {symbol}: no opening_strategy — skipping")
                continue

            compat = STRATEGY_REGIME_COMPAT.get(strat)
            if compat is None:
                # Unknown strategy → can't determine compatibility. Don't close.
                logger.debug(
                    f"[REGIME EXIT] {symbol}: strategy '{strat}' not in compat map — skipping"
                )
                continue

            if current_regime in compat:
                continue  # still compatible — leave it alone

            # Grace period — give entries opened today one full trading day.
            if _entry_is_today(state):
                logger.info(
                    f"[REGIME EXIT] {symbol} ({strat}) incompatible with {current_regime} "
                    f"but opened today — grace period, skipping"
                )
                continue

            side = (pos.get("side") or "").lower()
            qty = pos.get("qty") or 0.0
            if not qty:
                continue

            logger.info(
                f"[REGIME EXIT] Closing {symbol} ({side}) — {strat} incompatible with {current_regime}"
            )

            # 1) Cancel ALL open orders for this symbol first (frees locked shares).
            n_cancelled = _cancel_open_orders_for_symbol(broker, symbol)
            if n_cancelled:
                logger.info(f"[REGIME EXIT] {symbol}: cancelled {n_cancelled} open order(s)")

            # 2) Let the cancels settle before submitting the closing order.
            time.sleep(_CANCEL_SETTLE_SECONDS)

            # 3) Exit at market — sell for long, buy-to-cover for short.
            ok = False
            if side == "short":
                ok = _close_short(broker, symbol, qty)
            else:
                try:
                    result = broker.market_sell(symbol, abs(float(qty)), strategy="regime_exit")
                    ok = result is not None
                except Exception as e:
                    logger.warning(f"[REGIME EXIT] {symbol}: market_sell failed: {e}")
                    ok = False

            if ok:
                delete_position_state(conn, symbol)
                try:
                    from strategies.trade_management import clear_symbol as _tm_clear
                    _tm_clear(symbol)
                except Exception:
                    pass
                exited.append(symbol)
            else:
                logger.warning(f"[REGIME EXIT] {symbol}: close order not submitted — DB row kept")
    finally:
        conn.close()

    return exited
