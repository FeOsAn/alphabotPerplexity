"""
In-bot stop watchdog (v100.4).
------------------------------
Every open equity position must have a live protective stop covering its FULL
quantity. Historically this was "guaranteed" by (a) bracket placement at entry,
(b) the trailing-ratchet replace cycle, and (c) an external Perplexity-side
morning-watchdog cron. A live audit (2026-07-08) found all three failing in
production: PANW's ratchet never moved (stop below entry, 6/11 shares covered
at +19.7% peak gain), JNJ and QQQ had NO stop at all (TP-only), GOOGL/LMT were
partially covered, and MU carried an orphan buy-stop above market. The external
cron is dead now that the bot is off Perplexity.

This module makes the bot self-healing. Once per cycle (throttled):
  1. Cancel orphan stops (wrong side for the position: e.g. a BUY stop on a
     long with no short).
  2. For every position whose stop coverage < full qty:
       - compute a ratchet-aware floor: stop = entry * (1 + max(-base_stop,
         tier_floor(current_gain))). Using CURRENT gain (not peak) makes the
         floor conservative — it never over-tightens, and never loosens an
         existing stop (we keep the max of existing stop px and the floor).
       - cancel partial/stale stop + TP-only orders for the symbol
         (cancel -> sleep(3) -> replace, v88 pattern), then place ONE order
         covering the full qty: OCO (same TP + new stop) if a TP existed,
         else a plain GTC stop.
Crypto positions (self-managed trend exits, no equity stop support) and
options are skipped.
"""
import time
import logging

logger = logging.getLogger("alphabot.stop_watchdog")

_INTERVAL = 1800.0          # run at most every 30 min
_last_run: float = 0.0

_SKIP_SYMBOLS = {"BTCUSD", "ETHUSD", "SOLUSD"}   # crypto sleeve self-manages

# mirror of trade_management's ratchet tiers: (min_gain, lock_floor)
_TIERS = [(0.25, 0.18), (0.18, 0.12), (0.12, 0.07), (0.08, 0.04), (0.05, 0.02), (0.03, 0.0)]


def _tier_floor(gain: float) -> float | None:
    for mg, lf in _TIERS:
        if gain >= mg:
            return lf
    return None


def _target_stop_price(entry: float, current: float, is_short: bool, base_stop: float) -> float:
    if is_short:
        return round(entry * (1 + base_stop), 2)   # buy-stop above entry
    gain = current / entry - 1 if entry > 0 else 0.0
    floor = _tier_floor(gain)
    pct = floor if floor is not None else -base_stop
    return round(entry * (1 + pct), 2)


def ensure_stops(broker, db_conn=None, force: bool = False) -> int:
    """Audit + repair stop coverage. Returns number of positions repaired."""
    global _last_run
    now = time.time()
    if not force and now - _last_run < _INTERVAL:
        return 0
    _last_run = now

    try:
        from alpaca.trading.requests import (
            GetOrdersRequest, StopOrderRequest, LimitOrderRequest,
            TakeProfitRequest, StopLossRequest, OrderClass,
        )
        from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
    except Exception as e:
        logger.debug(f"[StopWatchdog] alpaca imports failed: {e}")
        return 0

    try:
        positions = broker.get_positions()
        open_orders = broker.trading.get_orders(
            GetOrdersRequest(status=QueryOrderStatus.OPEN, limit=500)) or []
    except Exception as e:
        logger.warning(f"[StopWatchdog] fetch failed: {e}")
        return 0

    from strategies.trade_management import _base_stop_for

    by_sym: dict[str, list] = {}
    for o in open_orders:
        by_sym.setdefault(o.symbol, []).append(o)

    repaired = 0
    for p in positions:
        sym = p["symbol"]
        if sym in _SKIP_SYMBOLS or p.get("asset_class", "equity") != "equity":
            continue
        qty = float(p.get("qty", 0))
        abs_qty = int(abs(qty))
        if abs_qty < 1:
            continue
        is_short = qty < 0
        entry = float(p.get("avg_entry", 0) or 0)
        current = float(p.get("current_price", 0) or 0)
        if entry <= 0 or current <= 0:
            continue
        close_side = "buy" if is_short else "sell"

        stops, tp_limits, orphans = [], [], []
        for o in by_sym.get(sym, []):
            side = str(getattr(o, "side", "")).replace("OrderSide.", "").lower()
            has_stop = getattr(o, "stop_price", None) is not None
            otype = str(getattr(o, "order_type", getattr(o, "type", ""))).lower()
            if has_stop:
                (stops if side.endswith(close_side) else orphans).append(o)
            elif "limit" in otype and side.endswith(close_side):
                tp_limits.append(o)

        # 1) cancel orphan wrong-side stops (e.g. MU's buy-stop on a long)
        for o in orphans:
            try:
                broker.trading.cancel_order_by_id(str(o.id))
                logger.info(f"[StopWatchdog] {sym}: cancelled orphan {o.side} stop @ {o.stop_price}")
            except Exception:
                pass

        covered = sum(int(float(getattr(o, "qty", 0) or 0)) for o in stops)
        if covered >= abs_qty:
            continue  # fully protected

        base = _base_stop_for(p.get("strategy", "") or "")
        floor_px = _target_stop_price(entry, current, is_short, base)
        # never loosen: keep the tightest (most protective) of existing stop px
        exist_px = [float(getattr(o, "stop_price", 0) or 0) for o in stops]
        if exist_px:
            floor_px = round(min(min(exist_px), floor_px), 2) if is_short \
                else round(max(max(exist_px), floor_px), 2)
        # sanity: a long's sell-stop must be below market, short's buy-stop above
        if (not is_short and floor_px >= current) or (is_short and floor_px <= current):
            floor_px = round(current * (0.99 if not is_short else 1.01), 2)

        tp_px = None
        if tp_limits:
            try:
                tp_px = float(getattr(tp_limits[0], "limit_price", 0) or 0) or None
            except Exception:
                tp_px = None

        # 2) cancel partial stops + TP-only orders, then replace with ONE full cover
        for o in stops + tp_limits:
            try:
                broker.trading.cancel_order_by_id(str(o.id))
            except Exception:
                pass
        time.sleep(3)  # v88 cancel -> sleep -> replace

        order_side = OrderSide.BUY if is_short else OrderSide.SELL
        try:
            if tp_px and not is_short:
                req = LimitOrderRequest(
                    symbol=sym, qty=abs_qty, side=order_side,
                    limit_price=round(tp_px, 2), time_in_force=TimeInForce.GTC,
                    order_class=OrderClass.OCO,
                    take_profit=TakeProfitRequest(limit_price=round(tp_px, 2)),
                    stop_loss=StopLossRequest(stop_price=floor_px),
                )
            else:
                req = StopOrderRequest(
                    symbol=sym, qty=abs_qty, side=order_side,
                    stop_price=floor_px, time_in_force=TimeInForce.GTC,
                )
            broker.trading.submit_order(req)
            repaired += 1
            logger.warning(
                f"[StopWatchdog] REPAIRED {sym}: was {covered}/{abs_qty} covered -> "
                f"full-qty stop @ {floor_px} ({(floor_px/current-1)*100:+.1f}% from px)"
                + (f" + TP {tp_px}" if tp_px and not is_short else "")
            )
        except Exception as e:
            logger.error(f"[StopWatchdog] {sym} repair failed: {e}")

    if repaired:
        logger.warning(f"[StopWatchdog] repaired {repaired} position(s) this pass")
    return repaired
