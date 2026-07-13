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
# v100.6: EMPTY — trailing tiers disabled everywhere (see the evidence note at
# trade_management._ATR_RATCHET_TIERS). The watchdog now enforces BASE-stop
# coverage only, and still never loosens an existing (tighter) stop.
_TIERS: list = []


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


_cooldowned_orders: set[str] = set()   # order ids already cooldown-synced
_fail_streak: dict[str, int] = {}      # symbol -> consecutive failed repair passes
_alerted: set[str] = set()             # de-dup keys for escalation alerts


def _sync_exit_cooldowns(broker) -> None:
    """v100.5 — close the MU-churn gap: exchange-side stop/TP fills never ran
    set_cooldown (only strategy-initiated closes did), so another strategy could
    re-buy the same symbol minutes after a stop-out (MU, 2026-07-07: 5 one-share
    in/out fills in a day). Sweep today's CLOSED exit orders (stop/limit fills
    that reduce a position) and set the cross-strategy cooldown for each."""
    try:
        from alpaca.trading.requests import GetOrdersRequest
        from alpaca.trading.enums import QueryOrderStatus
        from utils.cooldown import set_cooldown
        from datetime import datetime, timezone
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        closed = broker.trading.get_orders(GetOrdersRequest(
            status=QueryOrderStatus.CLOSED, after=start, limit=200)) or []
        for o in closed:
            oid = str(o.id)
            if oid in _cooldowned_orders:
                continue
            status = str(getattr(o, "status", "")).lower()
            otype = str(getattr(o, "order_type", getattr(o, "type", ""))).lower()
            if "filled" not in status:
                continue
            # exit fills = stop/stop_limit always; limit only when it's a TP
            # (sell-limit on a long / buy-limit on a short) — market orders are
            # strategy-initiated and already set their own cooldown.
            if ("stop" in otype) or (otype == "limit"):
                _cooldowned_orders.add(oid)
                try:
                    set_cooldown(o.symbol)
                    logger.info(f"[StopWatchdog] cooldown set: {o.symbol} "
                                f"(exchange {otype} fill {oid[:8]})")
                except Exception:
                    pass
        if len(_cooldowned_orders) > 2000:   # daily ids only; keep bounded
            _cooldowned_orders.clear()
    except Exception as e:
        logger.debug(f"[StopWatchdog] cooldown sync failed: {e}")


def ensure_stops(broker, db_conn=None, force: bool = False) -> int:
    """Audit + repair stop coverage. Returns number of positions repaired."""
    global _last_run
    now = time.time()
    if not force and now - _last_run < _INTERVAL:
        return 0
    _last_run = now

    _sync_exit_cooldowns(broker)

    try:
        from alpaca.trading.requests import (
            GetOrdersRequest, StopOrderRequest, LimitOrderRequest,
            StopLossRequest, OrderClass,
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
        placed = False
        if tp_px and not is_short:
            try:
                # v100.6 — OCO takes NO take_profit leg in alpaca-py (that's the
                # BRACKET class); the parent limit_price IS the TP. The old dual
                # spec 422'd, which left TP-only positions (IWM/QQQ/TGT) naked
                # for a full pass after their TP had been cancelled.
                req = LimitOrderRequest(
                    symbol=sym, qty=abs_qty, side=order_side,
                    limit_price=round(tp_px, 2), time_in_force=TimeInForce.GTC,
                    order_class=OrderClass.OCO,
                    stop_loss=StopLossRequest(stop_price=floor_px),
                )
                broker.trading.submit_order(req)
                placed = True
            except Exception as e:
                logger.warning(f"[StopWatchdog] {sym} OCO failed ({e}) — plain-stop fallback")
        if not placed:
            try:
                # protection first: never leave the position naked until next pass
                req = StopOrderRequest(
                    symbol=sym, qty=abs_qty, side=order_side,
                    stop_price=floor_px, time_in_force=TimeInForce.GTC,
                )
                broker.trading.submit_order(req)
                placed = True
            except Exception as e:
                logger.error(f"[StopWatchdog] {sym} repair failed: {e}")
        if placed:
            repaired += 1
            _fail_streak.pop(sym, None)
            logger.warning(
                f"[StopWatchdog] REPAIRED {sym}: was {covered}/{abs_qty} covered -> "
                f"full-qty stop @ {floor_px} ({(floor_px/current-1)*100:+.1f}% from px)"
            )
        else:
            # v100.7 — silent-failure class defense: a position the watchdog
            # cannot protect for 2+ consecutive passes (>1h naked) pages the
            # human instead of dying in the logs (the OCO 422 bug hid for
            # months exactly this way). One alert per symbol per day.
            _fail_streak[sym] = _fail_streak.get(sym, 0) + 1
            if _fail_streak[sym] >= 2:
                from datetime import date
                key = f"watchdog_naked_{sym}_{date.today().isoformat()}"
                if key not in _alerted:
                    _alerted.add(key)
                    logger.critical(f"[StopWatchdog] {sym} UNPROTECTED for "
                                    f"{_fail_streak[sym]} passes — escalating")
                    try:
                        from utils.notify import emergency as _emerg
                        _emerg("⚠️ Position unprotected",
                               f"{sym}: stop placement failing repeatedly "
                               f"({_fail_streak[sym]} watchdog passes). Check orders.",
                               key=key)
                    except Exception:
                        pass

    if repaired:
        logger.warning(f"[StopWatchdog] repaired {repaired} position(s) this pass")
    return repaired
