"""
Order Flow Imbalance (OFI) Monitor.
Streams real-time quotes via Alpaca WebSocket for open positions.
Tracks bid/ask volume imbalance — heavy sell-side pressure triggers early exit signal.
"""
import json
import logging
import os
import threading
import time
from collections import defaultdict, deque

logger = logging.getLogger(__name__)

_ofi_history: dict = defaultdict(lambda: deque(maxlen=100))
_ofi_lock = threading.Lock()
_watched_symbols: set = set()
_subscribed_symbols: set = set()
_running = False
_ws_thread = None
_ws_ref = None  # latest WebSocketApp for periodic resubscribe

OFI_ALERT_THRESHOLD = -0.6
OFI_WINDOW = 10


def get_ofi(symbol: str) -> float:
    """
    Return average OFI for symbol over last OFI_WINDOW quotes.
    Range: -1.0 (all sell) to +1.0 (all buy). 0.0 = balanced or no data.
    """
    with _ofi_lock:
        history = list(_ofi_history.get(symbol, []))
    if not history:
        return 0.0
    recent = history[-OFI_WINDOW:]
    return sum(recent) / len(recent)


def is_sell_pressure(symbol: str) -> bool:
    """Return True if sustained sell-side OFI detected."""
    return get_ofi(symbol) < OFI_ALERT_THRESHOLD


def _extract_position_symbols(broker) -> set:
    """Robustly extract symbols across broker API styles."""
    try:
        if hasattr(broker, "get_positions"):
            positions = broker.get_positions()
            return {p["symbol"] for p in positions}
        if hasattr(broker, "get_all_positions"):
            return {p.symbol for p in broker.get_all_positions()}
        if hasattr(broker, "trading"):
            return {p.symbol for p in broker.trading.get_all_positions()}
    except Exception as e:
        logger.debug(f"[OFI] Could not extract symbols: {e}")
    return set()


def update_watched(broker):
    """Update the set of symbols to watch based on current open positions."""
    global _watched_symbols
    try:
        syms = _extract_position_symbols(broker)
        _watched_symbols = syms
    except Exception as e:
        logger.debug(f"[OFI] Could not update watched symbols: {e}")


def _refresh_subscriptions(ws):
    """Subscribe to any new watched symbols (called periodically, not from on_message)."""
    global _subscribed_symbols
    try:
        new_syms = _watched_symbols - _subscribed_symbols
        if new_syms and ws is not None:
            ws.send(json.dumps({"action": "subscribe", "quotes": list(new_syms)}))
            _subscribed_symbols.update(new_syms)
            logger.info(f"[OFI] Subscribed to new symbols: {new_syms}")
    except Exception as e:
        logger.debug(f"[OFI] _refresh_subscriptions error: {e}")


def _periodic_resubscribe_loop(ws):
    """Background loop calling _refresh_subscriptions every 30s while ws alive."""
    while _running and ws is not None:
        try:
            _refresh_subscriptions(ws)
        except Exception:
            pass
        time.sleep(30)


def _ws_worker():
    """WebSocket worker streaming quotes for watched symbols."""
    global _ws_ref, _subscribed_symbols
    try:
        import websocket
    except ImportError:
        logger.warning("[OFI] websocket-client not installed — OFI monitor inactive")
        return

    api_key = os.environ.get("ALPACA_API_KEY", "")
    secret = os.environ.get("ALPACA_SECRET_KEY", "")
    if not api_key or not secret:
        logger.warning("[OFI] Alpaca creds not set — OFI monitor inactive")
        return
    WS_URL = "wss://stream.data.alpaca.markets/v2/iex"

    def on_open(ws):
        global _subscribed_symbols
        try:
            ws.send(json.dumps({"action": "auth", "key": api_key, "secret": secret}))
            time.sleep(0.5)
            syms = list(_watched_symbols) if _watched_symbols else ["SPY"]
            ws.send(json.dumps({"action": "subscribe", "quotes": syms}))
            _subscribed_symbols = set(syms)
            logger.info(f"[OFI] Subscribed to quotes for: {syms}")
            # Start the periodic resubscribe loop
            t = threading.Thread(target=_periodic_resubscribe_loop, args=(ws,),
                                 daemon=True, name="ofi_resubscribe")
            t.start()
        except Exception as e:
            logger.error(f"[OFI] on_open error: {e}")

    def on_message(ws, message):
        try:
            data = json.loads(message)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("T") != "q":
                    continue
                sym = item.get("S", "")
                bid_size = float(item.get("bs", 0) or 0)
                ask_size = float(item.get("as", 0) or 0)
                total = bid_size + ask_size
                if total > 0 and sym:
                    imbalance = (bid_size - ask_size) / total
                    with _ofi_lock:
                        _ofi_history[sym].append(imbalance)
        except Exception as e:
            logger.debug(f"[OFI] Message parse error: {e}")

    def on_error(ws, error):
        logger.error(f"[OFI] WS error: {error}")

    def on_close(ws, code, msg):
        logger.warning(f"[OFI] WS closed ({code})")

    while _running:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"[OFI] Connection error: {e}")
        if _running:
            time.sleep(15)


def start(broker=None):
    global _running, _ws_thread
    if _running:
        return
    _running = True
    if broker is not None:
        update_watched(broker)
    _ws_thread = threading.Thread(target=_ws_worker, daemon=True, name="ofi_monitor")
    _ws_thread.start()
    logger.info("[OFI] Order flow imbalance monitor started")


def stop():
    global _running
    _running = False
