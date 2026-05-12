"""
Real-time news scanner for AlphaBot.
Streams Alpaca news via WebSocket (falls back to REST polling).
Maintains a thread-safe EVENT_QUEUE consumed by event_driven.py.
"""
import os
import time
import json
import logging
import threading
import queue
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Thread-safe queue consumed by event_driven strategy
EVENT_QUEUE: queue.Queue = queue.Queue(maxsize=200)

# Deduplication: symbol -> last event timestamp
_dedup: dict = {}
_DEDUP_WINDOW_SEC = 1800  # 30 min

# Monitored universe
WATCHED_SYMBOLS = [
    # Crypto-adjacent
    "COIN", "MSTR", "RIOT", "MARA", "CLSK", "HUT",
    # Biotech catalysts
    "MRNA", "BNTX", "NVAX", "REGN", "BIIB", "GILD", "VRTX", "SGEN",
    # High-vol tech
    "NVDA", "AMD", "SMCI", "PLTR", "PALANTIR", "META", "GOOGL", "MSFT", "AAPL",
    "TSLA", "AMZN", "NFLX", "CRM", "SNOW", "DDOG", "CRWD", "ZS", "OKTA",
    # Financials
    "GS", "MS", "JPM", "BAC", "C", "WFC", "BLK", "SCHW",
    # Energy
    "XOM", "CVX", "OXY", "SLB", "HAL",
    # Semis
    "AVGO", "QCOM", "INTC", "MU", "TXN", "AMAT", "LRCX", "KLAC",
    # ETFs that move on macro
    "SPY", "QQQ", "IWM", "XLF", "XLE", "XLK", "XLRE",
    # Others with catalyst potential
    "PANW", "UBER", "LYFT", "ABNB", "DASH", "ROKU", "TTD",
]

# Keyword → event category mapping
KEYWORD_TRIGGERS = {
    "REGULATORY": [
        "sec", "cftc", "doj", "ftc", "ruling", "lawsuit", "settlement",
        "indictment", "charged", "fined", "penalty", "banned", "approved",
        "rejected", "court", "verdict", "injunction", "subpoena",
    ],
    "EARNINGS_CATALYST": [
        "earnings beat", "earnings miss", "revenue beat", "revenue miss",
        "raised guidance", "lowered guidance", "beats estimates", "misses estimates",
        "eps beat", "eps miss", "raised outlook", "cut outlook",
    ],
    "CATALYST": [
        "acquisition", "merger", "takeover", "buyout", "partnership",
        "deal", "contract", "awarded", "fda approval", "fda approved",
        "breakthrough", "exclusive", "patent", "spinoff", "spin-off",
        "dividend", "buyback", "share repurchase",
    ],
    "MACRO": [
        "fed rate", "federal reserve", "interest rate", "inflation", "cpi",
        "jobs report", "nonfarm", "gdp", "recession", "tariff", "sanction",
        "debt ceiling", "default", "bank failure",
    ],
}

_ws_thread: Optional[threading.Thread] = None
_poll_thread: Optional[threading.Thread] = None
_running = False
_api_key = ""
_secret_key = ""


def _classify_headline(text: str) -> Optional[str]:
    """Return event category if headline matches any trigger keyword."""
    lower = text.lower()
    for category, keywords in KEYWORD_TRIGGERS.items():
        for kw in keywords:
            if kw in lower:
                return category
    return None


def _should_enqueue(symbol: str) -> bool:
    """Dedup: skip if same symbol triggered within the window."""
    now = time.time()
    last = _dedup.get(symbol, 0)
    if now - last < _DEDUP_WINDOW_SEC:
        return False
    _dedup[symbol] = now
    return True


def _enqueue_event(symbol: str, headline: str, category: str, source: str = "alpaca"):
    if not _should_enqueue(symbol):
        logger.debug(f"[news_scanner] Dedup suppressed {symbol}")
        return
    event = {
        "symbol": symbol,
        "headline": headline,
        "category": category,
        "source": source,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        EVENT_QUEUE.put_nowait(event)
        logger.info(f"[news_scanner] EVENT queued: {symbol} [{category}] — {headline[:80]}")
    except queue.Full:
        logger.warning("[news_scanner] EVENT_QUEUE full — dropping event")


def _process_news_item(item: dict):
    """Parse a single Alpaca news item and enqueue if it matches."""
    headline = item.get("headline", "") or item.get("summary", "")
    symbols = item.get("symbols", [])
    category = _classify_headline(headline)
    if not category:
        return
    matched = [s for s in symbols if s in WATCHED_SYMBOLS]
    if not matched:
        # Still enqueue if headline is very high-impact macro
        if category == "MACRO":
            _enqueue_event("SPY", headline, category)
        return
    for sym in matched:
        _enqueue_event(sym, headline, category)


# ── WebSocket stream ───────────────────────────────────────────────────────────

def _ws_worker():
    """Connect to Alpaca news WebSocket stream."""
    try:
        import websocket  # websocket-client
    except ImportError:
        logger.warning("[news_scanner] websocket-client not installed — using REST fallback only")
        return

    WS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"

    def on_message(ws, message):
        try:
            data = json.loads(message)
            if isinstance(data, list):
                for item in data:
                    if item.get("T") == "n":  # news message type
                        _process_news_item(item)
            elif isinstance(data, dict) and data.get("T") == "n":
                _process_news_item(data)
        except Exception as e:
            logger.error(f"[news_scanner] WS parse error: {e}")

    def on_open(ws):
        auth = json.dumps({"action": "auth", "key": _api_key, "secret": _secret_key})
        ws.send(auth)
        subscribe = json.dumps({"action": "subscribe", "news": ["*"]})
        ws.send(subscribe)
        logger.info("[news_scanner] WebSocket connected and subscribed to all news")

    def on_error(ws, error):
        logger.error(f"[news_scanner] WS error: {error}")

    def on_close(ws, code, msg):
        logger.warning(f"[news_scanner] WS closed ({code}): {msg}")

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
            logger.error(f"[news_scanner] WS connection failed: {e}")
        if _running:
            logger.info("[news_scanner] Reconnecting in 30s...")
            time.sleep(30)


# ── REST polling fallback ─────────────────────────────────────────────────────

def _poll_worker():
    """Poll Alpaca REST news endpoint as fallback / supplement."""
    BASE = "https://data.alpaca.markets/v1beta1/news"
    headers = {"APCA-API-KEY-ID": _api_key, "APCA-API-SECRET-KEY": _secret_key}
    last_seen: set = set()

    while _running:
        try:
            since = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
            params = {"start": since, "limit": 50, "sort": "desc"}
            r = requests.get(BASE, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                items = r.json().get("news", [])
                for item in items:
                    item_id = item.get("id", "")
                    if item_id and item_id not in last_seen:
                        last_seen.add(item_id)
                        _process_news_item(item)
                # Keep last_seen bounded
                if len(last_seen) > 500:
                    last_seen = set(list(last_seen)[-200:])
            else:
                logger.warning(f"[news_scanner] REST poll HTTP {r.status_code}")
        except Exception as e:
            logger.error(f"[news_scanner] REST poll error: {e}")
        time.sleep(60)  # poll every 60 seconds


# ── Public API ────────────────────────────────────────────────────────────────

def start(api_key: str, secret_key: str):
    """Start news scanner background threads. Call once on bot startup."""
    global _running, _api_key, _secret_key, _ws_thread, _poll_thread
    if _running:
        logger.warning("[news_scanner] Already running")
        return
    _api_key = api_key
    _secret_key = secret_key
    _running = True

    _ws_thread = threading.Thread(target=_ws_worker, daemon=True, name="news_ws")
    _ws_thread.start()

    _poll_thread = threading.Thread(target=_poll_worker, daemon=True, name="news_poll")
    _poll_thread.start()

    logger.info("[news_scanner] Started (WS + REST polling threads)")


def stop():
    global _running
    _running = False
    logger.info("[news_scanner] Stopped")
