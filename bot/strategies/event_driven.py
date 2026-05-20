"""
Event-driven trading strategy for AlphaBot.
Consumes EVENT_QUEUE from news_scanner and executes trades on high-impact news.

Gate pipeline:
  1. Price-move gate  — skip if >5% already moved (news priced in)
  2. Claude research  — assess event significance and trade direction
  3. Confidence gate  — skip if Claude confidence < 0.65
  4. Market hours     — only execute during regular trading hours
"""
import logging
import queue
import time
from datetime import datetime, timezone

from utils.news_scanner import EVENT_QUEUE

logger = logging.getLogger(__name__)

# Thresholds
PRICE_MOVE_THRESHOLD = 0.05   # Skip if stock already moved >5%
MIN_CONFIDENCE = 0.65          # Minimum Claude confidence to trade
POSITION_PCT = 0.04            # 4% of portfolio per event trade (smaller, faster in/out)
MAX_EVENT_POSITIONS = 3        # Cap concurrent event-driven positions

_event_positions: set = set()  # Track open event-driven positions


def _get_price_move(symbol: str, broker) -> float:
    """Return today's intraday % move for symbol using yfinance."""
    import yfinance as yf
    import gc
    try:
        fi = yf.Ticker(symbol).fast_info
        current = getattr(fi, "last_price", None)
        prev_close = getattr(fi, "previous_close", None)
        if current and prev_close and prev_close > 0:
            return abs(current - prev_close) / prev_close
        return 0.0
    except Exception:
        return 0.0
    finally:
        gc.collect()


def _research_event(symbol: str, headline: str, category: str) -> dict:
    """
    Call Claude to assess event significance.
    Returns dict: {direction: 'long'|'short'|'skip', confidence: float, rationale: str}
    """
    try:
        import anthropic
        import os

        client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

        prompt = f"""You are an expert quantitative trader assessing a breaking news event.

Symbol: {symbol}
Category: {category}
Headline: {headline}
Current time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}

Assess whether this news event creates an actionable intraday trading opportunity.

Respond in this EXACT JSON format (no extra text):
{{
  "direction": "long" | "short" | "skip",
  "confidence": 0.0-1.0,
  "rationale": "brief explanation",
  "expected_move_pct": 0.0-20.0,
  "time_horizon": "intraday" | "swing"
}}

Rules:
- confidence must be between 0.0 and 1.0
- Only say "long" or "short" if the event has CLEAR directional impact not yet priced in
- Say "skip" if the event is ambiguous, already priced in, or not actionable
- Regulatory events (lawsuits, fines) are typically bearish → "short"
- Regulatory approvals (FDA, contract wins) are typically bullish → "long"
- Earnings beats → "long", earnings misses → "short"
"""

        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        import json
        result = json.loads(text)
        return result
    except Exception as e:
        logger.error(f"[event_driven] Claude research error for {symbol}: {e}")
        return {"direction": "skip", "confidence": 0.0, "rationale": str(e)}


def _is_market_open(broker) -> bool:
    """Check if market is currently open."""
    try:
        return broker.is_market_open()
    except Exception:
        import pytz
        from datetime import datetime
        et = pytz.timezone("America/New_York")
        now = datetime.now(et)
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_open <= now <= market_close and now.weekday() < 5


def _get_portfolio_value(broker) -> float:
    """Get current portfolio equity."""
    try:
        account = broker.get_account()
        return float(account.get("equity") or account.get("portfolio_value") or 100000.0)
    except Exception:
        return 100000.0


def _execute_trade(symbol: str, direction: str, broker, db_conn, portfolio_value: float):
    """Execute the event-driven trade."""
    try:
        import yfinance as yf
        trade_value = portfolio_value * POSITION_PCT
        price = getattr(yf.Ticker(symbol).fast_info, "last_price", None)
        if not price or price <= 0:
            logger.warning(f"[event_driven] Cannot get price for {symbol}")
            return

        qty = int(trade_value / price)
        if qty < 1:
            logger.warning(f"[event_driven] Position too small for {symbol} at ${price:.2f}")
            return

        side = "buy" if direction == "long" else "sell"
        logger.info(f"[event_driven] {side.upper()} {qty} {symbol} @ ~${price:.2f} (event-driven)")

        # Place market order
        broker.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type="market",
            time_in_force="day",
        )
        _event_positions.add(symbol)
        logger.info(f"[event_driven] Order placed: {side} {qty} {symbol}")
        # Cash guard
        if side == "buy":
            from broker import AlpacaBroker
            from config import MIN_CASH_RESERVE_PCT
            _cash, _pv = broker.get_live_cash()
            if _cash < 0 or (_pv > 0 and _cash / _pv < MIN_CASH_RESERVE_PCT):
                logger.warning("[EventDriven] Cash floor hit after order")
                from utils.notify import send as _n
                _n("⚠️ EventDriven Cash Floor", f"Cash ${_cash:,.0f} after buying {symbol}", priority="high")

    except Exception as e:
        logger.error(f"[event_driven] Trade execution error for {symbol}: {e}")


def run(broker, db_conn):
    """
    Consume EVENT_QUEUE and process events.
    Called every strategy cycle from run_all_strategies().
    """
    # Circuit breaker gate
    try:
        from main import _circuit_breaker_active
        if _circuit_breaker_active:
            logger.info("[event_driven] Circuit breaker active — skipping")
            return
    except ImportError:
        pass

    # Regime gate
    try:
        from utils.regime_weights import get_multiplier
        mult = get_multiplier("event_driven")
        if mult == 0.0:
            logger.info("[event_driven] Regime weight 0.0 — skipping entries")
            return
    except Exception:
        pass

    # Sync _event_positions with actual open positions — remove closed ones
    try:
        open_syms = {p["symbol"] for p in broker.get_positions()}
        stale = _event_positions - open_syms
        if stale:
            logger.info(f"[event_driven] Removing closed positions from tracker: {stale}")
            _event_positions -= stale
    except Exception as e:
        logger.debug(f"[event_driven] Position sync error: {e}")

    if EVENT_QUEUE.empty():
        return

    # Drain up to 5 events per cycle
    processed = 0
    while not EVENT_QUEUE.empty() and processed < 5:
        try:
            event = EVENT_QUEUE.get_nowait()
        except queue.Empty:
            break

        processed += 1
        symbol = event["symbol"]
        headline = event["headline"]
        category = event["category"]

        logger.info(f"[event_driven] Processing event: {symbol} [{category}]")

        # Gate 1: Market must be open
        if not _is_market_open(broker):
            logger.info(f"[event_driven] Market closed — skipping {symbol}")
            continue

        # Gate 2: Cap concurrent event positions
        if len(_event_positions) >= MAX_EVENT_POSITIONS:
            logger.info(f"[event_driven] Max event positions ({MAX_EVENT_POSITIONS}) reached — skipping {symbol}")
            continue

        # Gate 3: Price-move check — skip if already moved >5%
        move = _get_price_move(symbol, broker)
        if move > PRICE_MOVE_THRESHOLD:
            logger.info(f"[event_driven] {symbol} already moved {move:.1%} — news priced in, skipping")
            continue

        # Gate 4: Claude research
        research = _research_event(symbol, headline, category)
        direction = research.get("direction", "skip")
        confidence = float(research.get("confidence", 0.0))
        rationale = research.get("rationale", "")

        logger.info(f"[event_driven] {symbol} → {direction} (confidence {confidence:.2f}): {rationale}")

        if direction == "skip":
            logger.info(f"[event_driven] Claude says skip {symbol}")
            continue

        # Gate 5: Confidence threshold
        if confidence < MIN_CONFIDENCE:
            logger.info(f"[event_driven] {symbol} confidence {confidence:.2f} < {MIN_CONFIDENCE} — skipping")
            continue

        # Execute
        portfolio_value = _get_portfolio_value(broker)
        _execute_trade(symbol, direction, broker, db_conn, portfolio_value)

        # Small delay between trades
        time.sleep(1)
