"""
Earnings NLP Strategy — AlphaBot
Uses Claude to analyse earnings call transcripts for tone change vs prior quarter.
Combines with EPS surprise direction for a blended signal.
Enters PEAD trade 2 days after earnings, holds up to 20 trading days.
Take profit: +15% | Stop loss: -7%
Max 10 concurrent positions | 5% portfolio per trade
"""
import gc
import logging
import os
import json
import time
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MAX_POSITIONS = 20
# Conviction-based position sizing
POSITION_PCT_BASE    = 0.05   # confidence 0.60–0.74
POSITION_PCT_HIGH    = 0.08   # confidence 0.75–0.89
POSITION_PCT_VERY_HIGH = 0.12 # confidence ≥ 0.90
TAKE_PROFIT = 0.15           # +15%
STOP_LOSS = -0.07            # -7%
HOLD_DAYS = 20               # max trading days to hold
MIN_CONFIDENCE = 0.60        # minimum Claude confidence to trade
ENTRY_DELAY_DAYS = 2         # enter 2 calendar days after earnings

# Universe — liquid names with regular earnings catalysts
EARNINGS_UNIVERSE = [
    # Mega-cap tech
    "AAPL", "MSFT", "GOOGL", "GOOG", "META", "AMZN", "NVDA", "TSLA", "ORCL", "ADBE",
    # Semis
    "AMD", "AVGO", "INTC", "QCOM", "MU", "TXN", "AMAT", "LRCX", "KLAC", "MRVL",
    "MCHP", "ADI", "NXPI", "SWKS", "MPWR", "WOLF", "ON", "STM", "ACLS",
    # Cloud / SaaS
    "CRM", "NOW", "SNOW", "DDOG", "PANW", "CRWD", "ZS", "OKTA", "NET", "FTNT",
    "MDB", "ESTC", "CFLT", "HUBS", "BILL", "GTLB", "DXCM", "VEEV", "WDAY", "TEAM",
    "ZM", "DOCU", "BOX", "DBX", "TWLO", "FROG", "PD", "APPN", "NCNO",
    # Financials
    "JPM", "GS", "MS", "BAC", "WFC", "C", "BLK", "SCHW", "V", "MA",
    "AXP", "COF", "DFS", "SYF", "ALLY", "FITB", "RF", "CFG", "HBAN", "KEY",
    "MTB", "ZION", "CMA", "PBCT", "FRC", "SIVB",
    # Healthcare / Biotech
    "LLY", "JNJ", "MRK", "AMGN", "GILD", "ABBV", "BMY", "PFE", "MRNA", "BNTX",
    "REGN", "BIIB", "VRTX", "SGEN", "ALNY", "IONS", "RARE", "ACAD", "NBIX",
    "INCY", "EXEL", "HALO", "KRYS", "PRTA",
    # Consumer / Retail
    "NFLX", "SBUX", "NKE", "HD", "MCD", "AMZN", "TGT", "WMT", "COST", "LOW",
    "TJX", "ROST", "BURL", "M", "KSS", "GPS", "ANF", "AEO", "URBN",
    "LULU", "RH", "WSM", "CPRI", "TPR", "PVH", "HBI",
    # Industrials / Aerospace
    "CAT", "HON", "GE", "BA", "RTX", "LMT", "NOC", "GD", "HII",
    "DE", "EMR", "ETN", "PH", "ROK", "AME", "XYL", "CARR", "OTIS",
    # Energy
    "XOM", "CVX", "OXY", "SLB", "HAL", "BKR", "MPC", "VLO", "PSX",
    "COP", "PXD", "EOG", "DVN", "FANG", "APA",
    # Consumer staples
    "PEP", "KO", "PG", "CL", "KMB", "GIS", "K", "CPB", "HRL", "SJM",
    # Media / Telecom
    "DIS", "CMCSA", "T", "VZ", "TMUS", "CHTR", "PARA", "WBD",
    # Real estate / REITs
    "AMT", "PLD", "CCI", "EQIX", "SPG", "PSA", "EQR", "AVB",
    # Autos
    "GM", "F", "RIVN", "LCID", "NIO",
    # Misc high-vol
    "UBER", "LYFT", "ABNB", "DASH", "RBLX", "SNAP", "PINS", "RDDT",
    "COIN", "MSTR", "HOOD", "SOFI", "AFRM", "UPST",
]

# In-memory state (persists for process lifetime)
_active_positions: dict = {}   # symbol -> {entry_price, entry_date, side, qty}
_processed_earnings: set = set()  # "SYMBOL_YYYY-MM-DD" — avoid double-entry
_last_earnings_scan = 0        # unix timestamp of last scan
_state_restored: bool = False


def _restore_state(broker, db_conn=None):
    """Rebuild _active_positions from broker on startup.

    M4: entry_date is the most recent buy_* timestamp in the trades table
    for that symbol (or fallback to 1 day ago if no DB record).
    """
    global _state_restored
    if _state_restored:
        return
    _state_restored = True
    try:
        positions = broker.get_positions()
        for pos in positions:
            sym = pos["symbol"]
            tag = pos.get("strategy", "") or ""
            if "earnings" in tag.lower() and sym not in _active_positions:
                qty = float(pos.get("qty", 0))
                entry_date = None
                if db_conn is not None:
                    try:
                        row = db_conn.execute(
                            "SELECT created_at FROM trades "
                            "WHERE symbol=? AND side LIKE 'buy%' "
                            "ORDER BY created_at DESC LIMIT 1",
                            (sym,),
                        ).fetchone()
                        if row:
                            raw = row[0] if not hasattr(row, "keys") else row["created_at"]
                            try:
                                dt = datetime.fromisoformat(raw)
                            except Exception:
                                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            entry_date = dt
                    except Exception as e:
                        logger.debug(f"[EarningsNLP] DB entry_date lookup failed {sym}: {e}")
                if entry_date is None:
                    entry_date = datetime.now(timezone.utc) - timedelta(days=1)
                _active_positions[sym] = {
                    "entry_price": float(pos.get("avg_entry", 0)),
                    "entry_date": entry_date,
                    "side": "long" if qty > 0 else "short",
                    "qty": int(abs(qty)),
                }
                logger.info(f"[EarningsNLP] Restored position: {sym} (entry={entry_date.date()})")
    except Exception as e:
        logger.warning(f"[EarningsNLP] State restore failed: {e}")


def _conviction_size(confidence: float) -> float:
    """Return position size as fraction of portfolio based on Claude confidence."""
    if confidence >= 0.90:
        return POSITION_PCT_VERY_HIGH
    elif confidence >= 0.75:
        return POSITION_PCT_HIGH
    else:
        return POSITION_PCT_BASE


def _get_recent_earnings(lookback_days: int = 5) -> list:
    """
    Return list of symbols that reported earnings in the last lookback_days days.
    Uses parallel fetching with ThreadPoolExecutor for speed.
    """
    # Fast path: use the pre-loaded calendar if available
    try:
        from utils.earnings_calendar import UPCOMING_EARNINGS, _calendar_lock
        with _calendar_lock:
            if UPCOMING_EARNINGS:
                now = datetime.now(timezone.utc)
                since_dt = now - timedelta(days=lookback_days)
                preloaded = []
                for sym, dates in UPCOMING_EARNINGS.items():
                    for d in dates:
                        if since_dt <= d <= now:
                            preloaded.append({"symbol": sym, "earnings_date": d})
                if preloaded:
                    logger.debug(f"[EarningsNLP] Using pre-loaded calendar ({len(preloaded)} events)")
                    return preloaded
    except Exception:
        pass

    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = []
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    def check_symbol(sym):
        try:
            ticker = yf.Ticker(sym)
            cal = ticker.calendar
            if cal is None:
                return []
            earnings_dates = []
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date", [])
                if ed:
                    earnings_dates = ed if isinstance(ed, list) else [ed]
            elif hasattr(cal, "loc"):
                try:
                    ed = cal.loc["Earnings Date"].values
                    earnings_dates = list(ed)
                except Exception:
                    pass
            found = []
            for ed in earnings_dates:
                try:
                    if hasattr(ed, "to_pydatetime"):
                        ed = ed.to_pydatetime()
                    if isinstance(ed, str):
                        ed = datetime.fromisoformat(ed)
                    if ed.tzinfo is None:
                        ed = ed.replace(tzinfo=timezone.utc)
                    ed_utc = ed.astimezone(timezone.utc)
                    if since <= ed_utc <= datetime.now(timezone.utc):
                        found.append({"symbol": sym, "earnings_date": ed_utc})
                except Exception:
                    continue
            return found
        except Exception as e:
            logger.debug(f"[EarningsNLP] Calendar error for {sym}: {e}")
            return []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(check_symbol, sym): sym for sym in EARNINGS_UNIVERSE}
        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception:
                pass

    return results


def _get_eps_surprise(symbol: str) -> float:
    """
    Return EPS surprise as a float:
      positive = beat (e.g. 0.15 = beat by 15%)
      negative = miss
      0.0 = no data
    Uses yfinance earnings history.
    """
    import yfinance as yf
    try:
        ticker = yf.Ticker(symbol)
        eh = ticker.earnings_history
        if eh is None or (hasattr(eh, "empty") and eh.empty):
            return 0.0
        # Most recent row
        if hasattr(eh, "iloc"):
            row = eh.iloc[-1]
            est = row.get("epsEstimate", 0) or row.get("EPS Estimate", 0)
            actual = row.get("epsActual", 0) or row.get("Reported EPS", 0)
            if est and est != 0:
                return float((actual - est) / abs(est))
        return 0.0
    except Exception as e:
        logger.debug(f"[EarningsNLP] EPS surprise error for {symbol}: {e}")
        return 0.0
    finally:
        pass


def _get_price_reaction(symbol: str, earnings_date: datetime) -> float:
    """
    Return the price reaction on earnings day as a % move.
    Used as fallback when EPS data unavailable.
    """
    import yfinance as yf
    try:
        ticker = yf.Ticker(symbol)
        start = (earnings_date - timedelta(days=2)).strftime("%Y-%m-%d")
        end = (earnings_date + timedelta(days=2)).strftime("%Y-%m-%d")
        hist = ticker.history(start=start, end=end, interval="1d", auto_adjust=True)
        if hist is None or len(hist) < 2:
            return 0.0
        # Day-over-day change closest to earnings date
        return float((hist["Close"].iloc[-1] - hist["Close"].iloc[-2]) / hist["Close"].iloc[-2])
    except Exception:
        return 0.0
    finally:
        pass


def _fetch_transcript_text(symbol: str) -> str:
    """
    Attempt to fetch recent earnings call transcript text.
    Falls back to yfinance news summary if transcript unavailable.
    """
    import yfinance as yf
    import requests

    # Try yfinance news as proxy for transcript content
    transcript_text = ""
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if news:
            headlines = []
            for item in news[:10]:
                title = item.get("title", "")
                summary = item.get("summary", "") or item.get("description", "")
                if any(kw in (title + summary).lower() for kw in
                       ["earnings", "revenue", "guidance", "quarter", "eps", "profit", "outlook"]):
                    headlines.append(f"- {title}: {summary[:200]}")
            transcript_text = "\n".join(headlines)
    except Exception:
        pass
    finally:
        pass

    return transcript_text or f"No transcript available for {symbol}"


def _claude_analyse(symbol: str, eps_surprise: float, price_reaction: float,
                    transcript_text: str, earnings_date: str) -> dict:
    """
    Use Claude to analyse the earnings event holistically.
    Returns: {direction: 'long'|'short'|'skip', confidence: float, rationale: str}
    """
    import anthropic

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

    surprise_pct = f"{eps_surprise * 100:+.1f}%" if eps_surprise else "unknown"
    reaction_pct = f"{price_reaction * 100:+.1f}%" if price_reaction else "unknown"

    prompt = f"""You are an expert quantitative analyst assessing a Post-Earnings Announcement Drift (PEAD) trade.

Company: {symbol}
Earnings date: {earnings_date}
EPS surprise: {surprise_pct} (positive = beat, negative = miss)
Stock price reaction on earnings day: {reaction_pct}
Recent news/transcript excerpts:
{transcript_text[:1500]}

Your task: assess whether there is a PEAD opportunity 2 days after earnings.

PEAD logic:
- Earnings beats tend to continue drifting UP for 20-45 days (go LONG)
- Earnings misses tend to continue drifting DOWN for 20-45 days (go SHORT)
- EXCEPTION: if the stock already moved >8% on earnings day, most drift is priced in → SKIP
- EXCEPTION: if guidance was RAISED despite a miss, or CUT despite a beat → reverse or SKIP
- EXCEPTION: if management tone is significantly more cautious/optimistic than the raw EPS number suggests → adjust direction or confidence

Respond in this EXACT JSON format (no other text):
{{
  "direction": "long" | "short" | "skip",
  "confidence": 0.0-1.0,
  "rationale": "2-3 sentence explanation",
  "tone_assessment": "bullish" | "bearish" | "neutral" | "mixed",
  "guidance_flag": "raised" | "lowered" | "maintained" | "unknown",
  "already_priced_in": true | false
}}

Rules:
- confidence must be 0.0–1.0
- Only say long/short if drift is likely NOT fully priced in
- Be conservative — when in doubt, say skip
- The LLY case (miss EPS but guided UP strongly) should be SHORT→SKIP or SHORT→LONG depending on guidance
"""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        result = json.loads(text.strip())
        return result
    except Exception as e:
        logger.error(f"[EarningsNLP] Claude error for {symbol}: {e}")
        return {"direction": "skip", "confidence": 0.0, "rationale": str(e),
                "tone_assessment": "unknown", "guidance_flag": "unknown", "already_priced_in": False}


def _get_portfolio_value(broker) -> float:
    try:
        account = broker.get_account()
        return float(account.get("equity") or account.get("portfolio_value") or 100000.0)
    except Exception:
        return 100000.0


def _manage_open_positions(broker):
    """Check existing earnings NLP positions for take profit / stop loss / time exit."""
    to_close = []
    now = datetime.now(timezone.utc)

    for sym, pos in list(_active_positions.items()):
        try:
            import yfinance as yf
            fi = yf.Ticker(sym).fast_info
            current = getattr(fi, "last_price", None)
            if not current:
                continue
            entry = pos["entry_price"]
            side = pos["side"]
            entry_date = pos["entry_date"]
            age_days = (now - entry_date).days

            pnl_pct = (current - entry) / entry if side == "long" else (entry - current) / entry

            reason = None
            if pnl_pct >= TAKE_PROFIT:
                reason = f"TAKE_PROFIT ({pnl_pct:+.1%})"
            elif pnl_pct <= STOP_LOSS:
                reason = f"STOP_LOSS ({pnl_pct:+.1%})"
            elif age_days >= HOLD_DAYS:
                reason = f"TIME_EXIT ({age_days}d)"

            if reason:
                to_close.append((sym, pos, reason))
        except Exception as e:
            logger.debug(f"[EarningsNLP] Position check error {sym}: {e}")

    for sym, pos, reason in to_close:
        try:
            side_close = "sell" if pos["side"] == "long" else "buy"
            qty = pos["qty"]
            broker.submit_order(
                symbol=sym, qty=qty, side=side_close,
                type="market", time_in_force="day"
            )
            logger.info(f"[EarningsNLP] Closed {sym} — {reason}")
            del _active_positions[sym]
        except Exception as e:
            logger.error(f"[EarningsNLP] Close error {sym}: {e}")


def run(broker, db_conn=None):
    """
    Main entry point — called every strategy cycle from run_all_strategies().
    1. Manage existing positions (take profit / stop loss / time exit)
    2. Scan for new earnings events and enter PEAD trades
    """
    global _last_earnings_scan

    _restore_state(broker, db_conn)

    # Skip scans on weekends — market is closed, no point burning API calls
    from datetime import datetime, timezone
    if datetime.now(timezone.utc).weekday() >= 5:
        if _active_positions:
            _manage_open_positions(broker)
        return

    # Always manage open positions first
    if _active_positions:
        _manage_open_positions(broker)

    # Only scan for new earnings every 4 hours (not every 5-min cycle)
    now_ts = time.time()
    if now_ts - _last_earnings_scan < 3600:
        return
    _last_earnings_scan = now_ts

    # Check capacity
    if len(_active_positions) >= MAX_POSITIONS:
        logger.info(f"[EarningsNLP] At max positions ({MAX_POSITIONS}) — skipping scan")
        return

    logger.info("[EarningsNLP] Scanning for earnings events...")

    try:
        events = _get_recent_earnings(lookback_days=5)
    except Exception as e:
        logger.error(f"[EarningsNLP] Earnings scan failed: {e}")
        return

    if not events:
        logger.info("[EarningsNLP] No recent earnings events found")
        return

    portfolio_value = _get_portfolio_value(broker)

    for event in events:
        sym = event["symbol"]
        earnings_date = event["earnings_date"]
        key = f"{sym}_{earnings_date.strftime('%Y-%m-%d')}"

        # Skip if already processed or already in position
        if key in _processed_earnings:
            continue
        if sym in _active_positions:
            continue
        if len(_active_positions) >= MAX_POSITIONS:
            break

        # Check entry delay — must be at least ENTRY_DELAY_DAYS after earnings
        days_since = (datetime.now(timezone.utc) - earnings_date).days
        if days_since < ENTRY_DELAY_DAYS:
            logger.debug(f"[EarningsNLP] {sym} earnings {days_since}d ago — waiting for day {ENTRY_DELAY_DAYS}")
            continue
        if days_since > ENTRY_DELAY_DAYS + 2:
            # Entry window closed (more than 4 days after earnings — too late for PEAD)
            _processed_earnings.add(key)
            continue

        _processed_earnings.add(key)

        logger.info(f"[EarningsNLP] Analysing {sym} (earnings {days_since}d ago)")

        # Gather signals
        eps_surprise = _get_eps_surprise(sym)
        price_reaction = _get_price_reaction(sym, earnings_date)
        transcript = _fetch_transcript_text(sym)

        # Claude analysis
        analysis = _claude_analyse(
            symbol=sym,
            eps_surprise=eps_surprise,
            price_reaction=price_reaction,
            transcript_text=transcript,
            earnings_date=earnings_date.strftime("%Y-%m-%d")
        )

        direction = analysis.get("direction", "skip")
        confidence = float(analysis.get("confidence", 0.0))
        rationale = analysis.get("rationale", "")
        already_priced = analysis.get("already_priced_in", False)

        logger.info(
            f"[EarningsNLP] {sym}: {direction} (conf {confidence:.2f}) | "
            f"EPS surprise {eps_surprise:+.1%} | price reaction {price_reaction:+.1%} | "
            f"priced_in={already_priced} | {rationale}"
        )

        if direction == "skip":
            continue
        if confidence < MIN_CONFIDENCE:
            logger.info(f"[EarningsNLP] {sym} confidence {confidence:.2f} < {MIN_CONFIDENCE} — skipping")
            continue
        if already_priced:
            logger.info(f"[EarningsNLP] {sym} already priced in — skipping")
            continue

        # Correlation monitor — block if too similar to existing positions
        try:
            from utils.correlation_monitor import is_entry_allowed as _corr_ok
            allowed, reason = _corr_ok(sym, broker)
            if not allowed:
                logger.info(f"[EarningsNLP] {sym} blocked by correlation monitor: {reason}")
                continue
        except Exception as _e:
            logger.debug(f"[EarningsNLP] Correlation check error for {sym}: {_e}")

        # Regime-aware sizing
        try:
            from utils.regime_weights import get_multiplier as _regime_mult
            regime_mult = _regime_mult("earnings_nlp")
        except Exception:
            regime_mult = 1.0
        if regime_mult == 0.0:
            logger.info(f"[EarningsNLP] Regime weight 0.0 — skipping {sym}")
            continue

        # Execute
        try:
            import yfinance as yf
            price = getattr(yf.Ticker(sym).fast_info, "last_price", None)
            if not price or price <= 0:
                logger.warning(f"[EarningsNLP] Cannot get price for {sym}")
                continue
            pos_pct = _conviction_size(confidence) * regime_mult
            trade_value = portfolio_value * pos_pct
            logger.info(f"[EarningsNLP] Conviction sizing: conf {confidence:.2f} × regime {regime_mult:.2f} → {pos_pct:.1%} of portfolio (${trade_value:,.0f})")
            qty = int(trade_value / price)
            if qty < 1:
                logger.warning(f"[EarningsNLP] {sym} position too small at ${price:.2f}")
                continue

            side = "buy" if direction == "long" else "sell"
            broker.submit_order(
                symbol=sym, qty=qty, side=side,
                type="market", time_in_force="day"
            )
            _active_positions[sym] = {
                "entry_price": price,
                "entry_date": datetime.now(timezone.utc),
                "side": direction,
                "qty": qty,
            }
            logger.info(
                f"[EarningsNLP] ORDER: {side.upper()} {qty} {sym} @ ~${price:.2f} "
                f"(PEAD {direction}, conf {confidence:.2f})"
            )
            # M10: record the signal so back-tests can score hit rate
            try:
                if db_conn is not None:
                    from db import log_signal
                    log_signal(db_conn, "earnings_nlp", sym, direction, confidence, {
                        "earnings_date": earnings_date.strftime("%Y-%m-%d"),
                        "eps_surprise": eps_surprise,
                        "price_reaction": price_reaction,
                        "confidence": confidence,
                        "rationale": rationale[:200],
                    })
            except Exception as _e:
                logger.debug(f"[EarningsNLP] log_signal failed for {sym}: {_e}")
        except Exception as e:
            logger.error(f"[EarningsNLP] Order failed for {sym}: {e}")
        finally:
            pass
