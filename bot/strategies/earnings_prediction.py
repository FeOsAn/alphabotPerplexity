"""
Earnings Prediction Strategy — v49

Predicts post-earnings direction using 5 signals; enters LONG the day before
earnings close. Replaces earnings_nlp.py (which remains in repo but is unused).

Signals:
  1. Analyst EPS revision momentum (yfinance eps_trend)
  2. Beat streak (yfinance earnings_history)
  3. Insider activity (yfinance insider_transactions)
  4. Short interest / squeeze fuel (yfinance info)
  5. Claude transcript sentiment (earningscall / Alpha Vantage + Anthropic)

Plus one edge gate: predicted move > options implied move.

Required env vars:
    ANTHROPIC_API_KEY     — Claude API (already in Railway)
Optional env vars (for better transcript coverage):
    EARNINGSCALL_API_KEY  — earningscall.biz API key (free: AAPL+MSFT only, paid: 5000+ tickers)
    ALPHA_VANTAGE_API_KEY — Alpha Vantage API key (free tier: 25 req/day)
Strategy works without these optional vars — transcript signal will be "unavailable"
and the bonus point simply won't fire for most symbols (need 2/4 bonus points to enter).
"""
import logging
import os
import time
from datetime import datetime, timezone, timedelta, date as date_type

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
STRATEGY_NAME = "earnings_prediction"

EP_WATCHLIST = [
    # S&P 500 large caps most likely to have good data coverage
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AMD", "PANW", "CRM",
    "NOW", "SNOW", "NET", "DDOG", "MDB", "PLTR", "ARM", "ANET", "MRVL", "AVGO",
    "QCOM", "MU", "ORCL", "ADBE", "INTU", "SHOP", "COIN", "JPM", "GS", "V", "MA",
    "UNH", "LLY", "ABBV", "MRK", "AMGN", "XOM", "CVX", "CAT", "GE", "RTX", "LMT",
    "COST", "HD", "NKE", "SBUX", "WMT", "NEE", "PLD", "AMT", "EQIX",
    "ON", "WOLF", "MCHP", "TXN", "ADI", "LRCX", "KLAC", "MPWR", "WDC", "DELL",
]

# Gate thresholds
EP_MIN_BEAT_STREAK = 2              # must have beaten EPS estimate in >= 2 of recent quarters
EP_MIN_REVISION_DELTA = 0.0         # eps_trend current > 30daysAgo (any upward revision)
EP_MAX_INSIDER_SELL_PCT = 0.10      # if insider sales > 10% of their holdings in 60d, skip
EP_MAX_SHORT_FOR_ENTRY = 0.30       # skip if short interest > 30% float (too risky)
EP_MIN_SHORT_FOR_SQUEEZE = 0.08     # short interest > 8% = squeeze bonus point
EP_DAYS_BEFORE_EARNINGS = 1         # enter this many calendar days before earnings

# Sizing
EP_SCORE_ALLOC = {2: 0.03, 3: 0.05, 4: 0.07}
EP_MAX_CONCURRENT = 4
EP_STOP_LOSS_PCT = 0.08             # -8% stop
EP_POST_EARNINGS_TAKE_PCT = 0.05    # if up >5% at next open, sell 50%
EP_POST_EARNINGS_STOP_PCT = 0.04    # gap-down protection
EP_MAX_HOLD_DAYS = 7                # exit after 7 days regardless

# Claude model
CLAUDE_MODEL = "claude-haiku-4-5"
MAX_TRANSCRIPT_CHARS = 6000

# ── Module-level state ────────────────────────────────────────────────────────
_ep_positions: dict[str, dict] = {}
_signal_cache: dict[str, dict] = {}
_transcript_cache: dict[str, dict] = {}
SIGNAL_CACHE_TTL = 3600 * 4   # 4 hours


# ── Signal 1: EPS revision momentum ───────────────────────────────────────────
def _get_revision_signal(symbol: str) -> tuple[bool, float]:
    """EPS revision momentum: current consensus > 30daysAgo consensus."""
    try:
        ticker = yf.Ticker(symbol)
        trend = ticker.eps_trend
        if trend is None or trend.empty:
            return False, 0.0
        if "0q" in trend.index:
            row = trend.loc["0q"]
        else:
            row = trend.iloc[0]
        current = float(row.get("current", 0) or 0)
        ago_30 = float(row.get("30daysAgo", 0) or 0)
        if current == 0 or ago_30 == 0:
            return False, 0.0
        delta = current - ago_30
        passes = delta > EP_MIN_REVISION_DELTA
        return passes, delta
    except Exception as e:
        logger.debug(f"[EP] revision_signal {symbol}: {e}")
        return False, 0.0


# ── Signal 2: beat streak ─────────────────────────────────────────────────────
def _get_beat_streak(symbol: str) -> tuple[bool, int]:
    """Count consecutive EPS beats in recent quarters."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.earnings_history
        if hist is None or hist.empty:
            return False, 0
        hist = hist.sort_index(ascending=False)
        beats = 0
        for _, row in hist.iterrows():
            actual = float(row.get("epsActual") or 0)
            estimate = float(row.get("epsEstimate") or 0)
            if estimate == 0:
                continue
            if actual > estimate:
                beats += 1
            else:
                break
        passes = beats >= EP_MIN_BEAT_STREAK
        return passes, beats
    except Exception as e:
        logger.debug(f"[EP] beat_streak {symbol}: {e}")
        return False, 0


# ── Signal 3: insider activity ────────────────────────────────────────────────
def _get_insider_signal(symbol: str) -> tuple[bool, str]:
    """
    Check insider activity in last 60 days.
    Passes gate if: no large insider sells (>10% of holdings) in 60 days.
    """
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.insider_transactions
        if df is None or df.empty:
            return True, "no_data"

        cutoff = datetime.now(timezone.utc) - timedelta(days=60)

        df = df.copy()
        try:
            df.index = pd.to_datetime(df.index, utc=True)
        except Exception:
            return True, "index_parse_failed"
        recent = df[df.index >= cutoff]

        if recent.empty:
            return True, "no_recent_activity"

        if "Transaction" not in recent.columns or "Shares" not in recent.columns:
            return True, "missing_cols"

        sells = recent[recent["Transaction"].astype(str).str.contains("Sale", case=False, na=False)]
        buys = recent[recent["Transaction"].astype(str).str.contains("Purchase|Buy", case=False, na=False)]

        sell_shares = float(sells["Shares"].abs().sum()) if not sells.empty else 0.0
        buy_shares = float(buys["Shares"].abs().sum()) if not buys.empty else 0.0

        try:
            info = ticker.fast_info
            shares_outstanding = float(info.shares) if hasattr(info, "shares") and info.shares else 1e8
            total_insider_pct = float(ticker.info.get("heldPercentInsiders", 0.05) or 0.05)
            insider_holdings = shares_outstanding * total_insider_pct
        except Exception:
            insider_holdings = max(sell_shares * 10, 1e6)

        sell_pct = sell_shares / insider_holdings if insider_holdings > 0 else 0.0

        if sell_pct > EP_MAX_INSIDER_SELL_PCT:
            return False, f"heavy_selling_{sell_pct:.1%}"

        if buy_shares > sell_shares:
            return True, "net_buying"
        return True, "neutral"
    except Exception as e:
        logger.debug(f"[EP] insider_signal {symbol}: {e}")
        return True, "error_fail_open"


# ── Signal 4: short interest ──────────────────────────────────────────────────
def _get_short_interest(symbol: str) -> tuple[bool, float, bool]:
    """Get short interest % of float. High short = squeeze fuel on beat."""
    try:
        info = yf.Ticker(symbol).info
        si = float(info.get("shortPercentOfFloat") or 0)
        passes = si < EP_MAX_SHORT_FOR_ENTRY
        squeeze_bonus = si >= EP_MIN_SHORT_FOR_SQUEEZE
        return passes, si, squeeze_bonus
    except Exception as e:
        logger.debug(f"[EP] short_interest {symbol}: {e}")
        return True, 0.0, False


# ── Signal 5: Claude transcript sentiment ─────────────────────────────────────
def _get_transcript_sentiment(symbol: str) -> tuple[str, str]:
    """
    Fetch most recent earnings call transcript and ask Claude to score sentiment.
    Focus on analyst Q&A section. Caches result for 4 hours.
    """
    cached = _transcript_cache.get(symbol)
    if cached and (time.time() - cached["ts"]) < SIGNAL_CACHE_TTL:
        return cached["verdict"], cached["reasoning"]

    transcript_text = None

    # Try earningscall library first
    try:
        import earningscall
        earningscall.api_key = os.getenv("EARNINGSCALL_API_KEY", "")
        from earningscall import get_company
        company = get_company(symbol)
        transcript = company.get_transcript(level=4)
        if transcript:
            qa_text = getattr(transcript, "questions_and_answers", "") or ""
            prepared_text = getattr(transcript, "prepared_remarks", "") or ""
            transcript_text = (
                f"[Q&A SECTION]\n{qa_text[:4000]}\n\n"
                f"[PREPARED REMARKS]\n{prepared_text[:2000]}"
            )
    except Exception as e:
        logger.debug(f"[EP] earningscall failed for {symbol}: {e}")

    # Fallback: Alpha Vantage transcript
    if not transcript_text:
        try:
            import requests as req
            av_key = os.getenv("ALPHA_VANTAGE_API_KEY", "")
            if av_key:
                now = datetime.now(timezone.utc)
                quarter = f"{now.year}Q{max(1, (now.month - 1) // 3)}"
                url = (
                    f"https://www.alphavantage.co/query?"
                    f"function=EARNINGS_CALL_TRANSCRIPT&symbol={symbol}"
                    f"&quarter={quarter}&apikey={av_key}"
                )
                resp = req.get(url, timeout=10).json()
                if isinstance(resp, dict) and "transcript" in resp:
                    raw = resp["transcript"]
                    if isinstance(raw, list):
                        raw = " ".join(
                            str(x.get("content", "")) if isinstance(x, dict) else str(x)
                            for x in raw
                        )
                    transcript_text = str(raw)[:MAX_TRANSCRIPT_CHARS]
        except Exception as e:
            logger.debug(f"[EP] alpha_vantage transcript failed for {symbol}: {e}")

    # Fallback: recent news headlines via yfinance (last 7 days only)
    # Stale news is worse than no news — hard cutoff at 7 days
    if not transcript_text:
        try:
            import yfinance as yf
            import time as _time
            news = yf.Ticker(symbol).news or []
            cutoff_ts = _time.time() - (7 * 24 * 3600)  # 7 days ago in unix time
            fresh_headlines = [
                n.get("title", "") for n in news
                if n.get("providerPublishTime", 0) >= cutoff_ts and n.get("title")
            ]
            if len(fresh_headlines) >= 3:  # need at least 3 recent headlines to be meaningful
                transcript_text = (
                    f"[RECENT NEWS — last 7 days, {len(fresh_headlines)} articles]\n"
                    + "\n".join(f"- {h}" for h in fresh_headlines[:15])
                )
                logger.debug(f"[EP] {symbol}: using {len(fresh_headlines)} fresh news headlines as sentiment proxy")
            else:
                logger.debug(f"[EP] {symbol}: only {len(fresh_headlines)} fresh headlines (<3) — skipping sentiment")
        except Exception as e:
            logger.debug(f"[EP] {symbol}: news fallback failed: {e}")

    if not transcript_text:
        _transcript_cache[symbol] = {
            "verdict": "unavailable", "reasoning": "no_fresh_content", "ts": time.time()
        }
        return "unavailable", "no_fresh_content"

    # Ask Claude
    try:
        import anthropic
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            _transcript_cache[symbol] = {
                "verdict": "unavailable", "reasoning": "no_api_key", "ts": time.time()
            }
            return "unavailable", "no_api_key"
        client = anthropic.Anthropic(api_key=api_key)
        is_news = transcript_text.startswith("[RECENT NEWS")
        if is_news:
            prompt = (
                f"Company: {symbol}. These are recent news headlines from the last 7 days.\n\n"
                f"{transcript_text[:MAX_TRANSCRIPT_CHARS]}\n\n"
                f"Based on these headlines, is the near-term sentiment around {symbol} "
                f"positive, neutral, or negative going into their upcoming earnings?\n"
                f"Consider: product momentum, analyst upgrades/downgrades, macro tailwinds/headwinds.\n\n"
                f"Reply with exactly: BULLISH, NEUTRAL, or BEARISH — then one sentence of reasoning."
            )
        else:
            prompt = (
                f"Company: {symbol}. This is from their most recent earnings call.\n\n"
                f"{transcript_text[:MAX_TRANSCRIPT_CHARS]}\n\n"
                f"Based solely on management tone, analyst questions, and guidance language:\n"
                f"1. Is management confident or defensive about next quarter?\n"
                f"2. Are analysts pushing back on anything specific?\n"
                f"3. Is guidance language getting more or less optimistic?\n\n"
                f"Reply with exactly: BULLISH, NEUTRAL, or BEARISH — then one sentence of reasoning."
            )
        msg = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        response = msg.content[0].text.strip()
        verdict_word = response.split()[0].upper().rstrip(".,:") if response else ""
        verdict = verdict_word if verdict_word in ("BULLISH", "NEUTRAL", "BEARISH") else "NEUTRAL"
        reasoning = " ".join(response.split()[1:])
        _transcript_cache[symbol] = {
            "verdict": verdict.lower(), "reasoning": reasoning, "ts": time.time()
        }
        return verdict.lower(), reasoning
    except Exception as e:
        logger.debug(f"[EP] Claude sentiment failed for {symbol}: {e}")
        _transcript_cache[symbol] = {
            "verdict": "unavailable", "reasoning": str(e), "ts": time.time()
        }
        return "unavailable", str(e)


# ── Signal 6: implied move ────────────────────────────────────────────────────
def _get_implied_move(symbol: str, earnings_date) -> float | None:
    """
    Compute implied earnings move from ATM straddle price.
    Returns move as decimal (0.08 = ±8%) or None if no options data.
    """
    try:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            return None
        earn_dt = earnings_date if isinstance(earnings_date, date_type) else earnings_date.date()
        valid_expiries = [e for e in expirations if e >= str(earn_dt)]
        if not valid_expiries:
            return None
        expiry = valid_expiries[0]

        chain = ticker.option_chain(expiry)
        spot = float(ticker.fast_info.last_price)
        if spot <= 0:
            return None

        calls = chain.calls
        puts = chain.puts
        if calls.empty or puts.empty:
            return None

        atm_idx = (calls["strike"] - spot).abs().argsort().iloc[0]
        atm_strike = float(calls.iloc[atm_idx]["strike"])

        call_row = calls[calls["strike"] == atm_strike]
        put_row = puts[puts["strike"] == atm_strike]
        if call_row.empty or put_row.empty:
            return None

        call_mid = (float(call_row["bid"].iloc[0]) + float(call_row["ask"].iloc[0])) / 2
        put_mid = (float(put_row["bid"].iloc[0]) + float(put_row["ask"].iloc[0])) / 2

        straddle = call_mid + put_mid
        implied_move = straddle / spot * 0.85
        return implied_move
    except Exception as e:
        logger.debug(f"[EP] implied_move {symbol}: {e}")
        return None


# ── Core scoring ──────────────────────────────────────────────────────────────
def _score_opportunity(symbol: str, earnings_date) -> dict | None:
    """
    Run all 5 signals. Returns scoring dict or None if required gates fail
    or bonus score < 2.
    """
    cached = _signal_cache.get(symbol)
    if cached and (time.time() - cached["ts"]) < SIGNAL_CACHE_TTL:
        return cached.get("result")

    result = {"symbol": symbol, "earnings_date": str(earnings_date), "score": 0, "details": {}}

    # Gate 1: revision momentum
    rev_passes, rev_delta = _get_revision_signal(symbol)
    result["details"]["revision_delta"] = rev_delta
    if not rev_passes:
        logger.debug(f"[EP] {symbol}: FAIL revision gate (delta={rev_delta:.4f})")
        _signal_cache[symbol] = {"result": None, "ts": time.time()}
        return None

    # Gate 2: beat streak
    beat_passes, beat_streak = _get_beat_streak(symbol)
    result["details"]["beat_streak"] = beat_streak
    if not beat_passes:
        logger.debug(f"[EP] {symbol}: FAIL beat streak gate ({beat_streak} < {EP_MIN_BEAT_STREAK})")
        _signal_cache[symbol] = {"result": None, "ts": time.time()}
        return None

    # Gate 3: insider activity
    insider_passes, insider_summary = _get_insider_signal(symbol)
    result["details"]["insider"] = insider_summary
    if not insider_passes:
        logger.debug(f"[EP] {symbol}: FAIL insider gate ({insider_summary})")
        _signal_cache[symbol] = {"result": None, "ts": time.time()}
        return None

    # Bonus scoring
    bonus = 0

    if beat_streak >= 3:
        bonus += 1
        result["details"]["bonus_beat_streak"] = True

    si_passes, si_pct, squeeze_bonus = _get_short_interest(symbol)
    result["details"]["short_interest"] = si_pct
    if not si_passes:
        logger.debug(f"[EP] {symbol}: FAIL short interest gate ({si_pct:.1%})")
        _signal_cache[symbol] = {"result": None, "ts": time.time()}
        return None
    if squeeze_bonus:
        bonus += 1
        result["details"]["bonus_squeeze"] = True

    sentiment, sentiment_reason = _get_transcript_sentiment(symbol)
    result["details"]["transcript_sentiment"] = sentiment
    result["details"]["transcript_reason"] = sentiment_reason
    if sentiment == "bullish":
        bonus += 1
        result["details"]["bonus_transcript"] = True

    try:
        implied_move = _get_implied_move(symbol, earnings_date)
        result["details"]["implied_move"] = implied_move
        if implied_move is not None:
            predicted_move = 0.05 + rev_delta * 2  # rough proxy
            if predicted_move > implied_move:
                bonus += 1
                result["details"]["bonus_edge"] = True
                result["details"]["predicted_move"] = predicted_move
    except Exception:
        pass

    result["score"] = bonus
    result["beat_streak"] = beat_streak
    result["revision_delta"] = rev_delta

    _signal_cache[symbol] = {"result": result if bonus >= 2 else None, "ts": time.time()}

    if bonus < 2:
        logger.debug(f"[EP] {symbol}: score={bonus} < 2, skip")
        return None

    return result


# ── Main run loop ─────────────────────────────────────────────────────────────
def run(broker, db_conn):
    """
    Main strategy loop. Called every cycle from main.py.

    1. Manage existing EP positions (exits / partial profit / gap-down)
    2. Scan watchlist for earnings in EP_DAYS_BEFORE_EARNINGS days
    3. Score and enter qualifying positions
    """
    try:
        from utils.market_hours import is_entry_allowed
        from utils.earnings_calendar import get_next_earnings_date
        from utils import notify
        from db import log_trade, log_signal
        from config import MIN_CASH_RESERVE_PCT
    except Exception as e:
        logger.error(f"[EP] import failed: {e}")
        return

    try:
        if not broker.is_market_open():
            return
    except Exception:
        return

    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return

    # --- 1. Manage existing EP positions ---
    try:
        positions = broker.get_positions()
    except Exception as e:
        logger.warning(f"[EP] get_positions failed: {e}")
        return
    pos_map = {p["symbol"]: p for p in positions}

    for sym, state in list(_ep_positions.items()):
        if sym not in pos_map:
            del _ep_positions[sym]
            continue

        pos = pos_map[sym]
        try:
            current_price = float(pos.get("current_price", 0))
        except Exception:
            current_price = 0.0
        entry_price = state.get("entry_price", 0.0) or 0.0
        gain_pct = (current_price / entry_price - 1) if entry_price > 0 else 0.0
        entry_date = state.get("entry_date")
        days_held = (now.date() - entry_date).days if entry_date else 0

        # Max hold days
        if days_held >= EP_MAX_HOLD_DAYS:
            logger.info(f"[EP] {sym}: max hold {EP_MAX_HOLD_DAYS}d reached, exiting")
            try:
                broker.close_position(sym, STRATEGY_NAME)
                log_trade(db_conn, STRATEGY_NAME, sym, "sell_max_hold",
                          abs(float(pos.get("qty", 0))), current_price,
                          float(pos.get("unrealized_pnl", 0) or 0))
                # [ntfy silenced — logged only]
            except Exception as e:
                logger.warning(f"[EP] {sym} max_hold exit failed: {e}")
            del _ep_positions[sym]
            continue

        # Stop loss
        if gain_pct <= -EP_STOP_LOSS_PCT:
            logger.info(f"[EP] {sym}: stop loss hit ({gain_pct:.1%}), exiting")
            try:
                broker.close_position(sym, STRATEGY_NAME)
                log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop_loss",
                          abs(float(pos.get("qty", 0))), current_price,
                          float(pos.get("unrealized_pnl", 0) or 0))
                # [ntfy silenced — logged only]
            except Exception as e:
                logger.warning(f"[EP] {sym} stop_loss exit failed: {e}")
            del _ep_positions[sym]
            continue

        # Post-earnings action (day after earnings)
        earn_date = state.get("earnings_date")
        if earn_date and not state.get("post_earnings_checked"):
            days_since_earnings = (now.date() - earn_date).days
            if days_since_earnings >= 1:
                state["post_earnings_checked"] = True
                if gain_pct >= EP_POST_EARNINGS_TAKE_PCT:
                    try:
                        qty = abs(float(pos.get("qty", 0)))
                        half_qty = int(qty / 2)
                        if half_qty >= 1:
                            broker.submit_order(symbol=sym, qty=half_qty, side="sell",
                                                type="market", time_in_force="day")
                            log_trade(db_conn, STRATEGY_NAME, sym, "sell_partial_earnings",
                                      half_qty, current_price, 0.0)
                            # [ntfy silenced — logged only]
                    except Exception as e:
                        logger.warning(f"[EP] {sym} partial profit failed: {e}")
                elif gain_pct <= -EP_POST_EARNINGS_STOP_PCT:
                    try:
                        broker.close_position(sym, STRATEGY_NAME)
                        log_trade(db_conn, STRATEGY_NAME, sym, "sell_post_earnings_gap",
                                  abs(float(pos.get("qty", 0))), current_price,
                                  float(pos.get("unrealized_pnl", 0) or 0))
                        # [ntfy silenced — logged only]
                    except Exception as e:
                        logger.warning(f"[EP] {sym} gap-down exit failed: {e}")
                    del _ep_positions[sym]
                    continue

    # --- 2. Find new opportunities ---
    if not is_entry_allowed():
        return

    if len(_ep_positions) >= EP_MAX_CONCURRENT:
        return

    today = now.date()

    for symbol in EP_WATCHLIST:
        if symbol in _ep_positions:
            continue
        if symbol in pos_map and pos_map[symbol].get("strategy") == STRATEGY_NAME:
            continue

        try:
            earn_date = get_next_earnings_date(symbol)
            if earn_date is None:
                continue

            days_until = (earn_date - today).days
            # Allow any day inside the [0, EP_DAYS_BEFORE_EARNINGS] window so we
            # don't permanently lose an entry when the bot was down on the exact
            # day-before-earnings tick.
            if not (0 <= days_until <= EP_DAYS_BEFORE_EARNINGS):
                continue

            logger.info(f"[EP] {symbol}: earnings in {days_until}d — scoring...")
            scored = _score_opportunity(symbol, earn_date)
            if scored is None:
                continue

            score = scored["score"]
            alloc_pct = EP_SCORE_ALLOC.get(score, EP_SCORE_ALLOC[4])

            acc = broker.get_account()
            portfolio_value = float(acc["portfolio_value"])
            cash = float(acc["cash"])
            notional = portfolio_value * alloc_pct

            if cash - notional < portfolio_value * MIN_CASH_RESERVE_PCT:
                try:
                    from utils.capital_rotator import find_rotation_candidate, execute_rotation
                    candidate = find_rotation_candidate(
                        new_symbol=symbol, new_score=score / 4.0,
                        new_notional=notional,
                        current_positions=positions,
                        broker=broker, db_conn=db_conn,
                    )
                    if candidate:
                        execute_rotation(candidate, symbol, notional, score / 4.0,
                                         broker, db_conn, STRATEGY_NAME)
                        cash, _ = broker.get_live_cash()
                        if cash - notional < portfolio_value * MIN_CASH_RESERVE_PCT:
                            continue
                    else:
                        continue
                except Exception as e:
                    logger.warning(f"[EP] {symbol} rotation failed: {e}")
                    continue

            try:
                current_price = float(yf.Ticker(symbol).fast_info.last_price)
            except Exception as e:
                logger.warning(f"[EP] {symbol} price fetch failed: {e}")
                continue
            if current_price <= 0:
                continue
            qty = int(notional / current_price)
            if qty < 1:
                continue

            logger.info(
                f"[EP] ENTER {symbol} score={score}/4 alloc={alloc_pct:.1%} "
                f"beat_streak={scored['beat_streak']} rev_delta={scored['revision_delta']:+.4f} "
                f"details={scored['details']}"
            )

            broker.submit_order(symbol=symbol, qty=qty, side="buy",
                                type="market", time_in_force="day")

            _ep_positions[symbol] = {
                "entry_date": today,
                "entry_price": current_price,
                "earnings_date": earn_date,
                "score": score,
                "post_earnings_checked": False,
            }

            log_trade(db_conn, STRATEGY_NAME, symbol, "buy",
                      qty, current_price, 0.0)
            log_signal(db_conn, STRATEGY_NAME, symbol, f"score_{score}",
                       score / 4.0, scored["details"])

            # [ntfy silenced — logged only]

            cash, portfolio_value = broker.get_live_cash()
            if cash < 0:
                logger.critical(f"[{STRATEGY_NAME}] Cash went negative (${cash:,.0f}) — halting entries")
                from utils.notify import send as _notify, emergency as _notify_emergency
                _notify_emergency("🚨 Cash went negative", f"[earnings_prediction] cash ${cash:,.0f} — halting entries", key="negative_cash_earnings_prediction")
                break
            if cash < portfolio_value * MIN_CASH_RESERVE_PCT:
                logger.warning(f"[{STRATEGY_NAME}] Cash floor hit (${cash:,.0f}) — halting entries")
                break

            if len(_ep_positions) >= EP_MAX_CONCURRENT:
                break

        except Exception as e:
            logger.warning(f"[EP] {symbol} run() error: {e}")
