"""
Strategy 4: AI Research — Fundamental Analysis via Claude
----------------------------------------------------------
Architecture:
  1. FreshnessGate    — hard blocks any symbol with ZERO news in the last 48h.
                        No news = no signal. Does NOT penalise older-but-relevant news.
  2. ResearchAgent    — reads news, filings, earnings. Builds thesis with cited
                        sources. Scores bullish/bearish 1-10. Age alone does NOT
                        reduce confidence — relevance does.
  3. CheckerAgent     — independently triple-verifies every factual claim.
                        Checks source validity, logic consistency, recency.
  4. RecencyAuditor   — checks whether the stock has ALREADY MOVED on the news.
                        Compares price action since each headline dropped.
                        Blocks if the market has already reacted (priced in),
                        NOT simply because the news is a few hours old.
  5. Executor         — sizes and places the trade only if ALL 4 gates pass.

Hold period: 2–8 weeks (swing trading).
Max 5 concurrent AI-research positions.

EXIT CHECKS run every cycle (no time gate) — stops / take profits always enforced.
NEW RESEARCH fires once daily anytime between 9:45 AM and 3:30 PM ET.
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
import yfinance as yf
import anthropic

from broker import AlpacaBroker, tag_symbol
from config import MAX_POSITION_PCT, MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.ai_research")
STRATEGY_NAME   = "ai_research"
MAX_AI_POSITIONS    = 5
MIN_CONFIDENCE      = 7     # Research agent must score >= 7/10
MIN_CHECKER_SCORE   = 7     # Checker agent must also score >= 7/10
STOP_LOSS_PCT       = 0.08  # 8% stop — slightly wider for fundamental holds
TAKE_PROFIT_PCT     = 0.25  # 25% take profit
NEWS_MAX_AGE_HOURS  = 48    # Reject symbol if it has NO news at all in last 48h (no data = no trade)
                            # Older news that is still relevant is NOT penalised by age alone
PRICED_IN_MOVE_PCT  = 3.0   # If stock moved >3% in thesis direction since news → likely priced in

# Watchlist — high-profile stocks with lots of news coverage
RESEARCH_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "NFLX", "AMD", "CRM", "PLTR", "SHOP", "UBER", "ABNB",
    "COIN", "SNOW", "NET", "DDOG", "MDB", "SMCI",
]


# ─────────────────────────────────────────────────────────────────────────────
# Gate 1 — News Freshness (hard block before any API call)
# ─────────────────────────────────────────────────────────────────────────────

def _check_news_freshness(symbol: str, news: list) -> dict:
    """
    Returns:
      {
        "fresh": bool,           # False = block this symbol entirely
        "newest_age_hours": float,
        "newest_headline": str,
        "oldest_in_context_hours": float,
        "reason": str
      }
    """
    now_utc = datetime.now(timezone.utc)

    if not news:
        return {
            "fresh": False,
            "newest_age_hours": 9999,
            "newest_headline": "",
            "oldest_in_context_hours": 9999,
            "reason": "no news available — cannot assess freshness",
        }

    ages = []
    for n in news:
        pub_time = n.get("providerPublishTime", 0)
        if pub_time:
            age_hours = (now_utc - datetime.fromtimestamp(pub_time, tz=timezone.utc)).total_seconds() / 3600
            ages.append((age_hours, n.get("title", "")))

    if not ages:
        return {
            "fresh": False,
            "newest_age_hours": 9999,
            "newest_headline": "",
            "oldest_in_context_hours": 9999,
            "reason": "no timestamped news — cannot assess freshness",
        }

    ages.sort(key=lambda x: x[0])  # ascending = freshest first
    newest_age, newest_headline = ages[0]
    oldest_age = ages[-1][0]

    fresh = newest_age <= NEWS_MAX_AGE_HOURS
    reason = (
        f"newest article is {newest_age:.1f}h old (limit: {NEWS_MAX_AGE_HOURS}h)"
        if not fresh
        else f"newest article is {newest_age:.1f}h old — FRESH"
    )

    return {
        "fresh": fresh,
        "newest_age_hours": round(newest_age, 1),
        "newest_headline": newest_headline,
        "oldest_in_context_hours": round(oldest_age, 1),
        "reason": reason,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Data fetcher
# ─────────────────────────────────────────────────────────────────────────────

def _get_stock_context(symbol: str) -> dict:
    """Fetch fundamental + price data for a stock. Includes freshness metadata."""
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.info or {}
        hist   = ticker.history(period="3mo")
        news   = ticker.news or []

        price        = float(hist["Close"].iloc[-1]) if not hist.empty else 0
        price_1m_ago = float(hist["Close"].iloc[-22]) if len(hist) >= 22 else price
        price_change_1m = ((price - price_1m_ago) / price_1m_ago * 100) if price_1m_ago else 0

        now_utc = datetime.now(timezone.utc)

        # Build intraday price lookup: {date_str -> close} for the last 5 days
        # Used to measure how much the stock moved AFTER each news article dropped
        price_by_date = {}
        if not hist.empty:
            for ts, row in hist.tail(10).iterrows():
                try:
                    d = ts.date() if hasattr(ts, 'date') else ts
                    price_by_date[str(d)] = round(float(row["Close"]), 2)
                except Exception:
                    pass

        current_price_now = float(hist["Close"].iloc[-1]) if not hist.empty else 0

        headlines = []
        for n in news[:10]:
            title    = n.get("title", "")
            pub_time = n.get("providerPublishTime", 0)
            if pub_time:
                pub_dt   = datetime.fromtimestamp(pub_time, tz=timezone.utc)
                age_h    = (now_utc - pub_dt).total_seconds() / 3600
                pub_date = pub_dt.strftime("%Y-%m-%d %H:%M UTC")
                pub_date_str = str(pub_dt.date())
                # Price on the day the article dropped vs now
                price_at_news = price_by_date.get(pub_date_str)
                if price_at_news and current_price_now:
                    pct_move_since = round(
                        (current_price_now - price_at_news) / price_at_news * 100, 2
                    )
                else:
                    pct_move_since = None
            else:
                age_h          = 9999
                pub_date       = "unknown"
                pct_move_since = None
            url = n.get("link", "")
            if title:
                headlines.append({
                    "title":              title,
                    "date":               pub_date,
                    "age_hours":          round(age_h, 1),
                    "price_move_since_pct": pct_move_since,  # how much stock moved since this article
                    "url":                url,
                })

        # Sort so the freshest news is first — Claude sees it immediately
        headlines.sort(key=lambda x: x["age_hours"])

        return {
            "symbol":           symbol,
            "company_name":     info.get("longName", symbol),
            "sector":           info.get("sector", "Unknown"),
            "industry":         info.get("industry", "Unknown"),
            "current_price":    price,
            "market_cap_b":     round(info.get("marketCap", 0) / 1e9, 1),
            "pe_ratio":         info.get("trailingPE"),
            "forward_pe":       info.get("forwardPE"),
            "revenue_growth":   info.get("revenueGrowth"),
            "earnings_growth":  info.get("earningsGrowth"),
            "profit_margin":    info.get("profitMargins"),
            "debt_to_equity":   info.get("debtToEquity"),
            "free_cash_flow_b": round((info.get("freeCashflow") or 0) / 1e9, 2),
            "analyst_target":   info.get("targetMeanPrice"),
            "analyst_recommendation": info.get("recommendationKey", "none"),
            "price_change_1m_pct": round(price_change_1m, 2),
            "52w_high":         info.get("fiftyTwoWeekHigh"),
            "52w_low":          info.get("fiftyTwoWeekLow"),
            "short_float_pct":  info.get("shortPercentOfFloat"),
            "recent_news":      headlines,
            "business_summary": (info.get("longBusinessSummary") or "")[:800],
            "_news_raw":        news,  # kept for freshness check, not sent to Claude
        }
    except Exception as e:
        logger.warning(f"Could not fetch context for {symbol}: {e}")
        return {"symbol": symbol, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Gate 2 — Research Agent
# ─────────────────────────────────────────────────────────────────────────────

RESEARCH_PROMPT = """You are a quantitative equity analyst. Analyse the following stock data and produce a structured investment thesis.

STOCK DATA:
{context}

NOTE: Each news headline includes an "age_hours" field showing how old it is.
- Age alone does NOT make news irrelevant. A 36-hour-old earnings beat or FDA approval
  may still be a valid catalyst if the stock hasn't moved much since.
- What matters is whether the information is STILL UNPRICED. Use the price action data
  (price_since_news fields) alongside the headlines to judge this.
- Only cite news that is directly relevant to the investment decision.
- Do NOT fabricate catalysts or treat past events as upcoming ones.

Your job:
1. Evaluate the fundamental quality of this business (growth, margins, valuation, balance sheet)
2. Assess recent news sentiment — note the age of each article explicitly
3. Compare current price vs analyst targets and 52-week range
4. Identify key bull and bear risks
5. Give an overall directional verdict: BULLISH, BEARISH, or NEUTRAL

STRICT RULES:
- Every factual claim must be directly supported by the data provided above
- Do NOT invent numbers, events, or quotes
- If data is missing or ambiguous, say so explicitly
- Be specific — vague generalities are worthless

Respond in this EXACT JSON format (no other text):
{{
  "symbol": "{symbol}",
  "company_name": "...",
  "verdict": "BULLISH" | "BEARISH" | "NEUTRAL",
  "confidence": <integer 1-10>,
  "news_freshness_note": "<comment on whether the news is still unpriced, referencing the stock's price action since the news dropped>",
  "thesis_summary": "<2-3 sentence core thesis>",
  "bull_case": ["<point 1>", "<point 2>", "<point 3>"],
  "bear_case": ["<point 1>", "<point 2>"],
  "key_catalysts": ["<upcoming catalyst>"],
  "cited_facts": [
    {{"claim": "<factual claim>", "source_field": "<which data field supports this>", "value": "<exact value from data>"}}
  ],
  "suggested_hold_weeks": <integer 2-8>,
  "stop_loss_pct": <number, e.g. 8>,
  "target_upside_pct": <number, e.g. 15>
}}"""


def research_agent(client: anthropic.Anthropic, symbol: str, context: dict) -> Optional[dict]:
    """Research Agent: builds investment thesis with cited sources."""
    logger.info(f"[Research] Analysing {symbol}...")

    # Strip _news_raw before sending to Claude (it's only for our freshness check)
    ctx_for_claude = {k: v for k, v in context.items() if k != "_news_raw"}

    prompt = RESEARCH_PROMPT.format(
        context=json.dumps(ctx_for_claude, indent=2),
        symbol=symbol
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        logger.info(
            f"[Research] {symbol}: {result['verdict']} "
            f"(confidence: {result['confidence']}/10) | "
            f"freshness: {result.get('news_freshness_note', 'N/A')[:80]}"
        )
        return result

    except Exception as e:
        logger.error(f"[Research] Failed for {symbol}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Gate 3 — Checker Agent
# ─────────────────────────────────────────────────────────────────────────────

CHECKER_PROMPT = """You are an independent investment risk officer. Your ONLY job is to verify the research below and catch errors, hallucinations, or bad logic before real money is deployed.

ORIGINAL STOCK DATA (ground truth):
{context}

RESEARCH ANALYST'S THESIS TO VERIFY:
{thesis}

Run THREE independent checks:

CHECK 1 — FACTUAL ACCURACY
  For each "cited_fact" in the thesis, verify it matches the ground truth data exactly.
  Flag any number that is wrong, missing from the data, or cannot be verified.

CHECK 2 — LOGICAL CONSISTENCY
  Does the verdict (BULLISH/BEARISH/NEUTRAL) actually follow from the cited facts?
  Are the bull/bear points internally consistent with each other?
  Is the confidence score justified given the data quality?

CHECK 3 — RECENCY & COMPLETENESS
  Check the age_hours field on each news headline.
  Is the thesis based on genuinely recent news (< 24h)?
  Are there obvious risks NOT mentioned in the thesis?
  Is any critical data missing that would change the verdict?

STRICT RULES:
- You must find at least one issue if any exists — do not rubber-stamp blindly
- If data is genuinely solid and thesis is sound, say so clearly
- Your job is to PROTECT capital, not to be agreeable

Respond in this EXACT JSON format (no other text):
{{
  "symbol": "{symbol}",
  "check1_factual": {{
    "passed": true | false,
    "issues": ["<issue 1 if any>"],
    "verified_facts": <integer>,
    "failed_facts": <integer>
  }},
  "check2_logic": {{
    "passed": true | false,
    "issues": ["<issue 1 if any>"],
    "verdict_justified": true | false
  }},
  "check3_recency": {{
    "passed": true | false,
    "issues": ["<issue 1 if any>"],
    "missing_risks": ["<risk not mentioned>"]
  }},
  "overall_go": true | false,
  "checker_confidence": <integer 1-10>,
  "checker_notes": "<1-2 sentence summary of decision>",
  "amended_verdict": "BULLISH" | "BEARISH" | "NEUTRAL" | null
}}"""


def checker_agent(client: anthropic.Anthropic, symbol: str, context: dict, thesis: dict) -> Optional[dict]:
    """Checker Agent: independently triple-verifies the research thesis."""
    logger.info(f"[Checker] Triple-checking thesis for {symbol}...")

    ctx_for_claude = {k: v for k, v in context.items() if k != "_news_raw"}

    prompt = CHECKER_PROMPT.format(
        context=json.dumps(ctx_for_claude, indent=2),
        thesis=json.dumps(thesis, indent=2),
        symbol=symbol
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        checks_passed = sum([
            result["check1_factual"]["passed"],
            result["check2_logic"]["passed"],
            result["check3_recency"]["passed"],
        ])
        logger.info(
            f"[Checker] {symbol}: {'GO ✓' if result['overall_go'] else 'NO-GO ✗'} | "
            f"Checks passed: {checks_passed}/3 | "
            f"Confidence: {result['checker_confidence']}/10"
        )
        return result

    except Exception as e:
        logger.error(f"[Checker] Failed for {symbol}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Gate 4 — Recency Auditor (final staleness check before money leaves)
# ─────────────────────────────────────────────────────────────────────────────

RECENCY_AUDITOR_PROMPT = """You are a price-reaction auditor for an algorithmic trading system.
Your SOLE job: determine whether the market has ALREADY PRICED IN the news driving this thesis.
Age of news alone is NOT your metric. What matters is the stock's price action after the news dropped.

CURRENT TIME (UTC): {now_utc}

NEWS HEADLINES (with age and price move SINCE each article was published):
{headlines_json}

"price_move_since_pct" = how much the stock moved from that day's close to today's price.
A positive number means the stock is UP since then. Negative = DOWN.

INVESTMENT THESIS SUMMARY:
{thesis_summary}

KEY CATALYSTS CITED (these should be UPCOMING events, not past ones):
{catalysts}

CURRENT STOCK PRICE vs RECENT CLOSE:
{price_context}

Answer these three questions:

1. PRICED IN?
   Look at the most relevant headline(s) for this thesis.
   If the stock has already moved >3% in the direction of the thesis since that news dropped,
   the market has reacted — mark already_priced_in=true.
   If the stock moved <3% or moved the WRONG direction despite bullish news, it may still be actionable.

2. FALSE CATALYST?
   Are the "key_catalysts" listed actually UPCOMING events (earnings next week, product launch tomorrow)?
   Or are they PAST events being dressed up as future catalysts ("beat earnings last quarter")?
   Past events as catalysts = false_catalyst_detected=true.

3. NO RELEVANT NEWS?
   If none of the headlines are actually relevant to the thesis (e.g. all generic sector news),
   mark no_relevant_news=true.

RULES:
- A 36-hour-old earnings beat with only a 1% stock move = NOT priced in. PASS it.
- A 6-hour-old earnings beat with a 7% gap up = priced in. BLOCK it.
- "FDA approved X yesterday, stock up 12%" = priced in. BLOCK it.
- "FDA approval expected next week" = real upcoming catalyst. PASS it.
- When in doubt on pricing-in, check the price_move_since_pct. The data beats intuition.

Respond in this EXACT JSON format (no other text):
{{
  "symbol": "{symbol}",
  "most_relevant_headline": "<title of the headline most relevant to the thesis>",
  "price_move_since_relevant_news_pct": <float or null>,
  "already_priced_in": true | false,
  "priced_in_reasoning": "<1 sentence: what price move happened and why it is/isn't priced in>",
  "false_catalyst_detected": true | false,
  "false_catalyst_detail": "<explain if a past event is being treated as future catalyst, else empty string>",
  "no_relevant_news": true | false,
  "block_trade": true | false,
  "auditor_notes": "<1-2 sentence summary: what the auditor found and final decision>"
}}

block_trade must be true if ANY of: already_priced_in=true, false_catalyst_detected=true, no_relevant_news=true."""


def recency_auditor(client: anthropic.Anthropic, symbol: str, context: dict, thesis: dict) -> Optional[dict]:
    """
    Gate 4 — Recency Auditor.
    Checks whether the market has already priced in the news.
    Uses actual price movement since each headline to make the call — not news age alone.
    Blocks if already_priced_in, false_catalyst, or no relevant news.
    Fails safe: any error = block the trade.
    """
    logger.info(f"[RecencyAudit] Price-reaction check for {symbol}...")

    now_utc        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    headlines      = context.get("recent_news", [])
    headlines_json = json.dumps(headlines, indent=2)
    thesis_summary = thesis.get("thesis_summary", "")
    catalysts      = json.dumps(thesis.get("key_catalysts", []))

    # Summarise recent price action so auditor has concrete numbers
    current_price  = context.get("current_price", 0)
    price_1m_pct   = context.get("price_change_1m_pct", 0)
    price_context  = (
        f"Current price: ${current_price:.2f} | "
        f"1-month change: {price_1m_pct:+.1f}% | "
        f"(each headline's price_move_since_pct shows move from that day's close to today)"
    )

    prompt = RECENCY_AUDITOR_PROMPT.format(
        now_utc=now_utc,
        headlines_json=headlines_json,
        thesis_summary=thesis_summary,
        catalysts=catalysts,
        price_context=price_context,
        symbol=symbol,
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        status = "BLOCK" if result["block_trade"] else "PASS"
        logger.info(
            f"[RecencyAudit] {symbol}: {status} | "
            f"priced_in={result['already_priced_in']} | "
            f"move_since_news={result.get('price_move_since_relevant_news_pct')}% | "
            f"false_catalyst={result['false_catalyst_detected']} | "
            f"no_relevant_news={result.get('no_relevant_news')} | "
            f"reasoning: {result.get('priced_in_reasoning', '')[:80]}"
        )
        return result

    except Exception as e:
        logger.error(f"[RecencyAudit] Failed for {symbol}: {e}")
        # Fail safe — if auditor errors, block the trade
        return {
            "symbol":                symbol,
            "already_priced_in":    True,
            "false_catalyst_detected": False,
            "no_relevant_news":     False,
            "block_trade":          True,
            "auditor_notes":        f"Auditor errored ({e}) — blocking trade as fail-safe",
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main strategy runner
# ─────────────────────────────────────────────────────────────────────────────

def run(broker: AlpacaBroker, db_conn):
    """
    Run AI research strategy.
    ALWAYS: exit checks on existing AI positions (stop loss / take profit).
    ONCE DAILY (9:45 AM – 3:30 PM ET): new research cycle with 4-gate approval.
    """
    import pytz
    EASTERN = pytz.timezone("America/New_York")
    now_et  = datetime.now(EASTERN)

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("[AI Research] ANTHROPIC_API_KEY not set — skipping strategy")
        return

    client = anthropic.Anthropic(api_key=api_key)

    # ── 1. Exit checks — ALWAYS run, no time gate ─────────────────────────────
    all_positions = broker.get_positions()
    ai_positions  = [p for p in all_positions
                     if p.get("strategy") == STRATEGY_NAME
                     and p.get("asset_class", "equity") == "equity"]

    for pos in ai_positions:
        sym     = pos["symbol"]
        pnl_pct = pos["unrealized_pnl_pct"]

        if pnl_pct <= -STOP_LOSS_PCT * 100:
            logger.info(f"[AI Research] STOP LOSS {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_stop",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])

        elif pnl_pct >= TAKE_PROFIT_PCT * 100:
            logger.info(f"[AI Research] TAKE PROFIT {sym} @ {pnl_pct:.1f}%")
            broker.close_position(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "sell_tp",
                      pos["qty"], pos["current_price"], pos["unrealized_pnl"])

    # ── 2. New research window: 9:45 AM – 3:30 PM ET, once daily ─────────────
    from datetime import time as dtime
    in_window  = dtime(9, 45) <= now_et.time() <= dtime(15, 30)
    today_str  = now_et.strftime("%Y-%m-%d")
    already_fired = getattr(run, "_fired_date", "") == today_str

    if not in_window:
        logger.info(
            f"[AI Research] Outside research window "
            f"({now_et.strftime('%H:%M ET')}, window=09:45–15:30) — exits only"
        )
        return

    if already_fired:
        logger.info(f"[AI Research] Research already ran today ({today_str}) — exits only")
        return

    logger.info("=== AI Research: Starting 4-gate research cycle ===")

    # ── 3. Capacity check ─────────────────────────────────────────────────────
    current_ai_count = len([p for p in broker.get_positions()
                             if p.get("strategy") == STRATEGY_NAME
                             and p.get("asset_class", "equity") == "equity"])
    if current_ai_count >= MAX_AI_POSITIONS:
        logger.info(f"[AI Research] At max positions ({MAX_AI_POSITIONS}) — skipping new research")
        run._fired_date = today_str
        return

    slots_available  = MAX_AI_POSITIONS - current_ai_count
    current_symbols  = {p["symbol"] for p in broker.get_positions()}
    account          = broker.get_account()
    portfolio_value  = account["portfolio_value"]
    cash             = account["cash"]

    # ── 4. Research loop ──────────────────────────────────────────────────────
    candidates = [s for s in RESEARCH_WATCHLIST if s not in current_symbols]
    logger.info(
        f"[AI] Candidates: {len(candidates)} | Slots: {slots_available} | "
        f"List: {candidates}"
    )

    approved_trades     = []
    n_gate1_blocked     = 0   # freshness hard-block
    n_passed_research   = 0
    n_passed_checker    = 0
    n_passed_auditor    = 0

    for symbol in candidates:
        if len(approved_trades) >= slots_available:
            break

        logger.info(f"\n{'='*50}")
        logger.info(f"[AI Research] Processing {symbol}...")

        # ── Gate 0: VADER sentiment pre-filter ───────────────────────────────
        try:
            from utils.news_sentiment import is_sentiment_bullish
            from config import ALPACA_API_KEY, ALPACA_SECRET_KEY
            if not is_sentiment_bullish(symbol, ALPACA_API_KEY, ALPACA_SECRET_KEY):
                logger.info(f"[AI Research] {symbol}: BLOCKED by sentiment pre-filter")
                continue
        except Exception as e:
            logger.debug(f"[AI Research] {symbol}: sentiment check failed ({e}) — proceeding")

        # ── Fetch context ─────────────────────────────────────────────────────
        context = _get_stock_context(symbol)
        if "error" in context:
            logger.info(f"[AI Research] {symbol}: BLOCKED — could not fetch data: {context.get('error')}")
            continue

        # ── Gate 1: News freshness — hard block before any Claude calls ───────
        freshness = _check_news_freshness(symbol, context.get("_news_raw", []))
        logger.info(f"[AI Research] {symbol}: Freshness → {freshness['reason']}")

        if not freshness["fresh"]:
            logger.info(
                f"[AI Research] {symbol}: BLOCKED by freshness gate — "
                f"newest news is {freshness['newest_age_hours']}h old "
                f"(limit: {NEWS_MAX_AGE_HOURS}h) | '{freshness['newest_headline'][:60]}'"
            )
            n_gate1_blocked += 1
            continue

        logger.info(
            f"[AI Research] {symbol}: Gate 1 PASSED — "
            f"newest: '{freshness['newest_headline'][:60]}' ({freshness['newest_age_hours']}h ago)"
        )

        # ── Gate 2: Research Agent ────────────────────────────────────────────
        thesis = research_agent(client, symbol, context)
        if not thesis:
            logger.info(f"[AI Research] {symbol}: BLOCKED — research agent returned nothing")
            continue

        logger.info(
            f"[AI Research] {symbol}: Research response (first 400 chars): "
            f"{json.dumps(thesis)[:400]}"
        )

        if thesis.get("verdict") == "NEUTRAL":
            logger.info(f"[AI Research] {symbol}: BLOCKED — verdict NEUTRAL")
            continue
        if thesis.get("confidence", 0) < MIN_CONFIDENCE:
            logger.info(
                f"[AI Research] {symbol}: BLOCKED — confidence "
                f"{thesis.get('confidence')}/10 < {MIN_CONFIDENCE} | "
                f"freshness note: {thesis.get('news_freshness_note', '')[:80]}"
            )
            continue

        n_passed_research += 1
        logger.info(
            f"[AI Research] {symbol}: Gate 2 PASSED — "
            f"verdict={thesis['verdict']}, confidence={thesis.get('confidence')}/10"
        )

        # ── Gate 3: Checker Agent ─────────────────────────────────────────────
        check = checker_agent(client, symbol, context, thesis)
        if not check:
            logger.info(f"[AI Research] {symbol}: BLOCKED — checker returned nothing")
            continue

        checks_passed = sum([
            check["check1_factual"]["passed"],
            check["check2_logic"]["passed"],
            check["check3_recency"]["passed"],
        ])

        if not check["overall_go"]:
            logger.info(
                f"[AI Research] {symbol}: BLOCKED by checker — "
                f"overall_go=False | {check.get('checker_notes', '')}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check, "checker_no_go")
            continue

        if checks_passed < 3:
            failed = []
            if not check["check1_factual"]["passed"]:
                failed.append(f"factual: {check['check1_factual'].get('issues', [])}")
            if not check["check2_logic"]["passed"]:
                failed.append(f"logic: {check['check2_logic'].get('issues', [])}")
            if not check["check3_recency"]["passed"]:
                failed.append(f"recency: {check['check3_recency'].get('issues', [])}")
            logger.info(
                f"[AI Research] {symbol}: BLOCKED by checker — "
                f"{checks_passed}/3 checks passed | {failed}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check, f"only_{checks_passed}_of_3_checks_passed")
            continue

        if check["checker_confidence"] < MIN_CHECKER_SCORE:
            logger.info(
                f"[AI Research] {symbol}: BLOCKED by checker — "
                f"checker confidence {check['checker_confidence']}/10 < {MIN_CHECKER_SCORE}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check, "low_checker_confidence")
            continue

        final_verdict = check.get("amended_verdict") or thesis["verdict"]
        if final_verdict == "NEUTRAL":
            logger.info(f"[AI Research] {symbol}: BLOCKED — checker amended verdict to NEUTRAL")
            continue

        n_passed_checker += 1
        logger.info(
            f"[AI Research] {symbol}: Gate 3 PASSED — "
            f"verdict={final_verdict} | research={thesis['confidence']}/10 | "
            f"checker={check['checker_confidence']}/10 | all 3 checks: PASSED"
        )

        # ── Gate 4: Recency Auditor — final staleness check ───────────────────
        audit = recency_auditor(client, symbol, context, thesis)
        if audit is None or audit.get("block_trade", True):
            reason = "auditor_error" if audit is None else (
                "already_priced_in"  if audit.get("already_priced_in") else
                "false_catalyst"     if audit.get("false_catalyst_detected") else
                "no_relevant_news"   if audit.get("no_relevant_news") else
                "block_trade"
            )
            notes = audit.get("auditor_notes", "no details") if audit else "auditor returned None"
            logger.info(
                f"[AI Research] {symbol}: BLOCKED by Recency Auditor — "
                f"reason={reason} | move_since_news={audit.get('price_move_since_relevant_news_pct')}% | {notes}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check or {}, f"recency_auditor_{reason}")
            continue

        n_passed_auditor += 1
        logger.info(
            f"[AI Research] ✓✓✓ {symbol}: ALL 4 GATES PASSED — "
            f"verdict={final_verdict} | "
            f"move_since_news={audit.get('price_move_since_relevant_news_pct')}% | "
            f"priced_in={audit['already_priced_in']} | "
            f"reasoning: {audit.get('priced_in_reasoning', '')[:80]}"
        )

        approved_trades.append({
            "symbol":  symbol,
            "verdict": final_verdict,
            "thesis":  thesis,
            "check":   check,
            "audit":   audit,
            "context": context,
        })

        time.sleep(2)  # brief pause between heavy API cycles

    # ── 5. Execute approved trades ────────────────────────────────────────────
    trades_placed = 0
    for trade in approved_trades:
        sym     = trade["symbol"]
        verdict = trade["verdict"]
        thesis  = trade["thesis"]
        audit   = trade["audit"]

        notional = portfolio_value * MAX_POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        if cash - notional < min_cash:
            logger.info(f"[AI Research] Insufficient cash for {sym}")
            break

        if verdict == "BULLISH":
            logger.info(
                f"[AI Research] BUYING {sym} | ${notional:.0f} | "
                f"{thesis['thesis_summary'][:80]}"
            )
            broker.market_buy(sym, notional, STRATEGY_NAME)
            tag_symbol(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0,
                      trade["context"]["current_price"], 0,
                      metadata={
                          "thesis":               thesis["thesis_summary"],
                          "verdict":              verdict,
                          "research_confidence":  thesis["confidence"],
                          "checker_confidence":   trade["check"]["checker_confidence"],
                          "checks_passed":        3,
                          "bull_case":            thesis["bull_case"],
                          "target_upside_pct":    thesis.get("target_upside_pct"),
                          "suggested_hold_weeks": thesis.get("suggested_hold_weeks"),
                          "news_freshness_note":  thesis.get("news_freshness_note"),
                          "newest_news_age_h":    audit.get("newest_relevant_headline_age_hours"),
                          "auditor_notes":        audit.get("auditor_notes"),
                      })
            log_signal(db_conn, STRATEGY_NAME, sym, "buy",
                       thesis["confidence"],
                       {"checker_confidence": trade["check"]["checker_confidence"],
                        "verdict": verdict,
                        "auditor_pass": True})
            cash -= notional
            trades_placed += 1

        elif verdict == "BEARISH":
            logger.info(f"[AI Research] BEARISH signal for {sym} — skipping short (long-only mode)")

    # ── 6. Mark fired + summary ───────────────────────────────────────────────
    run._fired_date = today_str

    logger.info(
        f"[AI] Candidates: {len(candidates)} | "
        f"Gate1 blocked (stale news): {n_gate1_blocked} | "
        f"Passed research: {n_passed_research} | "
        f"Passed checker: {n_passed_checker} | "
        f"Passed auditor: {n_passed_auditor} | "
        f"Trades placed: {trades_placed}"
    )

    if trades_placed == 0:
        if n_passed_auditor == 0 and n_passed_checker > 0:
            logger.info("[AI] No trade fired — Recency Auditor blocked all candidates (stale/priced-in news)")
        elif n_passed_checker == 0 and n_passed_research > 0:
            logger.info("[AI] No trade fired — Checker rejected all candidates")
        elif n_passed_research == 0:
            logger.info("[AI] No trade fired — No BULLISH/BEARISH signals with confidence >= 7")
        elif n_gate1_blocked == len(candidates):
            logger.info("[AI] No trade fired — ALL candidates blocked by freshness gate (news too old)")
        else:
            logger.info("[AI] No trade fired — No candidates cleared all 4 gates")

    logger.info(f"[AI Research] Cycle complete. {trades_placed} trade(s) placed.")


def _log_blocked_trade(db_conn, symbol: str, thesis: dict, check: dict, reason: str):
    """Log a blocked trade for audit trail."""
    log_signal(db_conn, STRATEGY_NAME, symbol, "blocked",
               thesis.get("confidence", 0),
               {
                   "reason":               reason,
                   "research_verdict":     thesis.get("verdict"),
                   "research_confidence":  thesis.get("confidence"),
                   "checker_go":           check.get("overall_go"),
                   "checker_confidence":   check.get("checker_confidence"),
                   "checker_notes":        check.get("checker_notes"),
                   "check1_passed":        check.get("check1_factual", {}).get("passed"),
                   "check2_passed":        check.get("check2_logic", {}).get("passed"),
                   "check3_passed":        check.get("check3_recency", {}).get("passed"),
               })
