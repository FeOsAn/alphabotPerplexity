"""
Strategy 4: AI Research — Fundamental Analysis via Claude
----------------------------------------------------------
Architecture:
  1. ResearchAgent  — reads news, filings, earnings for each stock.
                      Builds a thesis with cited sources. Scores bullish/bearish 1-10.
  2. CheckerAgent   — independently re-verifies every factual claim.
                      Triple-checks: source validity, logic consistency, recency.
                      Only issues GO if all 3 checks pass and confidence >= threshold.
  3. Executor       — sizes and places the trade if CheckerAgent approves.

Hold period: 2–8 weeks (swing trading). Checks existing positions daily for thesis breaks.
Max 5 concurrent AI-research positions.
"""

import os
import json
import logging
import time
import requests
from datetime import datetime, timedelta
from typing import Optional
import yfinance as yf
import anthropic

from broker import AlpacaBroker, tag_symbol
from config import MAX_POSITION_PCT, MIN_CASH_RESERVE_PCT
from db import log_trade, log_signal

logger = logging.getLogger("alphabot.ai_research")
STRATEGY_NAME = "ai_research"
MAX_AI_POSITIONS = 5
MIN_CONFIDENCE = 7          # Research agent must score >= 7/10
MIN_CHECKER_SCORE = 7       # Checker agent must also score >= 7/10
STOP_LOSS_PCT = 0.08        # 8% stop — slightly wider for fundamental holds
TAKE_PROFIT_PCT = 0.25      # 25% take profit

# Watchlist — high-profile stocks with lots of news coverage for research quality
RESEARCH_WATCHLIST = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    "NFLX", "AMD", "CRM", "PLTR", "SHOP", "UBER", "ABNB",
    "COIN", "SNOW", "NET", "DDOG", "MDB", "SMCI",
]

# ─────────────────────────────────────────────────────────────────────────────
# Data fetchers
# ─────────────────────────────────────────────────────────────────────────────

def _get_stock_context(symbol: str) -> dict:
    """Fetch fundamental + price data for a stock."""
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
        hist = ticker.history(period="3mo")
        news = ticker.news or []

        price = float(hist["Close"].iloc[-1]) if not hist.empty else 0
        price_1m_ago = float(hist["Close"].iloc[-22]) if len(hist) >= 22 else price
        price_change_1m = ((price - price_1m_ago) / price_1m_ago * 100) if price_1m_ago else 0

        # Pull recent news headlines
        headlines = []
        for n in news[:8]:
            title = n.get("title", "")
            pub_time = n.get("providerPublishTime", 0)
            pub_date = datetime.fromtimestamp(pub_time).strftime("%Y-%m-%d") if pub_time else "unknown"
            url = n.get("link", "")
            if title:
                headlines.append({"title": title, "date": pub_date, "url": url})

        return {
            "symbol": symbol,
            "company_name": info.get("longName", symbol),
            "sector": info.get("sector", "Unknown"),
            "industry": info.get("industry", "Unknown"),
            "current_price": price,
            "market_cap_b": round(info.get("marketCap", 0) / 1e9, 1),
            "pe_ratio": info.get("trailingPE"),
            "forward_pe": info.get("forwardPE"),
            "revenue_growth": info.get("revenueGrowth"),
            "earnings_growth": info.get("earningsGrowth"),
            "profit_margin": info.get("profitMargins"),
            "debt_to_equity": info.get("debtToEquity"),
            "free_cash_flow_b": round((info.get("freeCashflow") or 0) / 1e9, 2),
            "analyst_target": info.get("targetMeanPrice"),
            "analyst_recommendation": info.get("recommendationKey", "none"),
            "price_change_1m_pct": round(price_change_1m, 2),
            "52w_high": info.get("fiftyTwoWeekHigh"),
            "52w_low": info.get("fiftyTwoWeekLow"),
            "short_float_pct": info.get("shortPercentOfFloat"),
            "recent_news": headlines,
            "business_summary": (info.get("longBusinessSummary") or "")[:800],
        }
    except Exception as e:
        logger.warning(f"Could not fetch context for {symbol}: {e}")
        return {"symbol": symbol, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Research Agent
# ─────────────────────────────────────────────────────────────────────────────

RESEARCH_PROMPT = """You are a quantitative equity analyst. Analyse the following stock data and produce a structured investment thesis.

STOCK DATA:
{context}

Your job:
1. Evaluate the fundamental quality of this business (growth, margins, valuation, balance sheet)
2. Assess recent news sentiment and any material catalysts
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

    prompt = RESEARCH_PROMPT.format(
        context=json.dumps(context, indent=2),
        symbol=symbol
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        result = json.loads(raw)
        logger.info(f"[Research] {symbol}: {result['verdict']} (confidence: {result['confidence']}/10)")
        return result

    except Exception as e:
        logger.error(f"[Research] Failed for {symbol}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Checker Agent — independent triple-verification
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
  Is the most recent news considered?
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
    "verified_facts": <integer count of claims verified>,
    "failed_facts": <integer count of claims that couldn't be verified>
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

    prompt = CHECKER_PROMPT.format(
        context=json.dumps(context, indent=2),
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
            f"Checker confidence: {result['checker_confidence']}/10"
        )
        return result

    except Exception as e:
        logger.error(f"[Checker] Failed for {symbol}: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main strategy runner
# ─────────────────────────────────────────────────────────────────────────────

def run(broker: AlpacaBroker, db_conn):
    """Run AI research strategy: research → check → trade."""

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("[AI Research] ANTHROPIC_API_KEY not set — skipping strategy")
        return

    client = anthropic.Anthropic(api_key=api_key)
    logger.info("=== AI Research Strategy: Starting research cycle ===")

    # ── 1. Exit checks on existing AI positions ──────────────────────────────
    all_positions = broker.get_positions()
    ai_positions = [p for p in all_positions
                    if p.get("strategy") == STRATEGY_NAME
                    and p.get("asset_class", "equity") == "equity"]

    for pos in ai_positions:
        sym = pos["symbol"]
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

    # ── 2. Check capacity ─────────────────────────────────────────────────────
    current_ai_count = len([p for p in broker.get_positions()
                             if p.get("strategy") == STRATEGY_NAME
                             and p.get("asset_class", "equity") == "equity"])
    if current_ai_count >= MAX_AI_POSITIONS:
        logger.info(f"[AI Research] At max positions ({MAX_AI_POSITIONS}) — skipping new research")
        return

    slots_available = MAX_AI_POSITIONS - current_ai_count
    current_symbols = {p["symbol"] for p in broker.get_positions()}

    account = broker.get_account()
    portfolio_value = account["portfolio_value"]
    cash = account["cash"]

    # ── 3. Research loop ──────────────────────────────────────────────────────
    candidates_to_research = [s for s in RESEARCH_WATCHLIST if s not in current_symbols]

    logger.info(
        f"[AI] Candidates to research: {len(candidates_to_research)} symbols | "
        f"Available slots: {slots_available} | "
        f"Full list: {candidates_to_research}"
    )

    approved_trades = []
    n_passed_research = 0
    n_passed_checker = 0

    for symbol in candidates_to_research:
        if len(approved_trades) >= slots_available:
            break

        logger.info(f"\n{'='*50}")
        logger.info(f"[AI Research] Researching {symbol}...")

        # Fetch data
        context = _get_stock_context(symbol)
        if "error" in context:
            logger.info(f"[AI Research] {symbol}: REJECTED — could not fetch stock context ({context.get('error')})")
            continue

        # Step 1: Research Agent
        thesis = research_agent(client, symbol, context)
        if not thesis:
            logger.info(f"[AI Research] {symbol}: REJECTED — Research agent returned no result")
            continue

        # Log what Claude actually returned (truncated)
        raw_thesis_str = json.dumps(thesis)
        logger.info(
            f"[AI Research] {symbol}: Claude response (first 500 chars): "
            f"{raw_thesis_str[:500]}"
        )

        # Skip low-confidence or neutral theses
        if thesis.get("verdict") == "NEUTRAL":
            logger.info(f"[AI Research] {symbol}: REJECTED by research — verdict NEUTRAL")
            continue
        if thesis.get("confidence", 0) < MIN_CONFIDENCE:
            logger.info(
                f"[AI Research] {symbol}: REJECTED by research — "
                f"confidence {thesis.get('confidence')}/10 < threshold {MIN_CONFIDENCE}"
            )
            continue

        n_passed_research += 1
        logger.info(
            f"[AI Research] {symbol}: PASSED research filter — "
            f"verdict={thesis['verdict']}, confidence={thesis.get('confidence')}/10"
        )

        # Step 2: Checker Agent
        check = checker_agent(client, symbol, context, thesis)
        if not check:
            logger.info(f"[AI Research] {symbol}: REJECTED by checker — Checker agent returned no result")
            continue

        # Hard gate: all 3 checks must pass + go signal + min confidence
        checks_passed = sum([
            check["check1_factual"]["passed"],
            check["check2_logic"]["passed"],
            check["check3_recency"]["passed"],
        ])

        if not check["overall_go"]:
            logger.info(
                f"[AI Research] {symbol}: REJECTED by checker — "
                f"overall_go=False | notes: {check.get('checker_notes', '')}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check, "checker_no_go")
            continue

        if checks_passed < 3:
            failed_checks = []
            if not check["check1_factual"]["passed"]:
                failed_checks.append(f"check1_factual: {check['check1_factual'].get('issues', [])}")
            if not check["check2_logic"]["passed"]:
                failed_checks.append(f"check2_logic: {check['check2_logic'].get('issues', [])}")
            if not check["check3_recency"]["passed"]:
                failed_checks.append(f"check3_recency: {check['check3_recency'].get('issues', [])}")
            logger.info(
                f"[AI Research] {symbol}: REJECTED by checker — "
                f"only {checks_passed}/3 checks passed | failed: {failed_checks}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check, f"only_{checks_passed}_of_3_checks_passed")
            continue

        if check["checker_confidence"] < MIN_CHECKER_SCORE:
            logger.info(
                f"[AI Research] {symbol}: REJECTED by checker — "
                f"checker confidence {check['checker_confidence']}/10 < threshold {MIN_CHECKER_SCORE}"
            )
            _log_blocked_trade(db_conn, symbol, thesis, check, "low_checker_confidence")
            continue

        # If checker amended the verdict, use the amendment
        final_verdict = check.get("amended_verdict") or thesis["verdict"]
        if final_verdict == "NEUTRAL":
            logger.info(f"[AI Research] {symbol}: REJECTED — Checker amended verdict to NEUTRAL")
            continue

        n_passed_checker += 1
        logger.info(
            f"[AI Research] ✓ {symbol} APPROVED | "
            f"Verdict: {final_verdict} | "
            f"Research: {thesis['confidence']}/10 | "
            f"Checker: {check['checker_confidence']}/10 | "
            f"All 3 checks: PASSED"
        )

        approved_trades.append({
            "symbol": symbol,
            "verdict": final_verdict,
            "thesis": thesis,
            "check": check,
            "context": context,
        })

        # Small delay between API calls
        time.sleep(2)

    # ── 4. Execute approved trades ────────────────────────────────────────────
    trades_placed = 0
    for trade in approved_trades:
        sym = trade["symbol"]
        verdict = trade["verdict"]
        thesis = trade["thesis"]

        notional = portfolio_value * MAX_POSITION_PCT
        min_cash = portfolio_value * MIN_CASH_RESERVE_PCT

        if cash - notional < min_cash:
            logger.info(f"[AI Research] Insufficient cash for {sym}")
            break

        if verdict == "BULLISH":
            logger.info(f"[AI Research] BUYING {sym} | ${notional:.0f} | {thesis['thesis_summary'][:80]}")
            broker.market_buy(sym, notional, STRATEGY_NAME)
            tag_symbol(sym, STRATEGY_NAME)
            log_trade(db_conn, STRATEGY_NAME, sym, "buy", 0,
                      trade["context"]["current_price"], 0,
                      metadata={
                          "thesis": thesis["thesis_summary"],
                          "verdict": verdict,
                          "research_confidence": thesis["confidence"],
                          "checker_confidence": trade["check"]["checker_confidence"],
                          "checks_passed": 3,
                          "bull_case": thesis["bull_case"],
                          "target_upside_pct": thesis.get("target_upside_pct"),
                          "suggested_hold_weeks": thesis.get("suggested_hold_weeks"),
                      })
            log_signal(db_conn, STRATEGY_NAME, sym, "buy",
                       thesis["confidence"],
                       {"checker_confidence": trade["check"]["checker_confidence"],
                        "verdict": verdict})
            cash -= notional
            trades_placed += 1

        elif verdict == "BEARISH":
            # Short selling — only if account has margin/shorting enabled
            logger.info(f"[AI Research] BEARISH signal for {sym} — skipping short (long-only mode)")

    # ── 5. Cycle summary ─────────────────────────────────────────────────────
    logger.info(
        f"[AI] Candidates considered: {len(candidates_to_research)} | "
        f"Passed research: {n_passed_research} | "
        f"Passed checker: {n_passed_checker} | "
        f"Trades placed: {trades_placed}"
    )

    if trades_placed == 0:
        if len(candidates_to_research) == 0:
            logger.info("[AI] No trade fired — No symbols passed all filters (watchlist already held)")
        elif n_passed_research == 0:
            logger.info("[AI] No trade fired — Claude returned no actionable picks")
        elif n_passed_checker == 0:
            logger.info("[AI] No trade fired — Checker rejected all candidates")
        else:
            logger.info("[AI] No trade fired — No symbols passed all filters")

    logger.info(f"[AI Research] Cycle complete. {trades_placed} trades placed.")


def _log_blocked_trade(db_conn, symbol: str, thesis: dict, check: dict, reason: str):
    """Log a blocked trade for audit trail."""
    log_signal(db_conn, STRATEGY_NAME, symbol, "blocked",
               thesis.get("confidence", 0),
               {
                   "reason": reason,
                   "research_verdict": thesis.get("verdict"),
                   "research_confidence": thesis.get("confidence"),
                   "checker_go": check.get("overall_go"),
                   "checker_confidence": check.get("checker_confidence"),
                   "checker_notes": check.get("checker_notes"),
                   "check1_passed": check.get("check1_factual", {}).get("passed"),
                   "check2_passed": check.get("check2_logic", {}).get("passed"),
                   "check3_passed": check.get("check3_recency", {}).get("passed"),
               })
