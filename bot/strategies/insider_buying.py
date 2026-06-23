"""
Strategy: Insider Buying — SEC Form 4 EDGAR
--------------------------------------------
Academic edge: Corporate insiders (directors, officers, 10%+ holders) buying
their OWN stock on the open market is one of the most predictive signals in
finance. Documented +6% alpha over 6 months vs matched controls (Seyhun 1998,
Lakonishok & Lee 2001, Cohen, Malloy & Pomorski 2012).

Signal logic:
  1. Parse SEC EDGAR Form 4 filings for transaction code "P" (open market purchase)
     filed within the last 48 hours for stocks in UNIVERSE.
  2. Filter: purchase must be > $50,000 notional (eliminates tiny token buys).
  3. Filter: buyer must be Director, Officer, or 10% holder (not just employee).
  4. Filter: stock must pass regime + RSI filter (no buying into overbought).
  5. Bonus: cluster buying (2+ insiders buying same stock within 5 days) = higher conviction.
  6. Size by conviction: single insider = 3% portfolio, cluster = 5% portfolio.
  7. Exit: 15 trading days OR ATR trailing stop OR -5% hard stop.

Data source: SEC EDGAR submissions API (free, no key needed).
  - CIK lookup: https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK={ticker}&type=4&action=getcompany&output=atom
  - Submissions: https://data.sec.gov/submissions/CIK{cik_padded}.json
  - Form 4 XML: https://www.sec.gov/Archives/edgar/data/{cik}/{accn_nodash}/{filename}.xml

Headers required: User-Agent: "AlphaBot foandrae@icloud.com"
Rate limit: 10 req/sec max. Use time.sleep(0.12) between EDGAR requests.

Max positions: 4 concurrent insider_buying positions.
Hold period: max 15 trading days.
"""

import logging, time, gc, sqlite3
from utils.clock import now_utc as _now_utc
from datetime import datetime, timezone, timedelta
from typing import Optional
import urllib.request, json, xml.etree.ElementTree as ET
import yfinance as yf

from broker import AlpacaBroker
from config import UNIVERSE, MIN_CASH_RESERVE_PCT, DEFAULT_STRATEGY_ALLOCATION_PCT
from db import log_trade, log_signal, get_state, set_state

logger = logging.getLogger("alphabot.insider_buying")
STRATEGY_NAME = "insider_buying"
MAX_POSITIONS = 4
SINGLE_INSIDER_ALLOC = 0.03   # 3% for single insider buy
CLUSTER_ALLOC = 0.05          # 5% for cluster (2+ insiders)
MIN_NOTIONAL = 50_000         # ignore buys < $50k
HOLD_DAYS = 15
COOLDOWN_DAYS = 7

_cooldown: dict[str, datetime] = {}   # symbol -> last entry datetime
_ran_today: str = ""                   # date string, run once per day

EDGAR_HEADERS = {"User-Agent": "AlphaBot foandrae@icloud.com"}


def _get(url: str, retries=2) -> Optional[bytes]:
    """HTTP GET with retries and rate limiting."""
    for attempt in range(retries):
        try:
            time.sleep(0.15)  # EDGAR rate limit: max 10/s
            req = urllib.request.Request(url, headers=EDGAR_HEADERS)
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.read()
        except Exception as e:
            if attempt == retries - 1:
                logger.debug(f"[Insider] GET failed {url}: {e}")
    return None


def _get_cik(ticker: str) -> Optional[str]:
    """Look up CIK for a ticker via EDGAR company search."""
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?company=&CIK={ticker}&type=4&dateb=&owner=include&count=1&search_text=&action=getcompany&output=atom"
    data = _get(url)
    if not data:
        return None
    try:
        text = data.decode("utf-8", errors="ignore")
        import re
        m = re.search(r'/cgi-bin/browse-edgar\?action=getcompany&CIK=(\d+)', text)
        if m:
            return m.group(1).lstrip("0") or m.group(1)
    except Exception:
        pass
    return None


def _get_recent_form4s(cik: str, days_back: int = 2) -> list[dict]:
    """
    Fetch recent Form 4 filings for a CIK.
    Returns list of {accn, filing_date} dicts.
    """
    cik_padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    data = _get(url)
    if not data:
        return []
    try:
        d = json.loads(data)
        filings = d.get("filings", {}).get("recent", {})
        forms = filings.get("form", [])
        dates = filings.get("filingDate", [])
        accns = filings.get("accessionNumber", [])
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
        result = []
        for i, ft in enumerate(forms):
            if ft != "4":
                continue
            try:
                fd = datetime.strptime(dates[i], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if fd >= cutoff:
                result.append({"accn": accns[i], "filing_date": dates[i], "cik": cik})
        return result
    except Exception as e:
        logger.debug(f"[Insider] submissions parse error: {e}")
        return []


def _parse_form4_xml(cik: str, accn: str) -> list[dict]:
    """
    Parse Form 4 XML. Returns list of open-market purchase transactions.
    """
    accn_nodash = accn.replace("-", "")
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accn_nodash}/{accn}-index.htm"
    index_data = _get(index_url)
    if not index_data:
        return []

    import re
    text = index_data.decode("utf-8", errors="ignore")
    m = re.search(r'href="(/Archives/edgar/data/\d+/\d+/[^"]+\.xml)"', text, re.IGNORECASE)
    if not m:
        return []

    xml_url = "https://www.sec.gov" + m.group(1)
    xml_data = _get(xml_url)
    if not xml_data:
        return []

    try:
        root = ET.fromstring(xml_data.decode("utf-8", errors="ignore"))
    except Exception as e:
        logger.debug(f"[Insider] XML parse error: {e}")
        return []

    def gett(tag):
        el = root.find(".//" + tag)
        return el.text.strip() if el is not None and el.text else None

    ticker = gett("issuerTradingSymbol")
    owner_name = gett("rptOwnerName")
    owner_title = gett("officerTitle") or ""
    is_director = root.find(".//isDirector") is not None and root.findtext(".//isDirector") == "1"
    is_officer = root.find(".//isOfficer") is not None and root.findtext(".//isOfficer") == "1"
    is_ten_pct = root.find(".//isTenPercentOwner") is not None and root.findtext(".//isTenPercentOwner") == "1"

    if not (is_director or is_officer or is_ten_pct):
        return []

    results = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        code = txn.findtext(".//transactionCode")
        acq = txn.findtext(".//transactionAcquiredDisposedCode/value")
        if code != "P" or acq != "A":
            continue
        try:
            shares = float(txn.findtext(".//transactionShares/value") or 0)
            price = float(txn.findtext(".//transactionPricePerShare/value") or 0)
            date = txn.findtext(".//transactionDate/value") or ""
            notional = shares * price
            if notional < MIN_NOTIONAL:
                continue
            results.append({
                "ticker": ticker,
                "owner_name": owner_name,
                "owner_title": owner_title or ("Director" if is_director else "Officer"),
                "date": date,
                "shares": shares,
                "price": price,
                "notional": notional,
                "cik": cik,
            })
        except Exception:
            continue

    return results


def _passes_filters(sym: str) -> bool:
    """RSI + regime filter before entering."""
    try:
        from utils.regime import is_bull_market
        if not is_bull_market():
            return False
        tk = yf.Ticker(sym)
        hist = tk.history(period="3mo", interval="1d")
        if hist.empty or len(hist) < 15:
            return False
        closes = hist["Close"].dropna()
        delta = closes.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] != 0 else 100
        rsi = 100 - 100 / (1 + rs)
        if rsi > 75:
            logger.debug(f"[Insider] {sym} RSI={rsi:.1f} — overbought, skip")
            return False
        return True
    except Exception:
        return False


def _scan_universe(db_conn) -> list[dict]:
    """
    Scan UNIVERSE for insider buys in the last 48h.
    """
    cik_cache = {}
    try:
        ts_str = get_state(db_conn, "insider_cik_cache")
        if ts_str:
            cik_cache = json.loads(ts_str)
    except Exception:
        pass

    signals = []

    for sym in UNIVERSE:
        try:
            if sym not in cik_cache:
                cik = _get_cik(sym)
                if cik:
                    cik_cache[sym] = cik
                else:
                    continue
            cik = cik_cache[sym]

            form4s = _get_recent_form4s(cik, days_back=2)
            if not form4s:
                continue

            buys = []
            for filing in form4s:
                txns = _parse_form4_xml(cik, filing["accn"])
                buys.extend(txns)

            if not buys:
                continue

            total_notional = sum(b["notional"] for b in buys)
            insider_count = len(set(b["owner_name"] for b in buys))

            signals.append({
                "sym": sym,
                "notional": total_notional,
                "insider_count": insider_count,
                "buys": buys,
                "is_cluster": insider_count >= 2,
            })
            logger.info(
                f"[Insider] {sym}: {insider_count} insider(s), "
                f"${total_notional:,.0f} total notional"
            )

        except Exception as e:
            logger.debug(f"[Insider] {sym} scan error: {e}")

    try:
        set_state(db_conn, "insider_cik_cache", json.dumps(cik_cache))
    except Exception:
        pass

    signals.sort(key=lambda x: (x["is_cluster"], x["notional"]), reverse=True)
    logger.info(f"[{STRATEGY_NAME}] Scanned {len(UNIVERSE)} symbols — {len(signals)} qualified for entry")
    return signals


def run(broker: AlpacaBroker, db_conn):
    """Main entry point — called once per cycle."""
    global _ran_today

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _ran_today == today:
        return

    from utils.market_hours import is_entry_allowed
    if not is_entry_allowed():
        return

    positions = broker.get_positions()
    existing = [p for p in positions if p.get("strategy") == STRATEGY_NAME]
    if len(existing) >= MAX_POSITIONS:
        logger.info(f"[Insider] Max positions ({MAX_POSITIONS}) reached — skipping scan")
        _ran_today = today
        return

    cash, pv = broker.get_live_cash()
    if cash < 0 or cash / pv < MIN_CASH_RESERVE_PCT:
        logger.warning(f"[Insider] Cash floor hit — skipping")
        _ran_today = today
        return

    logger.info("=== Insider Buying Strategy: Daily Scan ===")

    signals = _scan_universe(db_conn)

    if not signals:
        logger.info("[Insider] No qualifying insider buys found today")
        _ran_today = today
        return

    # Prune stale cooldown entries
    now = _now_utc()
    stale = [s for s, dt in _cooldown.items() if (now - dt).days >= COOLDOWN_DAYS]
    for s in stale:
        del _cooldown[s]

    slots_available = MAX_POSITIONS - len(existing)
    entered = 0

    for sig in signals:
        if entered >= slots_available:
            break

        sym = sig["sym"]

        if sym in _cooldown:
            days_since = (_now_utc() - _cooldown[sym]).days
            if days_since < COOLDOWN_DAYS:
                continue

        if any(p["symbol"] == sym for p in positions):
            continue

        if not _passes_filters(sym):
            continue

        alloc_pct = CLUSTER_ALLOC if sig["is_cluster"] else SINGLE_INSIDER_ALLOC
        notional = pv * alloc_pct

        log_signal(db_conn, STRATEGY_NAME, sym, "insider_buy", alloc_pct, {
            "insider_count": sig["insider_count"],
            "total_notional": sig["notional"],
            "is_cluster": sig["is_cluster"],
            "buyers": [b["owner_name"] for b in sig["buys"]],
        })

        try:
            order = broker.market_buy(sym, notional, strategy=STRATEGY_NAME)
            if order:
                _cooldown[sym] = _now_utc()
                entered += 1

                buyer_names = ", ".join(set(b["owner_name"] for b in sig["buys"]))
                cluster_tag = "CLUSTER" if sig["is_cluster"] else ""
                logger.info(
                    f"[Insider] Entered {sym} {cluster_tag} — "
                    f"{sig['insider_count']} insider(s) bought ${sig['notional']:,.0f} "
                    f"({buyer_names}); ${notional:,.0f} ({alloc_pct:.0%})"
                )

                cash, pv = broker.get_live_cash()
                if cash < 0 or cash / pv < MIN_CASH_RESERVE_PCT:
                    logger.warning(f"[Insider] Cash floor hit after {sym} — halting")
                    break

        except Exception as e:
            logger.error(f"[Insider] Order failed {sym}: {e}")

    _ran_today = today
    logger.info(f"[Insider] Scan complete — {entered} new positions")
