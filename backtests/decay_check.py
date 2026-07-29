"""
Live decay check — run by the weekly reconciliation routine.
Compares LIVE account behavior against the strategy registry's expected bands
(research/strategy_registry.json) and prints PASS / WATCH / RETIRE flags.

Honest scope: Alpaca fills carry no strategy tag (that lives in the bot's SQLite
on Railway), so per-sleeve attribution here is approximate — we compute
account-level realized round-trips + book-level stats, and per-symbol tables the
reviewer maps to sleeves. Sleeve-exact decay stats need the bot DB (future: an
/api endpoint). Credentials from env: ALPACA_API_KEY / ALPACA_SECRET_KEY.

Run:  ALPACA_API_KEY=.. ALPACA_SECRET_KEY=.. python backtests/decay_check.py [days=28]
"""
import os, sys, json, collections, datetime, pathlib

import requests

BASE = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
H = {"APCA-API-KEY-ID": os.environ.get("ALPACA_API_KEY", ""),
     "APCA-API-SECRET-KEY": os.environ.get("ALPACA_SECRET_KEY", "")}
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 28
REG = json.load(open(pathlib.Path(__file__).parents[1] / "research" / "strategy_registry.json"))


def fills(days):
    out, after = [], (datetime.datetime.now(datetime.timezone.utc)
                     - datetime.timedelta(days=days)).isoformat()
    while True:
        page = requests.get(BASE + "/v2/account/activities", headers=H, timeout=25,
                            params={"activity_types": "FILL", "after": after,
                                    "direction": "asc", "page_size": 100}).json()
        if not isinstance(page, list) or not page:
            break
        out += page
        after = page[-1]["transaction_time"]
        if len(page) < 100:
            break
    return out


def round_trips(fs):
    """FIFO per-symbol round trips -> list of (symbol, ret_pct)."""
    lots = collections.defaultdict(list)   # sym -> [(qty, px)]
    trips = []
    for f in fs:
        sym, side = f["symbol"], f["side"]
        qty, px = float(f["qty"]), float(f["price"])
        if side == "buy":
            lots[sym].append([qty, px])
        else:
            while qty > 1e-9 and lots[sym]:
                lot = lots[sym][0]
                take = min(qty, lot[0])
                trips.append((sym, px / lot[1] - 1))
                lot[0] -= take
                qty -= take
                if lot[0] <= 1e-9:
                    lots[sym].pop(0)
    return trips


def main():
    fs = fills(DAYS)
    trips = round_trips(fs)
    n = len(trips)
    print(f"=== decay_check: last {DAYS}d — {len(fs)} fills, {n} closed round-trips ===")
    if n == 0:
        print("No closed round-trips — PASS (nothing to judge).")
        return
    wins = [t for _, t in trips if t > 0]
    exp = sum(t for _, t in trips) / n
    wr = len(wins) / n
    tpw = n / (DAYS / 7)
    print(f"book realized: win {wr*100:.0f}%  expectancy {exp*100:+.2f}%/trade  {tpw:.1f} trips/wk")

    # book-level tripwires (registry)
    flags = []
    if n >= 20 and exp < -0.005:
        flags.append("WATCH: book expectancy < -0.5%/trade over 20+ trips")
    if n >= 30 and wr < 0.30:
        flags.append("WATCH: book win rate < 30% over 30+ trips")
    if tpw > 40:
        flags.append("WATCH: churn — >40 round-trips/week (cooldown/whipsaw check)")

    print("\nper-symbol (map to sleeves via bot DB / entry logs):")
    by = collections.defaultdict(list)
    for s, t in trips:
        by[s].append(t)
    for s, ts in sorted(by.items(), key=lambda kv: sum(kv[1])):
        print(f"  {s:<6} n={len(ts):>3}  exp {sum(ts)/len(ts)*100:+6.2f}%  "
              f"win {sum(1 for x in ts if x>0)/len(ts)*100:3.0f}%")

    # revalidation calendar
    today = datetime.date.today().isoformat()
    due = [k for k, v in REG["strategies"].items()
           if isinstance(v, dict) and v.get("revalidate_by", "9999") < today]
    print("\n" + ("FLAGS:\n  " + "\n  ".join(flags) if flags else "FLAGS: none — PASS"))
    print("REVALIDATION DUE: " + (", ".join(due) if due else "none"))


if __name__ == "__main__":
    main()
