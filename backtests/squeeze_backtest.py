"""
Backtest: squeeze_screener tradeable core (priority roadmap #1)
==============================================================
The live strategy (bot/strategies/squeeze_screener.py) gates on:
  - shortPercentOfFloat >= 0.15   (LIVE-ONLY: yfinance snapshot, no free history)
  - 5-day return >= +3%
  - current volume >= 1.5x 20-day avg
  - RSI(14) in [45, 72]
Exits (trade_management): +12% TP, -5% stop, 10-day timeout.

Two problems this backtest exposes / addresses:
  1. The live UNIVERSE is all mega-caps (short float ~1-3%) so the 15% gate
     NEVER fires in production. Here we point the SAME price/volume/RSI core at
     a curated squeeze-PRONE universe (persistently high-short-interest names)
     to measure whether the tradeable core has edge.
  2. Historical short interest isn't available free, so we cannot replicate the
     SI signal itself — we hold the universe fixed to squeeze-prone names and
     test the momentum/volume/RSI entry + the exit structure.

Entry executes at NEXT day's open (no look-ahead). Intrabar exits use High for
TP and Low for stop; if both hit same day, stop takes priority (conservative).

Run:  python backtests/squeeze_backtest.py
"""
from __future__ import annotations
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M

# Curated squeeze-prone universe: names that have carried persistently high
# short interest / been squeeze candidates across 2015-2026. Missing/short
# histories are dropped automatically by the data layer.
SQUEEZE_UNIVERSE = [
    "TSLA","GME","AMC","CVNA","W","BYND","PLUG","FCEL","SPCE","ROKU","SNAP",
    "RIOT","MARA","ETSY","PTON","UPST","CHWY","DKNG","FUBO","SOFI","RUN","ENPH",
    "CROX","TDOC","PENN","FSLY","CLOV","LYFT","AFRM","U","NET","BIGC","WKHS",
    "OPEN","RIVN","LCID","NKLA","BLNK","SDGR","BILL","APPS","IRBT","GT","M",
]

# core params (from the stub)
PRICE_5D_RET_MIN = 0.03
VOL_RATIO_MIN    = 1.5
RSI_LOW, RSI_HIGH = 45.0, 72.0
TP = 0.12
STOP = 0.05
TIMEOUT = 10
START, END = "2015-01-01", "2026-06-30"


def rsi(closes: pd.Series, n: int = 14) -> pd.Series:
    d = closes.diff()
    g = d.clip(lower=0).rolling(n).mean()
    l = (-d.clip(upper=0)).rolling(n).mean()
    rs = g / l.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def backtest_symbol(df: pd.DataFrame):
    """Return list of per-trade returns for one symbol."""
    if len(df) < 40:
        return []
    c = df["Close"]; v = df["Volume"]; h = df["High"]; lo = df["Low"]; o = df["Open"]
    ret5 = c / c.shift(5) - 1
    volavg = v.rolling(20).mean()
    r = rsi(c)
    trades = []
    i = 25
    n = len(df)
    while i < n - 1:
        vr = v.iloc[i] / volavg.iloc[i] if volavg.iloc[i] > 0 else 0
        cond = (
            ret5.iloc[i] >= PRICE_5D_RET_MIN and
            vr >= VOL_RATIO_MIN and
            RSI_LOW <= r.iloc[i] <= RSI_HIGH
        )
        if not cond or not np.isfinite(ret5.iloc[i]) or not np.isfinite(r.iloc[i]):
            i += 1
            continue
        # enter next day's open
        entry = o.iloc[i + 1]
        if not np.isfinite(entry) or entry <= 0:
            i += 1
            continue
        exit_ret = None
        for k in range(i + 1, min(i + 1 + TIMEOUT, n)):
            if lo.iloc[k] <= entry * (1 - STOP):      # stop first (conservative)
                exit_ret = -STOP; exit_i = k; break
            if h.iloc[k] >= entry * (1 + TP):
                exit_ret = TP; exit_i = k; break
        if exit_ret is None:
            exit_i = min(i + TIMEOUT, n - 1)
            exit_ret = c.iloc[exit_i] / entry - 1
        trades.append(exit_ret)
        i = exit_i + 1   # no overlapping trades per symbol
    return trades


def main():
    print(f"Fetching squeeze-prone universe ({len(SQUEEZE_UNIVERSE)} names)...")
    panel = D.get_panel(SQUEEZE_UNIVERSE, START, END)
    print(f"Usable symbols: {len(panel)}\n")

    all_trades = []
    per_sym = {}
    for s, df in panel.items():
        t = backtest_symbol(df)
        if t:
            per_sym[s] = t
            all_trades += t

    ts = M.trade_stats(all_trades)
    print("=== SQUEEZE CORE (momentum+vol+RSI, +12%/-5%/10d) — squeeze-prone universe ===")
    print(M.fmt(ts))
    exp = ts["win_rate"] * ts["avg_win"] + (1 - ts["win_rate"]) * ts["avg_loss"]
    print(f"Expectancy/trade: {exp*100:+.3f}%")

    # crude equity curve: sequential equal-size bets (compounded)
    eq = np.cumprod([1 + r for r in all_trades]) if all_trades else np.array([1.0])
    print(f"Sequential compounded (equal-size, {len(all_trades)} trades): "
          f"{(eq[-1]-1)*100:+.1f}% total")

    print("\nPer-symbol (top by trade count):")
    rows = sorted(per_sym.items(), key=lambda kv: len(kv[1]), reverse=True)[:12]
    print(f"{'Sym':<6}{'N':>4}{'Win%':>7}{'Exp%':>8}")
    for s, t in rows:
        st = M.trade_stats(t)
        e = st["win_rate"]*st["avg_win"] + (1-st["win_rate"])*st["avg_loss"]
        print(f"{s:<6}{st['trades']:>4}{st['win_rate']*100:>6.0f}%{e*100:>7.2f}%")

    # Benchmark: does the SAME core have edge on the live mega-cap universe?
    print("\n--- Control: same core on the LIVE mega-cap universe ---")
    from importlib import import_module
    live_uni = ["AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","JPM","V",
                "XOM","AVGO","MA","HD","AMD","NFLX","CRM","ADBE","COST"]
    lp = D.get_panel(live_uni, START, END)
    lt = []
    for s, df in lp.items():
        lt += backtest_symbol(df)
    lts = M.trade_stats(lt)
    le = lts["win_rate"]*lts["avg_win"] + (1-lts["win_rate"])*lts["avg_loss"]
    print(M.fmt(lts))
    print(f"Expectancy/trade: {le*100:+.3f}%")


if __name__ == "__main__":
    main()
