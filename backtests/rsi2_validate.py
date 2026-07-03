"""
Validate the RSI(2) mean-reversion upgrade before re-enabling mean_reversion live.
Entry: RSI(2) < 10 AND close > MA200 (Connors-style oversold pullback in uptrend).
Two exit variants tested + sub-period robustness:
  A. Connors quick exit: close > MA5  (fast snap-back)  OR 5% stop OR 10d cap
  B. shared engine exit (trailing ratchet + TP)  [what deep_analysis used]
Ship the one that's robustly +EV across sub-periods.

Run:  python backtests/rsi2_validate.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E
from scorecard import UNIVERSE

START, END = "2014-06-01", "2026-06-30"


def connors_trades(panel, a=None, b=None):
    trades = []
    for s, df in panel.items():
        if a: df = df.loc[a:b]
        if len(df) < 210: continue
        c, l, o = df["Close"], df["Low"], df["Open"]
        r2 = E.rsi(c, 2); ma200 = E.sma(c, 200); ma5 = E.sma(c, 5)
        i, n = 200, len(df)
        while i < n - 1:
            if r2.iloc[i] < 10 and c.iloc[i] > ma200.iloc[i]:
                entry = o.iloc[i+1]
                if not np.isfinite(entry) or entry <= 0: i += 1; continue
                ex = None
                for k in range(i+1, min(i+11, n)):
                    if l.iloc[k] <= entry*0.95: ex = -0.05; ei=k; break
                    if c.iloc[k] > ma5.iloc[k]: ex = c.iloc[k]/entry-1; ei=k; break
                if ex is None: ei=min(i+10,n-1); ex=c.iloc[ei]/entry-1
                trades.append(ex); i = ei+1
            else: i += 1
    return trades


def rsi2_entry(df):
    c = df["Close"]; return (E.rsi(c,2) < 10) & (c > E.sma(c,200))


def stats(tr):
    st = M.trade_stats(tr)
    return f"N={st['trades']:>4} win {st['win_rate']*100:4.0f}% exp {st['avg_ret']*100:+.2f}% PF {st['profit_factor']:.2f}"


def main():
    panel = D.get_panel(UNIVERSE, START, END)
    print("=== RSI(2)<10 & >MA200 mean-reversion ===\n")
    print("A. Connors quick exit (close>MA5 / 5% stop / 10d):")
    print("   FULL   ", stats(connors_trades(panel)))
    for a,b,lbl in [("2015-01-01","2019-01-01","15-18"),("2019-01-01","2023-01-01","19-22"),
                    ("2023-01-01","2026-06-30","23-26")]:
        print(f"   {lbl}  ", stats(connors_trades(panel, a, b)))
    print("\nB. Shared engine exit (trailing+TP):")
    tr,_ = E.run_event(panel, rsi2_entry, "mean_reversion")
    print("   FULL   ", stats(tr))
    print("\nvs OLD RSI(14) mean_reversion: exp +0.04%/trade PF 1.03 (disabled)")


if __name__ == "__main__":
    main()
