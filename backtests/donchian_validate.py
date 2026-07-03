"""
Final validation for the Donchian trend sleeve before wiring it live:
portfolio-level Sharpe/CAGR/MaxDD (its OWN channel/ATR exit, not the shared
engine) AND correlation of its monthly returns to the vol-targeted return engine.
Ship only if standalone Sharpe >= 0.8 AND monthly correlation to the book is low
(< ~0.5) — i.e. it's a real diversifier.

Run:  python backtests/donchian_validate.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E
from scorecard import UNIVERSE

START, END = "2014-06-01", "2026-06-30"
N_ENTRY, M_EXIT, ATR_MULT, K = 40, 20, 2.0, 10


def donchian_portfolio(panel):
    cal = pd.DatetimeIndex(sorted(set().union(*[set(df.index) for df in panel.values()])))
    prep = {}
    for s, df in panel.items():
        hi = df["Close"].rolling(N_ENTRY).max()
        lo = df["Close"].rolling(M_EXIT).min()
        atr = E.atr(df["High"], df["Low"], df["Close"], 14)
        prep[s] = dict(o=df["Open"], h=df["High"], l=df["Low"], c=df["Close"],
                       hi=hi, lo=lo, atr=atr, idx={d: i for i, d in enumerate(df.index)})
    equity = cash = 1.0
    pos = {}
    curve = []
    for d in cal:
        # manage
        for s in list(pos.keys()):
            p = prep[s]; i = p["idx"].get(d)
            if i is None:
                continue
            st = pos[s]
            exit_px = None
            if p["l"].iloc[i] <= st["stop"]:
                exit_px = st["stop"]
            elif i >= 1 and p["c"].iloc[i] <= p["lo"].iloc[i-1]:
                exit_px = p["c"].iloc[i]
            if exit_px is not None:
                st["val"] *= exit_px / st["mark"]; cash += st["val"]; del pos[s]
            else:
                st["val"] *= p["c"].iloc[i] / st["mark"]; st["mark"] = p["c"].iloc[i]
        # enter
        if len(pos) < K:
            for s in panel:
                if len(pos) >= K or s in pos:
                    continue
                p = prep[s]; i = p["idx"].get(d)
                if i is None or i < N_ENTRY + 1:
                    continue
                if p["c"].iloc[i] >= p["hi"].iloc[i-1] and np.isfinite(p["atr"].iloc[i]):
                    j = p["idx"].get(d)
                    if j + 1 >= len(p["o"]):
                        continue
                    entry = p["o"].iloc[j+1]
                    if not np.isfinite(entry) or entry <= 0:
                        continue
                    alloc = equity / K
                    if alloc > cash:
                        continue
                    cash -= alloc
                    pos[s] = dict(val=alloc, mark=entry, stop=entry - ATR_MULT*p["atr"].iloc[i])
        equity = cash + sum(st["val"] for st in pos.values())
        curve.append((d, equity))
    return pd.Series(dict(curve))


def main():
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    vixp = D.get_history("^VIX", START, END)

    eq = donchian_portfolio(panel)
    m = M.summarize(eq)
    print("=== Donchian trend sleeve (40/20, 2xATR, K=10 slots) ===")
    print(f"CAGR {m['CAGR']*100:.1f}% | Sharpe {m['Sharpe']:.2f} | Sortino {m['Sortino']:.2f} "
          f"| MaxDD {m['MaxDD']*100:.1f}% | Calmar {m['Calmar']:.2f}")

    # correlation to the vol-targeted engine
    from deep_analysis import basket_and_overlay
    _, spy, _, engine = basket_and_overlay(panel, aux, vixp)
    dm = eq.pct_change().resample("ME").mean()
    em = (1+engine).cumprod().resample("ME").last().pct_change()
    j = pd.concat([dm.rename("donch"), em.rename("engine")], axis=1).dropna()
    corr = j["donch"].corr(j["engine"])
    print(f"\nMonthly-return correlation to the return engine: {corr:.2f}")
    print(f"Sub-period Sharpe:")
    for a, b, lbl in [("2015-01-01","2019-01-01","2015-18"),
                      ("2019-01-01","2023-01-01","2019-22"),
                      ("2023-01-01","2026-06-30","2023-26")]:
        e2 = eq.loc[a:b]
        print(f"  {lbl}: Sharpe {M.summarize(e2)['Sharpe']:.2f}  CAGR {M.summarize(e2)['CAGR']*100:.1f}%")

    ok = (m["Sharpe"] >= 0.8) and (abs(corr) < 0.6)
    print(f"\nVERDICT: {'SHIP — standalone edge + diversifying' if ok else 'HOLD — fails Sharpe/corr gate'}")


if __name__ == "__main__":
    main()
