"""
Profit-max frontier: VOL_TARGET_ANNUAL x exposure cap.
Goal shifted to "maximize profit" — quantify exactly what loosening the two risk
knobs buys and costs, so the pick is a chosen point on the frontier, not vibes.

Model matches the live overlay: daily exposure = cap * min(target/rvol20, 1,
ma200_mult) applied to the equal-weight mega-cap basket (yesterday's signals).

Run:  python backtests/profit_max_sweep.py
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-06-30"


def main():
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    basket = closes.pct_change().mean(axis=1).dropna()
    spy = aux["SPY"]["Close"].reindex(basket.index).ffill()
    rv = (basket.rolling(20).std() * np.sqrt(252))
    ma200 = spy.rolling(200).mean()
    ma_mult_base = pd.Series(1.0, index=basket.index)
    ma_mult_base[spy <= ma200] = 0.60

    def run(cap, vt):
        scal = (vt / rv).clip(upper=1.0)
        scal = np.minimum(scal, ma_mult_base).shift(1).fillna(0.0).clip(lower=0.30*0)  # floor handled by min
        d = cap * scal * basket
        eq = (1 + d).cumprod()
        m = M.summarize(eq)
        dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
        return m, dd22

    print(f"{'cap':>5}{'volTgt':>8}{'CAGR':>8}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022DD':>8}")
    print("-" * 53)
    rows = []
    for cap, vt in itertools.product([0.80, 0.90, 1.00], [0.12, 0.15, 0.18, 0.22]):
        m, dd22 = run(cap, vt)
        rows.append((cap, vt, m, dd22))
        print(f"{cap:>5.2f}{vt:>8.2f}{m['CAGR']*100:>7.1f}%{m['Sharpe']:>8.2f}"
              f"{m['MaxDD']*100:>7.1f}%{m['Calmar']:>8.2f}{dd22*100:>7.1f}%")
    print("\ncurrent live = cap 0.80 / volTgt 0.12. SPY: CAGR ~14.2%, Sharpe 0.84, MaxDD -33.7%")
    print("Pick = most CAGR while MaxDD stays meaningfully better than SPY and Calmar >= ~0.9.")


if __name__ == "__main__":
    main()
