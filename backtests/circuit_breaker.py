"""
Daily-loss circuit breaker: threshold selection.
When the book drops more than X% in a day, de-risk for a short cooldown. This is
primarily a SAFETY control (stop deploying into a fast crash / a runaway bug) —
the live version only HALTS NEW ENTRIES (no forced selling). Here we model the
stronger "cut exposure" version to size the threshold and show the tail benefit;
the live effect is a softer subset of this.

Test X in {3,4,5}% and cooldown K in {1,2,3} days. Watch: does it cut the fast
tails (Volmageddon, COVID onset, 2018) without materially hurting Sharpe?

Run:  python backtests/circuit_breaker.py
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-06-30"


def engine_ret():
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    vixp = D.get_history("^VIX", START, END)
    from deep_analysis import basket_and_overlay
    _, spy, _, engine = basket_and_overlay(panel, aux, vixp)
    return engine, spy.pct_change().fillna(0.0)


def apply_cb(ret, thresh, cooldown, floor=0.30):
    """After a day with return < -thresh, scale exposure to `floor` for `cooldown`
    days (acting the NEXT day — no look-ahead)."""
    scal = np.ones(len(ret))
    cd = 0
    r = ret.values
    for i in range(len(r)):
        if cd > 0:
            scal[i] = floor; cd -= 1
        if r[i] < -thresh:      # trips for the FOLLOWING days
            cd = cooldown
    scal = pd.Series(scal, index=ret.index).shift(1).fillna(1.0)
    return scal * ret


def tail(daily, a, b):
    return M.summarize((1+daily.loc[a:b]).cumprod())["MaxDD"]


def main():
    engine, spy = engine_ret()
    base = M.summarize((1+engine).cumprod())
    print(f"{'config':<22}{'CAGR':>7}{'Sharpe':>8}{'MaxDD':>8}{'Volmg':>8}{'COVID':>8}{'2018Q4':>8}")
    def row(name, d):
        m=M.summarize((1+d).cumprod())
        print(f"{name:<22}{m['CAGR']*100:>6.1f}%{m['Sharpe']:>8.2f}{m['MaxDD']*100:>7.1f}%"
              f"{tail(d,'2018-02-01','2018-02-12')*100:>7.1f}%"
              f"{tail(d,'2020-02-19','2020-03-23')*100:>7.1f}%"
              f"{tail(d,'2018-10-01','2018-12-24')*100:>7.1f}%")
    row("BASE (no breaker)", engine)
    print("-"*69)
    for X, K in itertools.product([0.03,0.04,0.05],[1,2,3]):
        row(f"trip -{int(X*100)}% / {K}d cool", apply_cb(engine, X, K))
    print("\nNote: live breaker only halts NEW ENTRIES (softer than this cut model).")
    print("Pick the least-harmful threshold that still trims the fast tails.")


if __name__ == "__main__":
    main()
