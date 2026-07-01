"""
Robustness check for candidate exit configs vs the current live engine, across
independent sub-periods. Only ship an exit change if it beats CURRENT in most
sub-periods, not just full-sample (overfit guard). Reuses the portfolio sim from
exit_engine_sweep.py.

Run:  python backtests/exit_robustness.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import metrics as M
import exit_engine_sweep as X   # importing runs the fetch/build once

CANDIDATES = [
    ("CURRENT stop5/tp20/current/dead14", 0.05, 0.20, "current", "dead14"),
    ("A stop5/tp30/loose/dead14",         0.05, 0.30, "loose",   "dead14"),
    ("B stop4/tp30/none/dead14",          0.04, 0.30, "none",    "dead14"),
    ("C stop5/tp0.30/current/dead14",     0.05, 0.30, "current", "dead14"),
    ("D stop5/tpNone/loose/dead14",       0.05, None, "loose",   "dead14"),
]

PERIODS = {
    "2015-2018": ("2015-01-01", "2019-01-01"),
    "2019-2022": ("2019-01-01", "2023-01-01"),
    "2023-2026": ("2023-01-01", "2026-06-30"),
    "FULL":      ("2015-01-01", "2026-06-30"),
}


def run(cfg, win):
    _, stop, tp, trail, tstop = cfg
    cal_w = X.cal[(X.cal >= win[0]) & (X.cal <= win[1])]
    curve = X.simulate(X.panel, X.entries, cal_w, X.pos_of, stop, tp, trail, tstop)
    m = M.summarize(curve)
    return m["CAGR"], m["Sharpe"], m["MaxDD"]


def main():
    for pname, win in PERIODS.items():
        print(f"\n=== {pname} ===")
        print(f"{'config':<34}{'CAGR':>8}{'Sharpe':>8}{'MaxDD':>8}")
        for cfg in CANDIDATES:
            cagr, sharpe, mdd = run(cfg, win)
            print(f"{cfg[0]:<34}{cagr*100:>7.1f}%{sharpe:>8.2f}{mdd*100:>7.1f}%")
    print("\nShip a candidate only if it beats CURRENT on Sharpe in >=3 of 4 windows.")


if __name__ == "__main__":
    main()
