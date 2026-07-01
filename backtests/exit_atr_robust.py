"""Sub-period robustness for the chosen exit change: widen tp2 ATR multiple,
keep the current trailing. Ship only if it beats CURRENT across sub-periods."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import metrics as M
import exit_atr_sweep as A
import exit_engine_sweep as X

CANDS = [
    ("CURRENT stop5/tp2=3.5x/current", 0.05, 3.5, "current"),
    ("W1 stop5/tp2=5.5x/current",      0.05, 5.5, "current"),
    ("W2 stop5/tp2=7.0x/current",      0.05, 7.0, "current"),
    ("W3 stop6/tp2=7.0x/none",         0.06, 7.0, "none"),
]
PERIODS = {"2015-2018": ("2015-01-01","2019-01-01"),
           "2019-2022": ("2019-01-01","2023-01-01"),
           "2023-2026": ("2023-01-01","2026-06-30")}

full_cal = X.cal
for pname, (a, b) in PERIODS.items():
    print(f"\n=== {pname} ===")
    print(f"{'config':<32}{'CAGR':>8}{'Sharpe':>8}{'MaxDD':>8}")
    X.cal = full_cal[(full_cal >= a) & (full_cal <= b)]   # window the sim calendar
    for label, stop, tp2, trail in CANDS:
        curve = A.simulate_atr(stop, tp2, trail, "dead14")
        m = M.summarize(curve)
        print(f"{label:<32}{m['CAGR']*100:>7.1f}%{m['Sharpe']:>8.2f}{m['MaxDD']*100:>7.1f}%")
X.cal = full_cal
print("\nShip W1/W2 if they beat CURRENT on Sharpe in all 3 windows.")
