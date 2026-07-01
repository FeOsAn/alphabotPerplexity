"""
Faithful exit sweep using ATR-MULTIPLE take-profits, matching the live engine
(tp_engine.py: tp2 = entry + mult * ATR(14)). The flat-% sweep proved direction;
this pins the actual knob the bot uses. Reuses the portfolio sim + data from
exit_engine_sweep.py, but the TP is entry + tp2_mult * ATR_at_entry.

Current live tp2 multiples: momentum/multi_tf/quality ~3-4x ATR. On mega-caps
ATR~1.5-2.5% of price, so tp2 ~ +5-9% -> winners capped early.

Run:  python backtests/exit_atr_sweep.py
"""
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import metrics as M, engine as E
import exit_engine_sweep as X

# precompute ATR(14) as fraction of price, per symbol, aligned by date
ATRpct = {}
for s, df in X.panel.items():
    a = E.atr(df["High"], df["Low"], df["Close"], 14) / df["Close"]
    ATRpct[s] = {dt: (a.values[i] if np.isfinite(a.values[i]) else 0.02)
                 for i, dt in enumerate(df.index)}

TRAILS = X.TRAILS
DEAD_BAND = 0.025


def simulate_atr(stop, tp2_mult, trail, tstop):
    tiers = TRAILS[trail]
    hard = 40 if tstop == "hard40" else 10**9
    dead = (tstop == "dead14")
    equity = cash = 1.0
    open_pos = {}
    cal = X.cal
    curve = np.empty(len(cal)); curve[:] = np.nan
    for t in range(len(cal)):
        for s in list(open_pos.keys()):
            p = X.pos_of[s].get(cal[t])
            if p is None:
                continue
            o, h, l, c = p
            st = open_pos[s]
            st["peak"] = max(st["peak"], h); st["held"] += 1
            entry = st["entry"]
            stop_px = entry * (1 + X.trail_floor(st["peak"]/entry - 1, tiers, stop))
            tp_px = entry * (1 + tp2_mult * st["atr"]) if tp2_mult else None
            exit_px = None
            if l <= stop_px:
                exit_px = stop_px
            elif tp_px is not None and h >= tp_px:
                exit_px = tp_px
            elif dead and st["held"] >= 14 and abs(c/entry - 1) <= DEAD_BAND:
                exit_px = c
            elif st["held"] >= hard:
                exit_px = c
            if exit_px is not None:
                st["val"] *= exit_px / st["mark"]; cash += st["val"]; del open_pos[s]
            else:
                st["val"] *= c / st["mark"]; st["mark"] = c
        if len(open_pos) < X.K:
            for s in X.panel:
                if len(open_pos) >= X.K or s in open_pos:
                    continue
                ei = X.pos_of[s].get(cal[t])
                if ei is None:
                    continue
                idx = X.idx_of[s].get(cal[t])
                arr = X.entries[s]
                if idx is None or idx < 1 or idx >= len(arr):
                    continue
                if arr[idx-1]:
                    o = ei[0]
                    if not np.isfinite(o) or o <= 0:
                        continue
                    alloc = equity / X.K
                    if alloc > cash:
                        continue
                    cash -= alloc
                    open_pos[s] = {"entry": o, "peak": o, "val": alloc, "mark": o,
                                   "held": 0, "atr": ATRpct[s].get(cal[t], 0.02)}
        equity = cash + sum(st["val"] for st in open_pos.values())
        curve[t] = equity
    return pd.Series(curve, index=cal).dropna()


def score(curve):
    m = M.summarize(curve)
    dd22 = M.summarize(curve.loc["2021-12-31":"2023-01-31"])["MaxDD"]
    return m["CAGR"], m["Sharpe"], m["MaxDD"], m["Calmar"], dd22


def main():
    configs = [("CURRENT ~stop5/tp2=3.5xATR/current", 0.05, 3.5, "current", "dead14")]
    for stop, tp2, trail in itertools.product([0.05, 0.06], [3.5, 5.0, 7.0, None],
                                              ["current", "none", "loose"]):
        configs.append((f"stop{int(stop*100)}/tp2={tp2}x/{trail}", stop, tp2, trail, "dead14"))

    rows = []
    for label, stop, tp2, trail, tstop in configs:
        cagr, sh, mdd, cal, dd22 = score(simulate_atr(stop, tp2, trail, tstop))
        rows.append((label, cagr, sh, mdd, cal, dd22))
    base = rows[0]
    rest = sorted(rows[1:], key=lambda r: (r[2] if np.isfinite(r[2]) else -9), reverse=True)

    print(f"\n{'config':<34}{'CAGR':>7}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022DD':>8}")
    print("-"*73)
    for label, cagr, sh, mdd, cal, dd22 in [base] + rest:
        star = " *BASE" if label.startswith("CURRENT") else ""
        print(f"{label:<34}{cagr*100:>6.1f}%{sh:>8.2f}{mdd*100:>7.1f}%{cal:>8.2f}{dd22*100:>7.1f}%{star}")


if __name__ == "__main__":
    main()
