"""
Exit-engine optimization.
=========================
The scorecard proved entries ~= random on this universe and the exit engine
drives the P&L (same entry was +1.09% or -0.94%/trade purely from the exit rule).
So we hold a fixed, mechanical LONG entry constant and sweep the EXIT parameters,
scoring each on a real slot-based portfolio equity curve (CAGR / Sharpe / MaxDD /
2022-flip DD). The current live config is included as the baseline to beat.

Entry (held fixed across all configs, to isolate the exit):
  close crosses above MA50 AND RSI(14) < 70  -> enter next open, 1 slot.
  K=15 equal-weight slots (~the bot's MAX_TOTAL_POSITIONS).

Exit params swept:
  - init_stop   : initial hard stop below entry
  - tp          : take-profit cap (None = let winners run)
  - trail       : 'none' | 'current' | 'tight' | 'loose'  (peak ratchet floors)
  - time_stop   : 'dead14' (flat +/-2.5% for 14d) | 'none' | 'hard40'

Run:  python backtests/exit_engine_sweep.py
"""
from __future__ import annotations
import sys, pathlib, itertools
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E
from scorecard import UNIVERSE

START, END = "2014-06-01", "2026-06-30"
K = 15
DEAD_BAND = 0.025

# trailing ratchet tier sets: (peak_gain_min, lock_floor)
TRAILS = {
    "none":    [],
    "current": [(0.25,0.18),(0.18,0.12),(0.12,0.07),(0.08,0.04),(0.05,0.02),(0.03,0.0)],
    "tight":   [(0.15,0.12),(0.10,0.07),(0.07,0.04),(0.05,0.02),(0.03,0.0)],
    "loose":   [(0.30,0.18),(0.20,0.10),(0.12,0.05),(0.08,0.02)],
}


def trail_floor(peak_gain, tiers, init_stop):
    for mg, lf in tiers:
        if peak_gain >= mg:
            return lf
    return -init_stop


def build_entries(panel):
    """Fixed mechanical entry: MA50 upcross + RSI<70. Returns dict sym->bool array."""
    sig = {}
    for s, df in panel.items():
        c = df["Close"]
        ma50 = E.sma(c, 50)
        r = E.rsi(c, 14)
        cross = (c > ma50) & (c.shift(1) <= ma50.shift(1))
        sig[s] = (cross & (r < 70)).fillna(False).values
    return sig


def simulate(panel, entries, cal, pos_of, init_stop, tp, trail, time_stop):
    tiers = TRAILS[trail]
    hard = 40 if time_stop == "hard40" else 10**9
    dead = (time_stop == "dead14")
    equity = 1.0
    cash = 1.0
    open_pos = {}   # sym -> dict(entry, peak, val, held)
    curve = np.empty(len(cal)); curve[:] = np.nan

    for t in range(len(cal)):
        # 1) update open positions with today's bar, check exits
        for s in list(open_pos.keys()):
            p = pos_of[s].get(cal[t])
            if p is None:      # no bar for this symbol today -> hold flat
                continue
            o, h, l, c = p
            st = open_pos[s]
            st["peak"] = max(st["peak"], h)
            st["held"] += 1
            entry = st["entry"]
            # exit checks (stop priority, then tp, then time)
            stop_px = entry * (1 + trail_floor(st["peak"]/entry - 1, tiers, init_stop))
            exit_px = None
            if l <= stop_px:
                exit_px = stop_px
            elif tp is not None and h >= entry * (1 + tp):
                exit_px = entry * (1 + tp)
            elif dead and st["held"] >= 14 and abs(c/entry - 1) <= DEAD_BAND:
                exit_px = c
            elif st["held"] >= hard:
                exit_px = c
            # mark-to-market: value tracks close; on exit, realize at exit_px
            if exit_px is not None:
                st["val"] *= exit_px / st["mark"]
                cash += st["val"]
                del open_pos[s]
            else:
                st["val"] *= c / st["mark"]
                st["mark"] = c

        # 2) entries at today's open into free slots
        if len(open_pos) < K:
            for s in panel:
                if len(open_pos) >= K:
                    break
                if s in open_pos:
                    continue
                ei = pos_of[s].get(cal[t])
                if ei is None:
                    continue
                arr = entries[s]
                idx = idx_of[s].get(cal[t])
                if idx is None or idx < 1 or idx >= len(arr):
                    continue
                if arr[idx-1]:   # signal on prior bar -> fill at today's open
                    o = ei[0]
                    if not np.isfinite(o) or o <= 0:
                        continue
                    alloc = equity / K
                    if alloc > cash:
                        continue
                    cash -= alloc
                    open_pos[s] = {"entry": o, "peak": o, "val": alloc, "mark": o, "held": 0}

        equity = cash + sum(st["val"] for st in open_pos.values())
        curve[t] = equity

    return pd.Series(curve, index=cal).dropna()


# ── build shared structures ───────────────────────────────────────────────────
print("Fetching universe...")
panel = D.get_panel(UNIVERSE, START, END)
entries = build_entries(panel)
cal = sorted(set().union(*[set(df.index) for df in panel.values()]))
cal = pd.DatetimeIndex(cal)
# fast lookups: sym -> {date: (o,h,l,c)} and sym -> {date: iloc}
pos_of, idx_of = {}, {}
for s, df in panel.items():
    o, h, l, c = df["Open"].values, df["High"].values, df["Low"].values, df["Close"].values
    d = {dt: (o[i], h[i], l[i], c[i]) for i, dt in enumerate(df.index)}
    pos_of[s] = d
    idx_of[s] = {dt: i for i, dt in enumerate(df.index)}


def score(curve, label):
    m = M.summarize(curve)
    dd22 = M.summarize(curve.loc["2021-12-31":"2023-01-31"])["MaxDD"]
    return dict(label=label, CAGR=m["CAGR"], Sharpe=m["Sharpe"], MaxDD=m["MaxDD"],
                Calmar=m["Calmar"], dd22=dd22)


def main():
    configs = []
    # current live config
    configs.append(("CURRENT (stop5/tp20/current/dead14)", 0.05, 0.20, "current", "dead14"))
    # sweep
    for stop, tp, trail, tstop in itertools.product(
        [0.04, 0.05, 0.07, 0.10],
        [0.15, 0.20, 0.30, None],
        ["none", "current", "tight", "loose"],
        ["dead14", "none"],
    ):
        configs.append((f"stop{int(stop*100)}/tp{tp}/{trail}/{tstop}", stop, tp, trail, tstop))

    rows = []
    for label, stop, tp, trail, tstop in configs:
        curve = simulate(panel, entries, cal, pos_of, stop, tp, trail, tstop)
        rows.append(score(curve, label))

    base = next(r for r in rows if r["label"].startswith("CURRENT"))
    rows.sort(key=lambda r: (r["Sharpe"] if np.isfinite(r["Sharpe"]) else -9), reverse=True)

    print(f"\n{'config':<40}{'CAGR':>7}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022DD':>8}")
    print("-"*79)
    for r in [base] + [x for x in rows if not x["label"].startswith("CURRENT")][:14]:
        star = " *BASE" if r["label"].startswith("CURRENT") else ""
        print(f"{r['label']:<40}{r['CAGR']*100:>6.1f}%{r['Sharpe']:>8.2f}"
              f"{r['MaxDD']*100:>7.1f}%{r['Calmar']:>8.2f}{r['dd22']*100:>7.1f}%{star}")

    print(f"\nBaseline (current): CAGR {base['CAGR']*100:.1f}% Sharpe {base['Sharpe']:.2f} "
          f"MaxDD {base['MaxDD']*100:.1f}% 2022DD {base['dd22']*100:.1f}%")
    print("Ranked by Sharpe. Look for configs that beat baseline on Sharpe AND 2022DD.")


if __name__ == "__main__":
    main()
