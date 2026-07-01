"""
Cross-strategy correlation of the long-momentum sleeves.
Consolidation only makes sense if the sleeves are actually redundant. This
builds a monthly P&L series per sleeve (mean of trade returns by exit month),
correlates them, and reports each sleeve's standalone monthly Sharpe — so we
keep the best-diversifying subset rather than 7 copies of the same bet.

Run:  python backtests/correlation.py
"""
from __future__ import annotations
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M, engine as E, strategies_bt as S
from scorecard import UNIVERSE, ETFS, START, END


def monthly_series(dated):
    if not dated:
        return pd.Series(dtype=float)
    df = pd.DataFrame(dated, columns=["date", "ret"]).set_index("date")
    df.index = pd.to_datetime(df.index)
    return df["ret"].resample("ME").mean()


def main():
    panel = D.get_panel(UNIVERSE, START, END)
    etf = D.get_panel(ETFS, START, END)
    series = {}

    for name, (fn, native) in S.EVENT.items():
        if name in ("mean_reversion", "gap_scanner"):
            continue  # just disabled; not momentum sleeves anyway
        _, dated = E.run_event(panel, fn, name, native_exit=native)
        series[name] = monthly_series(dated)

    for name, cfg in S.RANKED.items():
        _, dated = S.run_ranked(panel, name, **cfg)
        series[name] = monthly_series(dated)

    series["dual_momentum"] = S.dual_momentum_returns({**panel, **etf})

    mat = pd.DataFrame(series).dropna(how="all")
    # align on overlapping months
    corr = mat.corr()

    print("=== Monthly-P&L correlation matrix (long sleeves) ===\n")
    cols = list(corr.columns)
    hdr = "".join(f"{c[:8]:>9}" for c in cols)
    print(f"{'':<16}{hdr}")
    for r in cols:
        row = "".join(f"{corr.loc[r, c]:>9.2f}" for c in cols)
        print(f"{r:<16}{row}")

    print("\n=== Standalone monthly Sharpe (annualised) & avg pairwise corr ===")
    print(f"{'sleeve':<18}{'Sharpe':>8}{'avgCorr':>9}{'months':>8}")
    for c in cols:
        s = mat[c].dropna()
        sharpe = s.mean()/s.std()*np.sqrt(12) if s.std() > 0 else float("nan")
        others = [corr.loc[c, o] for o in cols if o != c and np.isfinite(corr.loc[c, o])]
        avg_corr = np.mean(others) if others else float("nan")
        print(f"{c:<18}{sharpe:>8.2f}{avg_corr:>9.2f}{len(s):>8}")

    # SPY correlation for context
    spy = etf.get("SPY")
    if spy is not None:
        spym = spy["Close"].resample("ME").last().pct_change().rename("SPY")
        print("\n=== Correlation to SPY monthly returns ===")
        for c in cols:
            j = pd.concat([mat[c], spym], axis=1, sort=True).dropna()
            if len(j) > 6:
                print(f"  {c:<18}{j.iloc[:,0].corr(j.iloc[:,1]):>6.2f}")

    print("\nReading: sleeves with high mutual correlation + no Sharpe edge are")
    print("redundant — keep the best-Sharpe / lowest-avgCorr representative(s),")
    print("disable the rest to cut correlated drawdown and code surface.")


if __name__ == "__main__":
    main()
