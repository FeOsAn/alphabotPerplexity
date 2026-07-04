"""
Drawdown-reduction overlays on the return engine (equal-weight mega-cap basket).
Goal: keep most of the CAGR, cut MaxDD and the 2022-flip drawdown. Compares:

  A always-in            — reference (high return, deep DD)
  B 200DMA->cash (0.48)  — the overlay already shipped
  C vol-target 12%       — scale daily exposure to target portfolio vol
  D vol-target 10%       — tighter target
  E VIX-scaled           — cut exposure as VIX rises
  F 200DMA + vol-target  — combined
  G vol-target + VIX      — combined

Exposure decisions use yesterday's data (no look-ahead). Vol-target and VIX
scalars are capped at 1.0 (no leverage). Metric to watch: Calmar (CAGR/MaxDD)
and 2022flipDD, maximise return per unit of drawdown.

Run:  python backtests/dd_reduction.py
"""
from __future__ import annotations
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-06-30"
TARGET_VOL = 0.12


def basket_returns(panel):
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    return closes.pct_change().mean(axis=1)


def vol_target_scalar(ret, target):
    rv = ret.rolling(20).std() * np.sqrt(252)
    s = (target / rv).clip(upper=1.0)
    return s.shift(1).fillna(0.0)


def vix_scalar(vix, idx):
    v = vix.reindex(idx).ffill()
    s = pd.Series(1.0, index=idx)
    s[v >= 20] = 0.7
    s[v >= 27] = 0.4
    s[v >= 35] = 0.2
    return s.shift(1).fillna(1.0)


def ma200_scalar(spy, idx, mult=0.60):
    ma = spy.rolling(200).mean()
    s = pd.Series(1.0, index=spy.index)
    s[spy <= ma] = mult
    return s.shift(1).reindex(idx).ffill().fillna(1.0)


def metrics_row(name, daily):
    eq = (1 + daily).cumprod()
    m = M.summarize(eq)
    dd22 = M.summarize((1 + daily.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    return name, m["CAGR"], m["Sharpe"], m["MaxDD"], m["Calmar"], dd22


def main():
    print("Fetching basket + SPY + VIX ...")
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    vixp = D.get_history("^VIX", START, END)
    basket = basket_returns(panel).dropna()
    spy = aux["SPY"]["Close"].reindex(basket.index).ffill()
    vix = vixp["Close"] if not vixp.empty else pd.Series(15.0, index=basket.index)

    vt12 = vol_target_scalar(basket, 0.12)
    vt10 = vol_target_scalar(basket, 0.10)
    vx = vix_scalar(vix, basket.index)
    ma = ma200_scalar(spy, basket.index, 0.60)

    books = {
        "A always-in":          basket,
        "B 200DMA->cash":       ma * basket,
        "C vol-target 12%":     vt12 * basket,
        "D vol-target 10%":     vt10 * basket,
        "E VIX-scaled":         vx * basket,
        "F 200DMA+voltgt12":    np.minimum(ma, vt12) * basket,
        "G voltgt12+VIX":       np.minimum(vt12, vx) * basket,
        "SPY buy&hold":         spy.pct_change().fillna(0.0),
    }

    for label, win in [("=== 5-YEAR (2021-06..2026-06) ===", ("2021-06-30","2026-06-30")),
                       ("=== 10-YEAR (2015-06..2026-06) ===", ("2015-06-30","2026-06-30"))]:
        print(f"\n{label}")
        print(f"{'book':<22}{'CAGR':>8}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022DD':>9}")
        for name, daily in books.items():
            _, cagr, sh, mdd, cal, dd22 = metrics_row(name, daily.loc[win[0]:win[1]])
            print(f"{name:<22}{cagr*100:>7.1f}%{sh:>8.2f}{mdd*100:>7.1f}%{cal:>8.2f}{dd22*100:>8.1f}%")

    print("\nPick the book with the best Calmar (CAGR/MaxDD) and shallowest 2022DD")
    print("while keeping CAGR well above SPY. Vol-targeting usually wins on Calmar.")


if __name__ == "__main__":
    main()
