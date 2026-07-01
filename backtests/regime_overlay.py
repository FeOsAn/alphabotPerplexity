"""
Regime-flip protection: how much drawdown does a regime overlay save, and at
what return cost? Directly targets the goal: MAX 5-yr return, MIN loss in
regime flips.

Since the equity sleeves ~= being long the mega-cap universe (scorecard §4), we
model the consolidated long book as an equal-weight mega-cap basket, then test
regime overlays that de-risk when the market turns:

  A. always-in basket (no protection)
  B. SPY > 200DMA  -> basket, else CASH
  C. SPY > 200DMA  -> basket, else GLD   (the bot's GLD-rotation idea)
  D. SPY 50>200 (golden/death cross) -> basket, else GLD
  E. blend: 70% basket always + 30% overlay-C sleeve (partial de-risk)

Metrics on the 5-yr window (2021-06..2026-06, contains the 2022 bear flip) and
the 10-yr window, plus the SPECIFIC max drawdown during the 2022 flip.

Overlay decisions use only past data (yesterday's MA state) -> no look-ahead.

Run:  python backtests/regime_overlay.py
"""
from __future__ import annotations
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from scorecard import UNIVERSE

START, END = "2014-01-01", "2026-06-30"


def basket_returns(panel):
    """Equal-weight daily returns across the mega-cap universe."""
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    rets = closes.pct_change()
    return rets.mean(axis=1)   # equal weight, daily rebalance (proxy)


def overlay(basket_ret, spy, defense_ret, mode):
    """mode: 'in','ma200_cash','ma200_gld','cross_gld'. defense_ret=GLD daily ret."""
    ma200 = spy.rolling(200).mean()
    ma50 = spy.rolling(50).mean()
    if mode == "in":
        sig = pd.Series(1.0, index=spy.index)
    elif mode == "ma200_cash":
        sig = (spy > ma200).astype(float)
    elif mode == "ma200_gld":
        sig = (spy > ma200).astype(float)
    elif mode == "cross_gld":
        sig = (ma50 > ma200).astype(float)
    sig = sig.shift(1).fillna(0.0)   # act on yesterday's state
    idx = basket_ret.index
    sig = sig.reindex(idx).ffill().fillna(0.0)
    dfd = defense_ret.reindex(idx).fillna(0.0)
    if mode in ("ma200_gld", "cross_gld"):
        return sig * basket_ret + (1 - sig) * dfd
    return sig * basket_ret   # cash = 0 when out


def window(series, start, end):
    return series.loc[start:end]


def report(name, daily):
    eq = (1 + daily).cumprod()
    m = M.summarize(eq)
    dd22 = M.summarize((1 + window(daily, "2021-12-31", "2023-01-31")).cumprod())["MaxDD"]
    return name, m, dd22


def main():
    print("Fetching basket + SPY + GLD ...")
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY", "GLD"], START, END)
    basket = basket_returns(panel).dropna()
    spy = aux["SPY"]["Close"].reindex(basket.index).ffill()
    gld = aux["GLD"]["Close"].reindex(basket.index).ffill().pct_change().fillna(0.0)

    books = {
        "A basket always-in": overlay(basket, spy, gld, "in"),
        "B basket + 200DMA->cash": overlay(basket, spy, gld, "ma200_cash"),
        "C basket + 200DMA->GLD": overlay(basket, spy, gld, "ma200_gld"),
        "D basket + 50/200->GLD": overlay(basket, spy, gld, "cross_gld"),
    }
    # Partial cash-defense blends (w*always-in + (1-w)*200DMA->cash). GLD is
    # dropped from blends: overlay C showed GLD deepened the 2022 flip.
    A = books["A basket always-in"]
    Bc = books["B basket + 200DMA->cash"]
    for w in (0.8, 0.7, 0.6, 0.5):
        books[f"blend {int(w*100)}in/{int((1-w)*100)}cash"] = w*A + (1-w)*Bc
    # SPY benchmark
    books["SPY buy&hold"] = spy.pct_change().fillna(0.0)

    for label, win in [("=== 5-YEAR (2021-06 .. 2026-06) ===", ("2021-06-30", "2026-06-30")),
                       ("=== 10-YEAR (2015-06 .. 2026-06) ===", ("2015-06-30", "2026-06-30"))]:
        print(f"\n{label}")
        print(f"{'book':<26}{'CAGR':>8}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022flipDD':>11}")
        rows = []
        for name, daily in books.items():
            d = window(daily, *win)
            _, m, dd22 = report(name, d)
            rows.append((name, m, dd22))
        for name, m, dd22 in rows:
            print(f"{name:<26}{m['CAGR']*100:>7.1f}%{m['Sharpe']:>8.2f}"
                  f"{m['MaxDD']*100:>7.1f}%{m['Calmar']:>8.2f}{dd22*100:>10.1f}%")

    print("\nGoal read: pick the book with the best 5-yr CAGR/Calmar AND the")
    print("shallowest 2022-flip drawdown. The 200DMA->GLD overlay is the classic")
    print("regime-flip protector; column '2022flipDD' is the number to minimise.")


if __name__ == "__main__":
    main()
