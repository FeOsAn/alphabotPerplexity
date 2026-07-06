"""
Workstream 2: crypto trend sleeve as the missing uncorrelated diversifier.
Earlier cross-asset test (bonds/commodities) failed on a bad decade — but it
never tried crypto, which Alpaca supports natively. Classic construction:
  hold BTC (and optionally ETH 50/50) while price > 200DMA, else flat.
Weekend moves are compounded into the next equity trading day.

Measures: standalone sleeve, correlation to the engine, blend curve, and the
2022-flip behaviour (the trend gate exited BTC in late 2021 — the whole point).

Run:  python backtests/crypto_sleeve.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M
from frontier_shift import load, basket_ew, overlay


def trend_ret(sym, cal):
    px = D.get_history(sym, "2014-01-01", "2026-06-30")["Close"]
    if px.empty:
        return None
    sig = (px > px.rolling(200).mean()).shift(1).fillna(False)
    daily = px.pct_change().fillna(0.0) * sig
    # compound onto the equity calendar (weekend crypto moves land on Monday)
    eq = (1 + daily).cumprod()
    eq_cal = eq.reindex(eq.index.union(cal)).ffill().reindex(cal)
    return eq_cal.pct_change().fillna(0.0)


def stat(name, d, engine=None):
    m = M.summarize((1 + d.dropna()).cumprod())
    dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    corr = ""
    if engine is not None:
        j = pd.concat([d.rename("x"), engine.rename("e")], axis=1).dropna()
        corr = f"  corr={j['x'].corr(j['e']):.2f}"
    print(f"{name:<30}{m['CAGR']*100:>7.1f}%{m['Sharpe']:>7.2f}{m['MaxDD']*100:>8.1f}%"
          f"{m['Calmar']:>7.2f}{dd22*100:>8.1f}%{corr}")


def main():
    panel, closes, rets, spy = load()
    engine = overlay(basket_ew(rets, closes), spy)
    cal = engine.index

    btc = trend_ret("BTC-USD", cal)
    eth = trend_ret("ETH-USD", cal)
    if btc is None:
        print("BTC data unavailable — abort")
        return
    crypto = btc if eth is None else 0.5 * btc + 0.5 * eth

    print(f"{'series':<30}{'CAGR':>8}{'Sharpe':>7}{'MaxDD':>8}{'Calmar':>7}{'2022DD':>8}")
    print("-" * 78)
    stat("BTC trend (200DMA, long/flat)", btc, engine)
    if eth is not None:
        stat("BTC+ETH 50/50 trend", crypto, engine)
    stat("ENGINE (reference)", engine)

    print("\nblend curve: (1-w)*engine + w*crypto_trend")
    print(f"{'w':>5}{'CAGR':>8}{'Sharpe':>8}{'MaxDD':>8}{'Calmar':>8}{'2022DD':>8}")
    for w in [0.05, 0.10, 0.15, 0.20]:
        d = (1 - w) * engine + w * crypto
        m = M.summarize((1 + d).cumprod())
        dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
        print(f"{w:>5.2f}{m['CAGR']*100:>7.1f}%{m['Sharpe']:>8.2f}{m['MaxDD']*100:>7.1f}%"
              f"{m['Calmar']:>8.2f}{dd22*100:>7.1f}%")

    print("\nsub-period Sharpe of the 10% blend vs engine:")
    for a, b, l in [("2015-01-01","2019-01-01","15-18"),("2019-01-01","2023-01-01","19-22"),
                    ("2023-01-01","2026-06-30","23-26")]:
        e = M.summarize((1 + engine.loc[a:b]).cumprod())["Sharpe"]
        bl = M.summarize((1 + (0.9*engine + 0.1*crypto).loc[a:b]).cumprod())["Sharpe"]
        print(f"  {l}: engine {e:.2f} -> +10% crypto {bl:.2f}")


if __name__ == "__main__":
    main()
