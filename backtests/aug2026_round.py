"""
2026-08-17 round — pre-registered BEFORE running (golden rule).

Context: live book has run ~42% gross vs a 95% cap since the v101/v101.1 culls,
while every validation of the shipped stack modeled FULL deployment of the
45/30/15/10 sleeve weights. Three questions, thresholds declared now:

T1  Stack decay refresh (data extended 2026-06-30 -> 2026-08-14).
    The shipped capstone (engine/donchian/crypto/gold 45/30/15/10, vol overlay)
    re-run with ~6 new OOS weeks. PASS iff trailing-12m Sharpe > 0.6 (registry
    book tripwire). Health check only — no ship decision either way.

T2  Weight-grid robustness. Re-run the x5 capstone grid +-10 around 45/30/15/10.
    Pre-reg: shipped weights CHANGE only if a neighbor beats them by >0.15
    Sharpe AND has a better 2022DD — clear dominance, not noise. Otherwise the
    shipped point stands even if it is no longer the argmax.

T3  Cash-drag measurement + signal supply TODAY. Quantify what running the
    stack at 0.45x gross costs vs full (expect: CAGR roughly halves, Sharpe
    ~flat -> deployment gap is pure return left on the table, NOT a risk
    saving). Then compute, from yesterday's closes, what each non-equity
    sleeve SHOULD hold right now: GLD vs 200DMA, BTC/ETH/SOL vs 200DMA,
    count of universe names in Donchian long-state. This separates "cash is
    correct (gates say flat)" from "cash is a defect (signals exist, book
    empty)". No threshold — this is attribution, it feeds the config decision.

Ship decision (pre-reg): position-size increases for validated sleeves are
justified iff T1 passes AND T3 shows the deployment gap is not explained by
trend gates being correctly flat. Any increase stays inside each sleeve's
already-validated capital ceiling; no new strategy logic.

Run:  python backtests/aug2026_round.py
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import numpy as np, pandas as pd
import data as D, metrics as M

START, END = "2014-01-01", "2026-08-14"
CAP, VT, FLOOR = 0.90, 0.15, 0.30
from scorecard import UNIVERSE


def load():
    panel = D.get_panel(UNIVERSE, START, END)
    aux = D.get_panel(["SPY"], START, END)
    closes = pd.DataFrame({s: panel[s]["Close"] for s in panel}).sort_index()
    rets = closes.pct_change()
    spy = aux["SPY"]["Close"].reindex(closes.index).ffill()
    return panel, closes, rets, spy


def overlay(daily, spy, cap=CAP):
    rv = daily.rolling(20).std() * np.sqrt(252)
    vtsc = (VT / rv).clip(lower=FLOOR, upper=1.0)
    ma200 = spy.rolling(200).mean()
    mam = pd.Series(1.0, index=daily.index)
    mam[spy.reindex(daily.index).ffill() <= ma200.reindex(daily.index).ffill()] = 0.60
    scal = np.minimum(vtsc, mam).shift(1).fillna(0.0)
    return cap * scal * daily, scal


def sleeve_trend(sym, cal, ma=200):
    px = D.get_history(sym, START, END)["Close"]
    if px.empty:
        return None
    sig = (px > px.rolling(ma).mean()).shift(1).fillna(False)
    eq = (1 + px.pct_change().fillna(0) * sig).cumprod()
    return eq.reindex(eq.index.union(cal)).ffill().reindex(cal).pct_change().fillna(0.0)


def summ(tag, d):
    d = d.dropna()
    m = M.summarize((1 + d).cumprod())
    dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
    y1 = d.tail(252)
    s1 = y1.mean() / y1.std() * np.sqrt(252) if y1.std() > 0 else float("nan")
    print(f"{tag:<30}{m['CAGR']*100:>6.1f}%  Sh {m['Sharpe']:>5.2f}  DD {m['MaxDD']*100:>6.1f}%"
          f"  2022DD {dd22*100:>6.1f}%  12mSh {s1:>5.2f}")
    return m, s1


def build_sleeves():
    panel, closes, rets, spy = load()
    eng_raw, scal = overlay(rets.mean(axis=1), spy)
    cal = eng_raw.index
    import donchian_validate as DV
    deq = DV.donchian_portfolio(panel)
    don = deq.pct_change().reindex(cal).fillna(0.0)
    btc = sleeve_trend("BTC-USD", cal); eth = sleeve_trend("ETH-USD", cal)
    sol = sleeve_trend("SOL-USD", cal)
    crypto = pd.concat([x for x in (btc, eth, sol) if x is not None], axis=1).mean(axis=1)
    gold = sleeve_trend("GLD", cal)
    return panel, closes, spy, cal, scal, {"eng": eng_raw, "don": don,
                                           "cry": crypto, "gld": gold}


def stack(s, w):
    return (w[0] * s["eng"] + w[1] * s["don"] + w[2] * s["cry"] + w[3] * s["gld"])


def main():
    panel, closes, spy, cal, scal, s = build_sleeves()

    print("=== T1: shipped stack, data through 2026-08-14 ===")
    m, s1 = summ("stack 45/30/15/10", stack(s, (0.45, 0.30, 0.15, 0.10)))
    print(f"T1 {'PASS' if s1 > 0.6 else 'FAIL'}  [pre-reg: trailing-12m Sharpe > 0.6]")

    print("\n=== T2: weight-grid robustness (+-10 around shipped) ===")
    base = np.array([45, 30, 15, 10])
    rows = []
    seen = set()
    for d0 in (-10, 0, 10):
        for d1 in (-10, 0, 10):
            for d2 in (-5, 0, 5):
                w = base + np.array([d0, d1, d2, -(d0 + d1 + d2)])
                if (w < 0).any() or tuple(w) in seen:
                    continue
                seen.add(tuple(w))
                d = stack(s, w / 100)
                mm = M.summarize((1 + d.dropna()).cumprod())
                dd22 = M.summarize((1 + d.loc["2021-12-31":"2023-01-31"]).cumprod())["MaxDD"]
                rows.append((tuple(w), mm["Sharpe"], mm["CAGR"], mm["MaxDD"], dd22))
    rows.sort(key=lambda r: -r[1])
    shipped = next(r for r in rows if r[0] == (45, 30, 15, 10))
    for w, sh, cagr, dd, dd22 in rows[:6]:
        star = " <- shipped" if w == (45, 30, 15, 10) else ""
        print(f"  {str(w):<22} Sh {sh:5.2f}  CAGR {cagr*100:5.1f}%  DD {dd*100:6.1f}%  2022 {dd22*100:6.1f}%{star}")
    best = rows[0]
    dominated = (best[1] - shipped[1] > 0.15) and (best[4] > shipped[4])
    print(f"T2 verdict: {'CHANGE to ' + str(best[0]) if dominated else 'KEEP 45/30/15/10'}"
          f"  [pre-reg: change only on >0.15 Sharpe AND better 2022DD]")

    print("\n=== T3a: cash drag — full vs 0.45x gross ===")
    full = stack(s, (0.45, 0.30, 0.15, 0.10))
    summ("full deployment", full)
    summ("0.45x gross (live-like)", 0.45 * full)

    print("\n=== T3b: signal supply from yesterday's closes ===")
    for sym in ["GLD", "BTC-USD", "ETH-USD", "SOL-USD"]:
        px = D.get_history(sym, "2024-01-01", END)["Close"].dropna()
        ma = px.rolling(200).mean()
        above = px.iloc[-1] > ma.iloc[-1]
        print(f"  {sym:<8} close {px.iloc[-1]:>10.2f}  vs 200DMA {ma.iloc[-1]:>10.2f}"
              f"  -> sleeve should be {'LONG' if above else 'FLAT (correct to hold cash)'}")
    n_long = 0
    for sym in UNIVERSE:
        px = panel[sym]["Close"].dropna()
        if len(px) < 60:
            continue
        hi = px.rolling(40).max().shift(1)
        lo = px.rolling(20).min().shift(1)
        sig = pd.Series(np.nan, index=px.index)
        sig[px >= hi] = 1.0
        sig[px <= lo] = 0.0
        if sig.ffill().iloc[-1] == 1.0:
            n_long += 1
    print(f"  Donchian 40/20 long-state: {n_long}/{len(UNIVERSE)} names "
          f"(12 slots -> sleeve should hold {min(n_long, 12)}/12)")
    print(f"  vol-overlay scalar (latest): {scal.dropna().iloc[-1]:.2f}")


if __name__ == "__main__":
    main()
