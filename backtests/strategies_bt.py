"""
Faithful entry-signal replicas of the live strategies, for edge-testing under
the real exit engine (engine.py). Each returns a boolean Series (event) or is
driven by the ranked/rotation runners. Point-in-time, no look-ahead: every gate
at bar i uses only data through bar i; entries fill at bar i+1 open.

Thresholds mirror the live code; where the live value is adaptively calibrated
(utils/adaptive_filters) we use its documented central/default (rsi_max~75,
mr_oversold~37, breakout_proximity~0.93, vol floors ~1.2-1.5x).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import engine as E


def _volratio(df):
    v = df["Volume"]
    return v / v.rolling(20).mean()


# ── EVENT strategies (per-symbol) ─────────────────────────────────────────────
def breakout(df):
    c, v = df["Close"], df["Volume"]
    high252 = c.rolling(252).max()
    ma50 = E.sma(c, 50)
    r = E.rsi(c, 14)
    vr = _volratio(df)
    slope20 = c / c.shift(20) - 1
    return ((c / high252 >= 0.93) & (vr >= 1.2) & (r >= 60) & (r <= 78) &
            (c > ma50) & (slope20 >= 0.02))


def fifty_two_wh(df):
    c, v = df["Close"], df["Volume"]
    prev_high = c.shift(1).rolling(252).max()
    ma50 = E.sma(c, 50)
    vr = v / v.rolling(20).mean()
    return (c > prev_high) & (vr >= 1.1) & (c >= ma50 * 1.03)


def mean_reversion(df):
    # v100 live: Connors RSI(2)<10 in an uptrend (close>MA200).
    c = df["Close"]
    return (E.rsi(c, 2) < 10) & (c > E.sma(c, 200))


def mean_reversion_exit(df, k, entry):
    """Native MR exit (v100): quick snap-back above the 5-day MA, or RSI(2) hot."""
    c = df["Close"]
    return bool(c.iloc[k] > E.sma(c, 5).iloc[k] or E.rsi(c, 2).iloc[k] > 70)


def trend_pullback(df):
    c, h, l = df["Close"], df["High"], df["Low"]
    ma = E.sma(c, 50)
    r = E.rsi(c, 14)
    adx_, pdi, ndi = E.adx(h, l, c, 14)
    return ((adx_ >= 20) & (c > ma) & (pdi > ndi) &
            (r >= 35) & (r <= 65) & (r > r.shift(1)))


def multi_tf_rsi(df):
    c = df["Close"]
    e50 = E.ema(c, 50)
    r10 = E.rsi(c, 10)
    r40 = E.rsi(c, 40)
    return (r40 > 52) & (c > e50) & (r10 >= 38) & (r10 <= 56) & (r10 > r10.shift(1))


def gap_scanner(df):
    c, o, v = df["Close"], df["Open"], df["Volume"]
    gap = o / c.shift(1) - 1
    vr = v / v.rolling(20).mean()
    r = E.rsi(c, 14)
    # gap detected on bar i (conservative: fill next open via the engine)
    return (gap >= 0.02) & (gap <= 0.06) & (vr > 1.0) & (r < 80)


EVENT = {
    "breakout":       (breakout, None),
    "52wh_vol":       (fifty_two_wh, None),
    "mean_reversion": (mean_reversion, mean_reversion_exit),
    "trend_pullback": (trend_pullback, None),
    "multi_tf_rsi":   (multi_tf_rsi, None),
    "gap_scanner":    (gap_scanner, None),
}


# ── RANKED strategies (cross-sectional rebalance) ─────────────────────────────
def _ret(c, n):
    return c / c.shift(n) - 1


def run_ranked(panel, strategy, score_fn, gate_fn, rebalance, top_n, tp=E.TP):
    """Trade-based model of a ranked sleeve.
    score_fn(df,i)->float|None ; gate_fn(df,i)->bool. Each rebalance date, rank
    all symbols by score, enter the top_n that pass the gate (and aren't already
    in an open trade for that symbol), simulate each via the live exit engine."""
    syms = list(panel.keys())
    # common date index (union) — iterate on a reference calendar (SPY-like: use
    # the longest series)
    ref = max(panel.values(), key=len).index
    open_until = {s: -1 for s in syms}   # index position until which sym is held
    trades, dated = [], []
    pos = {s: 0 for s in syms}           # cursor per symbol into its own frame
    # precompute per-symbol arrays
    for start in range(260, len(ref), rebalance):
        date = ref[start]
        ranked = []
        for s in syms:
            df = panel[s]
            if date not in df.index:
                continue
            i = df.index.get_loc(date)
            if i < 260 or i <= open_until[s]:
                continue
            sc = score_fn(df, i)
            if sc is None or not np.isfinite(sc):
                continue
            if gate_fn(df, i):
                ranked.append((sc, s, i))
        ranked.sort(reverse=True)
        for sc, s, i in ranked[:top_n]:
            df = panel[s]
            res = E.simulate_trade(df, i, strategy, tp=tp)
            if res is None:
                continue
            ret, exit_i = res
            trades.append(ret)
            dated.append((df.index[exit_i], ret))
            open_until[s] = exit_i
    return trades, dated


def momentum_score(df, i):
    c = df["Close"]
    r3 = c.iloc[i] / c.iloc[i-63] - 1 if i >= 63 else None
    r1 = c.iloc[i] / c.iloc[i-21] - 1 if i >= 21 else None
    if r3 is None or r1 is None:
        return None
    return r3 - r1


def momentum_gate(df, i):
    c, v = df["Close"], df["Volume"]
    ma50 = c.iloc[max(0, i-49):i+1].mean()
    ma20 = c.iloc[max(0, i-19):i+1].mean()
    r = E.rsi(c, 14).iloc[i]
    vr = v.iloc[i] / v.iloc[max(0, i-19):i+1].mean()
    r1 = c.iloc[i] / c.iloc[i-21] - 1 if i >= 21 else 0
    sc = momentum_score(df, i) or 0
    return bool(c.iloc[i] > ma50 and c.iloc[i] > ma20 and r < 75 and vr >= 1.5 and sc > 0 and r1 > 0)


def cs_momentum_score(df, i):
    c = df["Close"]
    if i < 252:
        return None
    return (c.iloc[i]/c.iloc[i-252] - 1) - (c.iloc[i]/c.iloc[i-21] - 1)


def cs_momentum_gate(df, i):
    c = df["Close"]
    ma50 = c.iloc[max(0, i-49):i+1].mean()
    return bool(c.iloc[i] > ma50)


def quality_momentum_score(df, i):
    """6m return / 6m vol (return-to-risk) — proxy for quality+momentum rank."""
    c = df["Close"]
    if i < 126:
        return None
    ret6 = c.iloc[i]/c.iloc[i-126] - 1
    vol = c.pct_change().iloc[i-126:i+1].std()
    if vol <= 0:
        return None
    return ret6 / vol


def quality_momentum_gate(df, i):
    c = df["Close"]
    ma50 = c.iloc[max(0, i-49):i+1].mean()
    ma50_prev = c.iloc[max(0, i-69):i-19].mean() if i >= 69 else ma50
    return bool(c.iloc[i] > ma50 and ma50 > ma50_prev)


RANKED = {
    "momentum":         dict(score_fn=momentum_score, gate_fn=momentum_gate, rebalance=5, top_n=6, tp=E.TP),
    "cs_momentum":      dict(score_fn=cs_momentum_score, gate_fn=cs_momentum_gate, rebalance=21, top_n=4, tp=None),
    "quality_momentum": dict(score_fn=quality_momentum_score, gate_fn=quality_momentum_gate, rebalance=21, top_n=3, tp=E.TP),
}


# ── ROTATION sleeve (dual_momentum): monthly ETF rotation return series ────────
DUAL_UNIVERSE = ["SPY", "QQQ", "IWM", "GLD", "XLF", "XLE", "XLK", "XLV"]
EQUITY_ETFS = {"SPY", "QQQ", "IWM", "XLK", "XLF", "XLE", "XLV"}
SAFE = "GLD"
ABS_FLOOR = 0.048


def dual_momentum_returns(panel):
    """Monthly-rebalanced dual-momentum. Returns a monthly return Series."""
    closes = pd.DataFrame({s: panel[s]["Close"] for s in DUAL_UNIVERSE if s in panel}).dropna()
    if closes.empty:
        return pd.Series(dtype=float)
    monthly_idx = closes.resample("ME").last().index
    rets = []
    dates = []
    for j in range(13, len(monthly_idx) - 1):
        d = monthly_idx[j]
        i = closes.index.get_indexer([d], method="ffill")[0]
        if i < 252:
            continue
        row = closes.iloc[i]
        survivors = []
        for s in DUAL_UNIVERSE:
            if s not in closes.columns:
                continue
            c = closes[s]
            r12 = c.iloc[i]/c.iloc[i-252] - 1
            r6 = c.iloc[i]/c.iloc[i-126] - 1
            r3 = c.iloc[i]/c.iloc[i-63] - 1
            ma50 = c.iloc[i-49:i+1].mean()
            if s in EQUITY_ETFS and r12 > ABS_FLOOR and r6 > 0 and r3 > 0 and c.iloc[i] > ma50:
                survivors.append((r12 - (c.iloc[i]/c.iloc[i-21]-1), s))
        survivors.sort(reverse=True)
        picks = [s for _, s in survivors[:2]] or [SAFE]
        while len(picks) < 3:
            picks.append(SAFE)
        # next-month return, equal weight
        nd = monthly_idx[j+1]
        ni = closes.index.get_indexer([nd], method="ffill")[0]
        pr = np.mean([closes[s].iloc[ni]/closes[s].iloc[i] - 1 for s in picks])
        rets.append(pr); dates.append(nd)
    return pd.Series(rets, index=dates)
