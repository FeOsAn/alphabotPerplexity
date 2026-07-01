"""
Shared backtest engine: indicators + a faithful replica of the bot's LIVE exit
logic (trade_management.py), so every strategy is judged on the exits it would
ACTUALLY get in production — not a convenient fixed bracket.

Exit model (from bot/strategies/trade_management.py + config.py):
  - per-strategy initial hard stop (see _STRATEGY_BASE_STOP)
  - peak-based trailing ratchet (tier floors below), conservative vs the live
    max(floor, peak - ATR) rule
  - 20% take-profit cap
  - dead-money timeout: flat within +/-2.5% for >= 14 trading days -> close
  - hard cap 60 trading days for boundedness
Some strategies carry a NATIVE exit (e.g. mean_reversion exits at BB-mid / RSI
overbought). Those are passed as `native_exit` and take priority over the TP.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

# per-strategy base stops (trade_management._STRATEGY_BASE_STOP; default 0.05)
STRATEGY_BASE_STOP = {
    "momentum": 0.06, "breakout": 0.05, "mean_reversion": 0.05,
    "trend_following": 0.05, "ai_research": 0.08, "gap_scanner": 0.06,
    "spy_dip": 0.04, "vix_reversal": 0.03, "52wh_vol": 0.05,
    "cs_momentum": 0.08, "quality_momentum": 0.08, "dual_momentum": 0.15,
}
DEFAULT_STOP = 0.05
TP = 0.20
DEAD_DAYS, DEAD_BAND, HARD_CAP = 14, 0.025, 60
_TIERS = [(0.25, 0.18), (0.18, 0.12), (0.12, 0.07), (0.08, 0.04), (0.05, 0.02), (0.03, 0.0)]


def base_stop(strategy: str) -> float:
    return STRATEGY_BASE_STOP.get(strategy, DEFAULT_STOP)


# ── indicators (Wilder where relevant) ────────────────────────────────────────
def rsi(c: pd.Series, n=14) -> pd.Series:
    d = c.diff()
    up = d.clip(lower=0.0)
    dn = (-d).clip(lower=0.0)
    rma_up = up.ewm(alpha=1/n, adjust=False).mean()
    rma_dn = dn.ewm(alpha=1/n, adjust=False).mean()
    rs = rma_up / rma_dn.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def ema(c: pd.Series, n) -> pd.Series:
    return c.ewm(span=n, adjust=False).mean()


def sma(c: pd.Series, n) -> pd.Series:
    return c.rolling(n).mean()


def atr(h, l, c, n=14) -> pd.Series:
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()


def bbands(c: pd.Series, n=20, k=2.0):
    mid = c.rolling(n).mean()
    sd = c.rolling(n).std(ddof=0)
    return mid - k*sd, mid, mid + k*sd


def adx(h, l, c, n=14):
    up = h.diff()
    dn = -l.diff()
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    pc = c.shift(1)
    tr = pd.concat([(h - l), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr_ = tr.ewm(alpha=1/n, adjust=False).mean()
    pdi = 100 * pd.Series(plus_dm, index=c.index).ewm(alpha=1/n, adjust=False).mean() / atr_
    ndi = 100 * pd.Series(minus_dm, index=c.index).ewm(alpha=1/n, adjust=False).mean() / atr_
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi).replace(0, np.nan)
    return dx.ewm(alpha=1/n, adjust=False).mean(), pdi, ndi


def _trail_floor(peak_gain: float, init_stop: float) -> float:
    for mg, lf in _TIERS:
        if peak_gain >= mg:
            return lf
    return -init_stop


def simulate_trade(df, i, strategy, tp=TP, time_stop=None, native_exit=None):
    """Enter at bar i+1 open; return (exit_ret, exit_idx) under the live exit model.
    native_exit(df, k, entry) -> bool closes at that bar's close if True."""
    o, h, l, c = df["Open"], df["High"], df["Low"], df["Close"]
    n = len(df)
    if i + 1 >= n:
        return None
    entry = o.iloc[i + 1]
    if not np.isfinite(entry) or entry <= 0:
        return None
    init = base_stop(strategy)
    peak = entry
    cap = time_stop if time_stop else HARD_CAP
    exit_i = min(i + cap, n - 1)
    exit_ret = None
    for k in range(i + 1, min(i + 1 + cap, n)):
        peak = max(peak, h.iloc[k])
        stop_px = entry * (1 + _trail_floor(peak / entry - 1, init))
        if l.iloc[k] <= stop_px:
            exit_ret = stop_px / entry - 1; exit_i = k; break
        if native_exit is not None and native_exit(df, k, entry):
            exit_ret = c.iloc[k] / entry - 1; exit_i = k; break
        if tp is not None and h.iloc[k] >= entry * (1 + tp):
            exit_ret = tp; exit_i = k; break
        held = k - i
        if time_stop is None and held >= DEAD_DAYS and abs(c.iloc[k] / entry - 1) <= DEAD_BAND:
            exit_ret = c.iloc[k] / entry - 1; exit_i = k; break
    if exit_ret is None:
        exit_ret = c.iloc[exit_i] / entry - 1
    return exit_ret, exit_i


def run_event(panel, signal_fn, strategy, tp=TP, time_stop=None, native_exit=None):
    """Per-symbol event entries. signal_fn(df) -> boolean Series aligned to df.
    No overlapping trades per symbol. Returns (trades, dated_returns).
    dated_returns: list of (exit_date, ret) for building an equity curve."""
    trades, dated = [], []
    for sym, df in panel.items():
        if len(df) < 60:
            continue
        sig = signal_fn(df)
        if sig is None:
            continue
        sig = sig.fillna(False).values
        i, n = 30, len(df)
        while i < n - 1:
            if sig[i]:
                res = simulate_trade(df, i, strategy, tp=tp, time_stop=time_stop, native_exit=native_exit)
                if res is None:
                    break
                ret, exit_i = res
                trades.append(ret)
                dated.append((df.index[exit_i], ret))
                i = exit_i + 1
            else:
                i += 1
    return trades, dated
