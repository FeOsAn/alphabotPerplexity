"""Standard performance metrics for daily equity curves."""
from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS = 252


def summarize(equity: pd.Series, *, rf: float = 0.0) -> dict:
    """
    equity: daily portfolio value series (index = dates).
    Returns Sharpe, Sortino, CAGR, MaxDD, Calmar, Vol.
    """
    equity = equity.dropna()
    if len(equity) < 2:
        return {k: float("nan") for k in
                ("CAGR", "Sharpe", "Sortino", "MaxDD", "Calmar", "Vol", "TotRet")}

    rets = equity.pct_change().dropna()
    years = (equity.index[-1] - equity.index[0]).days / 365.25
    years = max(years, 1e-9)
    tot_ret = equity.iloc[-1] / equity.iloc[0] - 1.0
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1.0

    excess = rets - rf / TRADING_DAYS
    vol = rets.std() * np.sqrt(TRADING_DAYS)
    sharpe = (excess.mean() / rets.std() * np.sqrt(TRADING_DAYS)) if rets.std() > 0 else float("nan")

    downside = rets[rets < 0].std()
    sortino = (excess.mean() / downside * np.sqrt(TRADING_DAYS)) if downside and downside > 0 else float("nan")

    cummax = equity.cummax()
    dd = equity / cummax - 1.0
    max_dd = dd.min()
    calmar = (cagr / abs(max_dd)) if max_dd < 0 else float("nan")

    return {
        "CAGR": cagr,
        "Sharpe": sharpe,
        "Sortino": sortino,
        "MaxDD": max_dd,
        "Calmar": calmar,
        "Vol": vol,
        "TotRet": tot_ret,
    }


def trade_stats(pnls: list[float]) -> dict:
    """Win rate / avg win / avg loss / profit factor from a list of per-trade returns."""
    if not pnls:
        return {"trades": 0, "win_rate": float("nan"), "avg_win": float("nan"),
                "avg_loss": float("nan"), "profit_factor": float("nan"), "avg_ret": float("nan")}
    arr = np.array(pnls, dtype=float)
    wins = arr[arr > 0]
    losses = arr[arr < 0]
    gross_win = wins.sum()
    gross_loss = -losses.sum()
    return {
        "trades": len(arr),
        "win_rate": len(wins) / len(arr),
        "avg_win": wins.mean() if len(wins) else 0.0,
        "avg_loss": losses.mean() if len(losses) else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss > 0 else float("inf"),
        "avg_ret": arr.mean(),
    }


def fmt(m: dict) -> str:
    def g(k, pct=False, d=2):
        v = m.get(k)
        if v is None or (isinstance(v, float) and (v != v)):
            return "n/a"
        return f"{v*100:.{d}f}%" if pct else f"{v:.{d}f}"
    parts = []
    if "CAGR" in m:
        parts += [f"CAGR {g('CAGR', True)}", f"Sharpe {g('Sharpe')}",
                  f"Sortino {g('Sortino')}", f"MaxDD {g('MaxDD', True)}",
                  f"Calmar {g('Calmar')}", f"Vol {g('Vol', True)}",
                  f"TotRet {g('TotRet', True)}"]
    if "trades" in m:
        parts += [f"Trades {m['trades']}", f"Win {g('win_rate', True)}",
                  f"AvgW {g('avg_win', True)}", f"AvgL {g('avg_loss', True)}",
                  f"PF {g('profit_factor')}", f"AvgRet {g('avg_ret', True)}"]
    return " | ".join(parts)
