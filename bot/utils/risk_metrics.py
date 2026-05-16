"""
Risk metrics computed from Alpaca account history.
Sharpe, Sortino, Max Drawdown, Calmar, Win Rate, Profit Factor.
Used by the weekly report and logged every cycle.

Win-rate + profit-factor use REAL realised P&L from the local trades table
(via db.get_strategy_performance) — NOT Alpaca's order notional which is
gross-sell-price and meaningless as P&L.
"""
import logging
import numpy as np
import requests
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

RISK_FREE_RATE_ANNUAL = 0.05  # 5% annualised

def _get_portfolio_history(alpaca_key: str, alpaca_secret: str, base_url: str, days: int = 60) -> list[float]:
    """Fetch daily portfolio values from Alpaca account history endpoint."""
    try:
        headers = {
            "APCA-API-KEY-ID": alpaca_key,
            "APCA-API-SECRET-KEY": alpaca_secret,
        }
        resp = requests.get(
            f"{base_url}/v2/account/portfolio/history",
            headers=headers,
            params={"period": f"{days}D", "timeframe": "1D", "intraday_reporting": "market_hours"},
            timeout=10
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        equity = data.get("equity", [])
        return [float(v) for v in equity if v is not None and float(v) > 0]
    except Exception as e:
        logger.warning(f"[RiskMetrics] Could not fetch portfolio history: {e}")
        return []


def _winrate_pf_from_db(db_conn) -> tuple:
    """
    Compute win_rate and profit_factor from the local trades table using
    db.get_strategy_performance (which aggregates realized_pnl correctly).
    Returns (win_rate_fraction, profit_factor).
    """
    try:
        from db import get_strategy_performance
        perf = get_strategy_performance(db_conn)
        total_trades = 0
        total_wins = 0
        gross_profit = 0.0
        gross_loss = 0.0
        for row in perf:
            tc = int(row.get("total_trades") or 0)
            w = int(row.get("wins") or 0)
            pnl = float(row.get("total_pnl") or 0.0)
            total_trades += tc
            total_wins += w
            if pnl > 0:
                gross_profit += pnl
            else:
                gross_loss += abs(pnl)
        if total_trades == 0:
            return 0.0, 0.0
        win_rate = total_wins / total_trades
        profit_factor = gross_profit / (gross_loss + 1e-10)
        return win_rate, profit_factor
    except Exception as e:
        logger.warning(f"[RiskMetrics] DB win/PF lookup failed: {e}")
        return 0.0, 0.0


def compute_metrics(alpaca_key: str, alpaca_secret: str, base_url: str,
                    days: int = 60, db_conn=None) -> dict:
    """
    Compute all risk metrics. Returns a dict with all metrics.
    Safe to call at any time — returns empty dict on failure.

    db_conn is preferred for win/PF; falls back to a fresh connection if None.
    """
    try:
        equity_values = _get_portfolio_history(alpaca_key, alpaca_secret, base_url, days)
        if len(equity_values) < 5:
            return {"error": "Insufficient history"}

        # Daily returns
        equity = np.array(equity_values)
        daily_returns = np.diff(equity) / equity[:-1]
        rf_daily = (1 + RISK_FREE_RATE_ANNUAL) ** (1/252) - 1

        # Sharpe
        excess = daily_returns - rf_daily
        sharpe = (excess.mean() * 252) / (daily_returns.std() * np.sqrt(252) + 1e-10)

        # Sortino
        downside = daily_returns[daily_returns < 0]
        downside_std = np.sqrt((downside ** 2).mean()) * np.sqrt(252) if len(downside) > 0 else 1e-10
        sortino = (daily_returns.mean() * 252 - RISK_FREE_RATE_ANNUAL) / downside_std

        # Max drawdown
        peak = np.maximum.accumulate(equity)
        drawdown = (equity - peak) / peak
        max_dd = drawdown.min()

        # Calmar
        ann_return = daily_returns.mean() * 252
        calmar = ann_return / (abs(max_dd) + 1e-10)

        # Win rate + profit factor from local DB (real per-trade realised P&L)
        own_conn = False
        try:
            if db_conn is None:
                from db import get_connection
                db_conn = get_connection()
                own_conn = True
            win_rate, profit_factor = _winrate_pf_from_db(db_conn)
        finally:
            if own_conn and db_conn is not None:
                try:
                    db_conn.close()
                except Exception:
                    pass

        # VaR 95%
        var_95 = np.percentile(daily_returns, 5)

        metrics = {
            "sharpe": round(float(sharpe), 2),
            "sortino": round(float(sortino), 2),
            "max_drawdown_pct": round(float(max_dd) * 100, 2),
            "calmar": round(float(calmar), 2),
            "ann_return_pct": round(float(ann_return) * 100, 2),
            "ann_volatility_pct": round(float(daily_returns.std() * np.sqrt(252)) * 100, 2),
            "win_rate_pct": round(win_rate * 100, 1),
            "profit_factor": round(float(profit_factor), 2),
            "daily_var_95_pct": round(float(var_95) * 100, 2),
            "days_analysed": len(daily_returns),
        }

        # Log with health assessment
        health = "✅ HEALTHY" if sharpe > 0.5 and max_dd > -0.15 else "⚠️ REVIEW" if sharpe > 0 else "🚨 POOR"
        logger.info(
            f"[RiskMetrics] {health} | Sharpe={metrics['sharpe']} Sortino={metrics['sortino']} "
            f"MaxDD={metrics['max_drawdown_pct']}% Calmar={metrics['calmar']} "
            f"WinRate={metrics['win_rate_pct']}% PF={metrics['profit_factor']}"
        )
        return metrics

    except Exception as e:
        logger.error(f"[RiskMetrics] Compute failed: {e}")
        return {}
