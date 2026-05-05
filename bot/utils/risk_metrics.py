"""
Risk metrics computed from Alpaca account history.
Sharpe, Sortino, Max Drawdown, Calmar, Win Rate, Profit Factor.
Used by the weekly report and logged every cycle.
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

def _get_closed_orders(alpaca_key: str, alpaca_secret: str, base_url: str, days: int = 60) -> list[dict]:
    """Fetch closed orders from the past N days."""
    try:
        headers = {
            "APCA-API-KEY-ID": alpaca_key,
            "APCA-API-SECRET-KEY": alpaca_secret,
        }
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        resp = requests.get(
            f"{base_url}/v2/orders",
            headers=headers,
            params={"status": "closed", "after": since, "limit": 500, "direction": "asc"},
            timeout=10
        )
        return resp.json() if resp.status_code == 200 else []
    except Exception as e:
        logger.warning(f"[RiskMetrics] Could not fetch orders: {e}")
        return []

def compute_metrics(alpaca_key: str, alpaca_secret: str, base_url: str, days: int = 60) -> dict:
    """
    Compute all risk metrics. Returns a dict with all metrics.
    Safe to call at any time — returns empty dict on failure.
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

        # Win rate + profit factor from closed orders
        orders = _get_closed_orders(alpaca_key, alpaca_secret, base_url, days)
        filled = [o for o in orders if o.get("status") == "filled" and o.get("filled_avg_price")]

        # Pair buys and sells to compute per-trade P&L (simplified)
        trades_pnl = []
        sell_orders = [o for o in filled if o["side"] == "sell"]
        for o in sell_orders:
            try:
                pnl = float(o["filled_avg_price"]) * float(o.get("filled_qty") or o.get("qty", 0))
                trades_pnl.append(pnl)
            except Exception:
                continue

        win_rate = sum(1 for p in trades_pnl if p > 0) / len(trades_pnl) if trades_pnl else 0
        gross_profit = sum(p for p in trades_pnl if p > 0)
        gross_loss = abs(sum(p for p in trades_pnl if p < 0))
        profit_factor = gross_profit / (gross_loss + 1e-10)

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
