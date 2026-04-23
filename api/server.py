"""
AlphaBot API Server — FastAPI backend for the dashboard
Serves portfolio data, positions, trades, and strategy performance.
"""
import os
import sys
import json
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# Add bot directory to path so we can import from it
BOT_DIR = os.path.join(os.path.dirname(__file__), "../bot")
sys.path.insert(0, BOT_DIR)

from db import get_connection, get_trades, get_strategy_performance, get_daily_pnl, get_snapshots, init_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alphabot.api")

app = FastAPI(title="AlphaBot API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Lazy broker initialization (only if API keys are present)
_broker = None

def get_broker():
    global _broker
    if _broker is None:
        api_key = os.environ.get("ALPACA_API_KEY", "")
        secret_key = os.environ.get("ALPACA_SECRET_KEY", "")
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            return None
        try:
            from broker import AlpacaBroker
            _broker = AlpacaBroker()
        except Exception as e:
            logger.warning(f"Could not initialize broker: {e}")
            return None
    return _broker


@app.on_event("startup")
async def startup():
    init_db()
    logger.info("AlphaBot API started")


@app.get("/api/health")
def health():
    broker = get_broker()
    return {
        "status": "ok",
        "broker_connected": broker is not None,
        "market_open": broker.is_market_open() if broker else None,
    }


@app.get("/api/account")
def get_account():
    broker = get_broker()
    if not broker:
        # Return mock data for dashboard demo
        return {
            "portfolio_value": 100000.0,
            "cash": 45000.0,
            "equity": 100000.0,
            "buying_power": 90000.0,
            "pnl_today": 1250.50,
            "pnl_today_pct": 1.25,
            "demo": True,
        }
    try:
        return broker.get_account()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/positions")
def get_positions():
    broker = get_broker()
    if not broker:
        # Demo positions
        return [
            {"symbol": "AAPL", "qty": 25, "avg_entry": 185.20, "current_price": 192.40, "market_value": 4810.0, "unrealized_pnl": 180.0, "unrealized_pnl_pct": 3.88, "side": "long", "strategy": "momentum"},
            {"symbol": "MSFT", "qty": 15, "avg_entry": 415.80, "current_price": 428.50, "market_value": 6427.5, "unrealized_pnl": 190.5, "unrealized_pnl_pct": 3.05, "side": "long", "strategy": "momentum"},
            {"symbol": "NVDA", "qty": 18, "avg_entry": 875.40, "current_price": 912.30, "market_value": 16421.4, "unrealized_pnl": 664.2, "unrealized_pnl_pct": 4.21, "side": "long", "strategy": "trend_following"},
            {"symbol": "JPM", "qty": 30, "avg_entry": 198.60, "current_price": 195.20, "market_value": 5856.0, "unrealized_pnl": -102.0, "unrealized_pnl_pct": -1.71, "side": "long", "strategy": "mean_reversion"},
            {"symbol": "META", "qty": 12, "avg_entry": 512.30, "current_price": 534.80, "market_value": 6417.6, "unrealized_pnl": 270.0, "unrealized_pnl_pct": 4.39, "side": "long", "strategy": "trend_following"},
        ]
    try:
        return broker.get_positions()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/orders")
def get_orders(status: str = "open"):
    broker = get_broker()
    if not broker:
        return []
    try:
        return broker.get_orders(status)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trades")
def get_trades_endpoint(strategy: Optional[str] = None, limit: int = 50):
    conn = get_connection()
    try:
        trades = get_trades(conn, strategy=strategy, limit=limit)
        # Parse metadata JSON
        for t in trades:
            if t.get("metadata"):
                try:
                    t["metadata"] = json.loads(t["metadata"])
                except Exception:
                    pass
        return trades
    finally:
        conn.close()


@app.get("/api/strategy-performance")
def get_strategy_perf():
    conn = get_connection()
    try:
        return get_strategy_performance(conn)
    finally:
        conn.close()


@app.get("/api/daily-pnl")
def get_daily_pnl_endpoint(strategy: Optional[str] = None, days: int = 30):
    conn = get_connection()
    try:
        return get_daily_pnl(conn, strategy=strategy, days=days)
    finally:
        conn.close()


@app.get("/api/snapshots")
def get_snapshots_endpoint(limit: int = 90):
    conn = get_connection()
    try:
        snaps = get_snapshots(conn, limit=limit)
        # If no real data, return demo equity curve
        if not snaps:
            import random
            from datetime import datetime, timedelta
            random.seed(42)
            snaps = []
            val = 100000.0
            spy = 510.0
            for i in range(60, 0, -1):
                date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                val *= (1 + random.gauss(0.0008, 0.012))
                spy *= (1 + random.gauss(0.0005, 0.010))
                snaps.append({
                    "portfolio_value": round(val, 2),
                    "spy_price": round(spy, 2),
                    "snapshot_at": date,
                    "pnl_today": round(val * random.gauss(0.0008, 0.012), 2),
                    "cash": round(val * 0.3, 2),
                    "equity": round(val, 2),
                })
        return snaps
    finally:
        conn.close()


@app.get("/api/strategy-breakdown")
def get_strategy_breakdown():
    """Returns per-strategy summary including positions and performance."""
    conn = get_connection()
    broker = get_broker()

    try:
        perf = get_strategy_performance(conn)
        positions = broker.get_positions() if broker else []

        strategies = {}
        for p in perf:
            name = p["strategy"]
            strategies[name] = {
                "name": name,
                "total_pnl": p["total_pnl"] or 0,
                "total_trades": p["total_trades"] or 0,
                "win_rate": p["win_rate"] or 0,
                "wins": p["wins"] or 0,
                "losses": p["losses"] or 0,
                "open_positions": 0,
                "unrealized_pnl": 0,
            }

        for pos in positions:
            strat = pos.get("strategy", "unknown")
            if strat not in strategies:
                strategies[strat] = {
                    "name": strat,
                    "total_pnl": 0,
                    "total_trades": 0,
                    "win_rate": 0,
                    "wins": 0,
                    "losses": 0,
                    "open_positions": 0,
                    "unrealized_pnl": 0,
                }
            strategies[strat]["open_positions"] += 1
            strategies[strat]["unrealized_pnl"] += pos.get("unrealized_pnl", 0)

        # Ensure all three strategies always appear
        for name in ["momentum", "mean_reversion", "trend_following"]:
            if name not in strategies:
                strategies[name] = {
                    "name": name,
                    "total_pnl": 0,
                    "total_trades": 0,
                    "win_rate": 0,
                    "wins": 0,
                    "losses": 0,
                    "open_positions": 0,
                    "unrealized_pnl": 0,
                }

        return list(strategies.values())
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)


@app.get("/api/research-log")
def get_research_log(limit: int = 50):
    """Return AI research signals including blocked trades for audit trail."""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT * FROM signals
            WHERE strategy = 'ai_research'
            ORDER BY created_at DESC LIMIT ?
        """, (limit,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("metadata"):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except Exception:
                    pass
            result.append(d)
        return result
    finally:
        conn.close()


@app.get("/api/ai-status")
def get_ai_status():
    """Check whether AI research strategy is configured and ready."""
    has_key = bool(os.environ.get("ANTHROPIC_API_KEY", ""))
    return {
        "ready": has_key,
        "message": "AI Research strategy active" if has_key else "Add ANTHROPIC_API_KEY to .env to activate Strategy 4",
    }
