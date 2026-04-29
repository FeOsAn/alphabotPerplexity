# AlphaBot — Setup Guide

## Step 1: Add your API keys

Create a `.env` file in `/alphabot/`:

```bash
ALPACA_API_KEY=your_api_key_here
ALPACA_SECRET_KEY=your_secret_key_here

# Use paper trading URL to test without real money (recommended to start)
ALPACA_BASE_URL=https://paper-api.alpaca.markets

# Switch to live trading once you're confident:
# ALPACA_BASE_URL=https://api.alpaca.markets
```

Then load it before running:
```bash
export $(cat .env | xargs)
```

Or use `python-dotenv` — add `from dotenv import load_dotenv; load_dotenv()` at the top of `bot/main.py`.

---

## Step 2: Run the bot

```bash
# Install dependencies (once)
pip install -r requirements.txt

# Start the trading bot
cd alphabot/bot
python main.py
```

The bot will:
- Run every **5 minutes** during market hours
- Execute all 3 strategies and log trades to SQLite
- Take portfolio snapshots hourly + at EOD

---

## Step 3: Run the API server

```bash
# In a separate terminal
cd alphabot/api
python server.py
```

The API runs at `http://localhost:8000`. Docs available at `http://localhost:8000/docs`.

---

## Step 4: Run the Dashboard

```bash
cd alphabot/dashboard
npm install
npm run dev
# → open http://localhost:5000
```

The dashboard auto-refreshes every 30 seconds.

---

## Strategy Summary

| Strategy | Signal | Hold Period | Risk |
|----------|--------|-------------|------|
| **Momentum** | 12-month return ranking, monthly rebalance | 20-30 days | 7% stop loss |
| **Mean Reversion** | RSI < 32 + Bollinger Band lower + volume spike | 5-15 days | 7% stop loss, 12% take profit |
| **Trend Following** | EMA 9/21 crossover + VIX < 35 regime filter | 2-8 weeks | 7% stop loss, 20% take profit |

## Risk Parameters (editable in `bot/config.py`)

- Max position size: **5% of portfolio per stock**
- Max simultaneous positions: **15**
- Cash reserve: **10% minimum** always kept
- Stop loss: **7% per position**

---

## Adding More Strategies

1. Create `bot/strategies/your_strategy.py` with a `run(broker, db_conn)` function
2. Import and call it in `bot/main.py` inside `run_all_strategies()`
3. The dashboard will automatically pick up the new strategy in all charts

---

## Notes

- The bot starts in **paper trading mode** by default — no real money at risk
- Always backtest / paper trade before going live
- Past performance of any strategy does NOT guarantee future results
