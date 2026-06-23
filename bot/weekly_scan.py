#!/usr/bin/env python3
"""
AlphaBot Weekly Conviction Scanner
Runs every Sunday night to identify high-conviction long candidates.
Called by a scheduled cron, NOT by main.py.

Run with bot/ on the path (same as main.py):  python bot/weekly_scan.py
"""
import logging

from db import init_db
from broker import AlpacaBroker, tag_symbol  # noqa: F401  (tag_symbol used inside strategy)
from strategies.conviction_long import run_weekly_scan, open_conviction_positions


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("alphabot.weekly_scan")

    logger.info("=== AlphaBot Weekly Conviction Scanner ===")

    # Ensure the conviction_scan_log + positions_state tables exist.
    try:
        init_db()
    except Exception as e:
        logger.warning(f"init_db failed (continuing): {e}")

    # Get account state
    broker = AlpacaBroker()
    try:
        account = broker.get_account()
    except Exception as e:
        logger.error(f"Could not fetch account info — aborting scan: {e}")
        return
    if not account:
        logger.error("Could not fetch account info — aborting scan")
        return

    equity = float(account.get("equity", 0) or 0)
    logger.info(f"Account equity: ${equity:,.2f}")

    # Run the scan
    candidates = run_weekly_scan()
    logger.info(f"Found {len(candidates)} candidates above threshold")

    if not candidates:
        logger.info("No candidates this week — no positions opened")
        return

    # Open positions
    open_conviction_positions(broker, candidates, account)
    logger.info("Weekly scan complete")


if __name__ == "__main__":
    main()
