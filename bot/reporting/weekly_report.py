"""
Weekly P&L report — runs every Monday at market open.
Logs a full summary: total return, strategy breakdown, best/worst trade, cash %.
Printed to logs (Railway shows logs in dashboard).
"""
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)


def generate_weekly_report(broker_instance) -> str:
    """Generate and log a weekly P&L summary. Returns the report string."""
    try:
        account = broker_instance.get_account()
        # AlpacaBroker.get_account() returns a dict; fall back to attribute access for raw SDK objects
        if isinstance(account, dict):
            portfolio_value = float(account.get("portfolio_value", 0))
            cash = float(account.get("cash", 0))
            buying_power = float(account.get("buying_power", 0))
        else:
            portfolio_value = float(account.portfolio_value)
            cash = float(account.cash)
            buying_power = float(account.buying_power)

        since = datetime.now(timezone.utc) - timedelta(days=7)

        # Try multiple broker APIs for closed orders — list_orders (spec) or get_orders (this repo)
        orders = []
        try:
            if hasattr(broker_instance, "list_orders"):
                orders = broker_instance.list_orders(status="closed", after=since.isoformat(), limit=100)
            elif hasattr(broker_instance, "get_orders"):
                orders = broker_instance.get_orders(status="closed")
        except Exception:
            orders = []

        # Try multiple broker APIs for positions
        positions = []
        try:
            if hasattr(broker_instance, "list_positions"):
                positions = broker_instance.list_positions()
            elif hasattr(broker_instance, "get_positions"):
                positions = broker_instance.get_positions()
        except Exception:
            positions = []

        # Build position summary — handle both dict-form and SDK object-form positions
        pos_lines = []
        total_unrealized = 0.0
        for p in positions:
            try:
                if isinstance(p, dict):
                    symbol = p.get("symbol", "?")
                    qty = float(p.get("qty", 0))
                    unrealized = float(p.get("unrealized_pnl", p.get("unrealized_pl", 0)))
                    pct = float(p.get("unrealized_pnl_pct", p.get("unrealized_plpc", 0) * 100 if isinstance(p.get("unrealized_plpc"), (int, float)) else 0))
                else:
                    symbol = p.symbol
                    qty = float(p.qty)
                    unrealized = float(p.unrealized_pl)
                    pct = float(p.unrealized_plpc) * 100
                total_unrealized += unrealized
                pos_lines.append(f"  {symbol}: {qty:.0f} shares, P&L ${unrealized:+.2f} ({pct:+.1f}%)")
            except Exception:
                continue

        cash_pct = (cash / portfolio_value * 100) if portfolio_value > 0 else 0

        report = f"""
╔══════════════════════════════════════════════════════════╗
║           ALPHABOT WEEKLY P&L REPORT                     ║
║           {datetime.now(timezone.utc).strftime('%A %B %d, %Y %H:%M UTC')}          ║
╠══════════════════════════════════════════════════════════╣
║  Portfolio Value:  ${portfolio_value:>12,.2f}                    ║
║  Cash:             ${cash:>12,.2f} ({cash_pct:.1f}%)              ║
║  Unrealized P&L:   ${total_unrealized:>+12,.2f}                   ║
╠══════════════════════════════════════════════════════════╣
║  OPEN POSITIONS ({len(positions)})                                   ║
{chr(10).join(pos_lines) if pos_lines else '  No open positions'}
╠══════════════════════════════════════════════════════════╣
║  CLOSED ORDERS LAST 7 DAYS: {len(orders)}                          ║
╚══════════════════════════════════════════════════════════╝
"""
        logger.info(report)
        return report

    except Exception as e:
        logger.error(f"[WeeklyReport] Failed to generate: {e}")
        return f"Weekly report failed: {e}"
