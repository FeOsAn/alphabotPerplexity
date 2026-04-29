import { useQuery } from "@tanstack/react-query";
import { fetchApi, API_BASE } from "@/lib/queryClient";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import type { Position, AccountInfo } from "../../../shared/schema";

const STRATEGY_COLORS: Record<string, string> = {
  momentum: "text-blue-400 bg-blue-400/10 border-blue-400/20",
  mean_reversion: "text-amber-400 bg-amber-400/10 border-amber-400/20",
  trend_following: "text-green-400 bg-green-400/10 border-green-400/20",
  unknown: "text-gray-400 bg-gray-400/10 border-gray-400/20",
};
const STRATEGY_LABELS: Record<string, string> = {
  momentum: "Momentum",
  mean_reversion: "Mean Rev.",
  trend_following: "Trend",
};

function formatCurrency(v: number) {
  const formatted = Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return (v < 0 ? "-$" : "$") + formatted;
}

export default function Positions() {
  const { data: positions = [], isLoading } = useQuery({
    queryKey: ["/api/positions"],
    queryFn: () => fetchApi<Position[]>(`${API_BASE}/api/positions`),
    refetchInterval: 15000,
  });

  const { data: account } = useQuery({
    queryKey: ["/api/account"],
    queryFn: () => fetchApi<AccountInfo>(`${API_BASE}/api/account`),
  });

  const totalMV = positions.reduce((s, p) => s + p.market_value, 0);
  const totalPnl = positions.reduce((s, p) => s + p.unrealized_pnl, 0);
  const portfolioVal = account?.portfolio_value || 1;

  // Group positions by strategy
  const grouped: Record<string, Position[]> = {};
  for (const pos of positions) {
    const key = pos.strategy || "unknown";
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(pos);
  }

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Positions</h1>
        <p className="text-sm text-muted-foreground mt-0.5">
          {positions.length} open positions across all strategies
        </p>
      </div>

      {/* Summary bar */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Invested</div>
          <div className="text-lg font-semibold mono">{formatCurrency(totalMV)}</div>
          <div className="text-xs text-muted-foreground">{((totalMV / portfolioVal) * 100).toFixed(1)}% of portfolio</div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Unrealized P&L</div>
          <div className={`text-lg font-semibold mono ${totalPnl >= 0 ? "text-gain" : "text-loss"}`}>
            {totalPnl >= 0 ? "+" : ""}{formatCurrency(totalPnl)}
          </div>
          <div className="text-xs text-muted-foreground">across all positions</div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Best Performer</div>
          {positions.length > 0 ? (() => {
            const best = [...positions].sort((a, b) => b.unrealized_pnl_pct - a.unrealized_pnl_pct)[0];
            return <>
              <div className="text-lg font-semibold mono text-gain">{best.symbol}</div>
              <div className="text-xs text-gain">+{best.unrealized_pnl_pct.toFixed(2)}%</div>
            </>;
          })() : <div className="text-lg text-muted-foreground">—</div>}
        </div>
      </div>

      {/* Positions table */}
      {isLoading ? (
        <div className="space-y-2">
          {Array(5).fill(0).map((_, i) => <Skeleton key={i} className="h-14 rounded-lg" />)}
        </div>
      ) : positions.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <div className="text-muted-foreground text-sm">No open positions</div>
          <div className="text-xs text-muted-foreground mt-1">Bot will open positions when signals trigger</div>
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/30">
                <th className="text-left px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Symbol</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Strategy</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Qty</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Avg Entry</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Current</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Mkt Value</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">P&L</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">%</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {[...positions].sort((a, b) => b.unrealized_pnl - a.unrealized_pnl).map(pos => {
                const pnlPos = pos.unrealized_pnl >= 0;
                const stratClass = STRATEGY_COLORS[pos.strategy] || STRATEGY_COLORS.unknown;
                return (
                  <tr key={pos.symbol} className="hover:bg-accent/30 transition-colors" data-testid={`position-${pos.symbol}`}>
                    <td className="px-4 py-3">
                      <span className="font-semibold mono text-sm">{pos.symbol}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full border font-medium ${stratClass}`}>
                        {STRATEGY_LABELS[pos.strategy] || pos.strategy}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right mono">{pos.qty}</td>
                    <td className="px-4 py-3 text-right mono">${pos.avg_entry?.toFixed(2)}</td>
                    <td className="px-4 py-3 text-right mono">${pos.current_price?.toFixed(2)}</td>
                    <td className="px-4 py-3 text-right mono">${pos.market_value?.toLocaleString("en-US", { maximumFractionDigits: 0 })}</td>
                    <td className={`px-4 py-3 text-right mono font-medium ${pnlPos ? "text-gain" : "text-loss"}`}>
                      {pnlPos ? "+" : ""}{formatCurrency(pos.unrealized_pnl)}
                    </td>
                    <td className={`px-4 py-3 text-right mono font-medium ${pnlPos ? "text-gain" : "text-loss"}`}>
                      <span className={`px-2 py-0.5 rounded text-xs ${pnlPos ? "bg-gain-subtle" : "bg-loss-subtle"}`}>
                        {pnlPos ? "+" : ""}{pos.unrealized_pnl_pct?.toFixed(2)}%
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
