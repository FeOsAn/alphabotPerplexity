import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchApi, API_BASE } from "@/lib/queryClient";
import { Skeleton } from "@/components/ui/skeleton";
import type { Trade } from "../../../shared/schema";

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
const SIDE_COLORS: Record<string, string> = {
  buy: "text-gain bg-gain-subtle",
  sell: "text-loss bg-loss-subtle",
  sell_stop: "text-red-500 bg-red-500/10",
  sell_tp: "text-gain bg-gain-subtle",
};

function formatCurrency(v: number) {
  const formatted = Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return (v < 0 ? "-$" : "$") + formatted;
}

export default function Trades() {
  const [filter, setFilter] = useState<string>("all");

  const { data: trades = [], isLoading } = useQuery({
    queryKey: ["/api/trades", filter],
    queryFn: () => fetchApi<Trade[]>(`${API_BASE}/api/trades?strategy=${filter === "all" ? "" : filter}&limit=100`),
    refetchInterval: 30000,
  });

  const strategies = ["all", "momentum", "mean_reversion", "trend_following"];

  const totalRealizedPnl = trades.reduce((s, t) => s + (t.pnl || 0), 0);
  const wins = trades.filter(t => t.pnl > 0).length;
  const losses = trades.filter(t => t.pnl < 0).length;
  const closedTrades = trades.filter(t => t.side !== "buy");
  const winRate = closedTrades.length > 0 ? (closedTrades.filter(t => t.pnl > 0).length / closedTrades.length * 100) : 0;

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Trade Log</h1>
          <p className="text-sm text-muted-foreground mt-0.5">{trades.length} trades</p>
        </div>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-4 gap-4">
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Realized P&L</div>
          <div className={`text-lg font-semibold mono ${totalRealizedPnl >= 0 ? "text-gain" : "text-loss"}`}>
            {totalRealizedPnl >= 0 ? "+" : ""}{formatCurrency(totalRealizedPnl)}
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Win Rate</div>
          <div className="text-lg font-semibold mono">{winRate.toFixed(1)}%</div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Wins / Losses</div>
          <div className="text-lg font-semibold mono">
            <span className="text-gain">{wins}</span>
            <span className="text-muted-foreground mx-1">/</span>
            <span className="text-loss">{losses}</span>
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Total Trades</div>
          <div className="text-lg font-semibold mono">{trades.length}</div>
        </div>
      </div>

      {/* Filter tabs */}
      <div className="flex gap-2">
        {strategies.map(s => (
          <button
            key={s}
            onClick={() => setFilter(s)}
            className={`text-xs px-3 py-1.5 rounded-full border font-medium transition-colors ${
              filter === s
                ? "bg-primary text-primary-foreground border-primary"
                : "border-border text-muted-foreground hover:text-foreground hover:border-muted-foreground"
            }`}
            data-testid={`filter-${s}`}
          >
            {s === "all" ? "All Strategies" : (STRATEGY_LABELS[s] || s)}
          </button>
        ))}
      </div>

      {/* Trade table */}
      {isLoading ? (
        <div className="space-y-2">
          {Array(8).fill(0).map((_, i) => <Skeleton key={i} className="h-12 rounded-lg" />)}
        </div>
      ) : trades.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <div className="text-muted-foreground text-sm">No trades logged yet</div>
          <div className="text-xs text-muted-foreground mt-1">Trades will appear here once the bot executes</div>
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/30">
                <th className="text-left px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Time</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Symbol</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Strategy</th>
                <th className="text-left px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Action</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Qty</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">Price</th>
                <th className="text-right px-4 py-3 text-xs font-medium text-muted-foreground uppercase tracking-wider">P&L</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {trades.map(trade => {
                const pnlPositive = (trade.pnl || 0) >= 0;
                const stratClass = STRATEGY_COLORS[trade.strategy] || STRATEGY_COLORS.unknown;
                const sideClass = SIDE_COLORS[trade.side] || "text-muted-foreground";
                return (
                  <tr key={trade.id} className="hover:bg-accent/30 transition-colors" data-testid={`trade-${trade.id}`}>
                    <td className="px-4 py-3 text-xs text-muted-foreground mono whitespace-nowrap">
                      {new Date(trade.created_at).toLocaleString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}
                    </td>
                    <td className="px-4 py-3 font-semibold mono">{trade.symbol}</td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full border ${stratClass}`}>
                        {STRATEGY_LABELS[trade.strategy] || trade.strategy}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded font-medium uppercase ${sideClass}`}>
                        {trade.side === "sell_stop" ? "Stop" : trade.side === "sell_tp" ? "Take Profit" : trade.side}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right mono text-muted-foreground">{trade.qty || "—"}</td>
                    <td className="px-4 py-3 text-right mono">
                      {trade.price ? `$${trade.price.toFixed(2)}` : "—"}
                    </td>
                    <td className={`px-4 py-3 text-right mono font-medium ${trade.pnl === 0 ? "text-muted-foreground" : pnlPositive ? "text-gain" : "text-loss"}`}>
                      {trade.pnl === 0 ? "—" : `${pnlPositive ? "+" : ""}${formatCurrency(trade.pnl)}`}
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
