import { useQuery } from "@tanstack/react-query";
import { fetchApi, API_BASE } from "@/lib/queryClient";
import { Skeleton } from "@/components/ui/skeleton";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from "recharts";
import type { StrategyPerf, DailyPnl } from "../../../shared/schema";

const STRATEGY_META: Record<string, { label: string; color: string; description: string; holds: string; signal: string }> = {
  momentum: {
    label: "Momentum",
    color: "#60a5fa",
    description: "12-1 month cross-sectional momentum on S&P 500 stocks. Buy top decile performers, rebalance monthly.",
    holds: "20–30 days",
    signal: "12-month return ranking",
  },
  mean_reversion: {
    label: "Mean Reversion",
    color: "#f59e0b",
    description: "RSI + Bollinger Band oversold signals with volume confirmation. Buy extremes, sell on reversion to mean.",
    holds: "5–15 days",
    signal: "RSI < 32 + BB lower touch + volume",
  },
  trend_following: {
    label: "Trend Following",
    color: "#4ade80",
    description: "EMA 9/21 crossover with VIX regime filter. Enter on confirmed uptrend, exit on reversal. Pauses in fear regimes.",
    holds: "2–8 weeks",
    signal: "EMA crossover + VIX < 35",
  },
  ai_research: {
    label: "AI Research",
    color: "#c084fc",
    description: "Claude reads news, filings and analyst data to build a cited thesis. A Checker Agent triple-verifies before any trade fires.",
    holds: "2–8 weeks",
    signal: "Bullish thesis + 3/3 checks passed + confidence ≥ 7/10",
  },
};

function formatCurrency(v: number) {
  const formatted = Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return (v < 0 ? "-$" : "$") + formatted;
}

export default function Strategies() {
  const { data: strategies = [], isLoading } = useQuery({
    queryKey: ["/api/strategy-breakdown"],
    queryFn: () => fetchApi<StrategyPerf[]>(`${API_BASE}/api/strategy-breakdown`),
    refetchInterval: 30000,
  });

  const { data: dailyPnl = [] } = useQuery({
    queryKey: ["/api/daily-pnl"],
    queryFn: () => fetchApi<DailyPnl[]>(`${API_BASE}/api/daily-pnl`),
  });

  // Chart data: strategy comparison
  const barData = strategies.map(s => ({
    name: STRATEGY_META[s.name]?.label || s.name,
    "Total P&L": Math.round((s.total_pnl || 0) + (s.unrealized_pnl || 0)),
    "Trades": s.total_trades || 0,
    color: STRATEGY_META[s.name]?.color || "#6b7280",
  }));

  const winRateData = strategies.map(s => ({
    name: STRATEGY_META[s.name]?.label || s.name,
    "Win Rate": s.win_rate || 0,
    "Wins": s.wins || 0,
    "Losses": s.losses || 0,
    color: STRATEGY_META[s.name]?.color || "#6b7280",
  }));

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-xl font-semibold">Strategies</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Performance breakdown per strategy</p>
      </div>

      {/* Strategy Cards */}
      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {Array(3).fill(0).map((_, i) => <Skeleton key={i} className="h-52 rounded-lg" />)}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {strategies.map(s => {
            const meta = STRATEGY_META[s.name];
            if (!meta) return null;
            const totalPnl = (s.total_pnl || 0) + (s.unrealized_pnl || 0);
            const positive = totalPnl >= 0;
            return (
              <div
                key={s.name}
                className="bg-card border border-border rounded-lg p-5 space-y-4 card-hover"
                style={{ borderTopColor: meta.color, borderTopWidth: "3px" }}
                data-testid={`strategy-detail-${s.name}`}
              >
                <div>
                  <div className="text-sm font-semibold" style={{ color: meta.color }}>{meta.label}</div>
                  <p className="text-xs text-muted-foreground mt-1 leading-relaxed">{meta.description}</p>
                </div>

                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <div className="text-xs text-muted-foreground">Total P&L</div>
                    <div className={`text-base font-semibold mono ${positive ? "text-gain" : "text-loss"}`}>
                      {positive ? "+" : ""}{formatCurrency(totalPnl)}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs text-muted-foreground">Win Rate</div>
                    <div className="text-base font-semibold mono">{(s.win_rate || 0).toFixed(0)}%</div>
                  </div>
                  <div>
                    <div className="text-xs text-muted-foreground">Trades</div>
                    <div className="text-base font-semibold mono">{s.total_trades || 0}</div>
                  </div>
                  <div>
                    <div className="text-xs text-muted-foreground">Open</div>
                    <div className="text-base font-semibold mono">{s.open_positions || 0}</div>
                  </div>
                </div>

                <div className="pt-1 border-t border-border space-y-1">
                  <div className="flex justify-between text-xs">
                    <span className="text-muted-foreground">Signal</span>
                    <span className="text-foreground text-right max-w-[140px]">{meta.signal}</span>
                  </div>
                  <div className="flex justify-between text-xs">
                    <span className="text-muted-foreground">Avg Hold</span>
                    <span className="text-foreground">{meta.holds}</span>
                  </div>
                </div>

                {/* W/L bar */}
                {(s.total_trades || 0) > 0 && (
                  <div>
                    <div className="flex justify-between text-xs text-muted-foreground mb-1">
                      <span>{s.wins || 0}W / {s.losses || 0}L</span>
                    </div>
                    <div className="h-1.5 bg-muted rounded-full overflow-hidden flex">
                      <div
                        className="h-full bg-green-400 rounded-full"
                        style={{ width: `${s.win_rate || 0}%` }}
                      />
                      <div className="h-full flex-1 bg-red-500/50 rounded-full" />
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* P&L by Strategy */}
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-sm font-medium mb-4">P&L by Strategy</div>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={barData} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 12% 18%)" vertical={false} />
              <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(215 15% 55%)" }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: "hsl(215 15% 55%)", fontFamily: "JetBrains Mono" }} axisLine={false} tickLine={false} tickFormatter={v => `$${v >= 0 ? "" : "-"}${Math.abs(v).toLocaleString()}`} />
              <Tooltip
                contentStyle={{ background: "hsl(220 14% 10%)", border: "1px solid hsl(220 12% 18%)", borderRadius: "8px", fontSize: "12px" }}
                labelStyle={{ color: "hsl(215 15% 55%)" }}
                formatter={(v: number) => [formatCurrency(v), "P&L"]}
              />
              <Bar dataKey="Total P&L" radius={[4, 4, 0, 0]}>
                {barData.map((entry, i) => (
                  <Cell key={i} fill={entry["Total P&L"] >= 0 ? entry.color : "hsl(0 72% 56%)"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Win Rate */}
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-sm font-medium mb-4">Win Rate by Strategy</div>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={winRateData} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 12% 18%)" vertical={false} />
              <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(215 15% 55%)" }} axisLine={false} tickLine={false} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 11, fill: "hsl(215 15% 55%)", fontFamily: "JetBrains Mono" }} axisLine={false} tickLine={false} tickFormatter={v => `${v}%`} />
              <Tooltip
                contentStyle={{ background: "hsl(220 14% 10%)", border: "1px solid hsl(220 12% 18%)", borderRadius: "8px", fontSize: "12px" }}
                labelStyle={{ color: "hsl(215 15% 55%)" }}
                formatter={(v: number, n: string) => [n === "Win Rate" ? `${v.toFixed(1)}%` : v, n]}
              />
              <Bar dataKey="Win Rate" radius={[4, 4, 0, 0]}>
                {winRateData.map((entry, i) => (
                  <Cell key={i} fill={entry.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
