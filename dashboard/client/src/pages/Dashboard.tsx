import { useQuery } from "@tanstack/react-query";
import { fetchApi, API_BASE } from "@/lib/queryClient";
import StatCard from "@/components/StatCard";
import { Skeleton } from "@/components/ui/skeleton";
import { DollarSign, TrendingUp, Activity, Wallet } from "lucide-react";
import {
  AreaChart, Area, LineChart, Line, XAxis, YAxis, Tooltip,
  CartesianGrid, ResponsiveContainer, ReferenceLine
} from "recharts";
import type { AccountInfo, Position, StrategyPerf, Snapshot } from "../../../shared/schema";

const STRATEGY_COLORS: Record<string, string> = {
  momentum: "#60a5fa",
  mean_reversion: "#f59e0b",
  trend_following: "#4ade80",
  unknown: "#6b7280",
};
const STRATEGY_LABELS: Record<string, string> = {
  momentum: "Momentum",
  mean_reversion: "Mean Reversion",
  trend_following: "Trend Following",
};

function formatCurrency(v: number) {
  return "$" + Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function CustomTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-card border border-border rounded-lg p-3 text-xs shadow-xl">
      <div className="text-muted-foreground mb-1">{label}</div>
      {payload.map((p: any) => (
        <div key={p.name} className="flex gap-3 justify-between">
          <span style={{ color: p.color }}>{p.name}</span>
          <span className="mono font-medium">${p.value?.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 })}</span>
        </div>
      ))}
    </div>
  );
}

export default function Dashboard() {
  const { data: account, isLoading: loadingAccount } = useQuery({
    queryKey: ["/api/account"],
    queryFn: () => fetchApi<AccountInfo>(`${API_BASE}/api/account`),
    refetchInterval: 30000,
  });

  const { data: positions = [] } = useQuery({
    queryKey: ["/api/positions"],
    queryFn: () => fetchApi<Position[]>(`${API_BASE}/api/positions`),
    refetchInterval: 30000,
  });

  const { data: snapshots = [] } = useQuery({
    queryKey: ["/api/snapshots"],
    queryFn: () => fetchApi<Snapshot[]>(`${API_BASE}/api/snapshots`),
  });

  const { data: stratPerf = [] } = useQuery({
    queryKey: ["/api/strategy-breakdown"],
    queryFn: () => fetchApi<StrategyPerf[]>(`${API_BASE}/api/strategy-breakdown`),
    refetchInterval: 30000,
  });

  // Build equity curve data comparing portfolio vs SPY
  const chartData = [...snapshots]
    .sort((a, b) => a.snapshot_at.localeCompare(b.snapshot_at))
    .map(s => {
      const date = s.snapshot_at.slice(0, 10);
      return {
        date,
        Portfolio: Math.round(s.portfolio_value),
        SPY: s.spy_price ? Math.round((s.spy_price / snapshots[snapshots.length - 1]?.spy_price) * (account?.portfolio_value || 100000)) : null,
      };
    });

  // Normalize both to 100 at start for % comparison
  const startPortfolio = chartData[0]?.Portfolio || 1;
  const normalizedData = chartData.map(d => ({
    date: d.date,
    AlphaBot: d.Portfolio ? +((d.Portfolio / startPortfolio) * 100 - 100).toFixed(2) : null,
    SPY: d.SPY ? +((d.SPY / (chartData[0]?.SPY || 1)) * 100 - 100).toFixed(2) : null,
  }));

  const totalUnrealizedPnl = positions.reduce((sum, p) => sum + p.unrealized_pnl, 0);
  const pnlPositive = (account?.pnl_today ?? 0) >= 0;
  const totalPnlPositive = totalUnrealizedPnl >= 0;

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold text-foreground">Overview</h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            Multi-strategy trading dashboard
            {account?.demo && <span className="ml-2 text-amber-400 text-xs">(Demo Mode)</span>}
          </p>
        </div>
        <div className="text-xs text-muted-foreground mono">
          {new Date().toLocaleString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}
        </div>
      </div>

      {/* KPI Cards */}
      {loadingAccount ? (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {Array(4).fill(0).map((_, i) => <Skeleton key={i} className="h-24 rounded-lg" />)}
        </div>
      ) : (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Portfolio Value"
            value={formatCurrency(account?.portfolio_value ?? 0)}
            sub={`${formatCurrency(account?.cash ?? 0)} cash`}
            icon={<Wallet size={16} />}
            data-testid="stat-portfolio"
          />
          <StatCard
            label="Today's P&L"
            value={(pnlPositive ? "+" : "") + formatCurrency(account?.pnl_today ?? 0)}
            sub={`${(account?.pnl_today_pct ?? 0) >= 0 ? "+" : ""}${(account?.pnl_today_pct ?? 0).toFixed(2)}%`}
            positive={pnlPositive}
            negative={!pnlPositive}
            icon={<DollarSign size={16} />}
            data-testid="stat-pnl-today"
          />
          <StatCard
            label="Unrealized P&L"
            value={(totalPnlPositive ? "+" : "") + formatCurrency(totalUnrealizedPnl)}
            sub={`${positions.length} open positions`}
            positive={totalPnlPositive && totalUnrealizedPnl !== 0}
            negative={!totalPnlPositive && totalUnrealizedPnl !== 0}
            icon={<Activity size={16} />}
            data-testid="stat-unrealized"
          />
          <StatCard
            label="Open Positions"
            value={positions.length.toString()}
            sub={`$${(account?.buying_power ?? 0).toLocaleString("en-US", { maximumFractionDigits: 0 })} buying power`}
            icon={<TrendingUp size={16} />}
            data-testid="stat-positions"
          />
        </div>
      )}

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Equity Curve vs SPY */}
        <div className="lg:col-span-2 bg-card border border-border rounded-lg p-4">
          <div className="flex items-center justify-between mb-4">
            <div>
              <div className="text-sm font-medium">AlphaBot vs SPY</div>
              <div className="text-xs text-muted-foreground">Relative returns (%)</div>
            </div>
            <div className="flex items-center gap-4 text-xs">
              <div className="flex items-center gap-1.5">
                <div className="w-3 h-0.5 bg-green-400 rounded" />
                <span className="text-muted-foreground">AlphaBot</span>
              </div>
              <div className="flex items-center gap-1.5">
                <div className="w-3 h-0.5 bg-blue-500 rounded" style={{borderStyle: 'dashed'}} />
                <span className="text-muted-foreground">SPY</span>
              </div>
            </div>
          </div>
          {normalizedData.length < 2 ? (
            <div className="flex items-center justify-center h-[220px] text-muted-foreground text-sm">
              Waiting for portfolio data — updates every hour
            </div>
          ) : (
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={normalizedData} margin={{ top: 5, right: 5, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="hsl(220 12% 18%)" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 11, fill: "hsl(215 15% 55%)" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={v => v.slice(5)}
              />
              <YAxis
                tick={{ fontSize: 11, fill: "hsl(215 15% 55%)", fontFamily: "JetBrains Mono" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={v => `${v > 0 ? "+" : ""}${v.toFixed(1)}%`}
              />
              <Tooltip content={<CustomTooltip />} />
              <ReferenceLine y={0} stroke="hsl(220 12% 25%)" strokeDasharray="3 3" />
              <Line type="monotone" dataKey="AlphaBot" stroke="#4ade80" strokeWidth={2} dot={false} name="AlphaBot" />
              <Line type="monotone" dataKey="SPY" stroke="#60a5fa" strokeWidth={1.5} strokeDasharray="4 2" dot={false} name="SPY" />
            </LineChart>
          </ResponsiveContainer>
          )}
        </div>

        {/* Strategy Performance */}
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-sm font-medium mb-4">Strategy Breakdown</div>
          <div className="space-y-4">
            {stratPerf.length === 0 && (
              <div className="flex items-center justify-center h-32 text-muted-foreground text-sm">
                No trades yet — strategies fire at market open
              </div>
            )}
            {stratPerf.map(s => {
              const color = STRATEGY_COLORS[s.name] || "#6b7280";
              const label = STRATEGY_LABELS[s.name] || s.name;
              const pnl = (s.total_pnl || 0) + (s.unrealized_pnl || 0);
              const positive = pnl >= 0;
              return (
                <div key={s.name} className="space-y-1.5" data-testid={`strategy-card-${s.name}`}>
                  <div className="flex justify-between items-center">
                    <div className="flex items-center gap-2">
                      <span className="w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
                      <span className="text-sm font-medium" style={{ color }}>{label}</span>
                    </div>
                    <span className={`text-sm mono font-medium ${positive ? "text-gain" : "text-loss"}`}>
                      {positive ? "+" : ""}{formatCurrency(pnl)}
                    </span>
                  </div>
                  <div className="flex justify-between text-xs text-muted-foreground">
                    <span>{s.open_positions} positions</span>
                    <span>{s.win_rate?.toFixed(0) || 0}% win rate</span>
                    <span>{s.total_trades || 0} trades</span>
                  </div>
                  {/* PnL bar */}
                  <div className="h-1 bg-muted rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all"
                      style={{
                        width: `${Math.min(Math.abs(pnl) / 2000 * 100, 100)}%`,
                        backgroundColor: positive ? color : "hsl(0 72% 56%)",
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Open Positions Preview */}
      {positions.length > 0 && (
        <div className="bg-card border border-border rounded-lg">
          <div className="px-4 py-3 border-b border-border flex justify-between items-center">
            <div className="text-sm font-medium">Open Positions</div>
            <a href="/#/positions" className="text-xs text-primary hover:underline">View all →</a>
          </div>
          <div className="divide-y divide-border">
            {positions.slice(0, 5).map(pos => {
              const color = STRATEGY_COLORS[pos.strategy] || "#6b7280";
              const pnlPos = pos.unrealized_pnl >= 0;
              return (
                <div key={pos.symbol} className="px-4 py-3 flex items-center justify-between hover:bg-accent/40 transition-colors" data-testid={`position-row-${pos.symbol}`}>
                  <div className="flex items-center gap-3">
                    <div className="w-1.5 h-8 rounded-full" style={{ backgroundColor: color }} />
                    <div>
                      <div className="text-sm font-semibold mono">{pos.symbol}</div>
                      <div className="text-xs text-muted-foreground" style={{ color }}>
                        {STRATEGY_LABELS[pos.strategy] || pos.strategy}
                      </div>
                    </div>
                  </div>
                  <div className="text-center">
                    <div className="text-xs text-muted-foreground">Qty</div>
                    <div className="text-sm mono">{pos.qty}</div>
                  </div>
                  <div className="text-center">
                    <div className="text-xs text-muted-foreground">Entry</div>
                    <div className="text-sm mono">${pos.avg_entry?.toFixed(2)}</div>
                  </div>
                  <div className="text-center">
                    <div className="text-xs text-muted-foreground">Current</div>
                    <div className="text-sm mono">${pos.current_price?.toFixed(2)}</div>
                  </div>
                  <div className="text-right">
                    <div className={`text-sm font-medium mono ${pnlPos ? "text-gain" : "text-loss"}`}>
                      {pnlPos ? "+" : ""}{formatCurrency(pos.unrealized_pnl)}
                    </div>
                    <div className={`text-xs mono ${pnlPos ? "text-gain" : "text-loss"}`}>
                      {pnlPos ? "+" : ""}{pos.unrealized_pnl_pct?.toFixed(2)}%
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
