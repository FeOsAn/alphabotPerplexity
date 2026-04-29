import { Link, useLocation } from "wouter";
import { BarChart2, TrendingUp, Layers, History, Circle, Brain } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { fetchApi, API_BASE } from "@/lib/queryClient";

const NAV = [
  { href: "/", label: "Overview", icon: BarChart2 },
  { href: "/positions", label: "Positions", icon: Layers },
  { href: "/strategies", label: "Strategies", icon: TrendingUp },
  { href: "/trades", label: "Trade Log", icon: History },
  { href: "/ai-research", label: "AI Research", icon: Brain },
];

const STRATEGY_COLORS: Record<string, string> = {
  momentum: "text-blue-400",
  mean_reversion: "text-amber-400",
  trend_following: "text-green-400",
  ai_research: "text-purple-400",
  earnings_drift: "text-orange-400",
  sector_rotation: "text-cyan-400",
};
const STRATEGY_LABELS: Record<string, string> = {
  momentum: "Momentum",
  mean_reversion: "Mean Rev.",
  trend_following: "Trend",
  ai_research: "AI Research",
  earnings_drift: "Earn. Drift",
  sector_rotation: "Sector Rot.",
};

export default function Sidebar() {
  const [location] = useLocation();

  const { data: health } = useQuery({
    queryKey: ["/api/health"],
    queryFn: () => fetchApi<{ status: string; broker_connected: boolean; market_open: boolean | null }>(`${API_BASE}/api/health`),
    refetchInterval: 10000,
  });

  const { data: account } = useQuery({
    queryKey: ["/api/account"],
    queryFn: () => fetchApi<any>(`${API_BASE}/api/account`),
    refetchInterval: 30000,
  });

  const isMarketOpen = health?.market_open;
  const pnlPositive = (account?.pnl_today ?? 0) >= 0;

  return (
    <aside className="w-56 border-r border-border flex flex-col h-full bg-card shrink-0">
      {/* Logo */}
      <div className="px-5 py-5 border-b border-border">
        <div className="flex items-center gap-2.5">
          {/* SVG Logo */}
          <svg width="28" height="28" viewBox="0 0 28 28" fill="none" aria-label="AlphaBot logo">
            <rect width="28" height="28" rx="6" fill="hsl(142 76% 42% / 0.15)" />
            <path d="M7 21L14 7L21 21" stroke="hsl(142 76% 42%)" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
            <path d="M9.5 16.5H18.5" stroke="hsl(142 76% 42%)" strokeWidth="1.5" strokeLinecap="round"/>
            <circle cx="14" cy="7" r="1.5" fill="hsl(142 76% 42%)"/>
          </svg>
          <div>
            <span className="text-sm font-semibold text-foreground tracking-tight">AlphaBot</span>
            <div className="text-xs text-muted-foreground">Multi-Strategy</div>
          </div>
        </div>
      </div>

      {/* Market Status */}
      <div className="px-5 py-3 border-b border-border">
        <div className="flex items-center gap-2">
          <span className={`w-2 h-2 rounded-full live-dot ${isMarketOpen ? "bg-green-400" : "bg-red-500"}`} />
          <span className="text-xs text-muted-foreground">
            Market {isMarketOpen ? "Open" : "Closed"}
          </span>
        </div>
        {account && (
          <div className="mt-2">
            <div className="text-xs text-muted-foreground">Portfolio</div>
            <div className="text-sm font-semibold mono">
              ${account.portfolio_value?.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </div>
            <div className={`text-xs mono ${pnlPositive ? "text-gain" : "text-loss"}`}>
              {pnlPositive ? "+" : ""}${account.pnl_today?.toFixed(2)} today
            </div>
          </div>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-0.5">
        {NAV.map(({ href, label, icon: Icon }) => {
          const active = location === href;
          return (
            <Link key={href} href={href}>
              <a
                className={`flex items-center gap-3 px-3 py-2.5 rounded-md text-sm transition-colors ${
                  active
                    ? "bg-primary/15 text-primary font-medium"
                    : "text-muted-foreground hover:text-foreground hover:bg-accent"
                }`}
                data-testid={`nav-${label.toLowerCase().replace(/\s+/g, '-')}`}
              >
                <Icon size={16} />
                {label}
              </a>
            </Link>
          );
        })}
      </nav>

      {/* Strategy legend */}
      <div className="px-5 py-4 border-t border-border">
        <div className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">Strategies</div>
        <div className="space-y-2">
          {Object.entries(STRATEGY_LABELS).map(([key, label]) => (
            <div key={key} className="flex items-center gap-2">
              <Circle size={6} className={`fill-current ${STRATEGY_COLORS[key]}`} />
              <span className={`text-xs ${STRATEGY_COLORS[key]}`}>{label}</span>
            </div>
          ))}
        </div>
      </div>
    </aside>
  );
}
