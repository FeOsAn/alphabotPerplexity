import { useQuery } from "@tanstack/react-query";
import { fetchApi, API_BASE } from "@/lib/queryClient";
import { Skeleton } from "@/components/ui/skeleton";
import { CheckCircle, XCircle, AlertCircle, Brain, Shield, Zap } from "lucide-react";

interface ResearchSignal {
  id: number;
  symbol: string;
  signal: string;
  score: number;
  metadata: {
    thesis?: string;
    verdict?: string;
    research_confidence?: number;
    checker_confidence?: number;
    checks_passed?: number;
    reason?: string;
    bull_case?: string[];
    target_upside_pct?: number;
    suggested_hold_weeks?: number;
    checker_notes?: string;
    checker_go?: boolean;
    check1_passed?: boolean;
    check2_passed?: boolean;
    check3_passed?: boolean;
  };
  created_at: string;
}

interface AIStatus {
  ready: boolean;
  message: string;
}

export default function AIResearch() {
  const { data: status } = useQuery({
    queryKey: ["/api/ai-status"],
    queryFn: () => fetchApi<AIStatus>(`${API_BASE}/api/ai-status`),
    refetchInterval: 30000,
  });

  const { data: signals = [], isLoading } = useQuery({
    queryKey: ["/api/research-log"],
    queryFn: () => fetchApi<ResearchSignal[]>(`${API_BASE}/api/research-log`),
    refetchInterval: 60000,
  });

  const approved = signals.filter(s => s.signal === "buy");
  const blocked = signals.filter(s => s.signal === "blocked");
  const total = signals.length;
  const blockRate = total > 0 ? Math.round((blocked.length / total) * 100) : 0;

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-xl font-semibold flex items-center gap-2">
            <Brain size={20} className="text-purple-400" />
            AI Research
          </h1>
          <p className="text-sm text-muted-foreground mt-0.5">
            Fundamental analysis with triple-verification failsafe
          </p>
        </div>
        {/* Status badge */}
        <div className={`flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium border ${
          status?.ready
            ? "border-green-400/30 bg-green-400/10 text-green-400"
            : "border-amber-400/30 bg-amber-400/10 text-amber-400"
        }`}>
          <span className={`w-1.5 h-1.5 rounded-full ${status?.ready ? "bg-green-400 live-dot" : "bg-amber-400"}`} />
          {status?.ready ? "Active" : "Needs API Key"}
        </div>
      </div>

      {/* Not configured banner */}
      {!status?.ready && (
        <div className="bg-amber-400/10 border border-amber-400/30 rounded-lg p-4 flex items-start gap-3">
          <AlertCircle size={18} className="text-amber-400 mt-0.5 shrink-0" />
          <div>
            <div className="text-sm font-medium text-amber-400">Anthropic API key required</div>
            <div className="text-xs text-muted-foreground mt-1">
              Add <code className="bg-muted px-1 py-0.5 rounded text-xs mono">ANTHROPIC_API_KEY=sk-ant-...</code> to your{" "}
              <code className="bg-muted px-1 py-0.5 rounded text-xs mono">.env</code> file and restart the bot.
              Strategy 4 runs daily at 9:45 AM ET once activated.
            </div>
          </div>
        </div>
      )}

      {/* Architecture explainer */}
      <div className="grid grid-cols-3 gap-3">
        <div className="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 bg-purple-400/5 rounded-full -translate-y-1/2 translate-x-1/2" />
          <Brain size={16} className="text-purple-400 mb-2" />
          <div className="text-xs font-semibold text-purple-400 mb-1">Research Agent</div>
          <div className="text-xs text-muted-foreground leading-relaxed">
            Reads news, financials, analyst targets. Builds a cited thesis. Scores confidence 1–10.
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 bg-blue-400/5 rounded-full -translate-y-1/2 translate-x-1/2" />
          <Shield size={16} className="text-blue-400 mb-2" />
          <div className="text-xs font-semibold text-blue-400 mb-1">Checker Agent</div>
          <div className="text-xs text-muted-foreground leading-relaxed">
            Independently verifies every fact. Runs 3 checks: accuracy, logic, recency. Issues GO / NO-GO.
          </div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div className="absolute top-0 right-0 w-16 h-16 bg-green-400/5 rounded-full -translate-y-1/2 translate-x-1/2" />
          <Zap size={16} className="text-green-400 mb-2" />
          <div className="text-xs font-semibold text-green-400 mb-1">Executor</div>
          <div className="text-xs text-muted-foreground leading-relaxed">
            Only fires if Checker says GO, all 3 checks pass, and both agents score ≥ 7/10.
          </div>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-4 gap-4">
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Researched</div>
          <div className="text-xl font-semibold mono">{total}</div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Approved</div>
          <div className="text-xl font-semibold mono text-gain">{approved.length}</div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Blocked</div>
          <div className="text-xl font-semibold mono text-loss">{blocked.length}</div>
        </div>
        <div className="bg-card border border-border rounded-lg p-4">
          <div className="text-xs text-muted-foreground uppercase tracking-wider mb-1">Block Rate</div>
          <div className="text-xl font-semibold mono">{blockRate}%</div>
          <div className="text-xs text-muted-foreground">failsafe effectiveness</div>
        </div>
      </div>

      {/* Research log */}
      {isLoading ? (
        <div className="space-y-3">
          {Array(4).fill(0).map((_, i) => <Skeleton key={i} className="h-24 rounded-lg" />)}
        </div>
      ) : signals.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center space-y-2">
          <Brain size={32} className="text-muted-foreground mx-auto" />
          <div className="text-sm text-muted-foreground">No research runs yet</div>
          <div className="text-xs text-muted-foreground">
            {status?.ready
              ? "Strategy runs daily at 9:45 AM ET — results will appear here"
              : "Add your Anthropic API key to activate Strategy 4"}
          </div>
        </div>
      ) : (
        <div className="space-y-3">
          <div className="text-sm font-medium text-muted-foreground">Research Audit Trail</div>
          {signals.map(sig => {
            const approved = sig.signal === "buy";
            const meta = sig.metadata || {};
            return (
              <div
                key={sig.id}
                className={`bg-card border rounded-lg p-4 ${
                  approved ? "border-green-400/30" : "border-red-500/20"
                }`}
                data-testid={`research-${sig.symbol}-${sig.id}`}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-center gap-3">
                    {approved
                      ? <CheckCircle size={18} className="text-green-400 shrink-0" />
                      : <XCircle size={18} className="text-red-500 shrink-0" />
                    }
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-semibold mono">{sig.symbol}</span>
                        <span className={`text-xs px-2 py-0.5 rounded-full border font-medium ${
                          meta.verdict === "BULLISH"
                            ? "text-green-400 bg-green-400/10 border-green-400/20"
                            : meta.verdict === "BEARISH"
                            ? "text-red-400 bg-red-400/10 border-red-400/20"
                            : "text-gray-400 bg-gray-400/10 border-gray-400/20"
                        }`}>
                          {meta.verdict || "UNKNOWN"}
                        </span>
                        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                          approved ? "bg-green-400/10 text-green-400" : "bg-red-500/10 text-red-400"
                        }`}>
                          {approved ? "✓ EXECUTED" : `✗ BLOCKED: ${meta.reason?.replace(/_/g, " ")}`}
                        </span>
                      </div>
                      {meta.thesis && (
                        <p className="text-xs text-muted-foreground mt-1 max-w-xl">{meta.thesis}</p>
                      )}
                    </div>
                  </div>

                  {/* Confidence scores */}
                  <div className="flex gap-4 shrink-0 text-right">
                    <div>
                      <div className="text-xs text-purple-400">Research</div>
                      <div className="text-sm font-semibold mono">{meta.research_confidence ?? sig.score}/10</div>
                    </div>
                    <div>
                      <div className="text-xs text-blue-400">Checker</div>
                      <div className="text-sm font-semibold mono">{meta.checker_confidence ?? "—"}/10</div>
                    </div>
                  </div>
                </div>

                {/* 3-check status */}
                <div className="mt-3 flex items-center gap-4 pt-3 border-t border-border">
                  <div className="flex items-center gap-4 text-xs">
                    <CheckItem label="Factual" passed={meta.check1_passed} />
                    <CheckItem label="Logic" passed={meta.check2_passed} />
                    <CheckItem label="Recency" passed={meta.check3_passed} />
                  </div>
                  {meta.checker_notes && (
                    <p className="text-xs text-muted-foreground ml-2 italic">"{meta.checker_notes}"</p>
                  )}
                  <span className="ml-auto text-xs text-muted-foreground mono">
                    {new Date(sig.created_at).toLocaleString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })}
                  </span>
                </div>

                {/* Bull case */}
                {approved && meta.bull_case && meta.bull_case.length > 0 && (
                  <div className="mt-3 pt-3 border-t border-border">
                    <div className="text-xs text-muted-foreground mb-1.5">Bull case</div>
                    <div className="flex flex-wrap gap-2">
                      {meta.bull_case.map((point, i) => (
                        <span key={i} className="text-xs bg-green-400/10 text-green-400 px-2 py-1 rounded border border-green-400/20">
                          {point}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

function CheckItem({ label, passed }: { label: string; passed?: boolean }) {
  if (passed === undefined) {
    return <span className="text-muted-foreground">{label}: —</span>;
  }
  return (
    <span className={passed ? "text-green-400" : "text-red-400"}>
      {passed ? "✓" : "✗"} {label}
    </span>
  );
}
