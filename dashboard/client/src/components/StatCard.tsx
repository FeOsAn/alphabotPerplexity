interface StatCardProps {
  label: string;
  value: string;
  sub?: string;
  positive?: boolean;
  negative?: boolean;
  neutral?: boolean;
  icon?: React.ReactNode;
  "data-testid"?: string;
}

export default function StatCard({ label, value, sub, positive, negative, neutral, icon, "data-testid": testId }: StatCardProps) {
  const valueColor = positive ? "text-gain" : negative ? "text-loss" : "text-foreground";

  return (
    <div className="bg-card border border-border rounded-lg p-4 card-hover" data-testid={testId}>
      <div className="flex items-start justify-between mb-2">
        <span className="text-xs text-muted-foreground uppercase tracking-wider font-medium">{label}</span>
        {icon && <span className="text-muted-foreground">{icon}</span>}
      </div>
      <div className={`text-xl font-semibold mono ${valueColor}`}>{value}</div>
      {sub && (
        <div className={`text-xs mt-1 ${positive ? "text-gain" : negative ? "text-loss" : "text-muted-foreground"}`}>
          {sub}
        </div>
      )}
    </div>
  );
}
