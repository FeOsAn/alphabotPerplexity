export default function NotFound() {
  return (
    <div className="flex items-center justify-center h-full">
      <div className="text-center">
        <div className="text-4xl font-bold text-muted-foreground">404</div>
        <div className="text-sm text-muted-foreground mt-2">Page not found</div>
        <a href="/#/" className="text-primary text-sm hover:underline mt-4 block">← Back to Dashboard</a>
      </div>
    </div>
  );
}
