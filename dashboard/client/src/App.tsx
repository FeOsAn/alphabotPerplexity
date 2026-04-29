import { Switch, Route, Router } from "wouter";
import { useHashLocation } from "wouter/use-hash-location";
import { QueryClientProvider } from "@tanstack/react-query";
import { queryClient } from "@/lib/queryClient";
import { Toaster } from "@/components/ui/toaster";
import Sidebar from "@/components/Sidebar";
import Dashboard from "@/pages/Dashboard";
import Positions from "@/pages/Positions";
import Strategies from "@/pages/Strategies";
import Trades from "@/pages/Trades";
import AIResearch from "@/pages/AIResearch";
import NotFound from "@/pages/not-found";

function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-screen bg-background overflow-hidden">
      <Sidebar />
      <main className="flex-1 overflow-auto">
        {children}
      </main>
    </div>
  );
}

export default function App() {
  // Force dark mode
  document.documentElement.classList.add("dark");

  return (
    <QueryClientProvider client={queryClient}>
      <Router hook={useHashLocation}>
        <AppLayout>
          <Switch>
            <Route path="/" component={Dashboard} />
            <Route path="/positions" component={Positions} />
            <Route path="/strategies" component={Strategies} />
            <Route path="/trades" component={Trades} />
            <Route path="/ai-research" component={AIResearch} />
            <Route component={NotFound} />
          </Switch>
        </AppLayout>
      </Router>
      <Toaster />
    </QueryClientProvider>
  );
}
