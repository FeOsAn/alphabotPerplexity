import { QueryClient } from "@tanstack/react-query";

// API base — points to Railway backend
export const API_BASE = "https://alphabotperplexity-production.up.railway.app";

export async function apiRequest(method: string, path: string, body?: unknown) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return res.json();
}

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchInterval: 30000, // Refresh every 30s
      retry: 1,
      staleTime: 15000,
    },
  },
});

// Convenience GET wrapper
export function fetchApi<T>(path: string): Promise<T> {
  return apiRequest("GET", path);
}
