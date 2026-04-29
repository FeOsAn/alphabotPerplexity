import type { Express } from "express";
import { createServer, type Server } from "http";

export async function registerRoutes(server: Server, app: Express): Promise<void> {
  // The dashboard is a pure frontend app.
  // All /api/* calls are proxied by Vite to the Python AlphaBot API on port 8000
  // (configured in vite.config.ts proxy).
  // No Express routes needed here.
}
