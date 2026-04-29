// AlphaBot Dashboard — Type Definitions
// No database on frontend — all data proxied from Python API

import { pgTable, serial, text, real } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Minimal placeholder table to satisfy template imports
export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  username: text("username").notNull(),
  password: text("password").notNull(),
});

export const insertUserSchema = createInsertSchema(users).omit({ id: true });
export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;

// ---- API response types used on frontend ----

export interface AccountInfo {
  portfolio_value: number;
  cash: number;
  equity: number;
  buying_power: number;
  pnl_today: number;
  pnl_today_pct: number;
  demo?: boolean;
}

export interface Position {
  symbol: string;
  qty: number;
  avg_entry: number;
  current_price: number;
  market_value: number;
  unrealized_pnl: number;
  unrealized_pnl_pct: number;
  side: string;
  strategy: string;
}

export interface Order {
  id: string;
  symbol: string;
  side: string;
  qty: number;
  notional: number;
  status: string;
  created_at: string;
}

export interface Trade {
  id: number;
  strategy: string;
  symbol: string;
  side: string;
  qty: number;
  price: number;
  pnl: number;
  metadata: Record<string, any>;
  created_at: string;
}

export interface StrategyPerf {
  name: string;
  total_pnl: number;
  total_trades: number;
  win_rate: number;
  wins: number;
  losses: number;
  open_positions: number;
  unrealized_pnl: number;
}

export interface DailyPnl {
  date: string;
  pnl: number;
  trades: number;
}

export interface Snapshot {
  portfolio_value: number;
  spy_price: number;
  snapshot_at: string;
  pnl_today: number;
  cash: number;
  equity: number;
}
