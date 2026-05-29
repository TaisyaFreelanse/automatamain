-- Accelerate the creator/dev-stats pre-gate (get_creator_stats_in_sol) and any
-- per-coin sol/regular aggregation. The query joins `trades` on coin_address and
-- filters `currency = 'sol' AND role = 'regular'`, then does DISTINCT ON
-- (trader_address ORDER BY slot_time DESC, id DESC) for last-trade PnL. Without a
-- matching index Postgres falls back to two full sequential scans of the 25M-row
-- trades table (~28s for prolific devs). This partial composite index lets the
-- planner use index access for the join + filter and the per-coin last-trade
-- ordering.
--
-- On a fresh DB this is instant (empty table). On the live DB the index is
-- created out-of-band with CREATE INDEX CONCURRENTLY, so this IF NOT EXISTS is a
-- no-op there (no table lock).
CREATE INDEX IF NOT EXISTS idx_trades_coin_sol_regular
ON trades (coin_address, trader_address, slot_time DESC, id DESC)
WHERE currency = 'sol' AND role = 'regular';
