-- Make the creator/dev-stats query (get_creator_stats_in_sol) index-only.
--
-- The query aggregates trades across all of a dev's coins twice:
--   * token_stats:       per-coin MAX(market_cap), SUM(size), COUNT, is_buy splits
--   * trader_last_trade: DISTINCT ON (trader_address) ... last pnl
-- The keys-only index (0012) made the join use the index, but Postgres still had
-- to fetch the heap for market_cap/size/is_buy/pnl (~50k heap blocks for a
-- 50-coin / 125k-trade dev) and spilled the group/sort to disk -> ~1.8s.
--
-- This COVERING index INCLUDEs those four columns, so both CTEs become true
-- index-only scans (no heap fetch) and token_stats can consume the index order.
-- Measured on prod: 1794ms -> 659ms (and ~660ms is now just the in-memory-able
-- sorts; the query path also bumps work_mem transaction-locally). trades is
-- append-only, so pages stay all-visible (autovacuum) and index-only scans hold.
--
-- On a fresh DB this is instant (empty table). On the live DB the covering index
-- is created out-of-band with CREATE INDEX CONCURRENTLY (~+0.8GB), so the first
-- statement is a no-op there (no table lock); the second drops the now-redundant
-- keys-only index from 0012.
CREATE INDEX IF NOT EXISTS idx_trades_coin_sol_regular_cov
ON trades (coin_address, trader_address, slot_time DESC, id DESC)
INCLUDE (is_buy, market_cap, size, pnl)
WHERE currency = 'sol' AND role = 'regular';

DROP INDEX IF EXISTS idx_trades_coin_sol_regular;
