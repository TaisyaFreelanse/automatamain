-- Post-exit bonding-curve mcap samples (5m / 15m / 30m) for early-exit analysis.
ALTER TABLE bot_trades
    ADD COLUMN IF NOT EXISTS post_exit_mcap_5m float8,
    ADD COLUMN IF NOT EXISTS post_exit_mcap_15m float8,
    ADD COLUMN IF NOT EXISTS post_exit_mcap_30m float8,
    ADD COLUMN IF NOT EXISTS post_exit_max_mcap float8,
    ADD COLUMN IF NOT EXISTS post_exit_min_mcap float8,
    ADD COLUMN IF NOT EXISTS post_exit_pct_5m float8,
    ADD COLUMN IF NOT EXISTS post_exit_pct_15m float8,
    ADD COLUMN IF NOT EXISTS post_exit_pct_30m float8,
    ADD COLUMN IF NOT EXISTS post_exit_max_pct float8,
    ADD COLUMN IF NOT EXISTS post_exit_min_pct float8,
    ADD COLUMN IF NOT EXISTS post_exit_tracking_done boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_bot_trades_post_exit_pending
    ON bot_trades (closed_at)
    WHERE post_exit_tracking_done = false;
