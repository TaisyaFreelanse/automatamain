ALTER TABLE bot_trades
    ADD COLUMN IF NOT EXISTS entry_at bigint NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_bot_trades_entry_at ON bot_trades(entry_at);
