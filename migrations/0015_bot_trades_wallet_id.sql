ALTER TABLE bot_trades ADD COLUMN IF NOT EXISTS wallet_id varchar(32) NOT NULL DEFAULT 'wallet_1';
CREATE INDEX IF NOT EXISTS idx_bot_trades_wallet_closed ON bot_trades (wallet_id, closed_at DESC);
