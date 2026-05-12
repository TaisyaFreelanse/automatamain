CREATE TABLE bot_trades (
    id           bigserial PRIMARY KEY,
    mint         varchar(44) NOT NULL,
    entry_mcap_sol float8   NOT NULL,
    invested_sol   float8   NOT NULL,
    realized_pnl_pct float8 NOT NULL,
    close_reason text       NOT NULL,
    closed_at    bigint     NOT NULL
);

CREATE INDEX idx_bot_trades_mint ON bot_trades(mint);
CREATE INDEX idx_bot_trades_closed_at ON bot_trades(closed_at);
