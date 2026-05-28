-- Bonding-curve mcap samples for dashboard charts (unix timeline, pre-migration).
CREATE TABLE IF NOT EXISTS coin_mcap_tape (
    id           bigserial PRIMARY KEY,
    coin_address varchar(44) NOT NULL,
    ts_unix      bigint      NOT NULL,
    mcap_sol     float8      NOT NULL,
    source       varchar(16) NOT NULL DEFAULT 'trade'
);

CREATE INDEX IF NOT EXISTS idx_coin_mcap_tape_coin_ts
    ON coin_mcap_tape (coin_address, ts_unix);
