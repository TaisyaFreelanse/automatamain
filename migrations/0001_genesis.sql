CREATE TYPE currency_enum AS ENUM ('sol', 'usd');
CREATE TYPE trader_role_enum AS ENUM ('creator', 'sniper', 'regular');

CREATE TABLE traders (
    trader_address varchar(44) PRIMARY KEY,
    active_from_slot bigint NOT NULL
);

CREATE TABLE developers (
    developer_address varchar(44) PRIMARY KEY,
    active_from_slot bigint NOT NULL
);

CREATE TABLE coins (
    coin_address varchar(44) PRIMARY KEY,
    developer varchar(44) NOT NULL REFERENCES developers(developer_address),
    created_at bigint NOT NULL
);

CREATE TABLE trades (
    id bigserial PRIMARY KEY,
    trader_address varchar(44) NOT NULL REFERENCES traders(trader_address),
    coin_address varchar(44) NOT NULL REFERENCES coins(coin_address),
    pnl numeric,
    slot_time bigint NOT NULL,
    is_buy boolean NOT NULL,
    market_cap numeric,
    currency currency_enum NOT NULL,
    size numeric NOT NULL,
    role trader_role_enum NOT NULL
);

CREATE INDEX idx_trades_trader_address
ON trades(trader_address);

CREATE INDEX idx_trades_coin_address
ON trades(coin_address);
