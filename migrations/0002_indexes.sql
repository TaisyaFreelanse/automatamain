CREATE INDEX ON trades (coin_address, currency);
CREATE INDEX ON trades (coin_address, currency, slot_time);
CREATE INDEX ON trades (coin_address, trader_address);
CREATE INDEX ON trades (trader_address, slot_time DESC);
CREATE INDEX ON coins (developer);
