-- Dev cooldown after bot cliff/rug exits (our trades only).
CREATE TABLE dev_blacklist (
    id            bigserial PRIMARY KEY,
    dev_wallet    varchar(44) NOT NULL,
    reason        text        NOT NULL,
    mint          varchar(44) NOT NULL,
    pnl_sol       float8      NOT NULL,
    close_reason  text        NOT NULL,
    created_at    bigint      NOT NULL,
    expires_at    bigint      NOT NULL
);

CREATE INDEX idx_dev_blacklist_dev_active
    ON dev_blacklist (dev_wallet, expires_at DESC);

CREATE INDEX idx_dev_blacklist_created_at
    ON dev_blacklist (created_at DESC);
