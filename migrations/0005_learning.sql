-- Self-learning dataset: completed bot round-trips with scoring-time features.
CREATE TABLE learning_trades (
    id               bigserial PRIMARY KEY,
    mint             varchar(64) NOT NULL,
    dev              varchar(64) NOT NULL,
    entry_mcap_sol   float8 NOT NULL,
    exit_mcap_sol    float8 NOT NULL,
    smart_wallets    int NOT NULL,
    velocity_pct     float8 NOT NULL,
    bundle_similar   float8 NOT NULL,
    bundle_identical float8 NOT NULL,
    buyer_count      bigint NOT NULL,
    buy_to_sell_ratio float8 NOT NULL,
    buy_volume_sol   float8 NOT NULL,
    pnl_sol_pct      float8 NOT NULL,
    hold_time_secs   bigint NOT NULL,
    score_total      int NOT NULL,
    tier             varchar(16) NOT NULL,
    close_reason     text NOT NULL,
    closed_at        bigint NOT NULL,
    feature_json     jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_learning_trades_closed_at ON learning_trades (closed_at);
CREATE INDEX idx_learning_trades_mint ON learning_trades (mint);

-- Tokens skipped before buy (filters / score / live gates / sizing).
CREATE TABLE learning_skipped (
    id          bigserial PRIMARY KEY,
    mint        varchar(64) NOT NULL,
    dev         varchar(64),
    stage       varchar(64) NOT NULL,
    reason      text NOT NULL,
    payload     jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at  bigint NOT NULL
);

CREATE INDEX idx_learning_skipped_created ON learning_skipped (created_at);
