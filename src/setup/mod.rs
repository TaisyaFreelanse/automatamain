pub mod waiter;

use solana_rpc_client_types::config::RpcTransactionLogsConfig;
use sqlx::{PgPool, Pool, Postgres};

use crate::{
    autobuy::filters::config::Config,
    persistence::postgres::{
        bot_trades::BotTradesRepositoryPostgres, creators::CreatorsRepositoryPostgres,
        tokens::TokenRepositoryPostgres, traders::TraderRepositoryPostgres,
    },
};

pub fn setup_crypto() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
}

pub fn setup_logging() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::level_filters::LevelFilter::INFO)
        .pretty() // <- makes it look nicer
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

pub fn setup_solana_rpc() -> (String, RpcTransactionLogsConfig) {
    let ws_url = std::env::var("SOLANA_WEBSOCKET").expect("SOLANA_WEBSOCKET must be set");

    let commitment_config = solana_rpc_client_types::config::RpcTransactionLogsConfig {
        commitment: Some(solana_rpc_client_types::config::CommitmentConfig {
            commitment: solana_rpc_client_types::config::CommitmentLevel::Processed,
        }),
    };

    (ws_url, commitment_config)
}

pub async fn setup_postgres_pool(max_connections: u32) -> Pool<Postgres> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"))
        .await
        .unwrap();

    pool
}

pub async fn setup_repositories(
    pool: Pool<Postgres>,
) -> (
    CreatorsRepositoryPostgres,
    TokenRepositoryPostgres,
    TraderRepositoryPostgres,
    BotTradesRepositoryPostgres,
) {
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Migration failed");

    let token_repo = TokenRepositoryPostgres::new(pool.clone());
    let trader_repo = TraderRepositoryPostgres::new(pool.clone());
    let creators_repo = CreatorsRepositoryPostgres::new(pool.clone());
    let bot_trades_repo = BotTradesRepositoryPostgres::new(pool.clone());

    (creators_repo, token_repo, trader_repo, bot_trades_repo)
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string("filter_config.yaml")?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}
