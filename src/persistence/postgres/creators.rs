use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row};

use solana_address::Address;

use crate::{
    generalize::general_commands::Currency,
    persistence::{
        creators::{CreatorRepository, CreatorStatistics},
        error::Error,
    },
};

/// How long a computed creator-stats result stays fresh. The aggregate query is
/// expensive (full scan over a 25M-row trades table for prolific devs), and the
/// same dev frequently launches many tokens in a short window, so a short TTL
/// turns repeated buy-path lookups into microsecond cache hits without letting
/// stats drift meaningfully.
const CREATOR_STATS_TTL: Duration = Duration::from_secs(180);
/// Cap to keep the cache bounded; expired entries are pruned when this is hit.
const CREATOR_STATS_CACHE_MAX: usize = 50_000;

type CacheEntry = (Option<CreatorStatistics>, Instant);

pub struct CreatorsRepositoryPostgres {
    pool: sqlx::Pool<Postgres>,
    cache: Mutex<HashMap<String, CacheEntry>>,
}

impl CreatorsRepositoryPostgres {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: Mutex::new(HashMap::new()),
        }
    }

    fn cache_get(&self, dev: &str) -> Option<Option<CreatorStatistics>> {
        let cache = self.cache.lock().ok()?;
        let (val, at) = cache.get(dev)?;
        if at.elapsed() < CREATOR_STATS_TTL {
            Some(val.clone())
        } else {
            None
        }
    }

    fn cache_put(&self, dev: String, val: Option<CreatorStatistics>) {
        if let Ok(mut cache) = self.cache.lock() {
            if cache.len() >= CREATOR_STATS_CACHE_MAX {
                let now = Instant::now();
                cache.retain(|_, (_, at)| now.duration_since(*at) < CREATOR_STATS_TTL);
                // Still full of fresh entries: drop this insert rather than grow unbounded.
                if cache.len() >= CREATOR_STATS_CACHE_MAX {
                    return;
                }
            }
            cache.insert(dev, (val, Instant::now()));
        }
    }
}

#[async_trait]
impl CreatorRepository for CreatorsRepositoryPostgres {
    async fn count_creator_coins_capped(
        &self,
        dev_address: Address,
        cap: u64,
    ) -> Result<u64, Error> {
        let dev_address = dev_address.to_string();
        // `LIMIT cap+1` bounds the work: the index scan on coins(developer) stops
        // after cap+1 matches, so a 10-coin dev and a 10k-coin dev cost the same.
        let limit = (cap as i64).saturating_add(1);
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS n
            FROM (
                SELECT 1 FROM coins WHERE developer = $1 LIMIT $2
            ) t
            "#,
        )
        .bind(&dev_address)
        .bind(limit)
        .fetch_one(&self.pool)
        .await?;

        let n: i64 = row.get("n");
        Ok(n.max(0) as u64)
    }

    async fn get_creator_stats_in_sol(
        &self,
        dev_address: Address,
    ) -> Result<Option<CreatorStatistics>, Error> {
        let dev_address = dev_address.to_string();

        // Fast path: fresh cached result (covers repeat tokens by the same dev).
        if let Some(cached) = self.cache_get(&dev_address) {
            return Ok(cached);
        }

        // The two aggregate CTEs each sort ~100k+ rows for prolific devs; with the
        // default 4MB work_mem those sorts spill to disk (external merge). Bump
        // work_mem transaction-locally so they stay in memory. SET LOCAL only
        // applies inside a transaction and auto-resets on commit, so it never
        // leaks to other pooled queries.
        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL work_mem = '64MB'")
            .execute(&mut *tx)
            .await?;

        let row = sqlx::query(
            r#"
            WITH creator_coins AS (
                SELECT coin_address
                FROM coins
                WHERE developer = $1
            ),
            token_stats AS (
                SELECT
                    cc.coin_address,

                    MAX(t.market_cap::double precision) AS ath_market_cap,
                    SUM(t.size::double precision) AS volume,

                    COUNT(*) AS total_trades,

                    COUNT(DISTINCT t.trader_address) FILTER (WHERE t.is_buy) AS unique_buy_wallets,
                    COUNT(DISTINCT t.trader_address) FILTER (WHERE NOT t.is_buy) AS unique_sell_wallets,

                    AVG(t.size::double precision) FILTER (WHERE t.is_buy) AS avg_buy_size

                FROM creator_coins cc
                LEFT JOIN trades t
                  ON t.coin_address = cc.coin_address
                 AND t.currency = 'sol'
                 AND t.role = 'regular'

                GROUP BY cc.coin_address
            ),
            trader_last_trade AS (
                SELECT DISTINCT ON (t.trader_address)
                    t.trader_address,
                    t.pnl::double precision AS pnl
                FROM trades t
                JOIN creator_coins cc
                  ON cc.coin_address = t.coin_address
                WHERE t.role = 'regular'
                  AND t.currency = 'sol'
                ORDER BY t.trader_address, t.slot_time DESC, t.id DESC
            )
            SELECT
                COALESCE(
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY ath_market_cap),
                    0.0
                ) AS median_market_cap,

                COALESCE(
                    (SELECT AVG(pnl) FROM trader_last_trade),
                    0.0
                ) AS trader_pnl_average,

                COALESCE(
                    AVG(unique_buy_wallets::double precision),
                    0.0
                ) AS total_holders_average,

                COALESCE(
                    AVG(COALESCE(volume, 0.0)),
                    0.0
                ) AS average_volume,

                COALESCE(
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY total_trades),
                    0.0
                ) AS median_total_trades,

                COALESCE(
                    AVG(
                        unique_buy_wallets::double precision
                        / NULLIF(unique_sell_wallets::double precision, 0.0)
                    ),
                    0.0
                ) AS average_unique_buy_to_sell_ratio,

                COALESCE(
                    AVG(COALESCE(avg_buy_size, 0.0)),
                    0.0
                ) AS average_buy_trader_size,

                (SELECT COUNT(*) FROM creator_coins) AS total_coins

            FROM token_stats;
            "#,
        )
        .bind(&dev_address)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        let total_coins: i64 = row.get("total_coins");

        if total_coins == 0 {
            self.cache_put(dev_address, None);
            return Ok(None);
        }

        let stats = CreatorStatistics {
            median_market_cap: Currency::from_float_native(row.get::<f64, _>("median_market_cap")),
            trader_pnl_average: row.get("trader_pnl_average"),
            total_holders_average: row.get::<f64, _>("total_holders_average").round() as u64,
            average_volume: row.get("average_volume"),
            median_total_trades: row.get::<f64, _>("median_total_trades").round() as u64,
            average_unique_buy_to_sell_ratio: row.get("average_unique_buy_to_sell_ratio"),
            average_buy_trader_size: Currency::from_float_native(
                row.get::<f64, _>("average_buy_trader_size"),
            ),
            total_coins: total_coins as u64,
        };
        self.cache_put(dev_address, Some(stats.clone()));
        Ok(Some(stats))
    }

    async fn count_prior_coins(
        &self,
        dev_address: Address,
        exclude_mint: Address,
    ) -> Result<u64, Error> {
        let dev_address = dev_address.to_string();
        let exclude_mint = exclude_mint.to_string();
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) AS n
            FROM coins
            WHERE developer = $1 AND coin_address != $2
            "#,
        )
        .bind(&dev_address)
        .bind(&exclude_mint)
        .fetch_one(&self.pool)
        .await?;

        let n: i64 = row.get("n");
        Ok(n.max(0) as u64)
    }
}
