use serde_json::json;
use sqlx::PgPool;

use crate::learning::snapshot::LearningTradeSnapshot;

/// Postgres writer for learning tables.
#[derive(Clone)]
pub struct LearningLogPg {
    pool: PgPool,
}

impl LearningLogPg {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn log_closed_trade(
        &self,
        snap: &LearningTradeSnapshot,
        exit_mcap_sol: f64,
        pnl_sol_pct: f64,
        hold_time_secs: i64,
        close_reason: &str,
        closed_at: i64,
    ) -> Result<(), sqlx::Error> {
        let feature_json = serde_json::to_value(snap).unwrap_or_else(|_| json!({}));
        sqlx::query(
            r#"
            INSERT INTO learning_trades (
                mint, dev, entry_mcap_sol, exit_mcap_sol, smart_wallets, velocity_pct,
                bundle_similar, bundle_identical, buyer_count, buy_to_sell_ratio,
                buy_volume_sol, pnl_sol_pct, hold_time_secs, score_total, tier,
                close_reason, closed_at, feature_json
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            "#,
        )
        .bind(&snap.mint)
        .bind(&snap.dev)
        .bind(snap.entry_mcap_sol)
        .bind(exit_mcap_sol)
        .bind(snap.smart_wallet_count as i32)
        .bind(snap.velocity_pct)
        .bind(snap.bundle_similar)
        .bind(snap.bundle_identical)
        .bind(snap.buyer_count as i64)
        .bind(snap.buy_to_sell_ratio)
        .bind(snap.buy_volume_sol)
        .bind(pnl_sol_pct)
        .bind(hold_time_secs)
        .bind(snap.score_total)
        .bind(&snap.tier)
        .bind(close_reason)
        .bind(closed_at)
        .bind(feature_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn log_skipped(
        &self,
        mint: &str,
        dev: Option<&str>,
        stage: &str,
        reason: &str,
        payload: serde_json::Value,
        created_at: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO learning_skipped (mint, dev, stage, reason, payload, created_at)
            VALUES ($1,$2,$3,$4,$5,$6)
            "#,
        )
        .bind(mint)
        .bind(dev)
        .bind(stage)
        .bind(reason)
        .bind(payload)
        .bind(created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_trades(&self) -> Result<i64, sqlx::Error> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*)::bigint FROM learning_trades")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }

    pub async fn stats_all(&self) -> Result<TradeBatchStats, sqlx::Error> {
        let row = sqlx::query_as::<_, TradeBatchStats>(
            r#"
            SELECT
                COUNT(*)::bigint AS n,
                COUNT(*) FILTER (WHERE pnl_sol_pct > 0.0)::bigint AS wins,
                COALESCE(AVG(smart_wallets) FILTER (WHERE pnl_sol_pct > 0.0), 0.0)::float8 AS win_smart,
                COALESCE(AVG(smart_wallets) FILTER (WHERE pnl_sol_pct <= 0.0), 0.0)::float8 AS loss_smart,
                COALESCE(AVG(buyer_count) FILTER (WHERE pnl_sol_pct > 0.0), 0.0)::float8 AS win_buyers,
                COALESCE(AVG(buyer_count) FILTER (WHERE pnl_sol_pct <= 0.0), 0.0)::float8 AS loss_buyers,
                COALESCE(MAX(id), 0)::bigint AS max_id
            FROM learning_trades
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TradeBatchStats {
    pub n: i64,
    pub wins: i64,
    pub win_smart: f64,
    pub loss_smart: f64,
    pub win_buyers: f64,
    pub loss_buyers: f64,
    pub max_id: i64,
}
