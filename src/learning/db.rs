use serde_json::json;
use sqlx::PgPool;

use crate::learning::snapshot::LearningTradeSnapshot;
use crate::scoring::fresh_b::FreshBSubtype;

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

    /// Closed-trade stats for a scoring tier (e.g. `"B"`).
    pub async fn stats_by_tier(&self, tier: &str) -> Result<TierTradeStats, sqlx::Error> {
        let summary = sqlx::query_as::<_, TierSummaryRow>(
            r#"
            SELECT
                COUNT(*)::bigint AS n,
                COUNT(*) FILTER (WHERE pnl_sol_pct > 0.0)::bigint AS wins,
                COALESCE(AVG(pnl_sol_pct), 0.0)::float8 AS avg_pnl,
                COALESCE(SUM(pnl_sol_pct) FILTER (WHERE pnl_sol_pct > 0.0), 0.0)::float8 AS gross_profit,
                COALESCE(ABS(SUM(pnl_sol_pct) FILTER (WHERE pnl_sol_pct <= 0.0)), 0.0)::float8 AS gross_loss
            FROM learning_trades
            WHERE tier = $1
            "#,
        )
        .bind(tier)
        .fetch_one(&self.pool)
        .await?;

        let exit_reasons = sqlx::query_as::<_, TierExitReasonRow>(
            r#"
            SELECT close_reason, COUNT(*)::bigint AS n
            FROM learning_trades
            WHERE tier = $1
            GROUP BY close_reason
            ORDER BY n DESC
            "#,
        )
        .bind(tier)
        .fetch_all(&self.pool)
        .await?;

        let winrate_pct = if summary.n > 0 {
            summary.wins as f64 / summary.n as f64 * 100.0
        } else {
            0.0
        };
        let profit_factor = if summary.gross_loss > f64::EPSILON {
            summary.gross_profit / summary.gross_loss
        } else if summary.gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        Ok(TierTradeStats {
            tier: tier.to_string(),
            n: summary.n,
            wins: summary.wins,
            winrate_pct,
            avg_pnl_pct: summary.avg_pnl,
            profit_factor,
            exit_reasons: exit_reasons
                .into_iter()
                .map(|r| TierExitReasonCount {
                    reason: r.close_reason,
                    n: r.n,
                })
                .collect(),
        })
    }

    /// Tier B closed trades + fresh subtype breakdown + watchlist funnel from learning tables.
    pub async fn stats_tier_b_detailed(&self) -> Result<TierBDetailedStats, sqlx::Error> {
        let overall = self.stats_by_tier("B").await?;
        let b_true_fresh = self
            .stats_by_tier_fresh_subtype(FreshBSubtype::TRUE_FRESH)
            .await?;
        let b_unknown = self
            .stats_by_tier_fresh_subtype(FreshBSubtype::UNKNOWN)
            .await?;
        let fresh_watchlist = self.fresh_watchlist_skip_stats().await?;
        Ok(TierBDetailedStats {
            overall,
            b_true_fresh,
            b_unknown,
            fresh_watchlist,
        })
    }

    async fn stats_by_tier_fresh_subtype(
        &self,
        subtype: &str,
    ) -> Result<TierSubtypeStats, sqlx::Error> {
        let summary = sqlx::query_as::<_, TierSummaryRow>(
            r#"
            SELECT
                COUNT(*)::bigint AS n,
                COUNT(*) FILTER (WHERE pnl_sol_pct > 0.0)::bigint AS wins,
                COALESCE(AVG(pnl_sol_pct), 0.0)::float8 AS avg_pnl,
                COALESCE(SUM(pnl_sol_pct) FILTER (WHERE pnl_sol_pct > 0.0), 0.0)::float8 AS gross_profit,
                COALESCE(ABS(SUM(pnl_sol_pct) FILTER (WHERE pnl_sol_pct <= 0.0)), 0.0)::float8 AS gross_loss
            FROM learning_trades
            WHERE tier = 'B'
              AND feature_json->>'fresh_b_subtype' = $1
            "#,
        )
        .bind(subtype)
        .fetch_one(&self.pool)
        .await?;

        let exit_reasons = sqlx::query_as::<_, TierExitReasonRow>(
            r#"
            SELECT close_reason, COUNT(*)::bigint AS n
            FROM learning_trades
            WHERE tier = 'B'
              AND feature_json->>'fresh_b_subtype' = $1
            GROUP BY close_reason
            ORDER BY n DESC
            "#,
        )
        .bind(subtype)
        .fetch_all(&self.pool)
        .await?;

        Ok(tier_summary_to_subtype(summary, exit_reasons))
    }

    async fn fresh_watchlist_skip_stats(&self) -> Result<FreshWatchlistSkipStats, sqlx::Error> {
        let row = sqlx::query_as::<_, FreshWatchlistSkipRow>(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE reason = 'watchlist_added')::bigint AS added,
                COUNT(*) FILTER (WHERE reason = 'watchlist_passed')::bigint AS passed,
                COUNT(*) FILTER (
                    WHERE reason IN ('watchlist_rejected', 'timeout_b_gates')
                )::bigint AS rejected
            FROM learning_skipped
            WHERE stage = 'fresh_watchlist'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(FreshWatchlistSkipStats {
            added: row.added,
            passed: row.passed,
            rejected: row.rejected,
        })
    }
}

fn tier_summary_to_subtype(
    summary: TierSummaryRow,
    exit_reasons: Vec<TierExitReasonRow>,
) -> TierSubtypeStats {
    let winrate_pct = if summary.n > 0 {
        summary.wins as f64 / summary.n as f64 * 100.0
    } else {
        0.0
    };
    let profit_factor = if summary.gross_loss > f64::EPSILON {
        summary.gross_profit / summary.gross_loss
    } else if summary.gross_profit > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };
    TierSubtypeStats {
        n: summary.n,
        wins: summary.wins,
        winrate_pct,
        avg_pnl_pct: summary.avg_pnl,
        profit_factor,
        exit_reasons: exit_reasons
            .into_iter()
            .map(|r| TierExitReasonCount {
                reason: r.close_reason,
                n: r.n,
            })
            .collect(),
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct TierSummaryRow {
    n: i64,
    wins: i64,
    avg_pnl: f64,
    gross_profit: f64,
    gross_loss: f64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct TierExitReasonRow {
    close_reason: String,
    n: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TierExitReasonCount {
    pub reason: String,
    pub n: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TierTradeStats {
    pub tier: String,
    pub n: i64,
    pub wins: i64,
    pub winrate_pct: f64,
    pub avg_pnl_pct: f64,
    pub profit_factor: f64,
    pub exit_reasons: Vec<TierExitReasonCount>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TierSubtypeStats {
    pub n: i64,
    pub wins: i64,
    pub winrate_pct: f64,
    pub avg_pnl_pct: f64,
    pub profit_factor: f64,
    pub exit_reasons: Vec<TierExitReasonCount>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FreshWatchlistSkipStats {
    pub added: i64,
    pub passed: i64,
    pub rejected: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TierBDetailedStats {
    #[serde(flatten)]
    pub overall: TierTradeStats,
    pub b_true_fresh: TierSubtypeStats,
    pub b_unknown: TierSubtypeStats,
    pub fresh_watchlist: FreshWatchlistSkipStats,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct FreshWatchlistSkipRow {
    added: i64,
    passed: i64,
    rejected: i64,
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
