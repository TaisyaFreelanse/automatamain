//! Bonding-curve mcap tape for dashboard charts (unix timestamps).

use sqlx::PgPool;

use crate::persistence::error::Error;

const MCAP_ABS_MAX: f64 = 200_000.0;

pub fn mcap_valid(mcap: f64) -> bool {
    mcap.is_finite() && mcap > 0.0 && mcap <= MCAP_ABS_MAX
}

#[derive(Clone, Debug)]
pub struct TapeRow {
    pub coin_address: String,
    pub ts_unix: i64,
    pub mcap_sol: f64,
    pub source: String,
}

fn now_ts_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Build a tape row if mcap is valid (for batch writer).
pub fn tape_row(coin_address: &str, mcap_sol: f64, source: &str) -> Option<TapeRow> {
    if !mcap_valid(mcap_sol) {
        return None;
    }
    Some(TapeRow {
        coin_address: coin_address.to_string(),
        ts_unix: now_ts_unix(),
        mcap_sol,
        source: source.to_string(),
    })
}

/// Append one sample (best-effort; ignores invalid mcap).
pub async fn record(
    pool: &PgPool,
    coin_address: &str,
    mcap_sol: f64,
    source: &str,
) -> Result<(), Error> {
    let Some(row) = tape_row(coin_address, mcap_sol, source) else {
        return Ok(());
    };
    record_batch(pool, std::slice::from_ref(&row)).await
}

/// Multi-row insert for the persistence write queue.
pub async fn record_batch(pool: &PgPool, rows: &[TapeRow]) -> Result<(), Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut q = String::from(
        "INSERT INTO coin_mcap_tape (coin_address, ts_unix, mcap_sol, source) VALUES ",
    );
    for i in 0..rows.len() {
        if i > 0 {
            q.push(',');
        }
        let b = i * 4;
        q.push_str(&format!("(${}, ${}, ${}, ${})", b + 1, b + 2, b + 3, b + 4));
    }
    let mut query = sqlx::query(&q);
    for row in rows {
        query = query
            .bind(&row.coin_address)
            .bind(row.ts_unix)
            .bind(row.mcap_sol)
            .bind(&row.source);
    }
    query.execute(pool).await.map_err(Error::from)?;
    Ok(())
}
