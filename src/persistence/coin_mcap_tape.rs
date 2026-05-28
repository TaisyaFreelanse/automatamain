//! Bonding-curve mcap tape for dashboard charts (unix timestamps).

use sqlx::PgPool;

use crate::persistence::error::Error;

const MCAP_ABS_MAX: f64 = 200_000.0;

pub fn mcap_valid(mcap: f64) -> bool {
    mcap.is_finite() && mcap > 0.0 && mcap <= MCAP_ABS_MAX
}

/// Append one sample (best-effort; ignores invalid mcap).
pub async fn record(
    pool: &PgPool,
    coin_address: &str,
    mcap_sol: f64,
    source: &str,
) -> Result<(), Error> {
    if !mcap_valid(mcap_sol) {
        return Ok(());
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    sqlx::query(
        r#"
        INSERT INTO coin_mcap_tape (coin_address, ts_unix, mcap_sol, source)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(coin_address)
    .bind(ts)
    .bind(mcap_sol)
    .bind(source)
    .execute(pool)
    .await
    .map_err(Error::from)?;
    Ok(())
}
