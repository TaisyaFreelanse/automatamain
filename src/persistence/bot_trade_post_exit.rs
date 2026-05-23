use crate::persistence::error::Error;

/// Scheduled post-exit sample offsets (seconds after full close).
pub const POST_EXIT_CHECKPOINT_SECS: &[u64] = &[
    10, 30, 50, 70, 100, 180, 240, 300, 600, 900, 1800,
];

fn pct_from_exit(exit_mcap_sol: f64, mcap: Option<f64>) -> Option<f64> {
    let m = mcap?;
    if exit_mcap_sol > 0.0 && m.is_finite() && m > 0.0 {
        Some((m / exit_mcap_sol - 1.0) * 100.0)
    } else {
        None
    }
}

/// Mcap samples after full position close (30-minute window).
#[derive(Clone, Debug, Default)]
pub struct PostExitMetrics {
    pub post_exit_mcap_10s: Option<f64>,
    pub post_exit_mcap_30s: Option<f64>,
    pub post_exit_mcap_50s: Option<f64>,
    pub post_exit_mcap_70s: Option<f64>,
    pub post_exit_mcap_100s: Option<f64>,
    pub post_exit_mcap_180s: Option<f64>,
    pub post_exit_mcap_240s: Option<f64>,
    pub post_exit_mcap_300s: Option<f64>,
    /// Same snapshot as `post_exit_mcap_300s` (legacy column name).
    pub post_exit_mcap_5m: Option<f64>,
    pub post_exit_mcap_10m: Option<f64>,
    pub post_exit_mcap_15m: Option<f64>,
    pub post_exit_mcap_30m: Option<f64>,
    pub post_exit_max_mcap: Option<f64>,
    pub post_exit_min_mcap: Option<f64>,
    pub post_exit_time_to_max_secs: Option<i64>,
    pub post_exit_time_to_min_secs: Option<i64>,
    pub post_exit_pct_10s: Option<f64>,
    pub post_exit_pct_30s: Option<f64>,
    pub post_exit_pct_50s: Option<f64>,
    pub post_exit_pct_70s: Option<f64>,
    pub post_exit_pct_100s: Option<f64>,
    pub post_exit_pct_180s: Option<f64>,
    pub post_exit_pct_240s: Option<f64>,
    pub post_exit_pct_300s: Option<f64>,
    pub post_exit_pct_5m: Option<f64>,
    pub post_exit_pct_10m: Option<f64>,
    pub post_exit_pct_15m: Option<f64>,
    pub post_exit_pct_30m: Option<f64>,
    pub post_exit_max_pct: Option<f64>,
    pub post_exit_min_pct: Option<f64>,
}

impl PostExitMetrics {
    pub fn from_samples(exit_mcap_sol: f64, samples: &PostExitSampleState) -> Self {
        let m300 = samples.mcap_300s;
        Self {
            post_exit_mcap_10s: samples.mcap_10s,
            post_exit_mcap_30s: samples.mcap_30s,
            post_exit_mcap_50s: samples.mcap_50s,
            post_exit_mcap_70s: samples.mcap_70s,
            post_exit_mcap_100s: samples.mcap_100s,
            post_exit_mcap_180s: samples.mcap_180s,
            post_exit_mcap_240s: samples.mcap_240s,
            post_exit_mcap_300s: m300,
            post_exit_mcap_5m: m300,
            post_exit_mcap_10m: samples.mcap_10m,
            post_exit_mcap_15m: samples.mcap_15m,
            post_exit_mcap_30m: samples.mcap_30m,
            post_exit_max_mcap: samples.max_mcap,
            post_exit_min_mcap: samples.min_mcap,
            post_exit_time_to_max_secs: samples
                .time_to_max_secs
                .map(|s| i64::try_from(s).unwrap_or(i64::MAX)),
            post_exit_time_to_min_secs: samples
                .time_to_min_secs
                .map(|s| i64::try_from(s).unwrap_or(i64::MAX)),
            post_exit_pct_10s: pct_from_exit(exit_mcap_sol, samples.mcap_10s),
            post_exit_pct_30s: pct_from_exit(exit_mcap_sol, samples.mcap_30s),
            post_exit_pct_50s: pct_from_exit(exit_mcap_sol, samples.mcap_50s),
            post_exit_pct_70s: pct_from_exit(exit_mcap_sol, samples.mcap_70s),
            post_exit_pct_100s: pct_from_exit(exit_mcap_sol, samples.mcap_100s),
            post_exit_pct_180s: pct_from_exit(exit_mcap_sol, samples.mcap_180s),
            post_exit_pct_240s: pct_from_exit(exit_mcap_sol, samples.mcap_240s),
            post_exit_pct_300s: pct_from_exit(exit_mcap_sol, m300),
            post_exit_pct_5m: pct_from_exit(exit_mcap_sol, m300),
            post_exit_pct_10m: pct_from_exit(exit_mcap_sol, samples.mcap_10m),
            post_exit_pct_15m: pct_from_exit(exit_mcap_sol, samples.mcap_15m),
            post_exit_pct_30m: pct_from_exit(exit_mcap_sol, samples.mcap_30m),
            post_exit_max_pct: pct_from_exit(exit_mcap_sol, samples.max_mcap),
            post_exit_min_pct: pct_from_exit(exit_mcap_sol, samples.min_mcap),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PostExitSampleState {
    pub mcap_10s: Option<f64>,
    pub mcap_30s: Option<f64>,
    pub mcap_50s: Option<f64>,
    pub mcap_70s: Option<f64>,
    pub mcap_100s: Option<f64>,
    pub mcap_180s: Option<f64>,
    pub mcap_240s: Option<f64>,
    pub mcap_300s: Option<f64>,
    pub mcap_10m: Option<f64>,
    pub mcap_15m: Option<f64>,
    pub mcap_30m: Option<f64>,
    pub max_mcap: Option<f64>,
    pub min_mcap: Option<f64>,
    pub time_to_max_secs: Option<u64>,
    pub time_to_min_secs: Option<u64>,
}

impl PostExitSampleState {
    /// Baseline at exit (elapsed=0) for min/max tracking.
    pub fn record_exit_baseline(&mut self, exit_mcap_sol: f64) {
        if exit_mcap_sol.is_finite() && exit_mcap_sol > 0.0 {
            self.update_extrema(0, exit_mcap_sol);
        }
    }

    pub fn set_checkpoint(&mut self, checkpoint_secs: u64, mcap: f64) {
        if !mcap.is_finite() || mcap <= 0.0 {
            return;
        }
        self.update_extrema(checkpoint_secs, mcap);
        match checkpoint_secs {
            10 => self.mcap_10s = Some(mcap),
            30 => self.mcap_30s = Some(mcap),
            50 => self.mcap_50s = Some(mcap),
            70 => self.mcap_70s = Some(mcap),
            100 => self.mcap_100s = Some(mcap),
            180 => self.mcap_180s = Some(mcap),
            240 => self.mcap_240s = Some(mcap),
            300 => self.mcap_300s = Some(mcap),
            600 => self.mcap_10m = Some(mcap),
            900 => self.mcap_15m = Some(mcap),
            1800 => self.mcap_30m = Some(mcap),
            _ => {}
        }
    }

    fn update_extrema(&mut self, elapsed_secs: u64, mcap: f64) {
        match self.max_mcap {
            None => {
                self.max_mcap = Some(mcap);
                self.time_to_max_secs = Some(elapsed_secs);
            }
            Some(prev) if mcap > prev => {
                self.max_mcap = Some(mcap);
                self.time_to_max_secs = Some(elapsed_secs);
            }
            _ => {}
        }
        match self.min_mcap {
            None => {
                self.min_mcap = Some(mcap);
                self.time_to_min_secs = Some(elapsed_secs);
            }
            Some(prev) if mcap < prev => {
                self.min_mcap = Some(mcap);
                self.time_to_min_secs = Some(elapsed_secs);
            }
            _ => {}
        }
    }
}

#[async_trait::async_trait]
pub trait BotTradePostExitRepository: Send + Sync {
    async fn update_post_exit_metrics(
        &self,
        trade_id: i64,
        metrics: &PostExitMetrics,
    ) -> Result<(), Error>;
}
