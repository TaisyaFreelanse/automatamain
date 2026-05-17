use serde::{Deserialize, Serialize};

use crate::scoring::config::FeatureThresholds;

/// Optional overrides merged on top of YAML `scoring.thresholds` at score time.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FeatureThresholdPatch {
    /// When set, replaces `FeatureThresholds::buyers_low` (clamped vs base).
    #[serde(default)]
    pub buyers_low: Option<u64>,
    /// Multiplier applied to `volume_ok_sol` (e.g. 1.1 = +10%).
    #[serde(default)]
    pub volume_ok_sol_mult: Option<f64>,
    /// Minimum smart-wallet count for the top smart-money bucket (default in yaml: 3).
    #[serde(default)]
    pub smart_wallet_3plus_min: Option<u32>,
    /// Minimum for the lower smart-money bucket (default: 1; must stay < 3plus).
    #[serde(default)]
    pub smart_wallet_1plus_min: Option<u32>,
}

/// Persisted file: patch + light metadata for operators.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LearningOverridesFile {
    #[serde(default)]
    pub patch: FeatureThresholdPatch,
    #[serde(default)]
    pub last_optimized_unix: i64,
    #[serde(default)]
    pub last_sample_size: i64,
}

impl Default for LearningOverridesFile {
    fn default() -> Self {
        Self {
            patch: FeatureThresholdPatch::default(),
            last_optimized_unix: 0,
            last_sample_size: 0,
        }
    }
}

pub async fn load_patch(path: &str) -> LearningOverridesFile {
    match tokio::fs::read_to_string(path).await {
        Ok(s) => serde_json::from_str(&s).unwrap_or_default(),
        Err(_) => LearningOverridesFile::default(),
    }
}

pub async fn save_patch(path: &str, file: &LearningOverridesFile) -> std::io::Result<()> {
    if let Some(dir) = std::path::Path::new(path).parent() {
        let _ = tokio::fs::create_dir_all(dir).await;
    }
    let data = serde_json::to_string_pretty(file).unwrap_or_else(|_| "{}".into());
    tokio::fs::write(path, data).await
}

/// Merge YAML base thresholds with optional learning patch (safe clamps).
pub fn merge_thresholds(base: &FeatureThresholds, patch: &FeatureThresholdPatch) -> FeatureThresholds {
    let mut t = base.clone();

    if let Some(v) = patch.buyers_low {
        // ±2 from base, never above mid-1 (keep ordering meaningful).
        let lo = base.buyers_low.saturating_sub(2).max(1);
        let hi = base.buyers_low.saturating_add(2).min(base.buyers_mid.saturating_sub(1).max(1));
        t.buyers_low = v.max(lo).min(hi);
    }

    if let Some(m) = patch.volume_ok_sol_mult {
        let m = m.clamp(0.85, 1.15);
        t.volume_ok_sol = (base.volume_ok_sol * m).max(1.0);
    }

    if let Some(high) = patch.smart_wallet_3plus_min {
        let high = high.clamp(2, 8);
        let low_default = patch.smart_wallet_1plus_min.unwrap_or(base.smart_wallet_1plus_min);
        let low = low_default.min(high.saturating_sub(1).max(1));
        t.smart_wallet_3plus_min = high;
        t.smart_wallet_1plus_min = low;
    } else if let Some(low) = patch.smart_wallet_1plus_min {
        let high = base.smart_wallet_3plus_min;
        t.smart_wallet_1plus_min = low.clamp(1, high.saturating_sub(1).max(1));
    }

    t
}
