//! Heuristic anti-bundle / anti-fake-volume detection.
//!
//! Inputs are the raw SOL buy sizes observed in the early window. These come
//! from `Trader::total_spent()` aggregated per wallet — i.e. one entry per
//! wallet, not per transaction. That is intentional: an attacker that does
//! 6 identical transactions from the same wallet is just one wallet, while
//! 6 identical transactions from 6 fresh wallets is a real bundle signal.

#[derive(Debug, Clone)]
pub struct BundleStats {
    pub similar_size_ratio: f64,
    pub identical_size_ratio: f64,
    pub median_size_sol: f64,
    pub max_size_sol: f64,
}

impl BundleStats {
    pub fn empty() -> Self {
        Self {
            similar_size_ratio: 0.0,
            identical_size_ratio: 0.0,
            median_size_sol: 0.0,
            max_size_sol: 0.0,
        }
    }
}

pub fn compute_bundle_stats(buy_sizes_sol: &[f64], similar_tolerance: f64) -> BundleStats {
    let mut sizes: Vec<f64> = buy_sizes_sol
        .iter()
        .copied()
        .filter(|s| *s > 0.0)
        .collect();
    if sizes.len() < 3 {
        // Not enough samples to talk about "bundle similarity".
        return BundleStats::empty();
    }
    sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let median = sizes[sizes.len() / 2];
    let max = sizes.last().copied().unwrap_or(0.0);

    let similar_count = if median > 0.0 {
        let lo = median * (1.0 - similar_tolerance);
        let hi = median * (1.0 + similar_tolerance);
        sizes.iter().filter(|s| **s >= lo && **s <= hi).count()
    } else {
        0
    };

    // Bucket exactly-equal sizes by milli-SOL granularity (prices below 1µSOL
    // would otherwise never collide due to f64 noise from on-chain decimals).
    let mut buckets: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for s in &sizes {
        let key = (*s * 1_000.0).round() as i64;
        *buckets.entry(key).or_insert(0) += 1;
    }
    let identical_count = buckets.values().copied().max().unwrap_or(0);

    let total = sizes.len() as f64;
    BundleStats {
        similar_size_ratio: similar_count as f64 / total,
        identical_size_ratio: identical_count as f64 / total,
        median_size_sol: median,
        max_size_sol: max,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_when_too_few_samples() {
        let s = compute_bundle_stats(&[1.0, 1.0], 0.05);
        assert_eq!(s.similar_size_ratio, 0.0);
        assert_eq!(s.identical_size_ratio, 0.0);
    }

    #[test]
    fn detects_identical_buys() {
        let s = compute_bundle_stats(&[0.1, 0.1, 0.1, 0.1, 0.1, 5.0], 0.05);
        assert!(s.identical_size_ratio >= 0.83);
        assert!(s.similar_size_ratio >= 0.83);
    }

    #[test]
    fn organic_distribution_low_bundle() {
        let s = compute_bundle_stats(&[0.01, 0.05, 0.2, 0.3, 0.7, 1.5, 4.0], 0.05);
        assert!(s.identical_size_ratio < 0.5);
        assert!(s.similar_size_ratio < 0.5);
    }
}
