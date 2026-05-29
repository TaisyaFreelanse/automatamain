//! Offline counterfactual for the spam_dev gate.
//!
//! For each spam_dev mint (read from a CSV of `mint,skip_unix`) that later
//! reached a high absolute peak, reconstruct the scoring-window feature vector
//! from `trades` + `coin_mcap_tape` + the persisted smart-money registry, then
//! run the REAL `ScoreEngine` (prod `filter_config.yaml`, legacy path) under
//! several `spam_dev_penalty` values. Reports how many of the good runners would
//! still reach A+ (and thus pass `spam_dev_require_a_plus`) vs. be cut.
//!
//! Read-only. No live state is touched. Run on the prod box where DATABASE_URL,
//! filter_config.yaml and smart_money.json are available.
//!
//! Usage:
//!   cf_spamdev <filter_config.yaml> <smart_money.json> <spam_dev_mints.csv> [win_slots]

use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

use loggaper::scoring::anti_bundle::compute_bundle_stats;
use loggaper::scoring::config::ScoringConfig;
use loggaper::scoring::dev_ranker::{DevCategory, DevRecord};
use loggaper::scoring::features::{
    momentum_peak_pct, EarlyBuyersSnapshot, EarlyTapePoint, ScoringTapeDerived, TokenFeatures,
};
use loggaper::scoring::score_engine::{ScoreEngine, Tier};

const TTL_SECS: u64 = 172_800; // persistence.entity_ttl_secs (prod)
const PENALTIES: [i32; 4] = [0, -1, -2, -3];

#[derive(Deserialize)]
struct WalletRecord {
    trades: u64,
    wins: u64,
    #[serde(default)]
    pnl_pct_sum: f64,
    #[serde(default)]
    last_seen_unix: u64,
}

impl WalletRecord {
    fn winrate(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.wins as f64 / self.trades as f64
        }
    }
    fn avg_pnl_pct(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.pnl_pct_sum / self.trades as f64
        }
    }
    /// Same heuristic as smart_money::is_smart (ttl applied vs `now`).
    fn is_smart(&self, now: u64) -> bool {
        if now.saturating_sub(self.last_seen_unix) > TTL_SECS {
            return false;
        }
        if self.trades < 4 {
            return false;
        }
        self.winrate() >= 0.6 && self.avg_pnl_pct() >= 5.0
    }
}

struct TradeRow {
    trader: String,
    is_buy: bool,
    size: f64,
    is_sol: bool,
    mcap: f64,
    role: String,
    slot: i64,
}

#[derive(Default, Clone)]
struct Outcome {
    a_plus: usize,
    a: usize,
    skip: usize,
    buy_a_plus: usize, // tier==A+ AND momentum_good_satisfied (real buy condition)
    strong_a: usize,   // tier>=A AND score>=8 AND momentum_good_satisfied (alt scenario)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: cf_spamdev <filter_config.yaml> <smart_money.json> <mints.csv> [win_slots]");
        std::process::exit(2);
    }
    let cfg_path = &args[1];
    let sm_path = &args[2];
    let csv_path = &args[3];
    let win_slots: i64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(14);

    // --- prod scoring config (the `scoring:` node) ----------------------
    let raw: serde_yaml::Value = serde_yaml::from_str(&std::fs::read_to_string(cfg_path)?)?;
    let scoring_node = raw
        .get("scoring")
        .cloned()
        .ok_or("no `scoring:` node in config")?;
    let cfg: ScoringConfig = serde_yaml::from_value(scoring_node)?;
    println!(
        "[cfg] legacy={} window_ms={} a={} a_plus={} win_slots={} smart_bypass={}",
        cfg.legacy_scoring,
        cfg.scoring_window_ms,
        cfg.a_threshold,
        cfg.a_plus_threshold,
        win_slots,
        cfg.momentum_good_smart_bypass,
    );

    // --- smart-money registry ------------------------------------------
    let sm: HashMap<String, WalletRecord> =
        serde_json::from_slice(&std::fs::read(sm_path)?)?;
    let now = now_unix();
    let smart_total = sm.values().filter(|r| r.is_smart(now)).count();
    println!("[smart] records={} smart_now={}", sm.len(), smart_total);

    // --- spam_dev mint list --------------------------------------------
    let csv = std::fs::read_to_string(csv_path)?;
    let mints: Vec<String> = csv
        .lines()
        .filter_map(|l| l.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    println!("[mints] csv_rows={}", mints.len());

    let db_url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&db_url)
        .await?;

    // peak per mint
    let peak_rows = sqlx::query(
        "SELECT coin_address, max(mcap_sol)::float8 AS peak \
         FROM coin_mcap_tape WHERE coin_address = ANY($1) GROUP BY coin_address",
    )
    .bind(&mints)
    .fetch_all(&pool)
    .await?;
    let mut peak: HashMap<String, f64> = HashMap::new();
    for r in &peak_rows {
        peak.insert(r.get::<String, _>("coin_address"), r.get::<f64, _>("peak"));
    }

    // candidate sets
    let mut cands: Vec<(String, f64)> = mints
        .iter()
        .filter_map(|m| peak.get(m).map(|p| (m.clone(), *p)))
        .filter(|(_, p)| *p >= 150.0)
        .collect();
    cands.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    println!("[cands] peak>=150: {}", cands.len());

    // accumulators keyed by penalty
    let mut acc_150: HashMap<i32, Outcome> = HashMap::new();
    let mut acc_250: HashMap<i32, Outcome> = HashMap::new();
    let mut acc_405: HashMap<i32, Outcome> = HashMap::new();

    println!("\n================ PER-MINT (peak>=250) ================");
    println!(
        "{:<6} {:>6} {:>5} {:>6} {:>7} {:>6} {:>6}  score@[0,-1,-2,-3]  tier@[0,-1,-2,-3]",
        "mint", "peak", "smrt", "buyrs", "mom%", "bvol", "bndl"
    );

    for (mint, pk) in &cands {
        let created: i64 = match sqlx::query("SELECT created_at FROM coins WHERE coin_address=$1")
            .bind(mint)
            .fetch_optional(&pool)
            .await?
        {
            Some(r) => r.get::<i64, _>("created_at"),
            None => continue,
        };
        let win_end = created + win_slots;

        let trows = sqlx::query(
            "SELECT trader_address, is_buy, size::float8 AS size, currency::text AS currency, \
             COALESCE(market_cap,0)::float8 AS mcap, role::text AS role, slot_time \
             FROM trades WHERE coin_address=$1 AND slot_time BETWEEN $2 AND $3 \
             ORDER BY slot_time, id",
        )
        .bind(mint)
        .bind(created)
        .bind(win_end)
        .fetch_all(&pool)
        .await?;
        if trows.is_empty() {
            continue;
        }
        let trades: Vec<TradeRow> = trows
            .iter()
            .map(|r| TradeRow {
                trader: r.get::<String, _>("trader_address"),
                is_buy: r.get::<bool, _>("is_buy"),
                size: r.get::<f64, _>("size"),
                is_sol: r.get::<String, _>("currency") == "sol",
                mcap: r.get::<f64, _>("mcap"),
                role: r.get::<String, _>("role"),
                slot: r.get::<i64, _>("slot_time"),
            })
            .collect();

        let feats = reconstruct(
            &trades,
            created,
            win_slots,
            cfg.thresholds.momentum_good_low_pct,
            &sm,
            now,
        );
        if feats.is_none() {
            continue;
        }
        let (tf, smart_cnt, buyers, mom_pct, bvol) = feats.unwrap();

        // score under each penalty
        let mut scores = [0i32; 4];
        let mut tiers = ["?"; 4];
        for (i, pen) in PENALTIES.iter().enumerate() {
            let mut c = cfg.clone();
            c.spam_dev_penalty = *pen;
            let eng = ScoreEngine::new(&c);
            let bd = eng.score(&tf, &c.thresholds);
            scores[i] = bd.total;
            let has_mom = bd.items.iter().any(|(n, _)| *n == "momentum_good");
            let mom_ok = has_mom || (c.momentum_good_smart_bypass > 0 && smart_cnt >= c.momentum_good_smart_bypass);
            tiers[i] = match bd.tier {
                Tier::APlus => "A+",
                Tier::A => "A",
                Tier::Skip => "Sk",
            };
            let bump = |o: &mut Outcome| {
                match bd.tier {
                    Tier::APlus => o.a_plus += 1,
                    Tier::A => o.a += 1,
                    Tier::Skip => o.skip += 1,
                }
                if bd.tier == Tier::APlus && mom_ok {
                    o.buy_a_plus += 1;
                }
                if bd.tier != Tier::Skip && bd.total >= 8 && mom_ok {
                    o.strong_a += 1;
                }
            };
            bump(acc_150.entry(*pen).or_default());
            if *pk >= 250.0 {
                bump(acc_250.entry(*pen).or_default());
            }
            if *pk >= 405.0 {
                bump(acc_405.entry(*pen).or_default());
            }
        }

        if *pk >= 250.0 {
            println!(
                "{:<6} {:>6.0} {:>5} {:>6} {:>6.0}% {:>6.1} {:>6.2}  [{:>3},{:>3},{:>3},{:>3}]  [{:>2},{:>2},{:>2},{:>2}]",
                &mint[..mint.len().min(6)],
                pk,
                smart_cnt,
                buyers,
                mom_pct,
                bvol,
                tf.bundle.similar_size_ratio.max(tf.bundle.identical_size_ratio),
                scores[0], scores[1], scores[2], scores[3],
                tiers[0], tiers[1], tiers[2], tiers[3],
            );
        }
    }

    let report = |label: &str, acc: &HashMap<i32, Outcome>, n: usize| {
        println!("\n================ SUMMARY: {label} (n={n}) ================");
        println!(
            "{:>7}  {:>4} {:>4} {:>4}   {:>10}   {:>14}",
            "penalty", "A+", "A", "Skip", "BUY(A+&mom)", "strongA(>=8&mom)"
        );
        for pen in PENALTIES {
            let o = acc.get(&pen).cloned().unwrap_or_default();
            println!(
                "{:>7}  {:>4} {:>4} {:>4}   {:>10}   {:>14}",
                pen, o.a_plus, o.a, o.skip, o.buy_a_plus, o.strong_a
            );
        }
    };

    report("peak>=150", &acc_150, cands.len());
    report(
        "peak>=250 (graduation zone)",
        &acc_250,
        cands.iter().filter(|(_, p)| *p >= 250.0).count(),
    );
    report(
        "peak>=405 (full graduation)",
        &acc_405,
        cands.iter().filter(|(_, p)| *p >= 405.0).count(),
    );

    println!(
        "\nNOTE: BUY(A+&mom) = tier A+ AND (momentum_good OR smart>={}) — the real spam_dev \
         buy condition under spam_dev_require_a_plus=true. strongA = an alt gate allowing \
         tier>=A with score>=8. dev_ranker assumed Neutral (optimistic). continuation/parabolic \
         confirm gates are NOT modeled (would only cut further).",
        cfg.momentum_good_smart_bypass
    );

    Ok(())
}

/// Build a cumulative tape snapshot over trades with slot <= bound.
fn snapshot_at(trades: &[TradeRow], bound: i64, init_mcap: f64) -> (EarlyTapePoint, f64) {
    let mut cum_buyers: HashSet<&str> = HashSet::new();
    let mut cum_buy_vol = 0.0f64;
    let mut cum_sell_raw: u64 = 0;
    let mut cum_sell_events: u64 = 0;
    let mut mcap_at = init_mcap;
    let mut peak = init_mcap;
    for t in trades {
        if t.slot > bound {
            break;
        }
        if t.mcap > 0.0 {
            mcap_at = t.mcap;
            peak = peak.max(t.mcap);
        }
        if t.role != "regular" && t.role != "sniper" {
            continue;
        }
        if t.is_buy {
            cum_buyers.insert(&t.trader);
            if t.is_sol {
                cum_buy_vol += t.size;
            }
        } else if t.is_sol {
            cum_sell_raw = cum_sell_raw.saturating_add((t.size * 1e9) as u64);
            cum_sell_events += 1;
        }
    }
    (
        EarlyTapePoint {
            buyer_count: cum_buyers.len() as u64,
            still_long: cum_buyers.len() as u64,
            already_sold: 0,
            buy_volume_sol: cum_buy_vol,
            cum_sell_raw,
            cum_sell_events,
            mcap_sol: mcap_at,
        },
        peak,
    )
}

/// Reconstruct the scoring-window TokenFeatures from windowed trades, FAITHFULLY
/// modelling the live observer's early-exit (first sample at t=0 launch floor;
/// stop as soon as peak rises >= `mom_low_pct`).
/// Returns (features, smart_count, buyer_count, momentum_pct, buy_volume_sol).
fn reconstruct(
    trades: &[TradeRow],
    created: i64,
    win_slots: i64,
    mom_low_pct: f64,
    sm: &HashMap<String, WalletRecord>,
    now: u64,
) -> Option<(TokenFeatures, u32, u64, f64, f64)> {
    // launch floor = first trade mcap (slot == created)
    let init_mcap = trades.iter().find(|t| t.mcap > 0.0).map(|t| t.mcap)?;
    if init_mcap <= 0.0 {
        return None;
    }

    // Live sample times -> slot boundaries: t=0, t=W/3, t=2W/3, final at W.
    let b0 = created;
    let b1 = created + win_slots / 3;
    let b2 = created + (win_slots * 2) / 3;
    let b3 = created + win_slots;

    // Sample cumulative snapshots and apply early-exit at the >= mom_low_pct peak.
    let (p0, _) = snapshot_at(trades, b0, init_mcap);
    let (p1, peak1) = snapshot_at(trades, b1, init_mcap);
    let mut points = vec![p0.clone(), p1];
    let mut eff_end = b1;
    let early1 = momentum_peak_pct(init_mcap, peak1) >= mom_low_pct;
    if !early1 {
        let (p2, peak2) = snapshot_at(trades, b2, init_mcap);
        points.push(p2);
        eff_end = b2;
        let early2 = momentum_peak_pct(init_mcap, peak2) >= mom_low_pct;
        if !early2 {
            // No early exit: final sample at full window replaces the last point.
            let (p3, _) = snapshot_at(trades, b3, init_mcap);
            *points.last_mut().unwrap() = p3;
            eff_end = b3;
        }
    }

    // Effective window = [created, eff_end]: compute all per-wallet features here.
    let mut buy_size: HashMap<String, f64> = HashMap::new();
    let mut has_sell: HashSet<String> = HashSet::new();
    let mut regulars: HashSet<String> = HashSet::new();
    let mut snipers: HashSet<String> = HashSet::new();
    let mut seen_buyer: HashSet<String> = HashSet::new();
    let mut buy_volume_sol = 0.0f64;
    let mut peak_mcap = init_mcap;
    let mut last_mcap = init_mcap;
    for t in trades {
        if t.slot > eff_end {
            break;
        }
        if t.mcap > 0.0 {
            peak_mcap = peak_mcap.max(t.mcap);
            last_mcap = t.mcap;
        }
        if t.role != "regular" && t.role != "sniper" {
            continue;
        }
        if t.is_buy {
            if t.is_sol {
                buy_volume_sol += t.size;
                *buy_size.entry(t.trader.clone()).or_insert(0.0) += t.size;
            }
            if seen_buyer.insert(t.trader.clone()) {
                if t.role == "sniper" {
                    snipers.insert(t.trader.clone());
                } else {
                    regulars.insert(t.trader.clone());
                }
            }
        } else {
            has_sell.insert(t.trader.clone());
        }
    }

    let regular_buyer_count = regulars.len() as u64;
    let sniper_count = snipers.len() as u64;
    let buyer_count = regular_buyer_count + sniper_count;
    if buyer_count == 0 {
        return None;
    }

    let mut smart_cnt = 0u32;
    let mut smart_exits = 0u32;
    for w in seen_buyer.iter() {
        if let Some(rec) = sm.get(w) {
            if rec.is_smart(now) {
                smart_cnt += 1;
                if has_sell.contains(w) {
                    smart_exits += 1;
                }
            }
        }
    }

    let mut still_long = 0u64;
    let mut already_sold = 0u64;
    for w in seen_buyer.iter() {
        if has_sell.contains(w) {
            already_sold += 1;
        } else {
            still_long += 1;
        }
    }

    let sizes: Vec<f64> = buy_size.values().copied().collect();
    let bundle = compute_bundle_stats(&sizes, 0.05);

    let tape = ScoringTapeDerived::from_tape_points(&points, smart_exits);

    let mom_pct = momentum_peak_pct(init_mcap, peak_mcap.max(last_mcap));

    let buy_to_sell_ratio = if already_sold == 0 {
        if still_long > 0 {
            still_long as f64
        } else {
            0.0
        }
    } else {
        still_long as f64 / already_sold as f64
    };

    let tf = TokenFeatures {
        mint: solana_address::Address::new_from_array([0; 32]),
        dev: solana_address::Address::new_from_array([1; 32]),
        dev_has_history: false,
        dev_total_coins: 0,
        dev_pnl_avg: 0.0,
        dev_holders_avg: 0,
        dev_volume_avg: 0.0,
        dev_trades_avg: 0,
        dev_buy_to_sell_ratio: 0.0,
        dev_category: DevCategory::Neutral,
        dev_rank_score: 0.0,
        dev_rank_record: DevRecord::default(),
        is_spam_dev: true,
        current_mcap_sol: last_mcap,
        initial_mcap_sol: init_mcap,
        peak_mcap_sol: peak_mcap.max(last_mcap),
        buyers: EarlyBuyersSnapshot {
            regulars: Vec::new(),
            snipers: Vec::new(),
        },
        regular_buyer_count,
        sniper_count,
        buy_volume_sol,
        still_long_count: still_long,
        already_sold_count: already_sold,
        buy_to_sell_ratio,
        bundle,
        smart_wallet_count: smart_cnt,
        buyer_velocity_new_per_slice: tape.buyer_velocity_new_per_slice.clone(),
        buyer_velocity_persistence: tape.buyer_velocity_persistence,
        sell_pressure_score: tape.sell_pressure_score,
        absorb_quality_score: tape.absorb_quality_score,
        sell_events_window: tape.sell_events_window,
        sell_volume_window_sol: tape.sell_volume_window_sol,
        repeat_dump_slices: tape.repeat_dump_slices,
        smart_wallet_early_exits: tape.smart_wallet_early_exits,
    };

    Some((tf, smart_cnt, buyer_count, mom_pct, buy_volume_sol))
}
