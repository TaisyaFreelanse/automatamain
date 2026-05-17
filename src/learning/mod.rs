//! Self-learning pipeline: log trades + skipped mints, periodic analysis,
//! small persisted threshold overrides (merged into scoring).

mod db;
mod engine;
pub mod merge;
mod snapshot;

pub use db::LearningLogPg;
pub use engine::spawn_learning_engine;
pub use merge::{
    merge_thresholds, FeatureThresholdPatch, LearningOverridesFile, load_patch, save_patch,
};
pub use snapshot::LearningTradeSnapshot;
