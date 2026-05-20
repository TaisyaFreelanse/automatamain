use serde::Serialize;

use crate::persistence::creators::CreatorStatistics;

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum OpenReason {
    DevStats(CreatorStatistics),
    TraderStats,
}
