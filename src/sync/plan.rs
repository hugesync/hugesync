//! Sync plan generation

use crate::config::Config;
use crate::types::{PlannedAction, SyncAction};

/// A complete sync plan
#[derive(Debug)]
pub struct SyncPlan {
    /// All actions to perform
    pub actions: Vec<PlannedAction>,
    /// Total bytes to transfer
    pub total_bytes: u64,
    /// Estimated bytes saved by delta sync
    pub bytes_saved: u64,
}

impl SyncPlan {
    /// Create a new empty sync plan
    pub fn new() -> Self {
        Self {
            actions: Vec::new(),
            total_bytes: 0,
            bytes_saved: 0,
        }
    }

    /// Get counts by action type
    pub fn counts(&self) -> PlanCounts {
        let mut counts = PlanCounts::default();
        for action in &self.actions {
            match action.action {
                SyncAction::Upload => counts.uploads += 1,
                SyncAction::Download => counts.downloads += 1,
                SyncAction::Delete => counts.deletes += 1,
                SyncAction::Skip => counts.skips += 1,
                SyncAction::Delta => counts.deltas += 1,
                SyncAction::Mkdir => counts.mkdirs += 1,
            }
        }
        counts
    }
}

impl Default for SyncPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// Counts of actions in a sync plan
#[derive(Debug, Default)]
pub struct PlanCounts {
    pub uploads: usize,
    pub downloads: usize,
    pub deletes: usize,
    pub skips: usize,
    pub deltas: usize,
    pub mkdirs: usize,
}

/// Generate a sync plan from planned actions
pub fn generate_plan(actions: Vec<PlannedAction>, _config: &Config) -> SyncPlan {
    let mut plan = SyncPlan::new();

    for mut action in actions {
        // Estimate bytes for delta syncs
        if action.action == SyncAction::Delta {
            // Estimate: delta typically transfers 10-30% of file
            // This is a rough estimate; actual will be calculated during execution
            let estimated_transfer = action.entry.size / 5; // 20% estimate
            action.estimated_bytes = estimated_transfer;
            action.bytes_saved = action.entry.size - estimated_transfer;
            plan.bytes_saved += action.bytes_saved;
        }

        plan.total_bytes += action.estimated_bytes;
        plan.actions.push(action);
    }

    // Sort actions: directories first, then files, deletes last
    plan.actions.sort_by(|a, b| {
        let order_a = action_order(&a.action);
        let order_b = action_order(&b.action);
        order_a.cmp(&order_b).then_with(|| a.entry.path.cmp(&b.entry.path))
    });

    plan
}

/// Get sort order for action types
fn action_order(action: &SyncAction) -> u8 {
    match action {
        SyncAction::Mkdir => 0,
        SyncAction::Upload => 1,
        SyncAction::Download => 1,
        SyncAction::Delta => 2,
        SyncAction::Skip => 3,
        SyncAction::Delete => 4,
    }
}
