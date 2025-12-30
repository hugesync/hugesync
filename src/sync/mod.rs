//! Sync engine and orchestration

pub mod conflict;
pub mod delta_download;
pub mod delta_upload;
pub mod diff;
pub mod execute;
pub mod plan;
pub mod scan;

use crate::config::Config;
use crate::error::Result;
use crate::progress::ProgressTracker;
use crate::storage::StorageBackend;
use crate::types::SyncStats;
use crate::uri::Location;

/// The main sync engine
pub struct SyncEngine {
    /// Configuration
    config: Config,
    /// Source storage backend
    source: StorageBackend,
    /// Destination storage backend
    dest: StorageBackend,
    /// Progress tracker
    progress: ProgressTracker,
}

impl SyncEngine {
    /// Create a new sync engine
    pub fn new(
        config: Config,
        source: StorageBackend,
        dest: StorageBackend,
    ) -> Self {
        let progress = ProgressTracker::new(config.progress);
        Self {
            config,
            source,
            dest,
            progress,
        }
    }

    /// Run the sync operation
    pub async fn sync(&self) -> Result<SyncStats> {
        let start = std::time::Instant::now();
        let mut stats = SyncStats::default();

        // Step 1: Scan source and destination
        tracing::info!("Scanning files...");
        let source_files = self.source.list("").await?;
        let dest_files = self.dest.list("").await?;

        stats.files_scanned = source_files.len() as u64;
        stats.dirs_scanned = source_files.iter().filter(|f| f.is_dir).count() as u64;

        tracing::info!(
            source_files = source_files.len(),
            dest_files = dest_files.len(),
            "Scan complete"
        );

        // Step 2: Compute diff
        tracing::info!("Computing differences...");
        let actions = diff::compute_diff(&source_files, &dest_files, &self.config);

        tracing::info!(
            uploads = actions.iter().filter(|a| a.action == crate::types::SyncAction::Upload).count(),
            deletes = actions.iter().filter(|a| a.action == crate::types::SyncAction::Delete).count(),
            skips = actions.iter().filter(|a| a.action == crate::types::SyncAction::Skip).count(),
            "Diff complete"
        );

        // Step 3: Generate sync plan
        let plan = plan::generate_plan(actions, &self.config);

        // Update stats from plan
        for action in &plan.actions {
            match action.action {
                crate::types::SyncAction::Upload => {
                    stats.bytes_total += action.entry.size;
                    stats.bytes_transferred += action.estimated_bytes;
                }
                crate::types::SyncAction::Delta => {
                    stats.bytes_total += action.entry.size;
                    stats.bytes_transferred += action.estimated_bytes;
                    stats.bytes_saved += action.bytes_saved;
                }
                _ => {}
            }
        }

        // Step 4: Execute (or dry run)
        if self.config.dry_run {
            crate::progress::print_dry_run_summary(&stats);
        } else {
            self.progress.set_total_files(plan.actions.len() as u64);
            
            let exec_stats = execute::execute_plan(
                &plan,
                &self.source,
                &self.dest,
                &self.config,
                &self.progress,
            ).await?;

            stats.files_uploaded = exec_stats.files_uploaded;
            stats.files_downloaded = exec_stats.files_downloaded;
            stats.files_deleted = exec_stats.files_deleted;
            stats.files_skipped = exec_stats.files_skipped;
            stats.files_delta = exec_stats.files_delta;
            stats.bytes_transferred = exec_stats.bytes_transferred;
            stats.bytes_saved = exec_stats.bytes_saved;
            stats.errors = exec_stats.errors;

            self.progress.finish();
        }

        stats.duration_secs = start.elapsed().as_secs_f64();

        if !self.config.dry_run {
            crate::progress::print_summary(&stats);
        }

        Ok(stats)
    }
}

/// Create storage backends from locations
pub async fn create_backends(
    source: &Location,
    dest: &Location,
) -> Result<(StorageBackend, StorageBackend)> {
    let source_backend = match source {
        Location::Local(path) => StorageBackend::local(path.clone()),
        Location::S3 { bucket, prefix } => {
            StorageBackend::s3(bucket.clone(), prefix.clone()).await?
        }
        Location::Gcs { bucket, prefix } => {
            StorageBackend::gcs(bucket.clone(), prefix.clone()).await?
        }
        Location::Azure { container, prefix } => {
            StorageBackend::azure(container.clone(), prefix.clone()).await?
        }
        Location::Ssh { user, host, path } => {
            StorageBackend::ssh(user.clone(), host.clone(), path.clone(), None).await?
        }
    };

    let dest_backend = match dest {
        Location::Local(path) => StorageBackend::local(path.clone()),
        Location::S3 { bucket, prefix } => {
            StorageBackend::s3(bucket.clone(), prefix.clone()).await?
        }
        Location::Gcs { bucket, prefix } => {
            StorageBackend::gcs(bucket.clone(), prefix.clone()).await?
        }
        Location::Azure { container, prefix } => {
            StorageBackend::azure(container.clone(), prefix.clone()).await?
        }
        Location::Ssh { user, host, path } => {
            StorageBackend::ssh(user.clone(), host.clone(), path.clone(), None).await?
        }
    };

    Ok((source_backend, dest_backend))
}
