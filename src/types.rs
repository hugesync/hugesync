//! Core domain types for HugeSync

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

/// File entry representing a file or directory in a sync location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Relative path from the sync root
    pub path: PathBuf,

    /// File size in bytes (0 for directories)
    pub size: u64,

    /// Last modification time
    pub mtime: Option<SystemTime>,

    /// Whether this is a directory
    pub is_dir: bool,

    /// POSIX file mode (permissions)
    pub mode: Option<u32>,

    /// ETag or content hash (for remote files)
    pub etag: Option<String>,
}

impl FileEntry {
    /// Create a new file entry
    pub fn new(path: PathBuf, size: u64, is_dir: bool) -> Self {
        Self {
            path,
            size,
            mtime: None,
            is_dir,
            mode: None,
            etag: None,
        }
    }

    /// Create a file entry with full metadata
    pub fn with_metadata(
        path: PathBuf,
        size: u64,
        mtime: SystemTime,
        mode: u32,
    ) -> Self {
        Self {
            path,
            size,
            mtime: Some(mtime),
            is_dir: false,
            mode: Some(mode),
            etag: None,
        }
    }

    /// Get the sidecar signature file path (.hssig)
    pub fn sidecar_path(&self) -> PathBuf {
        let mut sidecar = self.path.clone();
        let filename = sidecar
            .file_name()
            .map(|s| format!("{}.hssig", s.to_string_lossy()))
            .unwrap_or_else(|| ".hssig".to_string());
        sidecar.set_file_name(filename);
        sidecar
    }
}

/// Action to perform during sync
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncAction {
    /// Upload file (new or full replacement)
    Upload,

    /// Download file
    Download,

    /// Delete file from destination
    Delete,

    /// Skip file (already in sync)
    Skip,

    /// Delta sync (upload only changed blocks)
    Delta,

    /// Create directory
    Mkdir,
}

impl SyncAction {
    /// Check if this action transfers data
    pub fn transfers_data(&self) -> bool {
        matches!(self, SyncAction::Upload | SyncAction::Download | SyncAction::Delta)
    }
}

/// A planned sync action with metadata
#[derive(Debug, Clone)]
pub struct PlannedAction {
    /// The file entry being acted upon
    pub entry: FileEntry,

    /// The action to perform
    pub action: SyncAction,

    /// Estimated bytes to transfer (may differ from file size for delta)
    pub estimated_bytes: u64,

    /// For delta: estimated bytes saved
    pub bytes_saved: u64,
}

impl PlannedAction {
    /// Create a new planned action
    pub fn new(entry: FileEntry, action: SyncAction) -> Self {
        let estimated_bytes = if action.transfers_data() && !entry.is_dir {
            entry.size
        } else {
            0
        };

        Self {
            entry,
            action,
            estimated_bytes,
            bytes_saved: 0,
        }
    }

    /// Create a delta action with savings estimate
    pub fn delta(entry: FileEntry, estimated_bytes: u64, bytes_saved: u64) -> Self {
        Self {
            entry,
            action: SyncAction::Delta,
            estimated_bytes,
            bytes_saved,
        }
    }
}

/// Statistics for a sync operation
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStats {
    /// Total files scanned
    pub files_scanned: u64,

    /// Total directories scanned
    pub dirs_scanned: u64,

    /// Files uploaded
    pub files_uploaded: u64,

    /// Files downloaded
    pub files_downloaded: u64,

    /// Files deleted
    pub files_deleted: u64,

    /// Files skipped (already in sync)
    pub files_skipped: u64,

    /// Files synced via delta
    pub files_delta: u64,

    /// Total bytes transferred
    pub bytes_transferred: u64,

    /// Total bytes saved by delta sync
    pub bytes_saved: u64,

    /// Total bytes that would have been transferred without delta
    pub bytes_total: u64,

    /// Errors encountered
    pub errors: u64,

    /// Duration in seconds
    pub duration_secs: f64,
}

impl SyncStats {
    /// Calculate bandwidth savings percentage
    pub fn savings_percent(&self) -> f64 {
        if self.bytes_total == 0 {
            0.0
        } else {
            (self.bytes_saved as f64 / self.bytes_total as f64) * 100.0
        }
    }

    /// Calculate transfer rate in bytes per second
    pub fn transfer_rate(&self) -> f64 {
        if self.duration_secs == 0.0 {
            0.0
        } else {
            self.bytes_transferred as f64 / self.duration_secs
        }
    }
}

/// Atomic statistics for thread-safe concurrent sync operations
///
/// Uses atomic operations for lock-free updates from multiple threads.
/// Can be converted to SyncStats for display and serialization.
#[derive(Debug)]
pub struct AtomicSyncStats {
    pub files_scanned: AtomicU64,
    pub dirs_scanned: AtomicU64,
    pub files_uploaded: AtomicU64,
    pub files_downloaded: AtomicU64,
    pub files_deleted: AtomicU64,
    pub files_skipped: AtomicU64,
    pub files_delta: AtomicU64,
    pub bytes_transferred: AtomicU64,
    pub bytes_saved: AtomicU64,
    pub bytes_total: AtomicU64,
    pub errors: AtomicU64,
}

impl AtomicSyncStats {
    /// Create new atomic stats with all counters at zero
    pub fn new() -> Self {
        Self {
            files_scanned: AtomicU64::new(0),
            dirs_scanned: AtomicU64::new(0),
            files_uploaded: AtomicU64::new(0),
            files_downloaded: AtomicU64::new(0),
            files_deleted: AtomicU64::new(0),
            files_skipped: AtomicU64::new(0),
            files_delta: AtomicU64::new(0),
            bytes_transferred: AtomicU64::new(0),
            bytes_saved: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Atomically increment files uploaded counter
    #[inline]
    pub fn inc_files_uploaded(&self) {
        self.files_uploaded.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increment files downloaded counter
    #[inline]
    pub fn inc_files_downloaded(&self) {
        self.files_downloaded.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increment files deleted counter
    #[inline]
    pub fn inc_files_deleted(&self) {
        self.files_deleted.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increment files skipped counter
    #[inline]
    pub fn inc_files_skipped(&self) {
        self.files_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increment delta sync counter
    #[inline]
    pub fn inc_files_delta(&self) {
        self.files_delta.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically increment errors counter
    #[inline]
    pub fn inc_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Atomically add to bytes transferred
    #[inline]
    pub fn add_bytes_transferred(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Atomically add to bytes saved
    #[inline]
    pub fn add_bytes_saved(&self, bytes: u64) {
        self.bytes_saved.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Atomically add to bytes total
    #[inline]
    pub fn add_bytes_total(&self, bytes: u64) {
        self.bytes_total.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Convert to non-atomic SyncStats (for display/serialization)
    ///
    /// Note: This provides a snapshot; values may change if updates are ongoing.
    pub fn snapshot(&self) -> SyncStats {
        SyncStats {
            files_scanned: self.files_scanned.load(Ordering::Relaxed),
            dirs_scanned: self.dirs_scanned.load(Ordering::Relaxed),
            files_uploaded: self.files_uploaded.load(Ordering::Relaxed),
            files_downloaded: self.files_downloaded.load(Ordering::Relaxed),
            files_deleted: self.files_deleted.load(Ordering::Relaxed),
            files_skipped: self.files_skipped.load(Ordering::Relaxed),
            files_delta: self.files_delta.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            bytes_saved: self.bytes_saved.load(Ordering::Relaxed),
            bytes_total: self.bytes_total.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            duration_secs: 0.0, // Set by caller
        }
    }
}

impl Default for AtomicSyncStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Sync direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    /// Local to remote (upload)
    Push,

    /// Remote to local (download)
    Pull,

    /// Bidirectional sync
    Bidirectional,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_entry_sidecar_path() {
        let entry = FileEntry::new(PathBuf::from("data/large.bin"), 1000, false);
        assert_eq!(entry.sidecar_path(), PathBuf::from("data/large.bin.hssig"));
    }

    #[test]
    fn test_sync_action_transfers_data() {
        assert!(SyncAction::Upload.transfers_data());
        assert!(SyncAction::Download.transfers_data());
        assert!(SyncAction::Delta.transfers_data());
        assert!(!SyncAction::Skip.transfers_data());
        assert!(!SyncAction::Delete.transfers_data());
    }

    #[test]
    fn test_sync_stats_savings() {
        let mut stats = SyncStats::default();
        stats.bytes_total = 1000;
        stats.bytes_saved = 900;
        assert!((stats.savings_percent() - 90.0).abs() < 0.01);
    }
}
