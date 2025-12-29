//! Diff computation between source and destination
//!
//! Implements rsync-compatible comparison modes:
//! - Default: Compare by size and mtime
//! - --checksum (-c): Compare by checksum (uses etag or computed hash)
//! - --size-only: Compare by size only
//! - --update (-u): Skip files newer on receiver
//! - --ignore-existing: Skip files that exist on receiver
//! - --existing: Only update files that already exist on receiver
//! - --modify-window: Compare mtimes with tolerance

use crate::config::Config;
use crate::sync::scan::build_file_map;
use crate::types::{FileEntry, PlannedAction, SyncAction};
use std::collections::HashSet;
use std::time::Duration;

/// Compute the diff between source and destination files
pub fn compute_diff(
    source_files: &[FileEntry],
    dest_files: &[FileEntry],
    config: &Config,
) -> Vec<PlannedAction> {
    let _source_map = build_file_map(source_files);
    let dest_map = build_file_map(dest_files);

    let mut actions = Vec::new();

    // Check each source file
    for source_entry in source_files {
        // Skip files outside size limits
        if !source_entry.is_dir && !config.file_within_size_limits(source_entry.size) {
            tracing::debug!(
                path = %source_entry.path.display(),
                size = source_entry.size,
                "Skipping file outside size limits"
            );
            continue;
        }

        if source_entry.is_dir {
            // Create directories at destination
            let path_str = source_entry.path.to_string_lossy().to_string();
            if !dest_map.contains_key(&path_str) {
                actions.push(PlannedAction::new(source_entry.clone(), SyncAction::Mkdir));
            }
            continue;
        }

        let path_str = source_entry.path.to_string_lossy().to_string();

        match dest_map.get(&path_str) {
            None => {
                // File doesn't exist at destination

                // --existing: Only update existing files, skip new files
                if config.existing {
                    tracing::debug!(
                        path = %source_entry.path.display(),
                        "Skipping new file (--existing)"
                    );
                    continue;
                }

                // New file - upload
                actions.push(PlannedAction::new(source_entry.clone(), SyncAction::Upload));
            }
            Some(dest_entry) => {
                // File exists at destination

                // --ignore-existing: Skip files that exist on receiver
                if config.ignore_existing {
                    tracing::debug!(
                        path = %source_entry.path.display(),
                        "Skipping existing file (--ignore-existing)"
                    );
                    actions.push(PlannedAction::new(source_entry.clone(), SyncAction::Skip));
                    continue;
                }

                // --update: Skip files newer on receiver
                if config.update && is_dest_newer(source_entry, dest_entry, config.modify_window) {
                    tracing::debug!(
                        path = %source_entry.path.display(),
                        "Skipping file newer on receiver (--update)"
                    );
                    actions.push(PlannedAction::new(source_entry.clone(), SyncAction::Skip));
                    continue;
                }

                // Check if files match based on comparison mode
                let matches = if config.checksum {
                    files_match_checksum(source_entry, dest_entry)
                } else if config.size_only {
                    files_match_size_only(source_entry, dest_entry)
                } else {
                    files_match_mtime_size(source_entry, dest_entry, config.modify_window)
                };

                if matches {
                    actions.push(PlannedAction::new(source_entry.clone(), SyncAction::Skip));
                } else {
                    // File differs - use delta for large files if supported
                    let action = if config.should_use_delta(source_entry.size) {
                        SyncAction::Delta
                    } else {
                        SyncAction::Upload
                    };
                    actions.push(PlannedAction::new(source_entry.clone(), action));
                }
            }
        }
    }

    // Check for files to delete (if --delete is enabled)
    if config.delete {
        let source_paths: HashSet<_> = source_files
            .iter()
            .map(|e| e.path.to_string_lossy().to_string())
            .collect();

        for dest_entry in dest_files {
            let path_str = dest_entry.path.to_string_lossy().to_string();
            if !source_paths.contains(&path_str) {
                actions.push(PlannedAction::new(dest_entry.clone(), SyncAction::Delete));
            }
        }
    }

    actions
}

/// Check if destination file is newer than source (for --update mode)
fn is_dest_newer(source: &FileEntry, dest: &FileEntry, modify_window: i64) -> bool {
    match (source.mtime, dest.mtime) {
        (Some(s_mtime), Some(d_mtime)) => {
            // Destination is newer if its mtime > source mtime + window
            let window = Duration::from_secs(modify_window.unsigned_abs());
            match d_mtime.duration_since(s_mtime) {
                Ok(diff) => diff > window,
                Err(_) => false, // source is newer or equal
            }
        }
        // Can't compare, assume not newer
        _ => false,
    }
}

/// Check if files match by checksum (etag comparison)
fn files_match_checksum(source: &FileEntry, dest: &FileEntry) -> bool {
    // If sizes differ, content must differ
    if source.size != dest.size {
        return false;
    }

    // Compare etags if both are available
    match (&source.etag, &dest.etag) {
        (Some(s_etag), Some(d_etag)) => s_etag == d_etag,
        // If we can't compare checksums, assume different
        _ => false,
    }
}

/// Check if files match by size only (ignore mtime)
fn files_match_size_only(source: &FileEntry, dest: &FileEntry) -> bool {
    source.size == dest.size
}

/// Check if files match by mtime and size (default mode)
fn files_match_mtime_size(source: &FileEntry, dest: &FileEntry, modify_window: i64) -> bool {
    // Different sizes means different content
    if source.size != dest.size {
        return false;
    }

    // If both have mtimes, compare them with window
    match (source.mtime, dest.mtime) {
        (Some(s_mtime), Some(d_mtime)) => {
            // Get the absolute difference in seconds
            let diff_secs = if s_mtime > d_mtime {
                s_mtime.duration_since(d_mtime).map(|d| d.as_secs()).unwrap_or(u64::MAX)
            } else {
                d_mtime.duration_since(s_mtime).map(|d| d.as_secs()).unwrap_or(u64::MAX)
            };

            // Default window is 1 second, but can be configured
            let window = if modify_window > 0 {
                modify_window as u64
            } else {
                1 // Default 1 second tolerance
            };

            diff_secs <= window
        }
        // If we can't compare mtimes, use size only
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::SystemTime;

    fn make_entry(path: &str, size: u64) -> FileEntry {
        FileEntry {
            path: PathBuf::from(path),
            size,
            mtime: Some(SystemTime::now()),
            is_dir: false,
            mode: None,
            etag: None,
        }
    }

    #[test]
    fn test_new_files_detected() {
        let source = vec![make_entry("file1.txt", 100)];
        let dest = vec![];
        let config = Config::default();

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Upload);
    }

    #[test]
    fn test_unchanged_files_skipped() {
        let entry = make_entry("file1.txt", 100);
        let source = vec![entry.clone()];
        let dest = vec![entry];
        let config = Config::default();

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Skip);
    }

    #[test]
    fn test_delete_detection() {
        let source = vec![];
        let dest = vec![make_entry("old_file.txt", 50)];
        let mut config = Config::default();
        config.delete = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Delete);
    }

    #[test]
    fn test_delta_for_large_files() {
        let source_entry = make_entry("large.bin", 100 * 1024 * 1024);
        let dest_entry = make_entry("large.bin", 99 * 1024 * 1024);

        let source = vec![source_entry];
        let dest = vec![dest_entry];
        let config = Config::default();

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Delta);
    }

    #[test]
    fn test_ignore_existing() {
        let source = vec![make_entry("file1.txt", 100)];
        let dest = vec![make_entry("file1.txt", 50)]; // Different size
        let mut config = Config::default();
        config.ignore_existing = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Skip);
    }

    #[test]
    fn test_existing_only() {
        let source = vec![make_entry("new_file.txt", 100)];
        let dest = vec![];
        let mut config = Config::default();
        config.existing = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 0); // New file skipped
    }

    #[test]
    fn test_size_only() {
        let mut source_entry = make_entry("file.txt", 100);
        let mut dest_entry = make_entry("file.txt", 100);
        // Different mtimes, same size
        source_entry.mtime = Some(SystemTime::UNIX_EPOCH);
        dest_entry.mtime = Some(SystemTime::now());

        let source = vec![source_entry];
        let dest = vec![dest_entry];
        let mut config = Config::default();
        config.size_only = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Skip); // Size matches
    }

    #[test]
    fn test_checksum_mode() {
        let mut source_entry = make_entry("file.txt", 100);
        let mut dest_entry = make_entry("file.txt", 100);
        source_entry.etag = Some("abc123".to_string());
        dest_entry.etag = Some("abc123".to_string());

        let source = vec![source_entry];
        let dest = vec![dest_entry];
        let mut config = Config::default();
        config.checksum = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Skip);
    }

    #[test]
    fn test_checksum_mode_different() {
        let mut source_entry = make_entry("file.txt", 100);
        let mut dest_entry = make_entry("file.txt", 100);
        source_entry.etag = Some("abc123".to_string());
        dest_entry.etag = Some("def456".to_string());

        let source = vec![source_entry];
        let dest = vec![dest_entry];
        let mut config = Config::default();
        config.checksum = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Upload);
    }

    #[test]
    fn test_max_size_filter() {
        let source = vec![make_entry("large.bin", 100 * 1024 * 1024)];
        let dest = vec![];
        let mut config = Config::default();
        config.max_size = 50 * 1024 * 1024; // 50MB limit

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 0); // File too large, skipped
    }

    #[test]
    fn test_min_size_filter() {
        let source = vec![make_entry("tiny.txt", 100)];
        let dest = vec![];
        let mut config = Config::default();
        config.min_size = 1024; // 1KB minimum

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 0); // File too small, skipped
    }

    #[test]
    fn test_whole_file_disables_delta() {
        let source_entry = make_entry("large.bin", 100 * 1024 * 1024);
        let dest_entry = make_entry("large.bin", 99 * 1024 * 1024);

        let source = vec![source_entry];
        let dest = vec![dest_entry];
        let mut config = Config::default();
        config.whole_file = true;

        let actions = compute_diff(&source, &dest, &config);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].action, SyncAction::Upload); // Not Delta
    }
}
