//! Diff computation between source and destination

use crate::config::Config;
use crate::sync::scan::build_file_map;
use crate::types::{FileEntry, PlannedAction, SyncAction};
use std::collections::HashSet;

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
                // File doesn't exist at destination - upload
                let action = if config.should_use_delta(source_entry.size) {
                    // For new files, we still do full upload (no delta possible)
                    SyncAction::Upload
                } else {
                    SyncAction::Upload
                };
                actions.push(PlannedAction::new(source_entry.clone(), action));
            }
            Some(dest_entry) => {
                // File exists - check if it needs updating
                if files_match(source_entry, dest_entry) {
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

/// Check if two files match (same size and mtime)
fn files_match(source: &FileEntry, dest: &FileEntry) -> bool {
    // Different sizes means different content
    if source.size != dest.size {
        return false;
    }

    // If both have mtimes, compare them
    match (source.mtime, dest.mtime) {
        (Some(s_mtime), Some(d_mtime)) => {
            // Allow 1 second tolerance for mtime comparison
            let diff = if s_mtime > d_mtime {
                s_mtime.duration_since(d_mtime).ok()
            } else {
                d_mtime.duration_since(s_mtime).ok()
            };

            match diff {
                Some(d) => d.as_secs() <= 1,
                None => true, // Can't compare, assume match
            }
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
}
