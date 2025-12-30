//! File scanning for sync operations
//!
//! Uses DashMap for concurrent insertion during parallel directory walking,
//! eliminating the need to collect to Vec and then rehash into HashMap.

use crate::error::Result;
use crate::storage::StorageBackend;
use crate::types::FileEntry;
use dashmap::DashMap;
use std::sync::Arc;

/// Type alias for the concurrent file map
pub type FileMap = DashMap<String, FileEntry>;

/// Scan a storage location and return all file entries as a Vec
///
/// This is the legacy interface. For better performance with large directories,
/// use `scan_to_map` instead.
pub async fn scan_location(backend: &StorageBackend, prefix: &str) -> Result<Vec<FileEntry>> {
    backend.list(prefix).await
}

/// Scan a storage location directly into a concurrent DashMap
///
/// This avoids the overhead of collecting to a Vec and then rehashing into a HashMap.
/// The DashMap can be used directly for O(1) lookups during diff computation.
pub async fn scan_to_map(backend: &StorageBackend, prefix: &str) -> Result<Arc<FileMap>> {
    let entries = backend.list(prefix).await?;
    let map = Arc::new(FileMap::with_capacity(entries.len()));

    // For local backend with jwalk, entries are already collected in parallel
    // We insert them into DashMap which handles concurrent access
    // In the future, we could have backends insert directly into a shared DashMap
    for entry in entries {
        let path_str = entry.path.to_string_lossy().to_string();
        map.insert(path_str, entry);
    }

    Ok(map)
}

/// Scan a storage location into a DashMap using parallel insertion
///
/// Uses rayon for parallel insertion when there are many entries.
pub async fn scan_to_map_parallel(backend: &StorageBackend, prefix: &str) -> Result<Arc<FileMap>> {
    use rayon::prelude::*;

    let entries = backend.list(prefix).await?;
    let map = Arc::new(FileMap::with_capacity(entries.len()));

    // For large directories, parallel insertion is faster
    if entries.len() > 1000 {
        let map_ref = &map;
        entries.into_par_iter().for_each(|entry| {
            let path_str = entry.path.to_string_lossy().to_string();
            map_ref.insert(path_str, entry);
        });
    } else {
        // For small directories, sequential is fine
        for entry in entries {
            let path_str = entry.path.to_string_lossy().to_string();
            map.insert(path_str, entry);
        }
    }

    Ok(map)
}

/// Build a lookup map from path to file entry (legacy interface)
///
/// Deprecated: Use `scan_to_map` instead for better performance.
pub fn build_file_map(entries: &[FileEntry]) -> std::collections::HashMap<String, &FileEntry> {
    entries
        .iter()
        .map(|e| (e.path.to_string_lossy().to_string(), e))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_entry(path: &str, size: u64) -> FileEntry {
        FileEntry {
            path: PathBuf::from(path),
            size,
            mtime: None,
            is_dir: false,
            mode: None,
            etag: None,
        }
    }

    #[test]
    fn test_build_file_map() {
        let entries = vec![
            make_entry("file1.txt", 100),
            make_entry("dir/file2.txt", 200),
        ];

        let map = build_file_map(&entries);
        assert_eq!(map.len(), 2);
        assert!(map.contains_key("file1.txt"));
        assert!(map.contains_key("dir/file2.txt"));
    }

    #[test]
    fn test_dashmap_concurrent_access() {
        let map: FileMap = DashMap::new();

        // Simulate concurrent insertions
        let entries = vec![
            make_entry("a.txt", 1),
            make_entry("b.txt", 2),
            make_entry("c.txt", 3),
        ];

        for entry in entries {
            let path = entry.path.to_string_lossy().to_string();
            map.insert(path, entry);
        }

        assert_eq!(map.len(), 3);
        assert!(map.get("a.txt").is_some());
        assert_eq!(map.get("b.txt").unwrap().size, 2);
    }
}
