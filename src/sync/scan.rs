//! File scanning for sync operations

use crate::error::Result;
use crate::storage::StorageBackend;
use crate::types::FileEntry;

/// Scan a storage location and return all file entries
pub async fn scan_location(backend: &StorageBackend, prefix: &str) -> Result<Vec<FileEntry>> {
    backend.list(prefix).await
}

/// Build a lookup map from path to file entry
pub fn build_file_map(entries: &[FileEntry]) -> std::collections::HashMap<String, &FileEntry> {
    entries
        .iter()
        .map(|e| (e.path.to_string_lossy().to_string(), e))
        .collect()
}
