//! Backup functionality for rsync-compatible file backups
//!
//! Supports:
//! - --backup / -b: Make backups of changed files
//! - --backup-dir=DIR: Store backups in a specified directory
//! - --suffix=SUFFIX: Backup suffix (default: ~)

use crate::config::Config;
use crate::error::{Error, Result};
use crate::storage::StorageBackend;
use std::path::PathBuf;

/// Create a backup of a file before it is overwritten
pub async fn create_backup(
    path: &str,
    storage: &StorageBackend,
    config: &Config,
) -> Result<Option<String>> {
    if !config.backup {
        return Ok(None);
    }

    // Check if file exists
    if storage.head(path).await?.is_none() {
        return Ok(None); // Nothing to backup
    }

    let backup_path = compute_backup_path(path, config);

    // Get original file data
    let data = storage.get(path).await?;

    // Create backup directory if using backup_dir
    if config.backup_dir.is_some() {
        // Ensure the backup directory structure exists
        let backup_path_buf = PathBuf::from(&backup_path);
        if let Some(parent) = backup_path_buf.parent() {
            let parent_str = parent.to_string_lossy().to_string();
            if !parent_str.is_empty() {
                // For local storage, create directories
                if let StorageBackend::Local(ref local) = storage {
                    let full_parent = local.base_path().join(parent);
                    tokio::fs::create_dir_all(&full_parent)
                        .await
                        .map_err(|e| Error::io("creating backup directory", e))?;
                }
            }
        }
    }

    // Write backup
    storage.put(&backup_path, data).await?;

    tracing::debug!(
        original = %path,
        backup = %backup_path,
        "Created backup"
    );

    Ok(Some(backup_path))
}

/// Compute the backup path for a file
pub fn compute_backup_path(path: &str, config: &Config) -> String {
    let suffix = &config.backup_suffix;

    match &config.backup_dir {
        Some(backup_dir) => {
            // Put backup in the backup directory with same relative path
            let backup_dir_str = backup_dir.to_string_lossy();
            format!("{}/{}{}", backup_dir_str.trim_end_matches('/'), path, suffix)
        }
        None => {
            // Put backup alongside original file with suffix
            format!("{}{}", path, suffix)
        }
    }
}

/// Check if a path is a backup file (ends with backup suffix)
pub fn is_backup_file(path: &str, config: &Config) -> bool {
    path.ends_with(&config.backup_suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_backup_path_default() {
        let mut config = Config::default();
        config.backup = true;
        config.backup_suffix = "~".to_string();

        let backup = compute_backup_path("dir/file.txt", &config);
        assert_eq!(backup, "dir/file.txt~");
    }

    #[test]
    fn test_compute_backup_path_custom_suffix() {
        let mut config = Config::default();
        config.backup = true;
        config.backup_suffix = ".bak".to_string();

        let backup = compute_backup_path("file.txt", &config);
        assert_eq!(backup, "file.txt.bak");
    }

    #[test]
    fn test_compute_backup_path_with_dir() {
        let mut config = Config::default();
        config.backup = true;
        config.backup_suffix = "~".to_string();
        config.backup_dir = Some(PathBuf::from("/backups"));

        let backup = compute_backup_path("dir/file.txt", &config);
        assert_eq!(backup, "/backups/dir/file.txt~");
    }

    #[test]
    fn test_is_backup_file() {
        let mut config = Config::default();
        config.backup_suffix = "~".to_string();

        assert!(is_backup_file("file.txt~", &config));
        assert!(!is_backup_file("file.txt", &config));
    }
}
