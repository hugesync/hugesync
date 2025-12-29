//! Resume state management for interrupted uploads

use crate::error::{Error, Result};
use crate::storage::CompletedPart;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// State of an interrupted multipart upload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    /// S3 upload ID
    pub upload_id: String,
    /// Remote path being uploaded to
    pub remote_path: String,
    /// Local file path
    pub local_path: PathBuf,
    /// Local file size at start
    pub local_size: u64,
    /// Local file modification time (Unix timestamp)
    pub local_mtime: u64,
    /// Completed parts
    pub completed_parts: Vec<CompletedPartInfo>,
    /// Timestamp when upload started
    pub started_at: u64,
    /// Block size used
    pub block_size: usize,
}

/// Information about a completed part
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedPartInfo {
    pub part_number: i32,
    pub etag: String,
    pub size: u64,
}

impl From<&CompletedPart> for CompletedPartInfo {
    fn from(part: &CompletedPart) -> Self {
        Self {
            part_number: part.part_number,
            etag: part.etag.clone(),
            size: 0, // Will be filled in if needed
        }
    }
}

impl From<&CompletedPartInfo> for CompletedPart {
    fn from(info: &CompletedPartInfo) -> Self {
        Self {
            part_number: info.part_number,
            etag: info.etag.clone(),
        }
    }
}

/// Resume state manager
pub struct ResumeManager {
    /// Directory to store resume state files
    cache_dir: PathBuf,
}

impl ResumeManager {
    /// Create a new resume manager
    pub fn new() -> Result<Self> {
        let cache_dir = Self::default_cache_dir()?;
        std::fs::create_dir_all(&cache_dir)
            .map_err(|e| Error::io("creating cache directory", e))?;
        Ok(Self { cache_dir })
    }

    /// Get the default cache directory
    pub fn default_cache_dir() -> Result<PathBuf> {
        dirs::cache_dir()
            .map(|p| p.join("hugesync").join("uploads"))
            .ok_or_else(|| Error::config("could not determine cache directory"))
    }

    /// Generate a unique key for a resume state
    fn state_key(local_path: &PathBuf, remote_path: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        local_path.hash(&mut hasher);
        remote_path.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Get the path to a state file
    fn state_path(&self, local_path: &PathBuf, remote_path: &str) -> PathBuf {
        let key = Self::state_key(local_path, remote_path);
        self.cache_dir.join(format!("{}.json", key))
    }

    /// Save resume state
    pub fn save(&self, state: &ResumeState) -> Result<()> {
        let path = self.state_path(&state.local_path, &state.remote_path);
        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::config(format!("serializing resume state: {}", e)))?;
        std::fs::write(&path, json)
            .map_err(|e| Error::io("writing resume state", e))?;
        tracing::debug!(path = ?path, "Saved resume state");
        Ok(())
    }

    /// Load resume state if it exists and is valid
    pub fn load(&self, local_path: &PathBuf, remote_path: &str) -> Result<Option<ResumeState>> {
        let path = self.state_path(local_path, remote_path);

        if !path.exists() {
            return Ok(None);
        }

        let json = std::fs::read_to_string(&path)
            .map_err(|e| Error::io("reading resume state", e))?;
        let state: ResumeState = serde_json::from_str(&json)
            .map_err(|e| Error::config(format!("parsing resume state: {}", e)))?;

        // Validate the state is still valid
        if !self.is_valid(&state, local_path)? {
            tracing::info!("Resume state is stale, removing");
            self.remove(local_path, remote_path)?;
            return Ok(None);
        }

        tracing::info!(
            upload_id = %state.upload_id,
            completed_parts = state.completed_parts.len(),
            "Found valid resume state"
        );
        Ok(Some(state))
    }

    /// Check if resume state is still valid
    fn is_valid(&self, state: &ResumeState, local_path: &PathBuf) -> Result<bool> {
        // Check if local file exists and matches
        let metadata = match std::fs::metadata(local_path) {
            Ok(m) => m,
            Err(_) => return Ok(false),
        };

        // Check size matches
        if metadata.len() != state.local_size {
            return Ok(false);
        }

        // Check mtime matches (with 1 second tolerance)
        if let Ok(mtime) = metadata.modified() {
            let mtime_secs = mtime
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if mtime_secs.abs_diff(state.local_mtime) > 1 {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Remove resume state
    pub fn remove(&self, local_path: &PathBuf, remote_path: &str) -> Result<()> {
        let path = self.state_path(local_path, remote_path);
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| Error::io("removing resume state", e))?;
            tracing::debug!(path = ?path, "Removed resume state");
        }
        Ok(())
    }

    /// List all pending resume states
    pub fn list_pending(&self) -> Result<Vec<ResumeState>> {
        let mut states = Vec::new();

        if !self.cache_dir.exists() {
            return Ok(states);
        }

        for entry in std::fs::read_dir(&self.cache_dir)
            .map_err(|e| Error::io("reading cache directory", e))?
        {
            let entry = entry.map_err(|e| Error::io("reading directory entry", e))?;
            let path = entry.path();

            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Ok(json) = std::fs::read_to_string(&path) {
                    if let Ok(state) = serde_json::from_str::<ResumeState>(&json) {
                        states.push(state);
                    }
                }
            }
        }

        Ok(states)
    }

    /// Clean up old/stale resume states
    pub fn cleanup(&self, max_age_secs: u64) -> Result<usize> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut removed = 0;

        for state in self.list_pending()? {
            if now.saturating_sub(state.started_at) > max_age_secs {
                self.remove(&state.local_path, &state.remote_path)?;
                removed += 1;
            }
        }

        Ok(removed)
    }
}

impl Default for ResumeManager {
    fn default() -> Self {
        Self::new().expect("failed to create resume manager")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_key_is_deterministic() {
        let path = PathBuf::from("/test/file.txt");
        let remote = "bucket/file.txt";

        let key1 = ResumeManager::state_key(&path, remote);
        let key2 = ResumeManager::state_key(&path, remote);

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_different_paths_different_keys() {
        let path1 = PathBuf::from("/test/file1.txt");
        let path2 = PathBuf::from("/test/file2.txt");
        let remote = "bucket/file.txt";

        let key1 = ResumeManager::state_key(&path1, remote);
        let key2 = ResumeManager::state_key(&path2, remote);

        assert_ne!(key1, key2);
    }
}
