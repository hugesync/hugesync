//! Configuration management for HugeSync

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Default delta threshold in bytes (50MB)
pub const DEFAULT_DELTA_THRESHOLD: u64 = 50 * 1024 * 1024;

/// Default number of parallel jobs (0 = auto)
pub const DEFAULT_JOBS: usize = 0;

/// Default block size for signatures (5MB - aligns with S3 minimum)
pub const DEFAULT_BLOCK_SIZE: usize = 5 * 1024 * 1024;

/// Minimum block size (64KB)
pub const MIN_BLOCK_SIZE: usize = 64 * 1024;

/// Maximum block size (5MB)
pub const MAX_BLOCK_SIZE: usize = 5 * 1024 * 1024;

/// S3 minimum part size (5MB)
pub const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Main configuration struct
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// File size threshold for delta sync (bytes)
    pub delta_threshold: u64,

    /// Number of parallel transfer jobs (0 = auto-detect CPU count)
    pub jobs: usize,

    /// Perform a dry run (no actual changes)
    pub dry_run: bool,

    /// Delete extraneous files from destination
    pub delete: bool,

    /// Delete excluded files from destination
    pub delete_excluded: bool,

    /// Show progress bars
    pub progress: bool,

    /// Verbose logging level (0-3)
    pub verbose: u8,

    /// Archive mode (preserve metadata)
    pub archive: bool,

    /// Fail immediately on conflict
    pub fail_on_conflict: bool,

    /// Disable sidecar (.hssig) generation
    pub no_sidecar: bool,

    /// Ignore missing source arguments
    pub ignore_missing_args: bool,

    /// Skip files that vanish during scan
    pub skip_vanished: bool,

    /// Block size for signature generation
    pub block_size: usize,

    /// Include patterns
    pub include: Vec<String>,

    /// Exclude patterns
    pub exclude: Vec<String>,

    /// Maximum retries for failed operations
    pub max_retries: u32,

    /// Retry delay base in milliseconds
    pub retry_delay_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            delta_threshold: DEFAULT_DELTA_THRESHOLD,
            jobs: DEFAULT_JOBS,
            dry_run: false,
            delete: false,
            delete_excluded: false,
            progress: true,
            verbose: 0,
            archive: false,
            fail_on_conflict: false,
            no_sidecar: false,
            ignore_missing_args: false,
            skip_vanished: true,
            block_size: DEFAULT_BLOCK_SIZE,
            include: Vec::new(),
            exclude: Vec::new(),
            max_retries: 3,
            retry_delay_ms: 1000,
        }
    }
}

impl Config {
    /// Load configuration from the default config file
    pub fn load() -> Result<Self> {
        let config_path = Self::default_config_path()?;
        if config_path.exists() {
            Self::load_from(&config_path)
        } else {
            Ok(Self::default())
        }
    }

    /// Load configuration from a specific file
    pub fn load_from(path: &PathBuf) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| Error::io("reading config", e))?;
        let config: Self = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Save configuration to the default config file
    pub fn save(&self) -> Result<()> {
        let config_path = Self::default_config_path()?;
        self.save_to(&config_path)
    }

    /// Save configuration to a specific file
    pub fn save_to(&self, path: &PathBuf) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io("creating config dir", e))?;
        }
        let contents = toml::to_string_pretty(self)
            .map_err(|e| Error::config(format!("serializing config: {}", e)))?;
        std::fs::write(path, contents).map_err(|e| Error::io("writing config", e))?;
        Ok(())
    }

    /// Get the default configuration file path
    pub fn default_config_path() -> Result<PathBuf> {
        dirs::config_dir()
            .map(|p| p.join("hugesync").join("config.toml"))
            .ok_or_else(|| Error::config("could not determine config directory"))
    }

    /// Get the effective number of jobs (resolves 0 to CPU count)
    pub fn effective_jobs(&self) -> usize {
        if self.jobs == 0 {
            num_cpus::get()
        } else {
            self.jobs
        }
    }

    /// Check if a file should use delta sync based on its size
    pub fn should_use_delta(&self, file_size: u64) -> bool {
        !self.no_sidecar && file_size >= self.delta_threshold
    }

    /// Get block size in KB for display
    pub fn block_size_kb(&self) -> usize {
        self.block_size / 1024
    }

    /// Validate and clamp block size to valid range (64KB - 5MB)
    pub fn validate_block_size(kb: usize) -> usize {
        let bytes = kb * 1024;
        bytes.clamp(MIN_BLOCK_SIZE, MAX_BLOCK_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.delta_threshold, DEFAULT_DELTA_THRESHOLD);
        assert_eq!(config.jobs, 0);
        assert!(!config.dry_run);
        assert!(config.progress);
    }

    #[test]
    fn test_effective_jobs() {
        let mut config = Config::default();
        assert!(config.effective_jobs() > 0);

        config.jobs = 4;
        assert_eq!(config.effective_jobs(), 4);
    }

    #[test]
    fn test_should_use_delta() {
        let config = Config::default();
        assert!(!config.should_use_delta(1024)); // 1KB
        assert!(!config.should_use_delta(49 * 1024 * 1024)); // 49MB
        assert!(config.should_use_delta(51 * 1024 * 1024)); // 51MB
    }
}
