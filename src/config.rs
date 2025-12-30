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
    // ==================== Basic Options ====================

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

    // ==================== Comparison Options ====================

    /// Use checksums instead of mtime+size for comparison
    pub checksum: bool,

    /// Skip files newer on receiver
    pub update: bool,

    /// Skip files that already exist on receiver
    pub ignore_existing: bool,

    /// Only update files that already exist on receiver
    pub existing: bool,

    /// Compare by size only (ignore mtime)
    pub size_only: bool,

    /// Modify-time comparison window in seconds
    pub modify_window: i64,

    // ==================== Transfer Options ====================

    /// Disable delta transfer (always copy whole files)
    pub whole_file: bool,

    /// Update destination files in-place
    pub inplace: bool,

    /// Append data to shorter files
    pub append: bool,

    /// Append with checksum verification
    pub append_verify: bool,

    // ==================== Symlink Options ====================

    /// Copy symlinks as symlinks
    pub links: bool,

    /// Transform symlinks into referent file/dir
    pub copy_links: bool,

    /// Ignore symlinks pointing outside source tree
    pub safe_links: bool,

    /// Transform unsafe symlinks into files
    pub copy_unsafe_links: bool,

    // ==================== Backup Options ====================

    /// Make backups of changed files
    pub backup: bool,

    /// Directory to store backups
    pub backup_dir: Option<PathBuf>,

    /// Backup suffix (default: ~)
    pub backup_suffix: String,

    // ==================== Size Filtering ====================

    /// Maximum file size to transfer (bytes, 0 = no limit)
    pub max_size: u64,

    /// Minimum file size to transfer (bytes, 0 = no limit)
    pub min_size: u64,

    // ==================== Bandwidth Limiting ====================

    /// Bandwidth limit in KiB/s (0 = unlimited)
    pub bwlimit: u64,

    // ==================== Partial Transfer ====================

    /// Keep partially transferred files
    pub partial: bool,

    /// Directory for partial files
    pub partial_dir: Option<PathBuf>,

    // ==================== Source Handling ====================

    /// Remove source files after successful transfer
    pub remove_source_files: bool,

    // ==================== Filesystem ====================

    /// Don't cross filesystem boundaries
    pub one_file_system: bool,

    // ==================== Output & Logging ====================

    /// Output itemized change summary
    pub itemize_changes: bool,

    /// Log file path
    pub log_file: Option<PathBuf>,

    /// Show detailed stats at end
    pub stats: bool,

    // ==================== Compare/Copy/Link Destination ====================

    /// Compare against files in this directory
    pub compare_dest: Option<PathBuf>,

    /// Copy from this directory if file matches
    pub copy_dest: Option<PathBuf>,

    /// Hardlink to files in this directory when unchanged
    pub link_dest: Option<PathBuf>,

    // ==================== Delta Sync Options ====================

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

    // ==================== Filtering ====================

    /// Include patterns
    pub include: Vec<String>,

    /// Exclude patterns
    pub exclude: Vec<String>,

    // ==================== Retry ====================

    /// Maximum retries for failed operations
    pub max_retries: u32,

    /// Retry delay base in milliseconds
    pub retry_delay_ms: u64,

    // ==================== Cloud Storage ====================

    /// S3 storage class for uploads (STANDARD, STANDARD_IA, GLACIER, etc.)
    pub storage_class: Option<String>,

    /// Custom S3 endpoint for S3-compatible storage (Hetzner, MinIO, Backblaze, etc.)
    pub s3_endpoint: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Basic
            delta_threshold: DEFAULT_DELTA_THRESHOLD,
            jobs: DEFAULT_JOBS,
            dry_run: false,
            delete: false,
            delete_excluded: false,
            progress: true,
            verbose: 0,
            archive: false,

            // Comparison
            checksum: false,
            update: false,
            ignore_existing: false,
            existing: false,
            size_only: false,
            modify_window: 0,

            // Transfer
            whole_file: false,
            inplace: false,
            append: false,
            append_verify: false,

            // Symlinks
            links: false,
            copy_links: false,
            safe_links: false,
            copy_unsafe_links: false,

            // Backup
            backup: false,
            backup_dir: None,
            backup_suffix: "~".to_string(),

            // Size filtering
            max_size: 0,
            min_size: 0,

            // Bandwidth
            bwlimit: 0,

            // Partial
            partial: false,
            partial_dir: None,

            // Source handling
            remove_source_files: false,

            // Filesystem
            one_file_system: false,

            // Output
            itemize_changes: false,
            log_file: None,
            stats: false,

            // Compare/copy/link dest
            compare_dest: None,
            copy_dest: None,
            link_dest: None,

            // Delta sync
            fail_on_conflict: false,
            no_sidecar: false,
            ignore_missing_args: false,
            skip_vanished: true,
            block_size: DEFAULT_BLOCK_SIZE,

            // Filtering
            include: Vec::new(),
            exclude: Vec::new(),

            // Retry
            max_retries: 3,
            retry_delay_ms: 1000,

            // Cloud storage
            storage_class: None,
            s3_endpoint: None,
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

    /// Check if a file should use delta sync based on its size and options
    pub fn should_use_delta(&self, file_size: u64) -> bool {
        // Delta is disabled if whole_file mode is enabled
        if self.whole_file {
            return false;
        }
        !self.no_sidecar && file_size >= self.delta_threshold
    }

    /// Check if a file should be transferred based on size limits
    pub fn file_within_size_limits(&self, file_size: u64) -> bool {
        // Check max_size (0 = no limit)
        if self.max_size > 0 && file_size > self.max_size {
            return false;
        }
        // Check min_size (0 = no limit)
        if self.min_size > 0 && file_size < self.min_size {
            return false;
        }
        true
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

    /// Parse a size string like "100", "100K", "100M", "100G" into bytes
    pub fn parse_size(s: &str) -> Result<u64> {
        let s = s.trim().to_uppercase();
        if s.is_empty() {
            return Ok(0);
        }

        let (num_part, suffix) = if s.ends_with('K') {
            (&s[..s.len() - 1], 1024u64)
        } else if s.ends_with('M') {
            (&s[..s.len() - 1], 1024u64 * 1024)
        } else if s.ends_with('G') {
            (&s[..s.len() - 1], 1024u64 * 1024 * 1024)
        } else if s.ends_with('T') {
            (&s[..s.len() - 1], 1024u64 * 1024 * 1024 * 1024)
        } else if s.ends_with("KB") {
            (&s[..s.len() - 2], 1024u64)
        } else if s.ends_with("MB") {
            (&s[..s.len() - 2], 1024u64 * 1024)
        } else if s.ends_with("GB") {
            (&s[..s.len() - 2], 1024u64 * 1024 * 1024)
        } else if s.ends_with("TB") {
            (&s[..s.len() - 2], 1024u64 * 1024 * 1024 * 1024)
        } else {
            (s.as_str(), 1u64)
        };

        let num: u64 = num_part.parse().map_err(|_| {
            Error::config(format!("invalid size value: {}", s))
        })?;

        Ok(num * suffix)
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
