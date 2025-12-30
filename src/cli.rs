//! CLI argument parsing for HugeSync

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// HugeSync - A Cloud-Era Delta Synchronization Tool
#[derive(Parser, Debug)]
#[command(name = "hsync")]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Increase logging verbosity (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Output logs as JSON
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Synchronize files between source and destination
    Sync(SyncArgs),

    /// Generate signature file for a large file
    Sign(SignArgs),

    /// Show configuration
    Config(ConfigArgs),

    /// List supported S3-compatible providers and their regions
    Providers,
}

/// Arguments for the sync command
#[derive(Parser, Debug)]
pub struct SyncArgs {
    /// Source path or URI (local path, s3://, gs://, az://, user@host:path)
    pub source: String,

    /// Destination path or URI (local path, s3://, gs://, az://, user@host:path)
    pub destination: String,

    // ==================== Basic Options ====================

    /// Archive mode: recursive, preserve attributes (-rlptgoD)
    #[arg(short = 'a', long)]
    pub archive: bool,

    /// Perform a trial run with no changes made
    #[arg(short = 'n', long)]
    pub dry_run: bool,

    /// Show progress bars
    #[arg(short = 'P', long)]
    pub progress: bool,

    /// Number of parallel transfers [default: auto]
    #[arg(short = 'j', long, default_value = "0")]
    pub jobs: usize,

    /// Delete extraneous files from destination
    #[arg(long)]
    pub delete: bool,

    /// Delete excluded files from destination
    #[arg(long)]
    pub delete_excluded: bool,

    // ==================== Comparison Options ====================

    /// Skip based on checksum, not mod-time & size
    #[arg(short = 'c', long)]
    pub checksum: bool,

    /// Skip files that are newer on the receiver
    #[arg(short = 'u', long)]
    pub update: bool,

    /// Skip updating files that already exist on receiver
    #[arg(long)]
    pub ignore_existing: bool,

    /// Skip creating new files on receiver (only update existing)
    #[arg(long)]
    pub existing: bool,

    /// Compare by size only (ignore mod-time)
    #[arg(long)]
    pub size_only: bool,

    /// Compare mod-times with reduced accuracy (seconds)
    #[arg(long, default_value = "0")]
    pub modify_window: i64,

    // ==================== Transfer Options ====================

    /// Disable delta-transfer algorithm (always send whole files)
    #[arg(short = 'W', long)]
    pub whole_file: bool,

    /// Update destination files in-place
    #[arg(long)]
    pub inplace: bool,

    /// Append data onto shorter files
    #[arg(long)]
    pub append: bool,

    /// Append with old data verified via checksum
    #[arg(long)]
    pub append_verify: bool,

    // ==================== Symlink Options ====================

    /// Copy symlinks as symlinks
    #[arg(short = 'l', long)]
    pub links: bool,

    /// Transform symlink into referent file/dir
    #[arg(short = 'L', long)]
    pub copy_links: bool,

    /// Ignore symlinks that point outside the source tree
    #[arg(long)]
    pub safe_links: bool,

    /// Transform unsafe symlinks into files
    #[arg(long)]
    pub copy_unsafe_links: bool,

    // ==================== Backup Options ====================

    /// Make backups of changed files
    #[arg(short = 'b', long)]
    pub backup: bool,

    /// Store backups in specified directory
    #[arg(long)]
    pub backup_dir: Option<PathBuf>,

    /// Backup suffix (default: ~)
    #[arg(long, default_value = "~")]
    pub suffix: String,

    // ==================== Size Filtering ====================

    /// Don't transfer files larger than SIZE (e.g., 100M, 1G)
    #[arg(long)]
    pub max_size: Option<String>,

    /// Don't transfer files smaller than SIZE (e.g., 1K, 100)
    #[arg(long)]
    pub min_size: Option<String>,

    // ==================== Bandwidth & Performance ====================

    /// Limit I/O bandwidth in KiB/s
    #[arg(long)]
    pub bwlimit: Option<u64>,

    // ==================== Cloud Storage Options ====================

    /// S3 storage class for uploads (STANDARD, STANDARD_IA, ONEZONE_IA,
    /// INTELLIGENT_TIERING, GLACIER, GLACIER_IR, DEEP_ARCHIVE)
    /// Note: Only supported on AWS S3, not S3-compatible providers
    #[arg(long)]
    pub storage_class: Option<String>,

    /// Custom S3 endpoint URL for S3-compatible storage
    /// (e.g., https://s3.us-west-001.backblazeb2.com, https://fsn1.your-objectstorage.com)
    #[arg(long, conflicts_with_all = ["s3_provider", "s3_region"])]
    pub s3_endpoint: Option<String>,

    /// S3-compatible provider shorthand (hetzner, digitalocean, backblaze, wasabi,
    /// vultr, linode, scaleway, ovh, exoscale, idrive, contabo)
    #[arg(long, requires = "s3_region")]
    pub s3_provider: Option<String>,

    /// Region/datacenter for the S3 provider (e.g., hel1, nyc3, eu-central-1)
    #[arg(long, requires = "s3_provider")]
    pub s3_region: Option<String>,

    // ==================== Partial Transfer ====================

    /// Keep partially transferred files
    #[arg(long)]
    pub partial: bool,

    /// Put partially transferred files into DIR
    #[arg(long)]
    pub partial_dir: Option<PathBuf>,

    // ==================== Source Handling ====================

    /// Remove synchronized files from source
    #[arg(long)]
    pub remove_source_files: bool,

    // ==================== Filesystem ====================

    /// Don't cross filesystem boundaries
    #[arg(short = 'x', long)]
    pub one_file_system: bool,

    // ==================== Output & Logging ====================

    /// Output a change-summary for all updates
    #[arg(short = 'i', long)]
    pub itemize_changes: bool,

    /// Log transfers to FILE
    #[arg(long)]
    pub log_file: Option<PathBuf>,

    /// Give some file-transfer stats at end
    #[arg(long)]
    pub stats: bool,

    // ==================== Compare/Copy/Link Destination ====================

    /// Compare against files in DIR
    #[arg(long)]
    pub compare_dest: Option<PathBuf>,

    /// Like --compare-dest but also use file from DIR
    #[arg(long)]
    pub copy_dest: Option<PathBuf>,

    /// Hardlink to files in DIR when unchanged
    #[arg(long)]
    pub link_dest: Option<PathBuf>,

    // ==================== Delta Sync Options ====================

    /// Ignore missing source arguments
    #[arg(long)]
    pub ignore_missing_args: bool,

    /// File size threshold for delta sync in MB [default: 50]
    #[arg(long, default_value = "50")]
    pub delta_threshold: u64,

    /// Disable sidecar (.hssig) generation
    #[arg(long)]
    pub no_sidecar: bool,

    /// Abort if remote file was modified externally
    #[arg(long)]
    pub fail_on_conflict: bool,

    /// Block size for delta signatures in KB (64-5120) [default: 5120]
    #[arg(long, default_value = "5120", value_parser = clap::value_parser!(u64).range(64..=5120))]
    pub block_size: u64,

    // ==================== Filtering ====================

    /// Include pattern (can be specified multiple times)
    #[arg(long = "include", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Exclude pattern (can be specified multiple times)
    #[arg(long = "exclude", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Configuration file path
    #[arg(long = "config")]
    pub config: Option<PathBuf>,
}

impl SyncArgs {
    /// Convert CLI args to Config, merging with file config
    pub fn to_config(&self) -> crate::config::Config {
        use crate::config::Config;

        let mut config = if let Some(ref path) = self.config {
            Config::load_from(path).unwrap_or_default()
        } else {
            Config::load().unwrap_or_default()
        };

        // Basic options
        config.archive = self.archive;
        config.dry_run = self.dry_run;
        config.progress = self.progress;
        config.jobs = self.jobs;
        config.delete = self.delete;
        config.delete_excluded = self.delete_excluded;

        // Comparison options
        config.checksum = self.checksum;
        config.update = self.update;
        config.ignore_existing = self.ignore_existing;
        config.existing = self.existing;
        config.size_only = self.size_only;
        config.modify_window = self.modify_window;

        // Transfer options
        config.whole_file = self.whole_file;
        config.inplace = self.inplace;
        config.append = self.append;
        config.append_verify = self.append_verify;

        // Symlink options
        config.links = self.links || self.archive; // -a implies -l
        config.copy_links = self.copy_links;
        config.safe_links = self.safe_links;
        config.copy_unsafe_links = self.copy_unsafe_links;

        // Backup options
        config.backup = self.backup;
        config.backup_dir = self.backup_dir.clone();
        config.backup_suffix = self.suffix.clone();

        // Size filtering (parse size strings)
        if let Some(ref max_size) = self.max_size {
            config.max_size = Config::parse_size(max_size).unwrap_or(0);
        }
        if let Some(ref min_size) = self.min_size {
            config.min_size = Config::parse_size(min_size).unwrap_or(0);
        }

        // Bandwidth limiting
        config.bwlimit = self.bwlimit.unwrap_or(0);

        // Partial transfer
        config.partial = self.partial;
        config.partial_dir = self.partial_dir.clone();

        // Source handling
        config.remove_source_files = self.remove_source_files;

        // Filesystem
        config.one_file_system = self.one_file_system;

        // Output & logging
        config.itemize_changes = self.itemize_changes;
        config.log_file = self.log_file.clone();
        config.stats = self.stats;

        // Compare/copy/link destination
        config.compare_dest = self.compare_dest.clone();
        config.copy_dest = self.copy_dest.clone();
        config.link_dest = self.link_dest.clone();

        // Delta sync options
        config.ignore_missing_args = self.ignore_missing_args;
        config.delta_threshold = self.delta_threshold * 1024 * 1024; // Convert MB to bytes
        config.no_sidecar = self.no_sidecar;
        config.fail_on_conflict = self.fail_on_conflict;
        config.block_size = Config::validate_block_size(self.block_size as usize);

        // Filtering
        if !self.include.is_empty() {
            config.include = self.include.clone();
        }
        if !self.exclude.is_empty() {
            config.exclude = self.exclude.clone();
        }

        // Cloud storage options
        if let Some(ref sc) = self.storage_class {
            config.storage_class = Some(sc.to_uppercase());
        }

        // Resolve S3 endpoint: either direct URL or provider/region shorthand
        if let Some(ref endpoint) = self.s3_endpoint {
            config.s3_endpoint = Some(endpoint.clone());
        } else if let (Some(ref provider), Some(ref region)) = (&self.s3_provider, &self.s3_region) {
            // Resolve provider/region to endpoint URL
            match crate::s3_providers::resolve_endpoint(provider, region) {
                Ok(endpoint) => config.s3_endpoint = Some(endpoint),
                Err(e) => {
                    // Log error but don't fail here - let it fail later with better context
                    tracing::error!("Failed to resolve S3 provider: {}", e);
                }
            }
        }

        config
    }
}

/// Arguments for the sign command
#[derive(Parser, Debug)]
pub struct SignArgs {
    /// File to generate signature for
    pub file: PathBuf,

    /// Output signature file path (default: <file>.hssig)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Block size in KB (64-5120) [default: 5120]
    #[arg(long, default_value = "5120", value_parser = clap::value_parser!(u64).range(64..=5120))]
    pub block_size: u64,
}

/// Arguments for the config command
#[derive(Parser, Debug)]
pub struct ConfigArgs {
    /// Show the configuration file path
    #[arg(long)]
    pub path: bool,

    /// Create default configuration file
    #[arg(long)]
    pub init: bool,
}
