//! CLI argument parsing for HugeSync

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// HugeSync - A Cloud-Era Delta Synchronization Tool
#[derive(Parser, Debug)]
#[command(name = "hugesync")]
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
}

/// Arguments for the sync command
#[derive(Parser, Debug)]
pub struct SyncArgs {
    /// Source path or URI (local path, s3://, gs://, az://)
    pub source: String,

    /// Destination path or URI (local path, s3://, gs://, az://)
    pub destination: String,

    /// Archive mode: recursive, preserve attributes
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

    /// Include pattern (can be specified multiple times)
    #[arg(long = "include", action = clap::ArgAction::Append)]
    pub include: Vec<String>,

    /// Exclude pattern (can be specified multiple times)
    #[arg(long = "exclude", action = clap::ArgAction::Append)]
    pub exclude: Vec<String>,

    /// Configuration file path
    #[arg(short = 'c', long)]
    pub config: Option<PathBuf>,
}

impl SyncArgs {
    /// Convert CLI args to Config, merging with file config
    pub fn to_config(&self) -> crate::config::Config {
        let mut config = if let Some(ref path) = self.config {
            crate::config::Config::load_from(path).unwrap_or_default()
        } else {
            crate::config::Config::load().unwrap_or_default()
        };

        // CLI args override config file
        config.archive = self.archive;
        config.dry_run = self.dry_run;
        config.progress = self.progress;
        config.jobs = self.jobs;
        config.delete = self.delete;
        config.delete_excluded = self.delete_excluded;
        config.ignore_missing_args = self.ignore_missing_args;
        config.delta_threshold = self.delta_threshold * 1024 * 1024; // Convert MB to bytes
        config.no_sidecar = self.no_sidecar;
        config.fail_on_conflict = self.fail_on_conflict;
        config.block_size = crate::config::Config::validate_block_size(self.block_size as usize);

        if !self.include.is_empty() {
            config.include = self.include.clone();
        }
        if !self.exclude.is_empty() {
            config.exclude = self.exclude.clone();
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
