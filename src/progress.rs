//! Progress tracking and display for HugeSync

use crate::types::SyncStats;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

/// Progress tracker for sync operations
pub struct ProgressTracker {
    /// Multi-progress container
    multi: MultiProgress,
    /// Overall progress bar (files)
    overall: ProgressBar,
    /// Current file progress bar (bytes)
    current: ProgressBar,
    /// Stats line
    stats: ProgressBar,
    /// Whether progress is enabled
    enabled: bool,
}

impl ProgressTracker {
    /// Create a new progress tracker
    pub fn new(enabled: bool) -> Self {
        let multi = MultiProgress::new();

        let overall = if enabled {
            let pb = multi.add(ProgressBar::new(0));
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({percent}%)")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb.enable_steady_tick(Duration::from_millis(100));
            pb
        } else {
            ProgressBar::hidden()
        };

        let current = if enabled {
            let pb = multi.add(ProgressBar::new(0));
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("  {msg:.dim} [{bar:40.yellow/red}] {bytes}/{total_bytes} ({bytes_per_sec})")
                    .unwrap()
                    .progress_chars("=>-"),
            );
            pb
        } else {
            ProgressBar::hidden()
        };

        let stats = if enabled {
            let pb = multi.add(ProgressBar::new_spinner());
            pb.set_style(ProgressStyle::default_spinner().template("  {msg}").unwrap());
            pb
        } else {
            ProgressBar::hidden()
        };

        Self {
            multi,
            overall,
            current,
            stats,
            enabled,
        }
    }

    /// Set the total number of files
    pub fn set_total_files(&self, count: u64) {
        self.overall.set_length(count);
    }

    /// Start processing a new file
    pub fn start_file(&self, name: &str, size: u64) {
        self.current.set_length(size);
        self.current.set_position(0);
        self.current.set_message(truncate_filename(name, 30));
    }

    /// Update current file progress
    pub fn update_file_progress(&self, bytes: u64) {
        self.current.set_position(bytes);
    }

    /// Finish processing a file
    pub fn finish_file(&self) {
        self.overall.inc(1);
        self.current.set_position(self.current.length().unwrap_or(0));
    }

    /// Update stats display
    pub fn update_stats(&self, stats: &SyncStats) {
        let msg = format!(
            "Transferred: {} | Saved: {} ({:.1}%)",
            human_bytes::human_bytes(stats.bytes_transferred as f64),
            human_bytes::human_bytes(stats.bytes_saved as f64),
            stats.savings_percent()
        );
        self.stats.set_message(msg);
    }

    /// Finish all progress bars
    pub fn finish(&self) {
        self.overall.finish();
        self.current.finish_and_clear();
        self.stats.finish();
    }

    /// Print a message (works with progress bars)
    pub fn println(&self, msg: &str) {
        if self.enabled {
            self.multi.println(msg).ok();
        } else {
            println!("{}", msg);
        }
    }

    /// Suspend progress bars for interactive input
    pub fn suspend<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        self.multi.suspend(f)
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new(true)
    }
}

/// Truncate a filename for display
fn truncate_filename(name: &str, max_len: usize) -> String {
    if name.len() <= max_len {
        name.to_string()
    } else {
        format!("...{}", &name[name.len() - max_len + 3..])
    }
}

/// Format a file size for display
pub fn format_size(bytes: u64) -> String {
    human_bytes::human_bytes(bytes as f64)
}

/// Format a duration for display
pub fn format_duration(secs: f64) -> String {
    if secs < 60.0 {
        format!("{:.1}s", secs)
    } else if secs < 3600.0 {
        let mins = secs / 60.0;
        format!("{:.1}m", mins)
    } else {
        let hours = secs / 3600.0;
        format!("{:.1}h", hours)
    }
}

/// Format transfer rate for display
pub fn format_rate(bytes_per_sec: f64) -> String {
    format!("{}/s", human_bytes::human_bytes(bytes_per_sec))
}

/// Print a dry-run summary
pub fn print_dry_run_summary(stats: &SyncStats) {
    println!("\n=== Dry Run Summary ===");
    println!("Files to upload:   {}", stats.files_uploaded);
    println!("Files to download: {}", stats.files_downloaded);
    println!("Files to delete:   {}", stats.files_deleted);
    println!("Files to skip:     {}", stats.files_skipped);
    println!("Files via delta:   {}", stats.files_delta);
    println!();
    println!(
        "Estimated transfer: {}",
        format_size(stats.bytes_transferred)
    );
    println!("Estimated savings:  {} ({:.1}%)",
        format_size(stats.bytes_saved),
        stats.savings_percent()
    );
    println!(
        "Would transfer {} instead of {}",
        format_size(stats.bytes_transferred),
        format_size(stats.bytes_total)
    );
}

/// Print a final summary after sync
pub fn print_summary(stats: &SyncStats) {
    println!("\n=== Sync Complete ===");
    println!("Duration:          {}", format_duration(stats.duration_secs));
    println!("Files transferred: {}", stats.files_uploaded + stats.files_downloaded);
    println!("Files deleted:     {}", stats.files_deleted);
    println!("Files skipped:     {}", stats.files_skipped);
    println!("Delta syncs:       {}", stats.files_delta);
    println!("Errors:            {}", stats.errors);
    println!();
    println!("Bytes transferred: {}", format_size(stats.bytes_transferred));
    println!(
        "Bandwidth saved:   {} ({:.1}%)",
        format_size(stats.bytes_saved),
        stats.savings_percent()
    );
    println!(
        "Transfer rate:     {}",
        format_rate(stats.transfer_rate())
    );
}
