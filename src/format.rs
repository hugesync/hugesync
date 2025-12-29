//! Output formatting utilities

use std::time::Duration;

/// Format file size in human-readable format
pub fn format_size(bytes: u64) -> String {
    human_bytes::human_bytes(bytes as f64)
}

/// Format duration in human-readable format
pub fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    format_duration_secs(secs)
}

/// Format duration from seconds
pub fn format_duration_secs(secs: f64) -> String {
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else if secs < 3600.0 {
        let mins = (secs / 60.0).floor();
        let remaining = secs - mins * 60.0;
        format!("{}m {:.0}s", mins as u64, remaining)
    } else {
        let hours = (secs / 3600.0).floor();
        let remaining = secs - hours * 3600.0;
        let mins = (remaining / 60.0).floor();
        format!("{}h {}m", hours as u64, mins as u64)
    }
}

/// Format transfer rate in human-readable format
pub fn format_rate(bytes_per_sec: f64) -> String {
    format!("{}/s", human_bytes::human_bytes(bytes_per_sec))
}

/// Format percentage
pub fn format_percent(value: f64) -> String {
    format!("{:.1}%", value)
}

/// Format a count with a unit
pub fn format_count(count: u64, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("{} {}", count, singular)
    } else {
        format!("{} {}", count, plural)
    }
}

/// Format a file count
pub fn format_files(count: u64) -> String {
    format_count(count, "file", "files")
}

/// Dry run report showing what would be done
pub struct DryRunReport {
    pub files_to_upload: u64,
    pub files_to_download: u64,
    pub files_to_delete: u64,
    pub files_to_skip: u64,
    pub files_delta: u64,
    pub bytes_to_transfer: u64,
    pub bytes_saved: u64,
    pub bytes_total: u64,
}

impl DryRunReport {
    /// Format the dry run report
    pub fn format(&self) -> String {
        let mut lines = Vec::new();
        
        lines.push("=== Dry Run Report ===".to_string());
        lines.push(String::new());
        
        if self.files_to_upload > 0 {
            lines.push(format!("Would upload:   {}", format_files(self.files_to_upload)));
        }
        if self.files_to_download > 0 {
            lines.push(format!("Would download: {}", format_files(self.files_to_download)));
        }
        if self.files_to_delete > 0 {
            lines.push(format!("Would delete:   {}", format_files(self.files_to_delete)));
        }
        if self.files_delta > 0 {
            lines.push(format!("Delta sync:     {}", format_files(self.files_delta)));
        }
        lines.push(format!("Would skip:     {}", format_files(self.files_to_skip)));
        
        lines.push(String::new());
        lines.push(format!(
            "Estimated transfer: {} (instead of {})",
            format_size(self.bytes_to_transfer),
            format_size(self.bytes_total)
        ));
        
        if self.bytes_saved > 0 {
            let savings_percent = if self.bytes_total > 0 {
                (self.bytes_saved as f64 / self.bytes_total as f64) * 100.0
            } else {
                0.0
            };
            lines.push(format!(
                "Bandwidth savings: {} ({})",
                format_size(self.bytes_saved),
                format_percent(savings_percent)
            ));
        }
        
        lines.join("\n")
    }

    /// Print the report to stdout
    pub fn print(&self) {
        println!("{}", self.format());
    }
}

/// Sync completion report
pub struct SyncReport {
    pub duration_secs: f64,
    pub files_uploaded: u64,
    pub files_downloaded: u64,
    pub files_deleted: u64,
    pub files_skipped: u64,
    pub files_delta: u64,
    pub bytes_transferred: u64,
    pub bytes_saved: u64,
    pub errors: u64,
}

impl SyncReport {
    /// Format the sync report
    pub fn format(&self) -> String {
        let mut lines = Vec::new();
        
        lines.push("=== Sync Complete ===".to_string());
        lines.push(String::new());
        lines.push(format!("Duration:     {}", format_duration_secs(self.duration_secs)));
        lines.push(String::new());
        
        let total_transferred = self.files_uploaded + self.files_downloaded;
        if total_transferred > 0 {
            lines.push(format!("Transferred:  {}", format_files(total_transferred)));
        }
        if self.files_deleted > 0 {
            lines.push(format!("Deleted:      {}", format_files(self.files_deleted)));
        }
        if self.files_delta > 0 {
            lines.push(format!("Delta synced: {}", format_files(self.files_delta)));
        }
        lines.push(format!("Skipped:      {}", format_files(self.files_skipped)));
        
        if self.errors > 0 {
            lines.push(format!("Errors:       {}", self.errors));
        }
        
        lines.push(String::new());
        lines.push(format!("Data transferred: {}", format_size(self.bytes_transferred)));
        
        if self.bytes_saved > 0 {
            lines.push(format!("Bandwidth saved:  {}", format_size(self.bytes_saved)));
        }
        
        if self.duration_secs > 0.0 {
            let rate = self.bytes_transferred as f64 / self.duration_secs;
            lines.push(format!("Transfer rate:    {}", format_rate(rate)));
        }
        
        lines.join("\n")
    }

    /// Print the report to stdout
    pub fn print(&self) {
        println!("{}", self.format());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(0), "0 B");
        // human_bytes uses binary prefixes (KiB, MiB)
        assert!(format_size(1024).contains("1"));
        assert!(format_size(1024 * 1024).contains("1"));
    }

    #[test]
    fn test_format_duration_secs() {
        assert_eq!(format_duration_secs(0.5), "500ms");
        assert_eq!(format_duration_secs(45.0), "45.0s");
        assert_eq!(format_duration_secs(90.0), "1m 30s");
        assert_eq!(format_duration_secs(3700.0), "1h 1m");
    }

    #[test]
    fn test_format_files() {
        assert_eq!(format_files(1), "1 file");
        assert_eq!(format_files(5), "5 files");
    }
}
