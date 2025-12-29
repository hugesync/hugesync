//! Itemized change output for rsync-compatible reporting
//!
//! Implements rsync's -i/--itemize-changes output format.
//! Format: YXcstpoguax  filename
//!
//! Y = update type:
//!   < = file sent (upload)
//!   > = file received (download)
//!   c = local change (e.g., creation)
//!   h = hard link
//!   . = not updated
//!   * = message (e.g., delete)
//!
//! X = file type:
//!   f = file
//!   d = directory
//!   L = symlink
//!   D = device
//!   S = special file
//!
//! Remaining 9 characters indicate what changed:
//!   c = checksum differs
//!   s = size differs
//!   t = mod time differs
//!   p = permissions differ
//!   o = owner differs
//!   g = group differs
//!   u = (reserved)
//!   a = ACL differs
//!   x = extended attrs differ

use crate::types::{FileEntry, SyncAction};
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Itemized change for a single file
#[derive(Debug, Clone)]
pub struct ItemizedChange {
    /// The type of update (send, receive, local, delete, etc.)
    pub update_type: UpdateType,
    /// The file type
    pub file_type: FileType,
    /// What attributes changed
    pub changes: ChangeFlags,
    /// The file path
    pub path: String,
    /// Optional message (for errors, etc.)
    pub message: Option<String>,
}

/// Type of update being performed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateType {
    /// File being sent (upload)
    Send,
    /// File being received (download)
    Receive,
    /// Local change (creation)
    LocalChange,
    /// Hard link created
    HardLink,
    /// Not updated (unchanged)
    Unchanged,
    /// Message (delete, error, etc.)
    Message,
}

impl UpdateType {
    pub fn as_char(&self) -> char {
        match self {
            UpdateType::Send => '<',
            UpdateType::Receive => '>',
            UpdateType::LocalChange => 'c',
            UpdateType::HardLink => 'h',
            UpdateType::Unchanged => '.',
            UpdateType::Message => '*',
        }
    }
}

/// File type indicator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
    Device,
    Special,
}

impl FileType {
    pub fn as_char(&self) -> char {
        match self {
            FileType::File => 'f',
            FileType::Directory => 'd',
            FileType::Symlink => 'L',
            FileType::Device => 'D',
            FileType::Special => 'S',
        }
    }

    pub fn from_entry(entry: &FileEntry) -> Self {
        if entry.is_dir {
            FileType::Directory
        } else {
            FileType::File
        }
    }
}

/// Flags indicating what changed
#[derive(Debug, Clone, Copy, Default)]
pub struct ChangeFlags {
    pub checksum: bool,
    pub size: bool,
    pub time: bool,
    pub permissions: bool,
    pub owner: bool,
    pub group: bool,
    pub acl: bool,
    pub xattr: bool,
}

impl ChangeFlags {
    /// Create flags indicating a new file
    pub fn new_file() -> Self {
        Self {
            checksum: true,
            size: true,
            time: true,
            permissions: true,
            owner: true,
            group: true,
            ..Default::default()
        }
    }

    /// Create flags from comparing two entries
    pub fn from_diff(source: &FileEntry, dest: &FileEntry) -> Self {
        let mut flags = ChangeFlags::default();

        if source.size != dest.size {
            flags.size = true;
            flags.checksum = true; // Size change implies content change
        }

        // Compare modification times
        if let (Some(s_mtime), Some(d_mtime)) = (source.mtime, dest.mtime) {
            if s_mtime != d_mtime {
                flags.time = true;
            }
        }

        // Compare permissions (mode)
        if source.mode != dest.mode {
            flags.permissions = true;
        }

        flags
    }

    /// Format as the 9-character change string
    pub fn as_string(&self) -> String {
        format!(
            "{}{}{}{}{}{}{}{}{}",
            if self.checksum { 'c' } else { '.' },
            if self.size { 's' } else { '.' },
            if self.time { 't' } else { '.' },
            if self.permissions { 'p' } else { '.' },
            if self.owner { 'o' } else { '.' },
            if self.group { 'g' } else { '.' },
            '.', // reserved (u)
            if self.acl { 'a' } else { '.' },
            if self.xattr { 'x' } else { '.' },
        )
    }
}

impl ItemizedChange {
    /// Create a new itemized change
    pub fn new(
        update_type: UpdateType,
        file_type: FileType,
        changes: ChangeFlags,
        path: impl Into<String>,
    ) -> Self {
        Self {
            update_type,
            file_type,
            changes,
            path: path.into(),
            message: None,
        }
    }

    /// Create from a sync action and file entry
    pub fn from_action(action: SyncAction, entry: &FileEntry, dest_entry: Option<&FileEntry>) -> Self {
        let file_type = FileType::from_entry(entry);
        let path = entry.path.to_string_lossy().to_string();

        match action {
            SyncAction::Upload => {
                let changes = match dest_entry {
                    Some(dest) => ChangeFlags::from_diff(entry, dest),
                    None => ChangeFlags::new_file(),
                };
                Self::new(UpdateType::Send, file_type, changes, path)
            }
            SyncAction::Download => {
                let changes = match dest_entry {
                    Some(dest) => ChangeFlags::from_diff(entry, dest),
                    None => ChangeFlags::new_file(),
                };
                Self::new(UpdateType::Receive, file_type, changes, path)
            }
            SyncAction::Delta => {
                let changes = match dest_entry {
                    Some(dest) => ChangeFlags::from_diff(entry, dest),
                    None => ChangeFlags::new_file(),
                };
                Self::new(UpdateType::Send, file_type, changes, path)
            }
            SyncAction::Delete => {
                let mut item = Self::new(UpdateType::Message, file_type, ChangeFlags::default(), path);
                item.message = Some("deleting".to_string());
                item
            }
            SyncAction::Skip => {
                Self::new(UpdateType::Unchanged, file_type, ChangeFlags::default(), path)
            }
            SyncAction::Mkdir => {
                Self::new(UpdateType::LocalChange, FileType::Directory, ChangeFlags::new_file(), path)
            }
        }
    }

    /// Create a delete message
    pub fn delete(path: impl Into<String>) -> Self {
        let mut item = Self::new(
            UpdateType::Message,
            FileType::File,
            ChangeFlags::default(),
            path,
        );
        item.message = Some("deleting".to_string());
        item
    }
}

impl fmt::Display for ItemizedChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(ref msg) = self.message {
            // Message format: *deleting   path
            write!(f, "*{}   {}", msg, self.path)
        } else {
            // Standard format: YXcstpoguax path
            write!(
                f,
                "{}{}{} {}",
                self.update_type.as_char(),
                self.file_type.as_char(),
                self.changes.as_string(),
                self.path
            )
        }
    }
}

/// Logger for itemized changes
pub struct ItemizedLogger {
    /// Whether itemized output is enabled
    enabled: bool,
    /// Log file writer (if any)
    log_file: Option<Mutex<std::fs::File>>,
}

impl ItemizedLogger {
    /// Create a new itemized logger
    pub fn new(enabled: bool, log_path: Option<&Path>) -> std::io::Result<Self> {
        let log_file = if let Some(path) = log_path {
            Some(Mutex::new(std::fs::File::create(path)?))
        } else {
            None
        };

        Ok(Self { enabled, log_file })
    }

    /// Log an itemized change
    pub fn log(&self, change: &ItemizedChange) {
        let line = format!("{}", change);

        // Print to stdout if enabled
        if self.enabled {
            println!("{}", line);
        }

        // Write to log file if configured
        if let Some(ref file) = self.log_file {
            if let Ok(mut f) = file.lock() {
                let _ = writeln!(f, "{}", line);
            }
        }
    }

    /// Log a simple message
    pub fn log_message(&self, message: &str) {
        if self.enabled {
            println!("{}", message);
        }

        if let Some(ref file) = self.log_file {
            if let Ok(mut f) = file.lock() {
                let _ = writeln!(f, "{}", message);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn make_entry(path: &str, size: u64, is_dir: bool) -> FileEntry {
        FileEntry {
            path: PathBuf::from(path),
            size,
            mtime: None,
            is_dir,
            mode: None,
            etag: None,
        }
    }

    #[test]
    fn test_itemized_upload_new_file() {
        let entry = make_entry("test.txt", 100, false);
        let change = ItemizedChange::from_action(SyncAction::Upload, &entry, None);

        let output = format!("{}", change);
        assert!(output.starts_with("<f"));
        assert!(output.contains("test.txt"));
    }

    #[test]
    fn test_itemized_delete() {
        let change = ItemizedChange::delete("old_file.txt");
        let output = format!("{}", change);
        assert!(output.contains("*deleting"));
        assert!(output.contains("old_file.txt"));
    }

    #[test]
    fn test_itemized_skip() {
        let entry = make_entry("unchanged.txt", 100, false);
        let change = ItemizedChange::from_action(SyncAction::Skip, &entry, None);

        let output = format!("{}", change);
        assert!(output.starts_with(".f"));
    }

    #[test]
    fn test_itemized_mkdir() {
        let entry = make_entry("new_dir", 0, true);
        let change = ItemizedChange::from_action(SyncAction::Mkdir, &entry, None);

        let output = format!("{}", change);
        assert!(output.starts_with("cd"));
    }

    #[test]
    fn test_change_flags_from_diff() {
        let source = make_entry("file.txt", 200, false);
        let dest = make_entry("file.txt", 100, false);

        let flags = ChangeFlags::from_diff(&source, &dest);
        assert!(flags.size);
        assert!(flags.checksum);
    }
}
