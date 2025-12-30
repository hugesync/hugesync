//! Safe memory-mapped file access with file locking
//!
//! This module provides safe wrappers around memory-mapped files that acquire
//! appropriate file locks before mapping to prevent SIGBUS/access violations
//! if the file is modified by another process.

use crate::error::{Error, Result};
#[allow(unused_imports)]
use fs2::FileExt;  // Provides lock_shared(), try_lock_shared() on File
use memmap2::Mmap;
use std::fs::File;
use std::ops::Deref;
use std::path::Path;

/// A memory-mapped file with an associated shared (read) lock.
///
/// The file lock is held for the lifetime of this struct, preventing other
/// processes from truncating or exclusively locking the file while we're
/// reading from the memory map.
///
/// # Safety
///
/// While this provides protection against well-behaved processes that respect
/// file locks, it cannot protect against:
/// - Processes that don't acquire locks before modifying files
/// - Direct writes to the underlying storage
/// - Network filesystem issues
///
/// For maximum safety when syncing live filesystems, consider using
/// `--inplace=false` mode which copies to a temp file first.
pub struct LockedMmap {
    /// The memory-mapped region
    mmap: Mmap,
    /// File handle kept open to maintain the lock
    /// Note: The lock is automatically released when the file is dropped
    #[allow(dead_code)]
    file: File,
}

impl LockedMmap {
    /// Open a file and create a memory map with a shared (read) lock.
    ///
    /// This acquires a shared lock on the file before creating the memory map,
    /// which prevents other cooperating processes from exclusively locking or
    /// truncating the file while we're reading it.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to map
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be opened
    /// - The shared lock cannot be acquired (file is exclusively locked)
    /// - Memory mapping fails
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| Error::io("opening file for mmap", e))?;

        Self::from_file(file)
    }

    /// Create a memory map from an already-opened file with a shared lock.
    ///
    /// This is useful when you already have a File handle and want to map it.
    pub fn from_file(file: File) -> Result<Self> {
        // Acquire a shared (read) lock
        // This will block if another process has an exclusive lock
        file.lock_shared()
            .map_err(|e| Error::io("acquiring shared file lock", e))?;

        // Safety: We hold the shared lock, so the file shouldn't be truncated
        // by cooperating processes. The mmap is valid for the lifetime of the lock.
        let mmap = unsafe { Mmap::map(&file) }
            .map_err(|e| Error::io("memory mapping file", e))?;

        Ok(Self { mmap, file })
    }

    /// Try to open and map a file without blocking.
    ///
    /// Returns `Ok(None)` if the file is exclusively locked by another process.
    pub fn try_open(path: &Path) -> Result<Option<Self>> {
        let file = File::open(path)
            .map_err(|e| Error::io("opening file for mmap", e))?;

        // Try to acquire lock without blocking
        match file.try_lock_shared() {
            Ok(()) => {
                let mmap = unsafe { Mmap::map(&file) }
                    .map_err(|e| Error::io("memory mapping file", e))?;
                Ok(Some(Self { mmap, file }))
            }
            Err(e) => {
                // Convert to io::Error to check kind
                let io_err: std::io::Error = e.into();
                if io_err.kind() == std::io::ErrorKind::WouldBlock {
                    Ok(None) // File is locked
                } else {
                    Err(Error::io("trying shared file lock", io_err))
                }
            }
        }
    }

    /// Get the length of the memory-mapped region.
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the memory-mapped region is empty.
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }
}

impl Deref for LockedMmap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

impl AsRef<[u8]> for LockedMmap {
    fn as_ref(&self) -> &[u8] {
        &self.mmap
    }
}

// Lock is released automatically when File is dropped

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_locked_mmap_basic() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"hello world").unwrap();
        temp.flush().unwrap();

        let mmap = LockedMmap::open(temp.path()).unwrap();
        assert_eq!(&mmap[..], b"hello world");
        assert_eq!(mmap.len(), 11);
    }

    #[test]
    fn test_locked_mmap_empty_file() {
        let temp = NamedTempFile::new().unwrap();

        let mmap = LockedMmap::open(temp.path()).unwrap();
        assert!(mmap.is_empty());
    }

    #[test]
    fn test_locked_mmap_deref() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"test data").unwrap();
        temp.flush().unwrap();

        let mmap = LockedMmap::open(temp.path()).unwrap();

        // Test Deref
        let slice: &[u8] = &mmap;
        assert_eq!(slice, b"test data");

        // Test AsRef
        let slice: &[u8] = mmap.as_ref();
        assert_eq!(slice, b"test data");
    }

    #[test]
    fn test_try_open_available() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"content").unwrap();
        temp.flush().unwrap();

        let result = LockedMmap::try_open(temp.path()).unwrap();
        assert!(result.is_some());
    }
}
