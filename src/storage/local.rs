//! Local filesystem storage backend

use crate::error::{Error, Result};
use crate::storage::CompletedPart;
use crate::types::FileEntry;
use bytes::Bytes;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncReadExt;

/// Local filesystem storage backend
#[derive(Clone)]
pub struct LocalBackend {
    /// Root path for this backend
    root: PathBuf,
}

impl LocalBackend {
    /// Create a new local backend with the given root path
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Resolve a relative path to an absolute path
    fn resolve(&self, path: &str) -> PathBuf {
        if path.is_empty() {
            self.root.clone()
        } else {
            self.root.join(path)
        }
    }

    /// List all files under the given prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        let root = self.resolve(prefix);

        // Use jwalk in a blocking task for parallel directory walking
        let entries = tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();

            for entry in jwalk::WalkDir::new(&root)
                .skip_hidden(false)
                .follow_links(false)
                .parallelism(jwalk::Parallelism::RayonNewPool(num_cpus::get()))
            {
                match entry {
                    Ok(e) => {
                        let path = e.path();
                        let relative = path
                            .strip_prefix(&root)
                            .unwrap_or(&path)
                            .to_path_buf();

                        if relative.as_os_str().is_empty() {
                            continue; // Skip root itself
                        }

                        let metadata = match e.metadata() {
                            Ok(m) => m,
                            Err(_) => continue,
                        };

                        let entry = FileEntry {
                            path: relative,
                            size: metadata.len(),
                            mtime: metadata.modified().ok(),
                            is_dir: metadata.is_dir(),
                            mode: Some(metadata.mode()),
                            etag: None,
                        };

                        results.push(entry);
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Error walking directory");
                    }
                }
            }

            results
        })
        .await
        .map_err(|e| Error::io("spawn_blocking", std::io::Error::new(
            std::io::ErrorKind::Other,
            e.to_string(),
        )))?;

        Ok(entries)
    }

    /// Get file metadata (head request)
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        let full_path = self.resolve(path);

        match fs::metadata(&full_path).await {
            Ok(metadata) => {
                let entry = FileEntry {
                    path: PathBuf::from(path),
                    size: metadata.len(),
                    mtime: metadata.modified().ok(),
                    is_dir: metadata.is_dir(),
                    mode: Some(metadata.mode()),
                    etag: None,
                };
                Ok(Some(entry))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io("reading metadata", e)),
        }
    }

    /// Read a file's contents
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let full_path = self.resolve(path);
        let data = fs::read(&full_path)
            .await
            .map_err(|e| Error::io("reading file", e))?;
        Ok(Bytes::from(data))
    }

    /// Read a range of bytes from a file
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let full_path = self.resolve(path);
        let mut file = fs::File::open(&full_path)
            .await
            .map_err(|e| Error::io("opening file", e))?;

        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|e| Error::io("seeking file", e))?;

        let len = (end - start + 1) as usize;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)
            .await
            .map_err(|e| Error::io("reading range", e))?;

        Ok(Bytes::from(buf))
    }

    /// Write a file's contents
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let full_path = self.resolve(path);

        // Create parent directories if needed
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| Error::io("creating directories", e))?;
        }

        fs::write(&full_path, &data)
            .await
            .map_err(|e| Error::io("writing file", e))?;

        Ok(())
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        let full_path = self.resolve(path);

        let metadata = fs::metadata(&full_path)
            .await
            .map_err(|e| Error::io("reading metadata", e))?;

        if metadata.is_dir() {
            fs::remove_dir_all(&full_path)
                .await
                .map_err(|e| Error::io("removing directory", e))?;
        } else {
            fs::remove_file(&full_path)
                .await
                .map_err(|e| Error::io("removing file", e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_backend_put_get() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalBackend::new(tmp.path().to_path_buf());

        // Put a file
        backend.put("test.txt", Bytes::from("hello world")).await.unwrap();

        // Get the file
        let data = backend.get("test.txt").await.unwrap();
        assert_eq!(data, Bytes::from("hello world"));
    }

    #[tokio::test]
    async fn test_local_backend_head() {
        let tmp = TempDir::new().unwrap();
        let backend = LocalBackend::new(tmp.path().to_path_buf());

        // File doesn't exist
        assert!(backend.head("nonexistent.txt").await.unwrap().is_none());

        // Create file
        backend.put("test.txt", Bytes::from("content")).await.unwrap();

        // Now it exists
        let entry = backend.head("test.txt").await.unwrap().unwrap();
        assert_eq!(entry.size, 7);
        assert!(!entry.is_dir);
    }
}
