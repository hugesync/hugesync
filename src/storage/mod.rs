//! Storage backends for HugeSync

pub mod azure;
pub mod gcs;
pub mod local;
pub mod s3;

use crate::error::Result;
use crate::types::FileEntry;
use bytes::Bytes;
use std::path::PathBuf;

pub use azure::AzureBackend;
pub use gcs::GcsBackend;
pub use local::LocalBackend;
pub use s3::S3Backend;

/// Completed multipart upload part info
#[derive(Debug, Clone)]
pub struct CompletedPart {
    /// Part number (1-indexed)
    pub part_number: i32,
    /// ETag of the uploaded part
    pub etag: String,
}

/// Storage backend enum for unified access to different storage systems
#[derive(Clone)]
pub enum StorageBackend {
    Local(LocalBackend),
    S3(S3Backend),
    Gcs(GcsBackend),
    Azure(AzureBackend),
}

impl StorageBackend {
    /// Create a local backend
    pub fn local(path: PathBuf) -> Self {
        StorageBackend::Local(LocalBackend::new(path))
    }

    /// Create an S3 backend
    pub async fn s3(bucket: String, prefix: String) -> Result<Self> {
        Ok(StorageBackend::S3(S3Backend::new(bucket, prefix).await?))
    }

    /// Create a GCS backend
    pub async fn gcs(bucket: String, prefix: String) -> Result<Self> {
        Ok(StorageBackend::Gcs(GcsBackend::new(bucket, prefix).await?))
    }

    /// Create an Azure backend
    pub async fn azure(container: String, prefix: String) -> Result<Self> {
        Ok(StorageBackend::Azure(AzureBackend::new(container, prefix).await?))
    }

    /// Get the name of this backend (for logging)
    pub fn name(&self) -> &'static str {
        match self {
            StorageBackend::Local(_) => "local",
            StorageBackend::S3(_) => "s3",
            StorageBackend::Gcs(_) => "gcs",
            StorageBackend::Azure(_) => "azure",
        }
    }

    /// List all files under the given prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        match self {
            StorageBackend::Local(b) => b.list(prefix).await,
            StorageBackend::S3(b) => b.list(prefix).await,
            StorageBackend::Gcs(b) => b.list(prefix).await,
            StorageBackend::Azure(b) => b.list(prefix).await,
        }
    }

    /// Get file metadata (head request)
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        match self {
            StorageBackend::Local(b) => b.head(path).await,
            StorageBackend::S3(b) => b.head(path).await,
            StorageBackend::Gcs(b) => b.head(path).await,
            StorageBackend::Azure(b) => b.head(path).await,
        }
    }

    /// Read a file's contents
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        match self {
            StorageBackend::Local(b) => b.get(path).await,
            StorageBackend::S3(b) => b.get(path).await,
            StorageBackend::Gcs(b) => b.get(path).await,
            StorageBackend::Azure(b) => b.get(path).await,
        }
    }

    /// Read a range of bytes from a file
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        match self {
            StorageBackend::Local(b) => b.get_range(path, start, end).await,
            StorageBackend::S3(b) => b.get_range(path, start, end).await,
            StorageBackend::Gcs(b) => b.get_range(path, start, end).await,
            StorageBackend::Azure(b) => b.get_range(path, start, end).await,
        }
    }

    /// Write a file's contents
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        match self {
            StorageBackend::Local(b) => b.put(path, data).await,
            StorageBackend::S3(b) => b.put(path, data).await,
            StorageBackend::Gcs(b) => b.put(path, data).await,
            StorageBackend::Azure(b) => b.put(path, data).await,
        }
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        match self {
            StorageBackend::Local(b) => b.delete(path).await,
            StorageBackend::S3(b) => b.delete(path).await,
            StorageBackend::Gcs(b) => b.delete(path).await,
            StorageBackend::Azure(b) => b.delete(path).await,
        }
    }

    /// Check if a file exists
    pub async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.head(path).await?.is_some())
    }

    /// Check if this backend supports multipart uploads
    pub fn supports_multipart(&self) -> bool {
        match self {
            StorageBackend::Local(_) => false,
            StorageBackend::S3(_) => true,
            StorageBackend::Gcs(_) => true,  // GCS supports compose
            StorageBackend::Azure(_) => true, // Azure supports block list
        }
    }

    /// Check if this backend supports upload_part_copy (for delta sync)
    pub fn supports_part_copy(&self) -> bool {
        match self {
            StorageBackend::Local(_) => false,
            StorageBackend::S3(_) => true,
            StorageBackend::Gcs(_) => false,  // GCS requires XML API for server-side copy
            StorageBackend::Azure(_) => true, // Azure can copy blocks
        }
    }

    // Multipart upload operations

    /// Start a multipart upload, returns upload ID
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String> {
        match self {
            StorageBackend::Local(_) => Err(crate::error::Error::storage("multipart not supported")),
            StorageBackend::S3(b) => b.create_multipart_upload(path).await,
            StorageBackend::Gcs(b) => b.create_multipart_upload(path).await,
            StorageBackend::Azure(b) => b.create_multipart_upload(path).await,
        }
    }

    /// Upload a part to an ongoing multipart upload
    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<CompletedPart> {
        match self {
            StorageBackend::Local(_) => Err(crate::error::Error::storage("multipart not supported")),
            StorageBackend::S3(b) => b.upload_part(path, upload_id, part_number, data).await,
            StorageBackend::Gcs(b) => b.upload_part(path, upload_id, part_number, data).await,
            StorageBackend::Azure(b) => b.upload_part(path, upload_id, part_number, data).await,
        }
    }

    /// Copy a part from an existing object (for delta sync)
    pub async fn upload_part_copy(
        &self,
        path: &str,
        upload_id: &str,
        part_number: i32,
        source_path: &str,
        source_start: u64,
        source_end: u64,
    ) -> Result<CompletedPart> {
        match self {
            StorageBackend::Local(_) => Err(crate::error::Error::storage("part copy not supported")),
            StorageBackend::S3(b) => {
                b.upload_part_copy(path, upload_id, part_number, source_path, source_start, source_end)
                    .await
            }
            StorageBackend::Gcs(b) => {
                b.upload_part_copy(path, upload_id, part_number, source_path, source_start, source_end)
                    .await
            }
            StorageBackend::Azure(b) => {
                b.upload_part_copy(path, upload_id, part_number, source_path, source_start, source_end)
                    .await
            }
        }
    }

    /// Complete a multipart upload
    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<()> {
        match self {
            StorageBackend::Local(_) => Err(crate::error::Error::storage("multipart not supported")),
            StorageBackend::S3(b) => b.complete_multipart_upload(path, upload_id, parts).await,
            StorageBackend::Gcs(b) => b.complete_multipart_upload(path, upload_id, parts).await,
            StorageBackend::Azure(b) => b.complete_multipart_upload(path, upload_id, parts).await,
        }
    }

    /// Abort a multipart upload
    pub async fn abort_multipart_upload(&self, path: &str, upload_id: &str) -> Result<()> {
        match self {
            StorageBackend::Local(_) => Err(crate::error::Error::storage("multipart not supported")),
            StorageBackend::S3(b) => b.abort_multipart_upload(path, upload_id).await,
            StorageBackend::Gcs(b) => b.abort_multipart_upload(path, upload_id).await,
            StorageBackend::Azure(b) => b.abort_multipart_upload(path, upload_id).await,
        }
    }
}
