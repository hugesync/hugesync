//! Storage backends for HugeSync

pub mod azure;
pub mod gcs;
pub mod local;
pub mod s3;
pub mod ssh;

use crate::error::Result;
use crate::types::FileEntry;
use bytes::Bytes;
use futures::Stream;
use std::path::PathBuf;
use std::pin::Pin;

pub use azure::AzureBackend;
pub use gcs::GcsBackend;
pub use local::LocalBackend;
pub use s3::S3Backend;
pub use ssh::SshBackend;

/// Completed multipart upload part info
#[derive(Debug, Clone)]
pub struct CompletedPart {
    /// Part number (1-indexed)
    pub part_number: i32,
    /// ETag of the uploaded part
    pub etag: String,
}

/// Type alias for a stream of byte chunks
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>>;

/// Storage backend enum for unified access to different storage systems
#[derive(Clone)]
pub enum StorageBackend {
    Local(LocalBackend),
    S3(S3Backend),
    Gcs(GcsBackend),
    Azure(AzureBackend),
    Ssh(SshBackend),
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

    /// Create an SSH backend
    pub async fn ssh(
        user: Option<String>,
        host: String,
        path: String,
        port: Option<u16>,
    ) -> Result<Self> {
        Ok(StorageBackend::Ssh(SshBackend::new(user, host, path, port).await?))
    }

    /// Get the name of this backend (for logging)
    pub fn name(&self) -> &'static str {
        match self {
            StorageBackend::Local(_) => "local",
            StorageBackend::S3(_) => "s3",
            StorageBackend::Gcs(_) => "gcs",
            StorageBackend::Azure(_) => "azure",
            StorageBackend::Ssh(_) => "ssh",
        }
    }

    /// List all files under the given prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        match self {
            StorageBackend::Local(b) => b.list(prefix).await,
            StorageBackend::S3(b) => b.list(prefix).await,
            StorageBackend::Gcs(b) => b.list(prefix).await,
            StorageBackend::Azure(b) => b.list(prefix).await,
            StorageBackend::Ssh(b) => b.list(prefix).await,
        }
    }

    /// Get file metadata (head request)
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        match self {
            StorageBackend::Local(b) => b.head(path).await,
            StorageBackend::S3(b) => b.head(path).await,
            StorageBackend::Gcs(b) => b.head(path).await,
            StorageBackend::Azure(b) => b.head(path).await,
            StorageBackend::Ssh(b) => b.head(path).await,
        }
    }

    /// Read a file's contents
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        match self {
            StorageBackend::Local(b) => b.get(path).await,
            StorageBackend::S3(b) => b.get(path).await,
            StorageBackend::Gcs(b) => b.get(path).await,
            StorageBackend::Azure(b) => b.get(path).await,
            StorageBackend::Ssh(b) => b.get(path).await,
        }
    }

    /// Read a range of bytes from a file
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        match self {
            StorageBackend::Local(b) => b.get_range(path, start, end).await,
            StorageBackend::S3(b) => b.get_range(path, start, end).await,
            StorageBackend::Gcs(b) => b.get_range(path, start, end).await,
            StorageBackend::Azure(b) => b.get_range(path, start, end).await,
            StorageBackend::Ssh(b) => b.get_range(path, start, end).await,
        }
    }

    /// Write a file's contents (for small files only)
    ///
    /// WARNING: This loads all data into memory. For large files, use
    /// `put_stream` or multipart upload methods instead.
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        match self {
            StorageBackend::Local(b) => b.put(path, data).await,
            StorageBackend::S3(b) => b.put(path, data).await,
            StorageBackend::Gcs(b) => b.put(path, data).await,
            StorageBackend::Azure(b) => b.put(path, data).await,
            StorageBackend::Ssh(b) => b.put(path, data).await,
        }
    }

    /// Write a file from a stream of chunks (memory-efficient for large files)
    ///
    /// This method automatically uses multipart upload for backends that support it,
    /// or buffers to a temp file for local backend.
    pub async fn put_stream(
        &self,
        path: &str,
        stream: ByteStream,
        size_hint: Option<u64>,
    ) -> Result<()> {
        match self {
            StorageBackend::Local(b) => b.put_stream(path, stream).await,
            StorageBackend::S3(b) => b.put_stream(path, stream, size_hint).await,
            StorageBackend::Gcs(b) => b.put_stream(path, stream, size_hint).await,
            StorageBackend::Azure(b) => b.put_stream(path, stream, size_hint).await,
            StorageBackend::Ssh(b) => b.put_stream(path, stream).await,
        }
    }

    /// Write a file from a local path (memory-efficient)
    /// 
    /// Uses memory mapping and streaming upload to avoid loading entire file into RAM.
    /// This is the recommended method for uploading large local files.
    pub async fn put_file(&self, remote_path: &str, local_path: &std::path::Path) -> Result<()> {
        let metadata = std::fs::metadata(local_path)
            .map_err(|e| crate::error::Error::io("reading file metadata", e))?;
        let file_size = metadata.len();

        // For small files (< 8MB), just read into memory
        const SMALL_FILE_THRESHOLD: u64 = 8 * 1024 * 1024;
        if file_size < SMALL_FILE_THRESHOLD {
            let data = std::fs::read(local_path)
                .map_err(|e| crate::error::Error::io("reading small file", e))?;
            return self.put(remote_path, Bytes::from(data)).await;
        }

        // For large files, use streaming upload
        let stream = file_to_stream(local_path)?;
        self.put_stream(remote_path, stream, Some(file_size)).await
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        match self {
            StorageBackend::Local(b) => b.delete(path).await,
            StorageBackend::S3(b) => b.delete(path).await,
            StorageBackend::Gcs(b) => b.delete(path).await,
            StorageBackend::Azure(b) => b.delete(path).await,
            StorageBackend::Ssh(b) => b.delete(path).await,
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
            StorageBackend::Ssh(_) => false,  // SSH uses streaming
        }
    }

    /// Check if this backend supports upload_part_copy (for delta sync)
    pub fn supports_part_copy(&self) -> bool {
        match self {
            StorageBackend::Local(_) => false,
            StorageBackend::S3(_) => true,
            StorageBackend::Gcs(_) => false,  // GCS requires XML API for server-side copy
            StorageBackend::Azure(_) => true, // Azure can copy blocks
            StorageBackend::Ssh(_) => false,  // SSH doesn't support server-side copy
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
            StorageBackend::Ssh(_) => Err(crate::error::Error::storage("multipart not supported for SSH")),
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
            StorageBackend::Ssh(_) => Err(crate::error::Error::storage("multipart not supported for SSH")),
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
            StorageBackend::Ssh(_) => Err(crate::error::Error::storage("part copy not supported for SSH")),
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
            StorageBackend::Ssh(_) => Err(crate::error::Error::storage("multipart not supported for SSH")),
        }
    }

    /// Abort a multipart upload
    pub async fn abort_multipart_upload(&self, path: &str, upload_id: &str) -> Result<()> {
        match self {
            StorageBackend::Local(_) => Err(crate::error::Error::storage("multipart not supported")),
            StorageBackend::S3(b) => b.abort_multipart_upload(path, upload_id).await,
            StorageBackend::Gcs(b) => b.abort_multipart_upload(path, upload_id).await,
            StorageBackend::Azure(b) => b.abort_multipart_upload(path, upload_id).await,
            StorageBackend::Ssh(_) => Err(crate::error::Error::storage("multipart not supported for SSH")),
        }
    }
}

/// Convert a local file path to a stream of chunks using memory mapping
/// 
/// This reads the file in 16MB chunks without loading the entire file into memory.
fn file_to_stream(path: &std::path::Path) -> Result<ByteStream> {
    use futures::stream;
    
    const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16MB chunks
    
    let file = std::fs::File::open(path)
        .map_err(|e| crate::error::Error::io("opening file for streaming", e))?;
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| crate::error::Error::io("mmapping file for streaming", e))?;
    
    let total_size = mmap.len();
    
    // Create an iterator that yields chunks
    let chunks: Vec<Bytes> = (0..total_size)
        .step_by(CHUNK_SIZE)
        .map(|offset| {
            let end = std::cmp::min(offset + CHUNK_SIZE, total_size);
            Bytes::copy_from_slice(&mmap[offset..end])
        })
        .collect();
    
    Ok(Box::pin(stream::iter(chunks.into_iter().map(Ok))))
}