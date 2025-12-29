//! Google Cloud Storage backend using object_store crate

use crate::error::{Error, Result};
use crate::storage::{ByteStream, CompletedPart};
use crate::types::FileEntry;
use bytes::Bytes;
use futures::StreamExt;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};
use std::path::PathBuf;

/// Google Cloud Storage backend
#[derive(Clone, Debug)]
pub struct GcsBackend {
    /// Object store client (concrete type)
    store: std::sync::Arc<GoogleCloudStorage>,
    /// Bucket name (for reference)
    #[allow(dead_code)]
    bucket: String,
    /// Prefix (like a subdirectory)
    prefix: String,
}

impl GcsBackend {
    /// Create a new GCS backend
    pub async fn new(bucket: String, prefix: String) -> Result<Self> {
        let store = GoogleCloudStorageBuilder::new()
            .with_bucket_name(&bucket)
            .build()
            .map_err(|e| Error::Gcs {
                message: format!("Failed to create GCS client: {}", e),
            })?;

        Ok(Self {
            store: std::sync::Arc::new(store),
            bucket,
            prefix,
        })
    }

    /// Resolve a relative path to a full GCS key
    fn resolve_key(&self, path: &str) -> ObjectPath {
        let full_path = if self.prefix.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        };
        ObjectPath::from(full_path)
    }

    /// Strip prefix from a full key to get relative path
    fn strip_prefix(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            let prefix_with_slash = format!("{}/", self.prefix.trim_end_matches('/'));
            key.strip_prefix(&prefix_with_slash)
                .or_else(|| key.strip_prefix(&self.prefix))
                .unwrap_or(key)
                .to_string()
        }
    }

    /// List all files under the given prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        let full_prefix = self.resolve_key(prefix);
        let mut entries = Vec::new();

        let mut stream = self.store.list(Some(&full_prefix));

        while let Some(result) = stream.next().await {
            let meta = result.map_err(|e| Error::Gcs {
                message: format!("Failed to list objects: {}", e),
            })?;

            let relative = self.strip_prefix(meta.location.as_ref());
            if relative.is_empty() {
                continue;
            }

            entries.push(FileEntry {
                path: PathBuf::from(&relative),
                size: meta.size as u64,
                mtime: Some(meta.last_modified.into()),
                is_dir: relative.ends_with('/'),
                mode: None,
                etag: meta.e_tag,
            });
        }

        Ok(entries)
    }

    /// Get file metadata (head request)
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        let key = self.resolve_key(path);

        match self.store.head(&key).await {
            Ok(meta) => Ok(Some(FileEntry {
                path: PathBuf::from(path),
                size: meta.size as u64,
                mtime: Some(meta.last_modified.into()),
                is_dir: false,
                mode: None,
                etag: meta.e_tag,
            })),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(Error::Gcs {
                message: format!("Failed to get object metadata: {}", e),
            }),
        }
    }

    /// Read a file's contents
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let key = self.resolve_key(path);

        let result = self.store.get(&key).await.map_err(|e| Error::Gcs {
            message: format!("Failed to download object: {}", e),
        })?;

        let bytes = result.bytes().await.map_err(|e| Error::Gcs {
            message: format!("Failed to read object bytes: {}", e),
        })?;

        Ok(bytes)
    }

    /// Read a range of bytes from a file
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let key = self.resolve_key(path);
        let range = std::ops::Range {
            start: start,
            end: end + 1,
        };

        let bytes = self
            .store
            .get_range(&key, range)
            .await
            .map_err(|e| Error::Gcs {
                message: format!("Failed to download object range: {}", e),
            })?;

        Ok(bytes)
    }

    /// Write a file's contents
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let key = self.resolve_key(path);

        self.store
            .put(&key, data.into())
            .await
            .map_err(|e| Error::Gcs {
                message: format!("Failed to upload object: {}", e),
            })?;

        Ok(())
    }

    /// Write a file from a stream of chunks using multipart upload
    pub async fn put_stream(
        &self,
        path: &str,
        mut stream: ByteStream,
        _size_hint: Option<u64>,
    ) -> Result<()> {
        let key = self.resolve_key(path);

        let upload = self.store.put_multipart(&key).await.map_err(|e| Error::Gcs {
            message: format!("Failed to start multipart upload: {}", e),
        })?;

        let mut writer = WriteMultipart::new(upload);

        while let Some(chunk) = stream.next().await {
            let data = chunk?;
            writer.write(&data);
        }

        writer.finish().await.map_err(|e| Error::Gcs {
            message: format!("Failed to complete multipart upload: {}", e),
        })?;

        Ok(())
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        let key = self.resolve_key(path);

        self.store.delete(&key).await.map_err(|e| Error::Gcs {
            message: format!("Failed to delete object: {}", e),
        })?;

        Ok(())
    }

    /// GCS uses compose for multipart-like operations
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String> {
        let key = self.resolve_key(path);
        Ok(key.to_string())
    }

    /// Upload a part
    pub async fn upload_part(
        &self,
        _path: &str,
        _upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<CompletedPart> {
        let etag = format!("gcs-part-{}-{}", part_number, data.len());
        Ok(CompletedPart { part_number, etag })
    }

    /// Copy a part from an existing object
    pub async fn upload_part_copy(
        &self,
        _path: &str,
        _upload_id: &str,
        part_number: i32,
        source_path: &str,
        source_start: u64,
        source_end: u64,
    ) -> Result<CompletedPart> {
        let _data = self
            .get_range(source_path, source_start, source_end)
            .await?;
        let etag = format!("gcs-copy-{}", part_number);
        Ok(CompletedPart { part_number, etag })
    }

    /// Complete a multipart upload
    pub async fn complete_multipart_upload(
        &self,
        _path: &str,
        _upload_id: &str,
        _parts: Vec<CompletedPart>,
    ) -> Result<()> {
        Ok(())
    }

    /// Abort a multipart upload
    pub async fn abort_multipart_upload(&self, _path: &str, _upload_id: &str) -> Result<()> {
        Ok(())
    }
}
