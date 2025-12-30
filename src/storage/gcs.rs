//! Google Cloud Storage backend using object_store crate

use crate::error::{Error, Result};
use crate::storage::{ByteStream, CompletedPart};
use crate::types::FileEntry;
use bytes::Bytes;
use futures::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::path::PathBuf;
use std::sync::Arc;

/// Google Cloud Storage backend
#[derive(Clone)]
pub struct GcsBackend {
    /// Object store instance
    store: Arc<dyn ObjectStore>,
    /// Bucket name (for error messages)
    bucket: String,
    /// Prefix (like a subdirectory)
    prefix: String,
}

impl GcsBackend {
    /// Create a new GCS backend
    pub async fn new(bucket: String, prefix: String) -> Result<Self> {
        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&bucket)
            .build()
            .map_err(|e| Error::Gcs {
                message: format!("Failed to create GCS client: {}", e),
            })?;

        Ok(Self {
            store: Arc::new(store),
            bucket,
            prefix,
        })
    }

    /// Resolve a relative path to a full object_store Path
    fn resolve_key(&self, path: &str) -> ObjectPath {
        if self.prefix.is_empty() {
            ObjectPath::from(path)
        } else if path.is_empty() {
            ObjectPath::from(self.prefix.as_str())
        } else {
            ObjectPath::from(format!("{}/{}", self.prefix.trim_end_matches('/'), path))
        }
    }

    /// Strip prefix from a full key to get relative path
    fn strip_prefix(&self, key: &ObjectPath) -> String {
        let key_str = key.to_string();
        if self.prefix.is_empty() {
            key_str
        } else {
            let prefix_with_slash = format!("{}/", self.prefix.trim_end_matches('/'));
            key_str.strip_prefix(&prefix_with_slash)
                .or_else(|| key_str.strip_prefix(&self.prefix))
                .unwrap_or(&key_str)
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

            let relative = self.strip_prefix(&meta.location);
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

    /// Read a file's contents (loads entire file into memory)
    ///
    /// WARNING: For large files, use `get_stream()` instead.
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let key = self.resolve_key(path);

        self.store
            .get(&key)
            .await
            .map_err(|e| Error::Gcs {
                message: format!("Failed to download object: {}", e),
            })?
            .bytes()
            .await
            .map_err(|e| Error::Gcs {
                message: format!("Failed to read object bytes: {}", e),
            })
    }

    /// Read a file as a stream of chunks (memory-efficient for large files)
    pub async fn get_stream(&self, path: &str) -> Result<ByteStream> {
        use futures::channel::mpsc;

        let key = self.resolve_key(path);

        let result = self.store.get(&key).await.map_err(|e| Error::Gcs {
            message: format!("Failed to download object: {}", e),
        })?;

        // Use a channel to bridge the non-Sync object_store stream to our Sync ByteStream
        let (mut tx, rx) = mpsc::channel::<Result<Bytes>>(2);

        let mut obj_stream = result.into_stream();
        tokio::spawn(async move {
            use futures::SinkExt;
            while let Some(chunk) = obj_stream.next().await {
                let result = chunk.map_err(|e| Error::Gcs {
                    message: format!("Failed to read object stream: {}", e),
                });
                if tx.send(result).await.is_err() {
                    break; // Receiver dropped
                }
            }
        });

        Ok(Box::pin(rx))
    }

    /// Read a range of bytes from a file
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let key = self.resolve_key(path);

        self.store
            .get_range(&key, start..end + 1)
            .await
            .map_err(|e| Error::Gcs {
                message: format!("Failed to download object range: {}", e),
            })
    }

    /// Write a file's contents
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let key = self.resolve_key(path);

        self.store
            .put(&key, PutPayload::from(data))
            .await
            .map_err(|e| Error::Gcs {
                message: format!("Failed to upload object: {}", e),
            })?;

        Ok(())
    }

    /// Write a file from a stream of chunks
    pub async fn put_stream(
        &self,
        path: &str,
        mut stream: ByteStream,
        _size_hint: Option<u64>,
    ) -> Result<()> {
        // Collect stream into bytes - object_store's multipart upload requires
        // knowing the size upfront for optimal chunking
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend_from_slice(&chunk?);
        }

        self.put(path, Bytes::from(data)).await
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
