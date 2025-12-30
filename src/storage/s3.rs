//! AWS S3 storage backend

use crate::error::{Error, Result};
use crate::storage::{ByteStream, CompletedPart};
use crate::types::FileEntry;
use aws_sdk_s3::primitives::ByteStream as AwsByteStream;
use aws_sdk_s3::Client;
use bytes::Bytes;
use futures::StreamExt;

/// AWS S3 storage backend
#[derive(Clone)]
pub struct S3Backend {
    /// S3 client
    client: Client,
    /// Bucket name
    bucket: String,
    /// Prefix (like a subdirectory)
    prefix: String,
}

impl S3Backend {
    /// Create a new S3 backend
    pub async fn new(bucket: String, prefix: String) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket,
            prefix,
        })
    }

    /// Resolve a relative path to a full S3 key
    fn resolve_key(&self, path: &str) -> String {
        if self.prefix.is_empty() {
            path.to_string()
        } else if path.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), path)
        }
    }

    /// List all files under the given prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        let full_prefix = self.resolve_key(prefix);
        let mut entries = Vec::new();

        let mut paginator = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .into_paginator()
            .send();

        while let Some(page) = paginator.next().await {
            let output = page.map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

            for obj in output.contents() {
                if let Some(key) = obj.key() {
                    // Safely strip prefix, handling empty prefix case
                    let relative = if self.prefix.is_empty() {
                        key.to_string()
                    } else {
                        let prefix_with_slash = format!("{}/", self.prefix.trim_end_matches('/'));
                        key.strip_prefix(&prefix_with_slash)
                            .or_else(|| {
                                // Also try without trailing slash for exact prefix match
                                key.strip_prefix(&self.prefix)
                            })
                            .unwrap_or(key)
                            .to_string()
                    };

                    entries.push(FileEntry {
                        path: relative.into(),
                        size: obj.size().unwrap_or(0) as u64,
                        mtime: obj.last_modified().and_then(|t| {
                            std::time::UNIX_EPOCH
                                .checked_add(std::time::Duration::from_secs(t.secs() as u64))
                        }),
                        is_dir: key.ends_with('/'),
                        mode: None,
                        etag: obj.e_tag().map(|s| s.trim_matches('"').to_string()),
                    });
                }
            }
        }

        Ok(entries)
    }

    /// Get file metadata (head request)
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        let key = self.resolve_key(path);

        match self.client.head_object().bucket(&self.bucket).key(&key).send().await {
            Ok(output) => {
                let entry = FileEntry {
                    path: path.into(),
                    size: output.content_length().unwrap_or(0) as u64,
                    mtime: output.last_modified().and_then(|t| {
                        std::time::UNIX_EPOCH
                            .checked_add(std::time::Duration::from_secs(t.secs() as u64))
                    }),
                    is_dir: false,
                    mode: output
                        .metadata()
                        .and_then(|m| m.get("mode"))
                        .and_then(|s| s.parse().ok()),
                    etag: output.e_tag().map(|s| s.trim_matches('"').to_string()),
                };
                Ok(Some(entry))
            }
            Err(e) => {
                if e.to_string().contains("NotFound") || e.to_string().contains("404") {
                    Ok(None)
                } else {
                    Err(Error::Aws {
                        message: e.to_string(),
                    })
                }
            }
        }
    }

    /// Read a file's contents (loads entire file into memory)
    ///
    /// WARNING: For large files, use `get_stream()` instead.
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let key = self.resolve_key(path);

        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?
            .into_bytes();

        Ok(bytes)
    }

    /// Read a file as a stream of chunks (memory-efficient for large files)
    pub async fn get_stream(&self, path: &str) -> Result<ByteStream> {
        use tokio_util::io::ReaderStream;

        let key = self.resolve_key(path);

        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        // Convert AWS ByteStream to AsyncRead, then wrap in ReaderStream
        // This avoids buffering the entire file into memory
        let async_read = output.body.into_async_read();
        let stream = ReaderStream::new(async_read).map(|result| {
            result
                .map(|bytes| bytes.into())
                .map_err(|e| Error::Aws {
                    message: format!("Error reading S3 stream: {}", e),
                })
        });

        Ok(Box::pin(stream))
    }

    /// Read a range of bytes from a file
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let key = self.resolve_key(path);

        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .range(format!("bytes={}-{}", start, end))
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?
            .into_bytes();

        Ok(bytes)
    }

    /// Write a file's contents
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let key = self.resolve_key(path);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(AwsByteStream::from(data))
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
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
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB minimum for S3

        let upload_id = self.create_multipart_upload(path).await?;
        let mut parts = Vec::new();
        let mut part_number = 1;
        let mut buffer = Vec::new();

        while let Some(chunk) = stream.next().await {
            let data = chunk?;
            buffer.extend_from_slice(&data);

            // Upload when buffer reaches minimum part size
            if buffer.len() >= MIN_PART_SIZE {
                let part = self
                    .upload_part(path, &upload_id, part_number, Bytes::from(std::mem::take(&mut buffer)))
                    .await?;
                parts.push(part);
                part_number += 1;
            }
        }

        // Upload remaining data
        if !buffer.is_empty() {
            let part = self
                .upload_part(path, &upload_id, part_number, Bytes::from(buffer))
                .await?;
            parts.push(part);
        }

        self.complete_multipart_upload(path, &upload_id, parts).await
    }

    /// Delete a file
    pub async fn delete(&self, path: &str) -> Result<()> {
        let key = self.resolve_key(path);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Start a multipart upload
    pub async fn create_multipart_upload(&self, path: &str) -> Result<String> {
        let key = self.resolve_key(path);

        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        output
            .upload_id()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::MultipartUpload {
                message: "no upload ID returned".to_string(),
                upload_id: None,
            })
    }

    /// Upload a part to an ongoing multipart upload
    pub async fn upload_part(
        &self,
        path: &str,
        upload_id: &str,
        part_number: i32,
        data: Bytes,
    ) -> Result<CompletedPart> {
        let key = self.resolve_key(path);

        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(AwsByteStream::from(data))
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        let etag = output
            .e_tag()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::MultipartUpload {
                message: "no ETag returned for part".to_string(),
                upload_id: Some(upload_id.to_string()),
            })?;

        Ok(CompletedPart { part_number, etag })
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
        let key = self.resolve_key(path);
        let source_key = self.resolve_key(source_path);
        let copy_source = format!("{}/{}", self.bucket, source_key);

        let output = self
            .client
            .upload_part_copy()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .part_number(part_number)
            .copy_source(&copy_source)
            .copy_source_range(format!("bytes={}-{}", source_start, source_end))
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        let etag = output
            .copy_part_result()
            .and_then(|r| r.e_tag())
            .map(|s| s.to_string())
            .ok_or_else(|| Error::MultipartUpload {
                message: "no ETag returned for copied part".to_string(),
                upload_id: Some(upload_id.to_string()),
            })?;

        Ok(CompletedPart { part_number, etag })
    }

    /// Complete a multipart upload
    pub async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<()> {
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart as AwsCompletedPart};

        let key = self.resolve_key(path);

        let aws_parts: Vec<AwsCompletedPart> = parts
            .into_iter()
            .map(|p| {
                AwsCompletedPart::builder()
                    .part_number(p.part_number)
                    .e_tag(p.etag)
                    .build()
            })
            .collect();

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(aws_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Abort a multipart upload
    pub async fn abort_multipart_upload(&self, path: &str, upload_id: &str) -> Result<()> {
        let key = self.resolve_key(path);

        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|e| Error::Aws {
                message: e.to_string(),
            })?;

        Ok(())
    }
}
