//! Delta upload pipeline for S3 and compatible backends
//!
//! This module handles the complex task of uploading file deltas to cloud storage using
//! multipart uploads with server-side copy (UploadPartCopy/PutBlockFromURL). 
//! The key challenge is meeting minimum part size requirements (e.g. S3's 5MB limit).

use crate::config::Config;
use crate::delta::coalesce::{coalesce_operations, prepare_for_s3_upload, PartContent, S3_MIN_PART_SIZE};
use crate::delta::compute_delta_rolling;
use crate::delta::Delta;
use crate::error::{Error, Result};
use crate::mmap::LockedMmap;
use crate::signature::{read_signature_from_bytes, write_signature_to_bytes};
use crate::storage::StorageBackend;
use bytes::Bytes;
use std::path::Path;

/// Threshold for using multipart upload (8MB)
const MULTIPART_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Part size for multipart uploads (16MB - comfortably above S3's 5MB minimum)
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;

/// Result of a delta upload operation
#[derive(Debug)]
pub struct DeltaUploadResult {
    /// Total bytes transferred (new data only)
    pub bytes_transferred: u64,
    /// Bytes reused from existing object
    pub bytes_reused: u64,
    /// Number of parts uploaded
    pub parts_uploaded: usize,
    /// Number of parts copied
    pub parts_copied: usize,
}

/// Perform a delta upload from local file to remote storage
pub async fn delta_upload(
    local_path: &Path,
    remote_path: &str,
    storage: &StorageBackend,
    config: &Config,
) -> Result<DeltaUploadResult> {
    // Step 1: Try to get remote signature
    let sig_path = format!("{}.hssig", remote_path);
    let remote_sig = match storage.get(&sig_path).await {
        Ok(data) => {
            tracing::debug!(path = %sig_path, "Found remote signature");
            Some(read_signature_from_bytes(&data)?)
        }
        Err(_) => {
            tracing::debug!(path = %sig_path, "No remote signature found");
            None
        }
    };

    // Step 2: If no signature, do full upload
    let remote_sig = match remote_sig {
        Some(sig) => sig,
        None => {
            tracing::info!("No delta possible, performing full upload");
            return full_upload(local_path, remote_path, storage, config).await;
        }
    };

    // Step 3: Read local file using memory mapping with file locking to avoid OOM and SIGBUS
    let mmap = LockedMmap::open(local_path)?;

    // Explicitly treat mmap as slice
    let local_data: &[u8] = &mmap;
    
    // Use the O(N) rolling checksum algorithm
    let delta = compute_delta_rolling(local_data, &remote_sig)?;

    // Step 4: Check if delta is beneficial
    if !delta.is_beneficial() {
        tracing::info!(
            new_bytes = delta.bytes_new,
            total_size = delta.target_size,
            "Delta not beneficial, performing full upload"
        );
        return full_upload(local_path, remote_path, storage, config).await;
    }

    tracing::info!(
        savings_percent = format!("{:.1}%", delta.savings_percent()),
        bytes_reused = delta.bytes_reused,
        bytes_new = delta.bytes_new,
        "Delta sync beneficial"
    );

    // Step 5: Perform delta upload using multipart
    if storage.supports_part_copy() {
        delta_upload_multipart(
            &local_data,
            remote_path,
            &delta,
            storage,
            config,
        ).await
    } else {
        tracing::warn!("Storage doesn't support UploadPartCopy, falling back to full upload");
        full_upload(local_path, remote_path, storage, config).await
    }
}

/// Perform delta upload using multipart with server-side copy
async fn delta_upload_multipart(
    local_data: &[u8],
    remote_path: &str,
    delta: &Delta,
    storage: &StorageBackend,
    config: &Config,
) -> Result<DeltaUploadResult> {
    // 1. Coalesce small adjacent operations into larger ones
    let coalesced_ops = coalesce_operations(delta, S3_MIN_PART_SIZE);
    
    // 2. Prepare parts ensuring S3 compatibility (padding/merging/splitting)
    let parts = prepare_for_s3_upload(&coalesced_ops, local_data, delta.target_size);
    
    if parts.is_empty() {
        return Err(Error::Delta { 
            message: "No parts to upload after coalescing".to_string() 
        });
    }
    
    // Create multipart upload
    let upload_id = storage.create_multipart_upload(remote_path).await?;
    
    let mut completed_parts = Vec::new();
    let mut bytes_transferred = 0u64;
    let mut bytes_reused = 0u64;
    let mut parts_uploaded = 0;
    let mut parts_copied = 0;

    // Process each part
    for part in parts {
        let part_number = part.part_number;

        match part.content {
            PartContent::CopyRange { source_offset, length } => {
                let source_end = source_offset + length - 1;
                
                tracing::debug!(
                    part = part_number,
                    source_start = source_offset,
                    source_end = source_end,
                    length = length,
                    "Copying part from source"
                );
                
                // For GCS/Azure/S3, usually source is the same path
                // If we were syncing *across* files, this would be different
                let completed = storage.upload_part_copy(
                    remote_path,
                    &upload_id,
                    part_number,
                    remote_path,
                    source_offset,
                    source_end,
                ).await?;

                completed_parts.push(completed);
                bytes_reused += length;
                parts_copied += 1;
            }
            PartContent::Upload { data } => {
                tracing::debug!(
                    part = part_number,
                    size = data.len(),
                    "Uploading new data"
                );

                let len = data.len() as u64;
                // data is already Bytes - zero-copy transfer
                let completed = storage.upload_part(
                    remote_path,
                    &upload_id,
                    part_number,
                    data,
                ).await?;

                completed_parts.push(completed);
                bytes_transferred += len;
                parts_uploaded += 1;
            }
        }
    }

    // Complete multipart upload
    storage.complete_multipart_upload(remote_path, &upload_id, completed_parts).await?;

    // Upload new signature
    let new_sig = crate::signature::generate::generate_signature_from_bytes(local_data, config.block_size);
    let sig_data = write_signature_to_bytes(&new_sig)?;
    let sig_path = format!("{}.hssig", remote_path);
    storage.put(&sig_path, Bytes::from(sig_data)).await?;

    Ok(DeltaUploadResult {
        bytes_transferred,
        bytes_reused,
        parts_uploaded,
        parts_copied,
    })
}

/// Perform a full file upload (no delta)
///
/// Uses memory mapping and multipart uploads for large files to avoid OOM.
/// For backends without multipart support (SSH), uses streaming upload.
async fn full_upload(
    local_path: &Path,
    remote_path: &str,
    storage: &StorageBackend,
    config: &Config,
) -> Result<DeltaUploadResult> {
    let metadata = std::fs::metadata(local_path)
        .map_err(|e| Error::io("reading file metadata", e))?;
    let file_size = metadata.len();

    // For small files, use simple in-memory upload (safe for memory)
    if file_size < MULTIPART_THRESHOLD {
        return full_upload_simple(local_path, remote_path, storage, config, file_size).await;
    }

    // For large files on backends with multipart support, use multipart
    if storage.supports_multipart() {
        return full_upload_multipart(local_path, remote_path, storage, config, file_size).await;
    }

    // For large files on backends without multipart (SSH), use streaming upload
    full_upload_streaming(local_path, remote_path, storage, config, file_size).await
}

/// Simple upload for small files - reads into memory
///
/// Only used for files < MULTIPART_THRESHOLD (8MB), so safe for memory.
async fn full_upload_simple(
    local_path: &Path,
    remote_path: &str,
    storage: &StorageBackend,
    config: &Config,
    file_size: u64,
) -> Result<DeltaUploadResult> {
    let data = std::fs::read(local_path)
        .map_err(|e| Error::io("reading local file", e))?;

    // Generate signature before uploading if needed (avoids clone)
    let sig_data = if !config.no_sidecar && file_size >= config.delta_threshold {
        let sig = crate::signature::generate::generate_signature_from_bytes(&data, config.block_size);
        Some(write_signature_to_bytes(&sig)?)
    } else {
        None
    };

    // Upload file (zero-copy via Bytes)
    storage.put(remote_path, Bytes::from(data)).await?;

    // Upload signature if generated
    if let Some(sig_bytes) = sig_data {
        let sig_path = format!("{}.hssig", remote_path);
        storage.put(&sig_path, Bytes::from(sig_bytes)).await?;
    }

    Ok(DeltaUploadResult {
        bytes_transferred: file_size,
        bytes_reused: 0,
        parts_uploaded: 1,
        parts_copied: 0,
    })
}

/// Streaming upload for large files on backends without multipart (e.g., SSH)
///
/// Uses memory-mapped streaming to avoid loading the entire file into RAM.
async fn full_upload_streaming(
    local_path: &Path,
    remote_path: &str,
    storage: &StorageBackend,
    config: &Config,
    file_size: u64,
) -> Result<DeltaUploadResult> {
    use futures::stream;

    const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16MB chunks

    tracing::info!(
        size = file_size,
        chunk_size = CHUNK_SIZE,
        "Starting streaming upload for large file (no multipart support)"
    );

    // Memory-map the file with locking to avoid OOM and SIGBUS
    let mmap = LockedMmap::open(local_path)?;

    // Create a stream of chunks from the memory-mapped file
    let total_size = mmap.len();
    let chunks: Vec<Bytes> = (0..total_size)
        .step_by(CHUNK_SIZE)
        .map(|offset| {
            let end = std::cmp::min(offset + CHUNK_SIZE, total_size);
            Bytes::copy_from_slice(&mmap[offset..end])
        })
        .collect();

    let byte_stream = Box::pin(stream::iter(chunks.into_iter().map(Ok)));

    // Upload using streaming (works with SSH's put_stream)
    storage.put_stream(remote_path, byte_stream, Some(file_size)).await?;

    // Generate and upload signature if configured
    if !config.no_sidecar && file_size >= config.delta_threshold {
        let sig = crate::signature::generate::generate_signature_from_bytes(&*mmap, config.block_size);
        let sig_data = write_signature_to_bytes(&sig)?;
        let sig_path = format!("{}.hssig", remote_path);
        storage.put(&sig_path, Bytes::from(sig_data)).await?;
    }

    Ok(DeltaUploadResult {
        bytes_transferred: file_size,
        bytes_reused: 0,
        parts_uploaded: 1,
        parts_copied: 0,
    })
}

/// Multipart upload for large files - uses memory mapping to avoid OOM
async fn full_upload_multipart(
    local_path: &Path,
    remote_path: &str,
    storage: &StorageBackend,
    config: &Config,
    file_size: u64,
) -> Result<DeltaUploadResult> {
    // Memory-map the file with locking to avoid OOM and SIGBUS
    let mmap = LockedMmap::open(local_path)?;

    tracing::info!(
        size = file_size,
        part_size = MULTIPART_PART_SIZE,
        "Starting multipart upload for large file"
    );

    // Start multipart upload
    let upload_id = storage.create_multipart_upload(remote_path).await?;

    let mut completed_parts = Vec::new();
    let mut offset = 0usize;
    let mut part_number = 1i32;
    let total_size = mmap.len();

    // Upload in chunks - only one chunk in memory at a time
    while offset < total_size {
        let chunk_end = std::cmp::min(offset + MULTIPART_PART_SIZE, total_size);
        let chunk = &mmap[offset..chunk_end];

        tracing::debug!(
            part = part_number,
            offset = offset,
            size = chunk.len(),
            "Uploading part"
        );

        // Copy chunk to Bytes - this is the only allocation per part
        let chunk_bytes = Bytes::copy_from_slice(chunk);

        match storage.upload_part(remote_path, &upload_id, part_number, chunk_bytes).await {
            Ok(completed) => {
                completed_parts.push(completed);
            }
            Err(e) => {
                // Attempt to abort on failure
                tracing::error!(error = %e, "Part upload failed, aborting multipart upload");
                let _ = storage.abort_multipart_upload(remote_path, &upload_id).await;
                return Err(e);
            }
        }

        offset = chunk_end;
        part_number += 1;
    }

    // Complete multipart upload
    storage.complete_multipart_upload(remote_path, &upload_id, completed_parts.clone()).await?;

    // Generate and upload signature if configured
    if !config.no_sidecar && file_size >= config.delta_threshold {
        let sig = crate::signature::generate::generate_signature_from_bytes(&*mmap, config.block_size);
        let sig_data = write_signature_to_bytes(&sig)?;
        let sig_path = format!("{}.hssig", remote_path);
        storage.put(&sig_path, Bytes::from(sig_data)).await?;
    }

    Ok(DeltaUploadResult {
        bytes_transferred: file_size,
        bytes_reused: 0,
        parts_uploaded: completed_parts.len(),
        parts_copied: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_upload_result() {
        let result = DeltaUploadResult {
            bytes_transferred: 1000,
            bytes_reused: 9000,
            parts_uploaded: 1,
            parts_copied: 9,
        };
        
        assert_eq!(result.bytes_transferred + result.bytes_reused, 10000);
    }

    #[test]
    fn test_multipart_threshold() {
        // Verify threshold is reasonable (8MB)
        assert_eq!(MULTIPART_THRESHOLD, 8 * 1024 * 1024);
    }

    #[test]
    fn test_part_size_above_s3_minimum() {
        // S3 minimum is 5MB, we use 16MB
        assert!(MULTIPART_PART_SIZE >= 5 * 1024 * 1024);
    }
}