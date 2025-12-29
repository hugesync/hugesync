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
use crate::signature::{read_signature_from_bytes, write_signature_to_bytes};
use crate::storage::StorageBackend;
use bytes::Bytes;
use std::path::Path;

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

    // Step 3: Read local file and compute delta using OPTIMIZED rolling algorithm
    let local_data = std::fs::read(local_path)
        .map_err(|e| Error::io("reading local file", e))?;
    
    // Use the O(N) rolling checksum algorithm
    let delta = compute_delta_rolling(std::io::Cursor::new(&local_data), &remote_sig)?;

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
                let completed = storage.upload_part(
                    remote_path,
                    &upload_id,
                    part_number,
                    Bytes::from(data),
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
async fn full_upload(
    local_path: &Path,
    remote_path: &str,
    storage: &StorageBackend,
    config: &Config,
) -> Result<DeltaUploadResult> {
    let data = std::fs::read(local_path)
        .map_err(|e| Error::io("reading local file", e))?;
    let size = data.len() as u64;

    // Upload file
    storage.put(remote_path, Bytes::from(data.clone())).await?;

    // Generate and upload signature if file is large enough
    if !config.no_sidecar && size >= config.delta_threshold {
        let sig = crate::signature::generate::generate_signature_from_bytes(&data, config.block_size);
        let sig_data = write_signature_to_bytes(&sig)?;
        let sig_path = format!("{}.hssig", remote_path);
        storage.put(&sig_path, Bytes::from(sig_data)).await?;
    }

    Ok(DeltaUploadResult {
        bytes_transferred: size,
        bytes_reused: 0,
        parts_uploaded: 1,
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
}
