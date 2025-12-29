//! Delta upload pipeline for S3

use crate::config::Config;
use crate::delta::{coalesce_operations, compute_delta, Delta};
use crate::error::{Error, Result};
use crate::signature::{generate_signature, read_signature_from_bytes, write_signature_to_bytes};
use crate::storage::StorageBackend;
use bytes::Bytes;
use std::path::Path;

/// Minimum size for a part in S3 multipart upload (5MB)
const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum parts in a multipart upload (10,000)
const S3_MAX_PARTS: i32 = 10000;

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

    // Step 2: If no signature or signature is stale, do full upload
    let remote_sig = match remote_sig {
        Some(sig) => sig,
        None => {
            tracing::info!("No delta possible, performing full upload");
            return full_upload(local_path, remote_path, storage, config).await;
        }
    };

    // Step 3: Generate local signature and compute delta
    let _local_sig = generate_signature(local_path, config.block_size)?;
    
    let local_data = std::fs::read(local_path)
        .map_err(|e| Error::io("reading local file", e))?;
    
    let delta = compute_delta(std::io::Cursor::new(&local_data), &remote_sig)?;

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
        // Storage doesn't support UploadPartCopy, fall back to full upload
        tracing::warn!("Storage doesn't support UploadPartCopy, falling back to full upload");
        full_upload(local_path, remote_path, storage, config).await
    }
}

/// Perform delta upload using S3 multipart with UploadPartCopy
async fn delta_upload_multipart(
    local_data: &[u8],
    remote_path: &str,
    delta: &Delta,
    storage: &StorageBackend,
    config: &Config,
) -> Result<DeltaUploadResult> {
    // Coalesce operations to meet S3 requirements
    let ops = coalesce_operations(delta, S3_MIN_PART_SIZE);
    
    // Create multipart upload
    let upload_id = storage.create_multipart_upload(remote_path).await?;
    
    let mut completed_parts = Vec::new();
    let mut bytes_transferred = 0u64;
    let mut bytes_reused = 0u64;
    let mut parts_uploaded = 0;
    let mut parts_copied = 0;

    // Process each coalesced operation as a part
    for (idx, op) in ops.iter().enumerate() {
        let part_number = (idx + 1) as i32;

        match op {
            crate::delta::coalesce::CoalescedOp::Copy { source_offset, length } => {
                // Use UploadPartCopy to copy from existing object
                let source_end = *source_offset + *length - 1;
                
                let part = storage.upload_part_copy(
                    remote_path,
                    &upload_id,
                    part_number,
                    remote_path, // Copy from the same object
                    *source_offset,
                    source_end,
                ).await?;

                completed_parts.push(part);
                bytes_reused += *length;
                parts_copied += 1;
            }
            crate::delta::coalesce::CoalescedOp::Insert { data } => {
                // Upload new data
                let part = storage.upload_part(
                    remote_path,
                    &upload_id,
                    part_number,
                    Bytes::from(data.clone()),
                ).await?;

                completed_parts.push(part);
                bytes_transferred += data.len() as u64;
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
