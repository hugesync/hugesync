//! Delta upload pipeline for S3
//!
//! This module handles the complex task of uploading file deltas to S3 using
//! multipart uploads with UploadPartCopy. The key challenge is meeting S3's
//! requirement that all parts (except the last) must be >= 5MB.

use crate::config::Config;
use crate::delta::compute_delta_rolling;
use crate::delta::Delta;
use crate::error::{Error, Result};
use crate::signature::{read_signature_from_bytes, write_signature_to_bytes};
use crate::storage::StorageBackend;
use bytes::Bytes;
use std::path::Path;

/// Minimum size for a part in S3 multipart upload (5MB)
const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

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
    
    // Use the O(N) rolling checksum algorithm, not the naive O(N*block_size) one
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

/// An S3-compatible upload part that guarantees minimum size requirements
#[derive(Debug, Clone)]
enum S3Part {
    /// Upload new data (uses UploadPart)
    Upload { data: Vec<u8> },
    /// Copy from source (uses UploadPartCopy) - guaranteed >= 5MB or is last part
    Copy { source_start: u64, source_end: u64 },
}

/// Convert delta operations into S3-compatible parts
/// 
/// This is the critical function that ensures all parts meet S3's 5MB minimum.
/// Strategy: Convert everything to upload data (from local_data) since we have it.
/// For Insert ops, use the data directly.
/// For Copy ops, read from local_data at the corresponding target offset.
fn build_s3_parts(delta: &Delta, local_data: &[u8]) -> Vec<S3Part> {
    // Simple strategy: Just upload the entire local data in 5MB chunks
    // This always works and meets S3 requirements.
    // For large files where delta would help, we check if there are large 
    // contiguous copy sections we can use.
    
    let total_size = local_data.len() as u64;
    if total_size == 0 {
        return Vec::new();
    }
    
    // Check if we have large copy sections worth using
    let mut large_copies: Vec<(u64, u64, u64)> = Vec::new(); // (target_offset, source_offset, length)
    let mut target_offset = 0u64;
    
    use crate::delta::DeltaOp;
    for op in &delta.operations {
        if let DeltaOp::Copy { source_offset, length } = op {
            if *length >= S3_MIN_PART_SIZE {
                large_copies.push((target_offset, *source_offset, *length));
            }
        }
        target_offset += op.length();
    }
    
    // If no large copy sections, just upload the whole thing in chunks
    if large_copies.is_empty() {
        return chunk_as_uploads(local_data);
    }
    
    // Build parts using copy operations for large sections, uploads for the rest
    let mut parts: Vec<S3Part> = Vec::new();
    let mut current_pos = 0u64;
    
    for (copy_target_offset, copy_source_offset, copy_length) in large_copies {
        // Upload any data before this copy
        if current_pos < copy_target_offset {
            let start = current_pos as usize;
            let end = copy_target_offset as usize;
            parts.extend(chunk_as_uploads(&local_data[start..end]));
        }
        
        // Add the copy operation (might need to split if > 5GB)
        let max_copy_size = 5 * 1024 * 1024 * 1024u64; // 5GB max per copy
        let mut remaining = copy_length;
        let mut offset = copy_source_offset;
        
        while remaining > 0 {
            let chunk_size = std::cmp::min(remaining, max_copy_size);
            parts.push(S3Part::Copy {
                source_start: offset,
                source_end: offset + chunk_size - 1,
            });
            offset += chunk_size;
            remaining -= chunk_size;
        }
        
        current_pos = copy_target_offset + copy_length;
    }
    
    // Upload any remaining data after last copy
    if current_pos < total_size {
        let start = current_pos as usize;
        parts.extend(chunk_as_uploads(&local_data[start..]));
    }
    
    // Ensure all non-last parts meet minimum size
    ensure_min_sizes(&mut parts, local_data);
    
    parts
}

/// Convert data into upload chunks of at least S3_MIN_PART_SIZE (except last)
fn chunk_as_uploads(data: &[u8]) -> Vec<S3Part> {
    if data.is_empty() {
        return Vec::new();
    }
    
    let chunk_size = S3_MIN_PART_SIZE as usize;
    let mut parts = Vec::new();
    
    for chunk in data.chunks(chunk_size) {
        parts.push(S3Part::Upload { data: chunk.to_vec() });
    }
    
    parts
}

/// Ensure all non-last parts meet minimum size by merging with neighbors
fn ensure_min_sizes(parts: &mut Vec<S3Part>, local_data: &[u8]) {
    if parts.len() <= 1 {
        return;
    }
    
    // Merge any small parts by converting to uploads and combining
    let mut i = 0;
    while i < parts.len() - 1 { // Don't check last part
        let part_size = match &parts[i] {
            S3Part::Upload { data } => data.len() as u64,
            S3Part::Copy { source_start, source_end } => source_end - source_start + 1,
        };
        
        if part_size < S3_MIN_PART_SIZE {
            // Convert this part and next to uploads, then merge
            let mut merged_data = Vec::new();
            
            // Get data from current part
            match &parts[i] {
                S3Part::Upload { data } => merged_data.extend_from_slice(data),
                S3Part::Copy { source_start, source_end } => {
                    let start = *source_start as usize;
                    let end = std::cmp::min(*source_end as usize + 1, local_data.len());
                    if start < local_data.len() {
                        merged_data.extend_from_slice(&local_data[start..end]);
                    }
                }
            }
            
            // Get data from next part  
            if i + 1 < parts.len() {
                match &parts[i + 1] {
                    S3Part::Upload { data } => merged_data.extend_from_slice(data),
                    S3Part::Copy { source_start, source_end } => {
                        let start = *source_start as usize;
                        let end = std::cmp::min(*source_end as usize + 1, local_data.len());
                        if start < local_data.len() {
                            merged_data.extend_from_slice(&local_data[start..end]);
                        }
                    }
                }
                parts.remove(i + 1);
            }
            
            parts[i] = S3Part::Upload { data: merged_data };
            // Don't increment i, check merged part again
        } else {
            i += 1;
        }
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
    // Build S3-compatible parts that guarantee 5MB minimum
    let parts = build_s3_parts(delta, local_data);
    
    if parts.is_empty() {
        return Err(Error::Delta { 
            message: "No parts to upload".to_string() 
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
    for (idx, part) in parts.iter().enumerate() {
        let part_number = (idx + 1) as i32;

        match part {
            S3Part::Copy { source_start, source_end } => {
                let length = source_end - source_start + 1;
                tracing::debug!(
                    part = part_number,
                    source_start = source_start,
                    source_end = source_end,
                    length = length,
                    "Copying part from source"
                );
                
                let completed = storage.upload_part_copy(
                    remote_path,
                    &upload_id,
                    part_number,
                    remote_path,
                    *source_start,
                    *source_end,
                ).await?;

                completed_parts.push(completed);
                bytes_reused += length;
                parts_copied += 1;
            }
            S3Part::Upload { data } => {
                tracing::debug!(
                    part = part_number,
                    size = data.len(),
                    "Uploading new data"
                );
                
                let completed = storage.upload_part(
                    remote_path,
                    &upload_id,
                    part_number,
                    Bytes::from(data.clone()),
                ).await?;

                completed_parts.push(completed);
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

    #[test]
    fn test_chunk_as_uploads() {
        let data = vec![0u8; 12 * 1024 * 1024]; // 12MB
        let parts = chunk_as_uploads(&data);
        
        // Should be 3 parts: 5MB, 5MB, 2MB
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_chunk_as_uploads_small() {
        let data = vec![0u8; 1024]; // 1KB
        let parts = chunk_as_uploads(&data);
        
        // Should be 1 part
        assert_eq!(parts.len(), 1);
    }
}
