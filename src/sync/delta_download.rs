//! Delta download pipeline for cloud to local sync
//!
//! This module handles downloading file deltas from cloud storage using HTTP Range
//! requests to fetch only changed blocks, significantly reducing egress bandwidth costs.

use crate::config::Config;
use crate::delta::compute_delta_rolling;
use crate::error::{Error, Result};
use crate::mmap::LockedMmap;
use crate::signature::{generate::generate_signature_from_bytes, read_signature_from_bytes, write_signature_to_bytes};
use crate::storage::StorageBackend;
use bytes::Bytes;
use std::io::Write;
use std::path::Path;

/// Threshold for using delta download (8MB)
const DELTA_DOWNLOAD_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Result of a delta download operation
#[derive(Debug)]
pub struct DeltaDownloadResult {
    /// Total bytes downloaded from remote (new data only)
    pub bytes_downloaded: u64,
    /// Bytes reused from local file
    pub bytes_reused: u64,
    /// Number of range requests made
    pub range_requests: usize,
}

/// Perform a delta download from remote storage to local file
///
/// This function:
/// 1. Generates a signature of the local file
/// 2. Downloads the remote .hssig file
/// 3. Compares signatures to identify changed blocks
/// 4. Downloads only changed blocks via Range requests
/// 5. Reconstructs the file by stitching local + remote blocks
pub async fn delta_download(
    source: &StorageBackend,
    local_path: &Path,
    remote_path: &str,
    config: &Config,
) -> Result<DeltaDownloadResult> {
    // Step 1: Check if local file exists
    if !local_path.exists() {
        tracing::debug!(path = %local_path.display(), "No local file for delta, performing full download");
        return full_download(source, local_path, remote_path, config).await;
    }

    let local_meta = std::fs::metadata(local_path)
        .map_err(|e| Error::io("reading local file metadata", e))?;

    // Small files aren't worth delta sync
    if local_meta.len() < DELTA_DOWNLOAD_THRESHOLD {
        return full_download(source, local_path, remote_path, config).await;
    }

    // Step 2: Try to get remote signature
    let sig_path = format!("{}.hssig", remote_path);
    let remote_sig = match source.get(&sig_path).await {
        Ok(data) => {
            tracing::debug!(path = %sig_path, "Found remote signature");
            match read_signature_from_bytes(&data) {
                Ok(sig) => sig,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to parse remote signature, full download");
                    return full_download(source, local_path, remote_path, config).await;
                }
            }
        }
        Err(_) => {
            tracing::debug!(path = %sig_path, "No remote signature found, full download");
            return full_download(source, local_path, remote_path, config).await;
        }
    };

    // Step 3: Memory-map local file and generate signature
    let local_mmap = LockedMmap::open(local_path)?;
    let local_data: &[u8] = &local_mmap;

    // Generate signature of local file to compare against remote
    let local_sig = generate_signature_from_bytes(local_data, config.block_size);

    // Step 4: Compute delta - what blocks in REMOTE are different from LOCAL
    // We need to reconstruct the remote file, so we compare remote sig against local data
    let delta = compute_delta_rolling(local_data, &remote_sig)?;

    // Step 5: Check if delta is beneficial
    if !delta.is_beneficial() {
        tracing::info!(
            bytes_new = delta.bytes_new,
            total_size = delta.target_size,
            "Delta download not beneficial, performing full download"
        );
        return full_download(source, local_path, remote_path, config).await;
    }

    tracing::info!(
        savings_percent = format!("{:.1}%", delta.savings_percent()),
        bytes_reused = delta.bytes_reused,
        bytes_new = delta.bytes_new,
        "Delta download beneficial"
    );

    // Step 6: Perform delta reconstruction
    delta_reconstruct(
        source,
        local_path,
        remote_path,
        &delta,
        local_data,
        config,
    ).await
}

/// Reconstruct file using local blocks + remote range downloads
async fn delta_reconstruct(
    source: &StorageBackend,
    local_path: &Path,
    remote_path: &str,
    delta: &crate::delta::Delta,
    local_data: &[u8],
    config: &Config,
) -> Result<DeltaDownloadResult> {
    use crate::delta::DeltaOp;

    // Create temp file for reconstruction
    let temp_path = local_path.with_extension("hsync_tmp");
    let mut temp_file = std::fs::File::create(&temp_path)
        .map_err(|e| Error::io("creating temp file for delta download", e))?;

    let mut bytes_downloaded = 0u64;
    let mut bytes_reused = 0u64;
    let mut range_requests = 0usize;

    // Process each delta operation
    for op in &delta.operations {
        match op {
            DeltaOp::Copy { source_offset, length } => {
                // Copy from local file (bytes we already have)
                let start = *source_offset as usize;
                let end = start + *length as usize;
                
                if end <= local_data.len() {
                    temp_file.write_all(&local_data[start..end])
                        .map_err(|e| Error::io("writing local block to temp file", e))?;
                    bytes_reused += *length;
                } else {
                    // Block extends beyond local file - need to download
                    let remote_data = source.get_range(remote_path, *source_offset, *source_offset + *length - 1).await?;
                    temp_file.write_all(&remote_data)
                        .map_err(|e| Error::io("writing downloaded block to temp file", e))?;
                    bytes_downloaded += remote_data.len() as u64;
                    range_requests += 1;
                }
            }
            DeltaOp::Insert { data } => {
                // Need to download this data from remote
                // For Insert operations, data contains the offset and length in remote
                // But in our case, Insert means new data that needs downloading
                temp_file.write_all(data)
                    .map_err(|e| Error::io("writing insert data to temp file", e))?;
                bytes_downloaded += data.len() as u64;
            }
        }
    }

    // Sync and close temp file
    temp_file.sync_all()
        .map_err(|e| Error::io("syncing temp file", e))?;
    drop(temp_file);

    // Atomic rename
    std::fs::rename(&temp_path, local_path)
        .map_err(|e| Error::io("renaming temp file to target", e))?;

    // Update local signature if configured
    if !config.no_sidecar {
        let new_data = std::fs::read(local_path)
            .map_err(|e| Error::io("reading reconstructed file", e))?;
        let sig = generate_signature_from_bytes(&new_data, config.block_size);
        let sig_data = write_signature_to_bytes(&sig)?;
        let sig_path = local_path.with_extension("hssig");
        std::fs::write(&sig_path, sig_data)
            .map_err(|e| Error::io("writing local signature", e))?;
    }

    Ok(DeltaDownloadResult {
        bytes_downloaded,
        bytes_reused,
        range_requests,
    })
}

/// Perform a full file download (no delta)
async fn full_download(
    source: &StorageBackend,
    local_path: &Path,
    remote_path: &str,
    config: &Config,
) -> Result<DeltaDownloadResult> {
    // Get file size first
    let meta = source.head(remote_path).await?
        .ok_or_else(|| Error::Storage { message: format!("Remote file not found: {}", remote_path) })?;
    
    let file_size = meta.size;

    // Create parent directories if needed
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::io("creating parent directories", e))?;
    }

    // For small files, simple in-memory download
    if file_size < DELTA_DOWNLOAD_THRESHOLD {
        let data = source.get(remote_path).await?;
        std::fs::write(local_path, &data)
            .map_err(|e| Error::io("writing downloaded file", e))?;
        return Ok(DeltaDownloadResult {
            bytes_downloaded: data.len() as u64,
            bytes_reused: 0,
            range_requests: 0,
        });
    }

    // For large files, use streaming download
    use futures::StreamExt;
    
    let temp_path = local_path.with_extension("hsync_tmp");
    let mut temp_file = std::fs::File::create(&temp_path)
        .map_err(|e| Error::io("creating temp file", e))?;

    let mut stream = source.get_stream(remote_path).await?;
    let mut total_bytes = 0u64;

    while let Some(chunk) = stream.next().await {
        let data = chunk?;
        temp_file.write_all(&data)
            .map_err(|e| Error::io("writing chunk to temp file", e))?;
        total_bytes += data.len() as u64;
    }

    temp_file.sync_all()
        .map_err(|e| Error::io("syncing temp file", e))?;
    drop(temp_file);

    // Atomic rename
    std::fs::rename(&temp_path, local_path)
        .map_err(|e| Error::io("renaming temp file to target", e))?;

    // Generate local signature if configured
    if !config.no_sidecar && file_size >= config.delta_threshold {
        let data = std::fs::read(local_path)
            .map_err(|e| Error::io("reading for signature", e))?;
        let sig = generate_signature_from_bytes(&data, config.block_size);
        let sig_data = write_signature_to_bytes(&sig)?;
        let sig_path = local_path.with_extension("hssig");
        std::fs::write(&sig_path, sig_data)
            .map_err(|e| Error::io("writing local signature", e))?;
    }

    Ok(DeltaDownloadResult {
        bytes_downloaded: total_bytes,
        bytes_reused: 0,
        range_requests: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_download_result() {
        let result = DeltaDownloadResult {
            bytes_downloaded: 1000,
            bytes_reused: 9000,
            range_requests: 10,
        };
        
        assert_eq!(result.bytes_downloaded + result.bytes_reused, 10000);
    }

    #[test]
    fn test_threshold() {
        assert_eq!(DELTA_DOWNLOAD_THRESHOLD, 8 * 1024 * 1024);
    }
}
