//! Sync plan execution

use super::plan::SyncPlan;
use crate::config::Config;
use crate::error::Result;
use crate::progress::ProgressTracker;
use crate::storage::StorageBackend;
use crate::types::{SyncAction, SyncStats};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Threshold for using streaming vs in-memory transfer (8MB)
const STREAMING_THRESHOLD: u64 = 8 * 1024 * 1024;

/// Execute a sync plan
pub async fn execute_plan(
    plan: &SyncPlan,
    source: &StorageBackend,
    dest: &StorageBackend,
    config: &Config,
    progress: &ProgressTracker,
) -> Result<SyncStats> {
    let mut stats = SyncStats::default();
    let semaphore = Arc::new(Semaphore::new(config.effective_jobs()));

    for action in &plan.actions {
        let path_str = action.entry.path.to_string_lossy().to_string();

        match action.action {
            SyncAction::Skip => {
                stats.files_skipped += 1;
                tracing::debug!(path = %path_str, "Skipping unchanged file");
            }

            SyncAction::Mkdir => {
                tracing::debug!(path = %path_str, "Creating directory");
                // Most cloud backends don't need explicit directory creation
            }

            SyncAction::Upload => {
                let _permit = semaphore.acquire().await.unwrap();
                let file_size = action.entry.size;

                progress.start_file(&path_str, file_size);
                tracing::info!(path = %path_str, size = file_size, "Uploading");

                match upload_file(source, dest, &path_str, file_size, progress).await {
                    Ok(bytes) => {
                        stats.files_uploaded += 1;
                        stats.bytes_transferred += bytes;
                        progress.finish_file();
                    }
                    Err(e) => {
                        tracing::error!(path = %path_str, error = %e, "Upload failed");
                        stats.errors += 1;
                    }
                }
            }

            SyncAction::Download => {
                let _permit = semaphore.acquire().await.unwrap();
                let file_size = action.entry.size;

                progress.start_file(&path_str, file_size);
                tracing::info!(path = %path_str, size = file_size, "Downloading");

                match download_file(source, dest, &path_str, file_size, progress).await {
                    Ok(bytes) => {
                        stats.files_downloaded += 1;
                        stats.bytes_transferred += bytes;
                        progress.finish_file();
                    }
                    Err(e) => {
                        tracing::error!(path = %path_str, error = %e, "Download failed");
                        stats.errors += 1;
                    }
                }
            }

            SyncAction::Delta => {
                let _permit = semaphore.acquire().await.unwrap();
                
                progress.start_file(&path_str, action.estimated_bytes);
                tracing::info!(
                    path = %path_str,
                    size = action.entry.size,
                    estimated = action.estimated_bytes,
                    "Delta sync"
                );

                // Get local path for delta upload
                let local_path = match source {
                    StorageBackend::Local(backend) => {
                        backend.root().join(&path_str)
                    }
                    _ => {
                        // For cloud-to-cloud, fall back to full upload
                        tracing::warn!("Delta sync only supported from local source, falling back to full upload");
                        match upload_file(source, dest, &path_str, action.entry.size, progress).await {
                            Ok(bytes) => {
                                stats.files_delta += 1;
                                stats.bytes_transferred += bytes;
                                progress.finish_file();
                            }
                            Err(e) => {
                                tracing::error!(path = %path_str, error = %e, "Upload failed");
                                stats.errors += 1;
                            }
                        }
                        continue;
                    }
                };

                // Perform delta upload
                match super::delta_upload::delta_upload(&local_path, &path_str, dest, config).await {
                    Ok(result) => {
                        stats.files_delta += 1;
                        stats.bytes_transferred += result.bytes_transferred;
                        stats.bytes_saved += result.bytes_reused;
                        tracing::info!(
                            path = %path_str,
                            transferred = result.bytes_transferred,
                            reused = result.bytes_reused,
                            parts_uploaded = result.parts_uploaded,
                            parts_copied = result.parts_copied,
                            "Delta sync complete"
                        );
                        progress.finish_file();
                    }
                    Err(e) => {
                        tracing::error!(path = %path_str, error = %e, "Delta sync failed, falling back to full upload");
                        // Fall back to full upload on delta failure
                        match upload_file(source, dest, &path_str, action.entry.size, progress).await {
                            Ok(bytes) => {
                                stats.files_uploaded += 1;
                                stats.bytes_transferred += bytes;
                                progress.finish_file();
                            }
                            Err(e2) => {
                                tracing::error!(path = %path_str, error = %e2, "Full upload also failed");
                                stats.errors += 1;
                            }
                        }
                    }
                }
            }

            SyncAction::Delete => {
                tracing::info!(path = %path_str, "Deleting");

                match dest.delete(&path_str).await {
                    Ok(()) => {
                        stats.files_deleted += 1;
                        progress.finish_file();
                    }
                    Err(e) => {
                        tracing::error!(path = %path_str, error = %e, "Delete failed");
                        stats.errors += 1;
                    }
                }
            }
        }

        progress.update_stats(&stats);
    }

    Ok(stats)
}

/// Upload a single file from source to destination using streaming
///
/// For local source files, uses memory-mapped streaming upload.
/// For cloud sources, streams chunks directly to destination without
/// loading the entire file into memory.
async fn upload_file(
    source: &StorageBackend,
    dest: &StorageBackend,
    path: &str,
    size: u64,
    progress: &ProgressTracker,
) -> Result<u64> {
    // For small files, use simple in-memory transfer
    if size < STREAMING_THRESHOLD {
        let data = source.get(path).await?;
        let actual_size = data.len() as u64;
        progress.update_file_progress(actual_size / 2);
        dest.put(path, data).await?;
        progress.update_file_progress(actual_size);
        return Ok(actual_size);
    }

    // For local source, use optimized put_file which uses mmap
    if let StorageBackend::Local(local) = source {
        let local_path = local.root().join(path);
        progress.update_file_progress(size / 3);
        dest.put_file(path, &local_path).await?;
        progress.update_file_progress(size);
        return Ok(size);
    }

    // For cloud-to-cloud, stream chunks from source to destination
    let stream = source.get_stream(path).await?;
    progress.update_file_progress(size / 3);
    dest.put_stream(path, stream, Some(size)).await?;
    progress.update_file_progress(size);

    Ok(size)
}

/// Download a single file from source to destination using streaming
///
/// Streams chunks from source to destination without loading
/// the entire file into memory.
async fn download_file(
    source: &StorageBackend,
    dest: &StorageBackend,
    path: &str,
    size: u64,
    progress: &ProgressTracker,
) -> Result<u64> {
    // For small files, use simple in-memory transfer
    if size < STREAMING_THRESHOLD {
        let data = source.get(path).await?;
        let actual_size = data.len() as u64;
        progress.update_file_progress(actual_size / 2);
        dest.put(path, data).await?;
        progress.update_file_progress(actual_size);
        return Ok(actual_size);
    }

    // For local destination from cloud, stream directly
    let stream = source.get_stream(path).await?;
    progress.update_file_progress(size / 3);
    dest.put_stream(path, stream, Some(size)).await?;
    progress.update_file_progress(size);

    Ok(size)
}
