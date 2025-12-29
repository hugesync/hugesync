//! Conflict resolution for sync operations

use crate::error::{Error, Result};
use crate::storage::StorageBackend;
use crate::types::FileEntry;

/// Conflict detection result
#[derive(Debug, Clone)]
pub enum ConflictResult {
    /// No conflict, safe to proceed
    NoConflict,
    /// Remote file was modified since we scanned it
    RemoteModified {
        expected_etag: String,
        actual_etag: String,
    },
    /// Remote file was deleted
    RemoteDeleted,
    /// Remote file was created (we expected it not to exist)
    RemoteCreated,
}

/// Check for conflicts before uploading
pub async fn check_conflict(
    storage: &StorageBackend,
    path: &str,
    expected: Option<&FileEntry>,
) -> Result<ConflictResult> {
    let current = storage.head(path).await?;

    match (expected, current) {
        // Expected nothing, nothing exists -> no conflict
        (None, None) => Ok(ConflictResult::NoConflict),

        // Expected nothing, but file exists -> conflict
        (None, Some(_)) => Ok(ConflictResult::RemoteCreated),

        // Expected file, but it's gone -> conflict
        (Some(_), None) => Ok(ConflictResult::RemoteDeleted),

        // Expected file exists, check ETag
        (Some(expected), Some(actual)) => {
            match (&expected.etag, &actual.etag) {
                (Some(expected_etag), Some(actual_etag)) if expected_etag != actual_etag => {
                    Ok(ConflictResult::RemoteModified {
                        expected_etag: expected_etag.clone(),
                        actual_etag: actual_etag.clone(),
                    })
                }
                _ => {
                    // No ETag or ETags match, check size and mtime
                    if expected.size != actual.size {
                        Ok(ConflictResult::RemoteModified {
                            expected_etag: format!("size:{}", expected.size),
                            actual_etag: format!("size:{}", actual.size),
                        })
                    } else {
                        Ok(ConflictResult::NoConflict)
                    }
                }
            }
        }
    }
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictStrategy {
    /// Fail immediately on any conflict
    Fail,
    /// Overwrite remote regardless of conflicts
    Overwrite,
    /// Skip conflicting files
    Skip,
    /// Retry with fresh scan
    Retry,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::Retry
    }
}

/// Handle a detected conflict according to strategy
pub fn handle_conflict(
    conflict: &ConflictResult,
    strategy: ConflictStrategy,
    path: &str,
) -> Result<ConflictAction> {
    match conflict {
        ConflictResult::NoConflict => Ok(ConflictAction::Proceed),

        ConflictResult::RemoteModified { expected_etag, actual_etag } => {
            tracing::warn!(
                path = %path,
                expected = %expected_etag,
                actual = %actual_etag,
                "Remote file was modified"
            );

            match strategy {
                ConflictStrategy::Fail => Err(Error::Conflict {
                    path: path.to_string(),
                    message: format!(
                        "Remote file was modified (expected {}, got {})",
                        expected_etag, actual_etag
                    ),
                }),
                ConflictStrategy::Overwrite => Ok(ConflictAction::Proceed),
                ConflictStrategy::Skip => Ok(ConflictAction::Skip),
                ConflictStrategy::Retry => Ok(ConflictAction::Retry),
            }
        }

        ConflictResult::RemoteDeleted => {
            tracing::warn!(path = %path, "Remote file was deleted");

            match strategy {
                ConflictStrategy::Fail => Err(Error::Conflict {
                    path: path.to_string(),
                    message: "Remote file was deleted during sync".to_string(),
                }),
                ConflictStrategy::Overwrite => Ok(ConflictAction::Proceed),
                ConflictStrategy::Skip => Ok(ConflictAction::Skip),
                ConflictStrategy::Retry => Ok(ConflictAction::Retry),
            }
        }

        ConflictResult::RemoteCreated => {
            tracing::warn!(path = %path, "Remote file was created during sync");

            match strategy {
                ConflictStrategy::Fail => Err(Error::Conflict {
                    path: path.to_string(),
                    message: "Remote file was created during sync".to_string(),
                }),
                ConflictStrategy::Overwrite => Ok(ConflictAction::Proceed),
                ConflictStrategy::Skip => Ok(ConflictAction::Skip),
                ConflictStrategy::Retry => Ok(ConflictAction::Retry),
            }
        }
    }
}

/// Action to take after conflict detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictAction {
    /// Proceed with the operation
    Proceed,
    /// Skip this file
    Skip,
    /// Retry with a fresh scan
    Retry,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict_strategy_default() {
        assert_eq!(ConflictStrategy::default(), ConflictStrategy::Retry);
    }

    #[test]
    fn test_handle_no_conflict() {
        let result = handle_conflict(
            &ConflictResult::NoConflict,
            ConflictStrategy::Fail,
            "test.txt",
        );
        assert!(matches!(result, Ok(ConflictAction::Proceed)));
    }

    #[test]
    fn test_handle_conflict_fail() {
        let conflict = ConflictResult::RemoteModified {
            expected_etag: "abc".to_string(),
            actual_etag: "def".to_string(),
        };
        let result = handle_conflict(&conflict, ConflictStrategy::Fail, "test.txt");
        assert!(result.is_err());
    }

    #[test]
    fn test_handle_conflict_skip() {
        let conflict = ConflictResult::RemoteModified {
            expected_etag: "abc".to_string(),
            actual_etag: "def".to_string(),
        };
        let result = handle_conflict(&conflict, ConflictStrategy::Skip, "test.txt");
        assert!(matches!(result, Ok(ConflictAction::Skip)));
    }
}
