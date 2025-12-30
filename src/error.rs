//! Error types for HugeSync

use std::path::PathBuf;
use thiserror::Error;

/// Result type alias for HugeSync operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for HugeSync
#[derive(Error, Debug)]
pub enum Error {
    /// I/O errors (file system operations)
    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    /// Network errors (HTTP, connection issues)
    #[error("Network error: {message}")]
    Network {
        message: String,
        #[source]
        source: Option<reqwest::Error>,
    },

    /// Storage backend errors (S3, GCS, Azure)
    #[error("Storage error: {message}")]
    Storage { message: String },

    /// Configuration errors
    #[error("Configuration error: {message}")]
    Config { message: String },

    /// Delta/signature computation errors
    #[error("Delta error: {message}")]
    Delta { message: String },

    /// Conflict detected (remote file changed during sync)
    #[error("Conflict at {path}: {message}")]
    Conflict {
        path: String,
        message: String,
    },

    /// Operation was cancelled
    #[error("Operation cancelled")]
    Cancelled,

    /// Invalid URI format
    #[error("Invalid URI: {uri} - {reason}")]
    InvalidUri { uri: String, reason: String },

    /// File not found
    #[error("File not found: {path}")]
    NotFound { path: PathBuf },

    /// Permission denied
    #[error("Permission denied: {path}")]
    PermissionDenied { path: PathBuf },

    /// Multipart upload error
    #[error("Multipart upload error: {message}")]
    MultipartUpload { message: String, upload_id: Option<String> },

    /// AWS SDK error
    #[error("AWS error: {message}")]
    Aws { message: String },

    /// GCS error
    #[error("GCS error: {message}")]
    Gcs { message: String },

    /// Azure error
    #[error("Azure error: {message}")]
    Azure { message: String },

    /// SSH error
    #[error("SSH error: {message}")]
    Ssh { message: String },
}

impl Error {
    /// Create an I/O error with context
    pub fn io(message: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            message: message.into(),
            source,
        }
    }

    /// Create a storage error
    pub fn storage(message: impl Into<String>) -> Self {
        Self::Storage {
            message: message.into(),
        }
    }

    /// Create a config error
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Network { .. } | Error::MultipartUpload { .. }
        )
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            message: err.to_string(),
            source: err,
        }
    }
}

impl From<toml::de::Error> for Error {
    fn from(err: toml::de::Error) -> Self {
        Self::Config {
            message: format!("TOML parse error: {}", err),
        }
    }
}

