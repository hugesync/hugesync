//! HugeSync - A Cloud-Era Delta Synchronization Tool
//!
//! This library provides efficient delta synchronization between local filesystems
//! and cloud object storage (S3, GCS, Azure).

pub mod cli;
pub mod config;
pub mod delta;
pub mod error;
pub mod format;
pub mod progress;
pub mod resume;
pub mod retry;
pub mod signature;
pub mod storage;
pub mod sync;
pub mod types;
pub mod uri;

pub use config::Config;
pub use error::{Error, Result};
pub use types::*;

