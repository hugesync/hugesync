//! Signature generation and parsing for delta sync

pub mod file;
pub mod generate;

pub use file::{read_signature, read_signature_from_bytes, write_signature, write_signature_to_bytes};
pub use generate::{generate_signature, generate_signature_from_bytes};

use serde::{Deserialize, Serialize};

/// Magic bytes for .hssig files
pub const SIGNATURE_MAGIC: &[u8; 6] = b"HSSIG\x01";

/// Current signature format version
pub const SIGNATURE_VERSION: u8 = 1;

/// A file signature containing block checksums
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    /// Version of the signature format
    pub version: u8,

    /// Block size used for chunking
    pub block_size: usize,

    /// Total file size
    pub file_size: u64,

    /// ETag or hash of the source file (for staleness detection)
    pub source_etag: Option<String>,

    /// Individual block checksums
    pub blocks: Vec<BlockChecksum>,
}

impl Signature {
    /// Create a new empty signature
    pub fn new(block_size: usize, file_size: u64) -> Self {
        Self {
            version: SIGNATURE_VERSION,
            block_size,
            file_size,
            source_etag: None,
            blocks: Vec::new(),
        }
    }

    /// Get the number of blocks
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Check if this signature is stale compared to a new etag
    pub fn is_stale(&self, new_etag: &str) -> bool {
        match &self.source_etag {
            Some(etag) => etag != new_etag,
            None => true,
        }
    }
}

/// Checksum for a single block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockChecksum {
    /// Block index (0-based)
    pub index: usize,

    /// Offset in the file
    pub offset: u64,

    /// Actual size of this block (may be less than block_size for last block)
    pub size: usize,

    /// Rolling checksum (fast, weak hash for initial matching)
    pub rolling: u32,

    /// Strong hash (BLAKE3 for verification)
    pub strong: [u8; 32],
}

impl BlockChecksum {
    /// Create a new block checksum
    pub fn new(index: usize, offset: u64, size: usize, rolling: u32, strong: [u8; 32]) -> Self {
        Self {
            index,
            offset,
            size,
            rolling,
            strong,
        }
    }
}
