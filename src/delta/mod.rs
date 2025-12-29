//! Delta computation and types

pub mod compute;
pub mod coalesce;

pub use compute::compute_delta;
pub use compute::compute_delta_rolling;
pub use coalesce::coalesce_operations;

use serde::{Deserialize, Serialize};

/// A delta representing the differences between two versions of a file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta {
    /// Target file size after applying delta
    pub target_size: u64,

    /// Operations to reconstruct the target file
    pub operations: Vec<DeltaOp>,

    /// Bytes that can be copied from source (reused)
    pub bytes_reused: u64,

    /// Bytes that must be transferred (new data)
    pub bytes_new: u64,
}

impl Delta {
    /// Create a new empty delta
    pub fn new(target_size: u64) -> Self {
        Self {
            target_size,
            operations: Vec::new(),
            bytes_reused: 0,
            bytes_new: 0,
        }
    }

    /// Add a copy operation
    pub fn add_copy(&mut self, source_offset: u64, length: u64) {
        self.operations.push(DeltaOp::Copy {
            source_offset,
            length,
        });
        self.bytes_reused += length;
    }

    /// Add an insert operation
    pub fn add_insert(&mut self, data: Vec<u8>) {
        let len = data.len() as u64;
        self.operations.push(DeltaOp::Insert { data });
        self.bytes_new += len;
    }

    /// Calculate savings percentage
    pub fn savings_percent(&self) -> f64 {
        if self.target_size == 0 {
            0.0
        } else {
            (self.bytes_reused as f64 / self.target_size as f64) * 100.0
        }
    }

    /// Check if delta sync is beneficial (transfers less than full file)
    pub fn is_beneficial(&self) -> bool {
        self.bytes_new < self.target_size
    }

    /// Get total number of operations
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

/// A single delta operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOp {
    /// Copy bytes from the source file
    Copy {
        /// Offset in the source file
        source_offset: u64,
        /// Number of bytes to copy
        length: u64,
    },

    /// Insert new data
    Insert {
        /// The new data to insert
        data: Vec<u8>,
    },
}

impl DeltaOp {
    /// Get the length of this operation in bytes
    pub fn length(&self) -> u64 {
        match self {
            DeltaOp::Copy { length, .. } => *length,
            DeltaOp::Insert { data } => data.len() as u64,
        }
    }

    /// Check if this is a copy operation
    pub fn is_copy(&self) -> bool {
        matches!(self, DeltaOp::Copy { .. })
    }

    /// Check if this is an insert operation
    pub fn is_insert(&self) -> bool {
        matches!(self, DeltaOp::Insert { .. })
    }
}
