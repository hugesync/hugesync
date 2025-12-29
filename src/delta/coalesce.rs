//! Coalesce delta operations for S3 compatibility

use super::{Delta, DeltaOp};
use crate::config::S3_MIN_PART_SIZE;

/// Coalesce delta operations to meet S3's minimum part size requirements
/// 
/// S3 requires multipart upload parts to be at least 5MB (except the last part).
/// This function merges small operations together to meet this requirement.
pub fn coalesce_operations(delta: &Delta, min_part_size: u64) -> Vec<CoalescedOp> {
    if delta.operations.is_empty() {
        return Vec::new();
    }

    let mut result: Vec<CoalescedOp> = Vec::new();
    let mut current: Option<CoalescedOp> = None;

    for op in &delta.operations {
        match op {
            DeltaOp::Copy { source_offset, length } => {
                match &mut current {
                    None => {
                        current = Some(CoalescedOp::Copy {
                            source_offset: *source_offset,
                            length: *length,
                        });
                    }
                    Some(CoalescedOp::Copy { source_offset: cur_offset, length: cur_len }) => {
                        // Check if this copy is contiguous with the previous
                        if *cur_offset + *cur_len == *source_offset {
                            *cur_len += *length;
                        } else {
                            // Not contiguous, finalize current and start new
                            result.push(current.take().unwrap());
                            current = Some(CoalescedOp::Copy {
                                source_offset: *source_offset,
                                length: *length,
                            });
                        }
                    }
                    Some(other) => {
                        result.push(current.take().unwrap());
                        current = Some(CoalescedOp::Copy {
                            source_offset: *source_offset,
                            length: *length,
                        });
                    }
                }
            }
            DeltaOp::Insert { data } => {
                match &mut current {
                    None => {
                        current = Some(CoalescedOp::Insert {
                            data: data.clone(),
                        });
                    }
                    Some(CoalescedOp::Insert { data: cur_data }) => {
                        // Merge consecutive inserts
                        cur_data.extend_from_slice(data);
                    }
                    Some(other) => {
                        result.push(current.take().unwrap());
                        current = Some(CoalescedOp::Insert {
                            data: data.clone(),
                        });
                    }
                }
            }
        }
    }

    // Don't forget the last one
    if let Some(op) = current {
        result.push(op);
    }

    // Now ensure minimum part sizes
    ensure_min_part_size(&mut result, min_part_size);

    result
}

/// Ensure all parts meet minimum size by padding with adjacent data
fn ensure_min_part_size(ops: &mut Vec<CoalescedOp>, min_size: u64) {
    // For now, we'll mark small parts that need padding
    // In actual S3 upload, we'll need to read extra data to meet the minimum
    
    // This is a simplified version - in production, we'd need to:
    // 1. Identify parts < min_size
    // 2. Expand them by reading more source data
    // 3. Or merge with adjacent parts
    
    // For now, just merge very small consecutive copies
    let mut i = 0;
    while i < ops.len() {
        let should_merge = if let CoalescedOp::Copy { source_offset, length } = &ops[i] {
            if *length < min_size && i + 1 < ops.len() {
                if let CoalescedOp::Copy { source_offset: next_offset, length: next_len } = &ops[i + 1] {
                    *source_offset + *length == *next_offset
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if should_merge {
            if let CoalescedOp::Copy { length: next_len, .. } = ops[i + 1] {
                if let CoalescedOp::Copy { length, .. } = &mut ops[i] {
                    *length += next_len;
                }
                ops.remove(i + 1);
                continue;
            }
        }
        i += 1;
    }
}

/// A coalesced operation ready for multipart upload
#[derive(Debug, Clone)]
pub enum CoalescedOp {
    /// Copy a range from the source object
    Copy {
        source_offset: u64,
        length: u64,
    },
    /// Upload new data
    Insert {
        data: Vec<u8>,
    },
}

impl CoalescedOp {
    /// Get the length of this operation
    pub fn length(&self) -> u64 {
        match self {
            CoalescedOp::Copy { length, .. } => *length,
            CoalescedOp::Insert { data } => data.len() as u64,
        }
    }

    /// Check if this operation meets minimum part size
    pub fn meets_min_size(&self, min_size: u64) -> bool {
        self.length() >= min_size
    }

    /// Check if this is a copy operation
    pub fn is_copy(&self) -> bool {
        matches!(self, CoalescedOp::Copy { .. })
    }
}

/// Split a coalesced operation into S3-compatible parts
pub fn split_into_parts(ops: &[CoalescedOp], max_part_size: u64) -> Vec<PartSpec> {
    let mut parts = Vec::new();
    let mut part_number = 1;

    for op in ops {
        match op {
            CoalescedOp::Copy { source_offset, length } => {
                let mut remaining = *length;
                let mut offset = *source_offset;

                while remaining > 0 {
                    let part_len = std::cmp::min(remaining, max_part_size);
                    parts.push(PartSpec {
                        part_number,
                        op: CoalescedOp::Copy {
                            source_offset: offset,
                            length: part_len,
                        },
                    });
                    part_number += 1;
                    offset += part_len;
                    remaining -= part_len;
                }
            }
            CoalescedOp::Insert { data } => {
                for chunk in data.chunks(max_part_size as usize) {
                    parts.push(PartSpec {
                        part_number,
                        op: CoalescedOp::Insert {
                            data: chunk.to_vec(),
                        },
                    });
                    part_number += 1;
                }
            }
        }
    }

    parts
}

/// A part specification for multipart upload
#[derive(Debug, Clone)]
pub struct PartSpec {
    /// S3 part number (1-indexed)
    pub part_number: i32,
    /// The operation for this part
    pub op: CoalescedOp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coalesce_contiguous_copies() {
        let mut delta = Delta::new(100);
        delta.add_copy(0, 10);
        delta.add_copy(10, 20);
        delta.add_copy(30, 30);

        let coalesced = coalesce_operations(&delta, S3_MIN_PART_SIZE);
        
        // Should merge into one copy
        assert_eq!(coalesced.len(), 1);
        if let CoalescedOp::Copy { length, .. } = &coalesced[0] {
            assert_eq!(*length, 60);
        }
    }

    #[test]
    fn test_coalesce_mixed_operations() {
        let mut delta = Delta::new(100);
        delta.add_copy(0, 20);
        delta.add_insert(vec![1, 2, 3]);
        delta.add_insert(vec![4, 5, 6]);
        delta.add_copy(50, 30);

        let coalesced = coalesce_operations(&delta, 1);
        
        // Copy, merged Insert, Copy
        assert_eq!(coalesced.len(), 3);
        if let CoalescedOp::Insert { data } = &coalesced[1] {
            assert_eq!(data.len(), 6);
        }
    }

    #[test]
    fn test_split_into_parts() {
        let ops = vec![
            CoalescedOp::Copy { source_offset: 0, length: 25 * 1024 * 1024 },
        ];

        let parts = split_into_parts(&ops, 10 * 1024 * 1024);
        assert_eq!(parts.len(), 3); // 10 + 10 + 5
    }
}
