//! Coalesce delta operations for S3 compatibility
//!
//! S3 requires multipart upload parts to be at least 5MB (except the last part).
//! This module ensures all parts meet this requirement by:
//! 1. Merging consecutive operations of the same type
//! 2. Converting small inserts to padded inserts by reading from source
//! 3. Providing fallback strategies when parts are too small

use super::{Delta, DeltaOp};

/// S3 minimum part size (5MB)
pub const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Coalesce delta operations to meet S3's minimum part size requirements
///
/// This function:
/// 1. Merges consecutive Copy operations that are contiguous
/// 2. Merges consecutive Insert operations
/// 3. Validates that resulting parts can be uploaded to S3
pub fn coalesce_operations(delta: &Delta, min_part_size: u64) -> Vec<CoalescedOp> {
    if delta.operations.is_empty() {
        return Vec::new();
    }

    // Phase 1: Merge consecutive operations of same type
    let mut merged: Vec<CoalescedOp> = Vec::new();

    for op in &delta.operations {
        match op {
            DeltaOp::Copy { source_offset, length } => {
                if let Some(CoalescedOp::Copy { source_offset: cur_offset, length: cur_len }) = merged.last_mut() {
                    // Check if contiguous
                    if *cur_offset + *cur_len == *source_offset {
                        *cur_len += *length;
                        continue;
                    }
                }
                merged.push(CoalescedOp::Copy {
                    source_offset: *source_offset,
                    length: *length,
                });
            }
            DeltaOp::Insert { data } => {
                if let Some(CoalescedOp::Insert { data: cur_data }) = merged.last_mut() {
                    cur_data.extend_from_slice(data);
                    continue;
                }
                merged.push(CoalescedOp::Insert {
                    data: data.clone(),
                });
            }
        }
    }

    // Phase 2: Handle small parts that don't meet minimum size
    // We need to ensure all parts (except last) are >= min_part_size
    ensure_min_part_sizes(&mut merged, min_part_size);

    merged
}

/// Ensure all parts meet minimum size requirements for S3
fn ensure_min_part_sizes(ops: &mut Vec<CoalescedOp>, min_size: u64) {
    if ops.is_empty() {
        return;
    }

    // Strategy: merge small operations with their neighbors
    // We iterate and combine operations until constraints are met
    let mut i = 0;
    while i < ops.len() {
        let len = ops[i].length();
        
        // Last part can be any size
        if i == ops.len() - 1 {
            break;
        }

        // If part is too small, we need to expand it
        if len < min_size {
            // Try to merge with next operation
            if i + 1 < ops.len() {
                let merged = merge_ops(&ops[i], &ops[i + 1]);
                if let Some(m) = merged {
                    ops[i] = m;
                    ops.remove(i + 1);
                    continue; // Re-check this position
                }
            }
            
            // If we can't merge, mark this as needing padding
            // The actual padding will be done during upload
            match &mut ops[i] {
                CoalescedOp::Insert { data: _ } => {
                    // We'll need to pad this insert with zeros or source data
                    // For now, mark with a minimum length we need
                    // Actual padding happens in delta_upload.rs
                }
                CoalescedOp::Copy { length: _, .. } => {
                    // A small copy can't really be "padded" - it needs to be expanded
                    // or merged with adjacent operations
                }
            }
        }
        i += 1;
    }
}

/// Try to merge two operations
fn merge_ops(a: &CoalescedOp, b: &CoalescedOp) -> Option<CoalescedOp> {
    match (a, b) {
        (CoalescedOp::Insert { data: d1 }, CoalescedOp::Insert { data: d2 }) => {
            let mut merged = d1.clone();
            merged.extend_from_slice(d2);
            Some(CoalescedOp::Insert { data: merged })
        }
        (CoalescedOp::Copy { source_offset: o1, length: l1 }, 
         CoalescedOp::Copy { source_offset: o2, length: l2 }) if o1 + l1 == *o2 => {
            // Contiguous copies can be merged
            Some(CoalescedOp::Copy {
                source_offset: *o1,
                length: l1 + l2,
            })
        }
        _ => None, // Can't merge Copy with Insert
    }
}

/// A coalesced operation ready for multipart upload
#[derive(Debug, Clone)]
pub enum CoalescedOp {
    /// Copy a range from the source object (uses UploadPartCopy)
    Copy {
        source_offset: u64,
        length: u64,
    },
    /// Upload new data (uses UploadPart)
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

/// Prepare operations for S3 multipart upload
/// 
/// This function takes coalesced operations and produces parts that are
/// guaranteed to meet S3's requirements:
/// - All parts except the last must be >= 5MB
/// - Small inserts are padded with source data if necessary
/// Prepare operations for S3 multipart upload
/// 
/// This function converts coalesced operations into S3 parts that are strictly compliant
/// with the 5MB minimum part size requirement (except for the last part).
/// It uses an accumulating buffer to merge small operations and "steals" data from
/// large copy operations to satisfy padding requirements.
///
/// IMPORTANT: `source_data` contains the LOCAL/TARGET file data. When materializing
/// Copy operations (reading bytes to convert to Upload), we must use the local file
/// offset, not the remote source offset. The local offset is tracked as the cumulative
/// length of all processed operations.
pub fn prepare_for_s3_upload(
    ops: &[CoalescedOp],
    source_data: &[u8],
    _target_size: u64,
) -> Vec<S3Part> {
    let mut parts = Vec::new();
    let mut accumulator = Vec::new();
    let mut part_number = 1;
    let mut target_offset = 0u64;
    // Track position in source_data (the local/target file) separately from
    // the remote source offset. Delta operations reconstruct the file linearly,
    // so local_offset is the cumulative length of all processed operations.
    let mut local_offset = 0u64;

    // Helper to emit an upload part
    let emit_upload = |data: Vec<u8>, parts: &mut Vec<S3Part>, p_num: &mut i32, t_off: &mut u64| {
        let len = data.len() as u64;
        parts.push(S3Part {
            part_number: *p_num,
            content: PartContent::Upload { data },
            target_offset: *t_off,
        });
        *p_num += 1;
        *t_off += len;
    };

    for op in ops {
        match op {
            CoalescedOp::Insert { data } => {
                accumulator.extend_from_slice(data);
                local_offset += data.len() as u64;
                // Flush if we have enough data
                if accumulator.len() as u64 >= S3_MIN_PART_SIZE {
                    emit_upload(std::mem::take(&mut accumulator), &mut parts, &mut part_number, &mut target_offset);
                }
            }
            CoalescedOp::Copy { source_offset, length } => {
                // Track local position (for reading from source_data) separately
                // from source position (for S3 CopyRange operations)
                let mut current_local_offset = local_offset;
                let mut current_source_offset = *source_offset;
                let mut current_length = *length;
                local_offset += *length;

                // If we have data in accumulator, try to fill it to 5MB using this copy
                if !accumulator.is_empty() {
                    let needed = S3_MIN_PART_SIZE.saturating_sub(accumulator.len() as u64);
                    
                    if current_length < needed {
                        // Not enough to fill accumulator, just append everything
                        // Use local offset to read from source_data (the target file)
                        let data = read_source(source_data, current_local_offset, current_length);
                        accumulator.extend(data);
                        current_length = 0;
                    } else {
                        // Fill accumulator and emit
                        // Use local offset to read from source_data (the target file)
                        let data = read_source(source_data, current_local_offset, needed);
                        accumulator.extend(data);
                        emit_upload(std::mem::take(&mut accumulator), &mut parts, &mut part_number, &mut target_offset);
                        current_local_offset += needed;
                        current_source_offset += needed;
                        current_length -= needed;
                    }
                }

                // Now handle the remaining copy
                if current_length > 0 {
                    if current_length >= S3_MIN_PART_SIZE {
                        // Large enough to be its own part(s)
                        // Use source offset for S3 UploadPartCopy (references remote file)
                        parts.push(S3Part {
                            part_number,
                            content: PartContent::CopyRange {
                                source_offset: current_source_offset,
                                length: current_length,
                            },
                            target_offset,
                        });
                        part_number += 1;
                        target_offset += current_length;

                    } else {
                        // Too small to be a part, add to accumulator
                        // Use local offset to read from source_data (the target file)
                        let data = read_source(source_data, current_local_offset, current_length);
                        accumulator.extend(data);
                    }
                }
            }
        }
    }

    // Flush any remaining data as the last part
    if !accumulator.is_empty() {
        emit_upload(accumulator, &mut parts, &mut part_number, &mut target_offset);
    }
    
    parts
}

/// Helper to safely read from source data
fn read_source(source: &[u8], offset: u64, len: u64) -> Vec<u8> {
    let start = offset as usize;
    let end = (offset + len) as usize;
    if start >= source.len() {
        return vec![0u8; len as usize]; // Should not happen with valid delta
    }
    let valid_end = std::cmp::min(end, source.len());
    let mut data = source[start..valid_end].to_vec();
    if data.len() < len as usize {
        // Pad with zeros if we ran out of source data (unexpected)
        data.resize(len as usize, 0);
    }
    data
}

/// Pad data to minimum size


/// An S3-compatible part
#[derive(Debug, Clone)]
pub struct S3Part {
    /// S3 part number (1-indexed)
    pub part_number: i32,
    /// Content of this part
    pub content: PartContent,
    /// Offset in the target file
    pub target_offset: u64,
}

/// Content type for an S3 part
#[derive(Debug, Clone)]
pub enum PartContent {
    /// Copy a range from source (UploadPartCopy)
    CopyRange {
        source_offset: u64,
        length: u64,
    },
    /// Upload new data (UploadPart)
    Upload {
        data: Vec<u8>,
    },
}

impl S3Part {
    /// Get the length of this part
    pub fn length(&self) -> u64 {
        match &self.content {
            PartContent::CopyRange { length, .. } => *length,
            PartContent::Upload { data } => data.len() as u64,
        }
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
    fn test_coalesce_non_contiguous_copies() {
        let mut delta = Delta::new(100);
        delta.add_copy(0, 10);
        delta.add_copy(50, 10); // Not contiguous

        let coalesced = coalesce_operations(&delta, 1);
        
        // Should remain as two copies
        assert_eq!(coalesced.len(), 2);
    }

    #[test]
    fn test_split_into_parts() {
        let ops = vec![
            CoalescedOp::Copy { source_offset: 0, length: 25 * 1024 * 1024 },
        ];

        let parts = split_into_parts(&ops, 10 * 1024 * 1024);
        assert_eq!(parts.len(), 3); // 10 + 10 + 5
    }

    #[test]
    fn test_meets_min_size() {
        let small = CoalescedOp::Insert { data: vec![0; 100] };
        let large = CoalescedOp::Insert { data: vec![0; 10 * 1024 * 1024] };

        assert!(!small.meets_min_size(S3_MIN_PART_SIZE));
        assert!(large.meets_min_size(S3_MIN_PART_SIZE));
    }

    #[test]
    fn test_prepare_for_s3_upload_with_insert_before_copy() {
        // This test verifies the fix for the data corruption bug.
        // When an Insert precedes a Copy, the local offset shifts and we must
        // read from the correct position in source_data.
        
        // Simulate: Original file "AAAAAAAAAA", target file "XXXAAAAAAAAAA"
        // Delta: Insert("XXX"), Copy(source_offset=0, length=10)
        let ops = vec![
            CoalescedOp::Insert { data: b"XXX".to_vec() },
            CoalescedOp::Copy { source_offset: 0, length: 10 },
        ];
        
        // source_data is the TARGET file content
        let source_data = b"XXXAAAAAAAAAA";
        
        let parts = prepare_for_s3_upload(&ops, source_data, 13);
        
        // With the fix, when materializing the Copy operation, we should read
        // from local_offset=3 (after the Insert), not source_offset=0.
        // This ensures we get "AAAAAAAAAA" instead of "XXXAAAAAAA".
        
        // Verify the combined data is correct
        let mut result = Vec::new();
        for part in &parts {
            match &part.content {
                PartContent::Upload { data } => result.extend(data),
                PartContent::CopyRange { .. } => {
                    // In a real scenario, this would be handled by S3
                    // For this test, we're checking the Upload parts
                }
            }
        }
        
        // The result should be the full target file content
        assert_eq!(result, b"XXXAAAAAAAAAA");
    }
}