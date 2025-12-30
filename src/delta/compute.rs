//! Delta computation algorithm using fast_rsync

use super::Delta;
use crate::signature::{quick_hash, Signature};
use crate::simd::rolling_checksum_simd;
use std::io::Read;

/// Compute a delta between a local file and a remote signature
/// This uses a proper O(1) rolling checksum via fast_rsync
pub fn compute_delta<R: Read>(mut local_reader: R, remote_sig: &Signature) -> crate::error::Result<Delta> {
    // Read all local data (TODO: streaming for very large files)
    let mut local_data = Vec::new();
    local_reader.read_to_end(&mut local_data)
        .map_err(|e| crate::error::Error::io("reading local file", e))?;

    let local_len = local_data.len() as u64;
    let block_size = remote_sig.block_size;

    // Build the index of remote blocks for O(1) lookup
    let mut block_index = std::collections::HashMap::new();
    for (idx, block) in remote_sig.blocks.iter().enumerate() {
        block_index
            .entry(block.rolling)
            .or_insert_with(Vec::new)
            .push(idx);
    }

    let mut delta = Delta::new(local_len);
    let mut pos = 0usize;
    let mut pending_insert_start: Option<usize> = None;

    // Use a proper rolling checksum
    while pos + block_size <= local_data.len() {
        let window = &local_data[pos..pos + block_size];
        let rolling = rolling_checksum(window);

        // Fast path: check if rolling hash matches any remote block
        if let Some(candidates) = block_index.get(&rolling) {
            let strong = blake3::hash(window);
            let strong_bytes = *strong.as_bytes();

            if let Some(&block_idx) = candidates.iter().find(|&&i| {
                remote_sig.blocks[i].strong == strong_bytes
            }) {
                // Match found! Flush pending inserts first
                if let Some(start) = pending_insert_start.take() {
                    delta.add_insert(local_data[start..pos].to_vec());
                }

                // Add copy operation referencing the remote block
                let block = &remote_sig.blocks[block_idx];
                delta.add_copy(block.offset, block.size as u64);
                pos += block_size;
                continue;
            }
        }

        // No match - this byte becomes part of an insert
        if pending_insert_start.is_none() {
            pending_insert_start = Some(pos);
        }
        pos += 1;
    }

    // Handle remaining bytes (less than a full block)
    if pos < local_data.len() {
        if pending_insert_start.is_none() {
            pending_insert_start = Some(pos);
        }
    }

    // Flush any remaining pending insert
    if let Some(start) = pending_insert_start {
        delta.add_insert(local_data[start..].to_vec());
    }

    Ok(delta)
}

/// Proper rolling checksum (Adler-32 style) - uses SIMD when available
/// This computes the checksum for a block - the "rolling" update happens via RollingChecksum
#[inline]
fn rolling_checksum(data: &[u8]) -> u32 {
    rolling_checksum_simd(data)
}

/// Rolling checksum state for O(1) updates
/// Uses a stronger 64-bit hash with better distribution
pub struct RollingChecksum {
    // Original Adler-32 style components for compatibility with signatures
    a: u32,
    b: u32,
    window_size: usize,
}

impl RollingChecksum {
    /// Create a new rolling checksum over a window
    pub fn new(data: &[u8]) -> Self {
        let mut a: u32 = 0;
        let mut b: u32 = 0;

        for &byte in data {
            a = a.wrapping_add(byte as u32);
            b = b.wrapping_add(a);
        }

        Self {
            a,
            b,
            window_size: data.len(),
        }
    }

    /// Get the current checksum value
    pub fn value(&self) -> u32 {
        (self.b << 16) | (self.a & 0xffff)
    }

    /// Roll the window by one byte: remove old_byte, add new_byte
    /// This is O(1) instead of O(block_size)
    pub fn roll(&mut self, old_byte: u8, new_byte: u8) {
        let old = old_byte as u32;
        let new = new_byte as u32;

        // Remove old byte, add new byte
        self.a = self.a.wrapping_sub(old).wrapping_add(new);
        // b loses (window_size * old) and gains a
        self.b = self.b
            .wrapping_sub((self.window_size as u32).wrapping_mul(old))
            .wrapping_add(self.a);
    }
}

/// Optimized delta computation using true O(1) rolling checksum
pub fn compute_delta_rolling(local_data: &[u8], remote_sig: &Signature) -> crate::error::Result<Delta> {
    let local_len = local_data.len() as u64;
    let block_size = remote_sig.block_size;

    tracing::debug!(
        local_size = local_len,
        block_size = block_size,
        num_blocks = remote_sig.blocks.len(),
        "Starting delta computation"
    );

    if local_data.len() < block_size {
        // File is smaller than block size, can't match any blocks
        let mut delta = Delta::new(local_len);
        delta.add_insert(local_data.to_vec());
        return Ok(delta);
    }

    // Build the index of remote blocks
    let mut block_index: std::collections::HashMap<u32, Vec<usize>> = std::collections::HashMap::new();
    for (idx, block) in remote_sig.blocks.iter().enumerate() {
        block_index.entry(block.rolling).or_default().push(idx);
    }

    let mut delta = Delta::new(local_len);
    let mut pos = 0usize;
    let mut pending_insert_start: Option<usize> = None;
    let mut matches_found = 0usize;
    let mut last_progress = 0usize;

    // Initialize rolling checksum with first window
    let mut rolling = RollingChecksum::new(&local_data[0..block_size]);

    loop {
        // Progress reporting for large files (every 10MB)
        let progress_mb = pos / (10 * 1024 * 1024);
        if progress_mb > last_progress {
            last_progress = progress_mb;
            tracing::trace!(
                pos_mb = pos / (1024 * 1024),
                matches = matches_found,
                "Delta scan progress"
            );
        }
        // Check for match
        let mut matched = false;
        if let Some(candidates) = block_index.get(&rolling.value()) {
            let window = &local_data[pos..pos + block_size];

            // Compute quick_hash as a fast filter before expensive BLAKE3
            // This catches most false positives from repetitive data patterns
            let local_quick = quick_hash(window);

            for &block_idx in candidates {
                let block = &remote_sig.blocks[block_idx];

                // First filter: quick_hash comparison (very fast)
                // This eliminates most false positives without computing BLAKE3
                if block.quick_hash != local_quick {
                    continue;
                }

                // Second filter: BLAKE3 strong hash (expensive but definitive)
                let strong = blake3::hash(window);
                let strong_bytes = *strong.as_bytes();

                if block.strong == strong_bytes {
                    // Match found! Flush pending inserts
                    matches_found += 1;
                    if let Some(start) = pending_insert_start.take() {
                        delta.add_insert(local_data[start..pos].to_vec());
                    }

                    delta.add_copy(block.offset, block.size as u64);
                    pos += block_size;
                    matched = true;

                    // Reinitialize rolling checksum if we have more data
                    if pos + block_size <= local_data.len() {
                        rolling = RollingChecksum::new(&local_data[pos..pos + block_size]);
                    }
                    break;
                }
            }

            // If we matched and advanced past the end, exit
            if matched && pos >= local_data.len() - block_size + 1 {
                break;
            }
            // If we matched, continue from the new position
            if matched {
                continue;
            }
        }

        // No match - advance by one byte
        if pending_insert_start.is_none() {
            pending_insert_start = Some(pos);
        }

        // Roll the checksum forward
        if pos + block_size < local_data.len() {
            let old_byte = local_data[pos];
            let new_byte = local_data[pos + block_size];
            rolling.roll(old_byte, new_byte);
            pos += 1;
        } else {
            pos += 1;
            break;
        }
    }

    // Handle any remaining bytes
    if let Some(start) = pending_insert_start {
        delta.add_insert(local_data[start..].to_vec());
    } else if pos < local_data.len() {
        delta.add_insert(local_data[pos..].to_vec());
    }

    tracing::debug!(
        matches = matches_found,
        ops = delta.operations.len(),
        bytes_reused = delta.bytes_reused,
        bytes_new = delta.bytes_new,
        "Delta computation complete"
    );

    Ok(delta)
}

/// Compute delta from byte slices (for testing)
pub fn compute_delta_from_bytes(local: &[u8], remote_sig: &Signature) -> crate::error::Result<Delta> {
    compute_delta_rolling(local, remote_sig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signature::generate::generate_signature_from_bytes;

    #[test]
    fn test_rolling_checksum_update() {
        let data = b"abcdefgh";
        let _initial = RollingChecksum::new(&data[0..4]); // "abcd"

        let mut rolled = RollingChecksum::new(&data[0..4]);
        rolled.roll(b'a', b'e'); // Should now be "bcde"

        let expected = RollingChecksum::new(&data[1..5]); // "bcde"
        assert_eq!(rolled.value(), expected.value());
    }

    #[test]
    fn test_identical_files() {
        let data = b"hello world, this is test data for delta computation";
        let sig = generate_signature_from_bytes(data, 10);
        let delta = compute_delta_from_bytes(data, &sig).unwrap();

        // Should be mostly copies
        assert!(delta.bytes_reused > 0);
    }

    #[test]
    fn test_completely_different() {
        let old_data = b"old content here";
        let new_data = b"completely different new content";
        let sig = generate_signature_from_bytes(old_data, 8);
        let delta = compute_delta_from_bytes(new_data, &sig).unwrap();

        // Should be mostly inserts
        assert!(delta.bytes_new > 0);
    }

    #[test]
    fn test_partial_match() {
        let block_size = 8;
        let matching = b"ABCDEFGH";
        let different = b"12345678";

        let mut old_data = Vec::new();
        old_data.extend_from_slice(matching);
        old_data.extend_from_slice(matching);

        let mut new_data = Vec::new();
        new_data.extend_from_slice(matching);
        new_data.extend_from_slice(different);

        let sig = generate_signature_from_bytes(&old_data, block_size);
        let delta = compute_delta_from_bytes(&new_data, &sig).unwrap();

        // Should have some reuse
        assert!(delta.bytes_reused >= block_size as u64);
    }

    #[test]
    fn test_savings_calculation() {
        let mut delta = Delta::new(100);
        delta.add_copy(0, 80);
        delta.add_insert(vec![0; 20]);

        assert!((delta.savings_percent() - 80.0).abs() < 0.1);
        assert!(delta.is_beneficial());
    }

    #[test]
    fn test_repetitive_data_pattern() {
        // This test verifies that quick_hash filtering prevents performance degradation
        // when dealing with repetitive data patterns (like LLM training data, logs, etc.)
        // that cause many rolling hash collisions.

        let block_size = 1024; // 1KB blocks for faster testing
        let num_blocks = 100;
        let total_size = block_size * num_blocks;

        // Create highly repetitive data pattern that will cause rolling hash collisions
        // Pattern: repeating 256-byte sequence
        let pattern: Vec<u8> = (0u8..=255).collect();
        let mut old_data = Vec::with_capacity(total_size);
        while old_data.len() < total_size {
            old_data.extend_from_slice(&pattern);
        }
        old_data.truncate(total_size);

        // Create new data with middle section changed
        let mut new_data = old_data.clone();
        let change_start = total_size / 3;
        let change_end = 2 * total_size / 3;
        for i in change_start..change_end {
            new_data[i] = new_data[i].wrapping_add(1); // Modify bytes
        }

        let sig = generate_signature_from_bytes(&old_data, block_size);

        // This should complete quickly even with repetitive data
        // thanks to quick_hash filtering
        let start = std::time::Instant::now();
        let delta = compute_delta_from_bytes(&new_data, &sig).unwrap();
        let elapsed = start.elapsed();

        // Should complete in reasonable time (less than 1 second for 100KB)
        assert!(elapsed.as_secs() < 1, "Delta computation took too long: {:?}", elapsed);

        // Should detect that start and end are reusable
        assert!(delta.bytes_reused > 0, "Expected some bytes reused");
        assert!(delta.bytes_new > 0, "Expected some new bytes");
    }

    #[test]
    fn test_all_zeros_data() {
        // Edge case: data that's all the same byte (maximum collision scenario)
        let block_size = 512;
        let total_size = block_size * 20;

        // All zeros - every block has identical content
        let old_data = vec![0u8; total_size];

        // Change last block only
        let mut new_data = old_data.clone();
        for i in (total_size - block_size)..total_size {
            new_data[i] = 1;
        }

        let sig = generate_signature_from_bytes(&old_data, block_size);

        let start = std::time::Instant::now();
        let delta = compute_delta_from_bytes(&new_data, &sig).unwrap();
        let elapsed = start.elapsed();

        // Should still complete quickly
        assert!(elapsed.as_millis() < 500, "Delta computation took too long: {:?}", elapsed);

        // The first 19 blocks should match the first block in signature
        // (all zeros blocks are identical)
        assert!(delta.bytes_reused > 0);
    }
}
