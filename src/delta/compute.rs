//! Delta computation algorithm

use super::{Delta, DeltaOp};
use crate::signature::{BlockChecksum, Signature};
use std::collections::HashMap;
use std::io::Read;

/// Compute a delta between a local file and a remote signature
pub fn compute_delta<R: Read>(mut local_reader: R, remote_sig: &Signature) -> crate::error::Result<Delta> {
    // Build a hash map of rolling checksums -> block indices
    let mut rolling_map: HashMap<u32, Vec<usize>> = HashMap::new();
    for (idx, block) in remote_sig.blocks.iter().enumerate() {
        rolling_map.entry(block.rolling).or_default().push(idx);
    }

    let block_size = remote_sig.block_size;
    let mut delta = Delta::new(0); // Will update target_size at end
    let mut buffer = vec![0u8; block_size * 2]; // Double buffer for rolling window
    let mut window_start = 0usize;
    let mut window_end = 0usize;
    let mut file_offset = 0u64;
    let mut pending_insert: Vec<u8> = Vec::new();

    // Read initial chunk
    let initial_read = local_reader.read(&mut buffer)
        .map_err(|e| crate::error::Error::io("reading local file", e))?;
    window_end = initial_read;

    while window_start < window_end {
        let available = window_end - window_start;

        if available >= block_size {
            // We have enough for a full block, check for match
            let window = &buffer[window_start..window_start + block_size];
            let rolling = rolling_checksum(window);

            if let Some(indices) = rolling_map.get(&rolling) {
                // Potential match, verify with strong hash
                let strong = blake3::hash(window);
                let strong_bytes = *strong.as_bytes();

                if let Some(block_idx) = indices.iter().find(|&&i| {
                    remote_sig.blocks[i].strong == strong_bytes
                }) {
                    // Match found! Flush pending insert first
                    if !pending_insert.is_empty() {
                        delta.add_insert(std::mem::take(&mut pending_insert));
                    }

                    // Add copy operation
                    let block = &remote_sig.blocks[*block_idx];
                    delta.add_copy(block.offset, block.size as u64);

                    // Move window past the matched block
                    file_offset += block_size as u64;
                    window_start += block_size;

                    // Refill buffer if needed
                    if window_start >= block_size && window_end < buffer.len() {
                        // Shift buffer
                        buffer.copy_within(window_start..window_end, 0);
                        window_end -= window_start;
                        window_start = 0;

                        // Read more data
                        let bytes_read = local_reader.read(&mut buffer[window_end..])
                            .map_err(|e| crate::error::Error::io("reading local file", e))?;
                        window_end += bytes_read;
                    }

                    continue;
                }
            }

            // No match, add byte to pending insert
            pending_insert.push(buffer[window_start]);
            window_start += 1;
            file_offset += 1;
        } else {
            // Not enough for a full block, these are all inserts
            pending_insert.extend_from_slice(&buffer[window_start..window_end]);
            file_offset += (window_end - window_start) as u64;
            break;
        }

        // Refill buffer if we've consumed a lot
        if window_start >= block_size && window_end < buffer.len() {
            buffer.copy_within(window_start..window_end, 0);
            window_end -= window_start;
            window_start = 0;

            let bytes_read = local_reader.read(&mut buffer[window_end..])
                .map_err(|e| crate::error::Error::io("reading local file", e))?;
            window_end += bytes_read;
        }
    }

    // Flush any remaining pending insert
    if !pending_insert.is_empty() {
        delta.add_insert(pending_insert);
    }

    delta.target_size = file_offset;

    Ok(delta)
}

/// Simple rolling checksum (same as in signature generation)
fn rolling_checksum(data: &[u8]) -> u32 {
    let mut a: u32 = 0;
    let mut b: u32 = 0;

    for &byte in data {
        a = a.wrapping_add(byte as u32);
        b = b.wrapping_add(a);
    }

    (b << 16) | (a & 0xffff)
}

/// Compute delta from byte slices (for testing)
pub fn compute_delta_from_bytes(local: &[u8], remote_sig: &Signature) -> crate::error::Result<Delta> {
    compute_delta(std::io::Cursor::new(local), remote_sig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signature::generate::generate_signature_from_bytes;

    #[test]
    fn test_identical_files() {
        let data = b"hello world, this is test data for delta computation";
        let sig = generate_signature_from_bytes(data, 10);
        let delta = compute_delta_from_bytes(data, &sig).unwrap();

        // Should be all copies, no inserts
        assert!(delta.bytes_new == 0 || delta.bytes_new < 10);
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
        // Create data where first half matches, second half is new
        let block_size = 8;
        let matching = b"ABCDEFGH"; // One block
        let different = b"12345678"; // One block

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
}
