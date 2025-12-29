//! Signature generation using rolling checksums and BLAKE3

use super::{BlockChecksum, Signature};
use crate::error::{Error, Result};
use memmap2::Mmap;
use rayon::prelude::*;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Rolling checksum implementation (Adler32-like)
fn rolling_checksum(data: &[u8]) -> u32 {
    let mut a: u32 = 0;
    let mut b: u32 = 0;

    for &byte in data {
        a = a.wrapping_add(byte as u32);
        b = b.wrapping_add(a);
    }

    (b << 16) | (a & 0xffff)
}

/// Generate a signature for a file
pub fn generate_signature(path: &Path, block_size: usize) -> Result<Signature> {
    let file = File::open(path).map_err(|e| Error::io("opening file", e))?;
    let metadata = file.metadata().map_err(|e| Error::io("reading metadata", e))?;
    let file_size = metadata.len();

    if file_size == 0 {
        return Ok(Signature::new(block_size, 0));
    }

    // Use memory mapping for large files
    if file_size > 10 * 1024 * 1024 {
        // > 10MB
        generate_signature_mmap(path, block_size, file_size)
    } else {
        generate_signature_read(path, block_size, file_size)
    }
}

/// Generate signature using memory mapping (for large files)
fn generate_signature_mmap(path: &Path, block_size: usize, file_size: u64) -> Result<Signature> {
    let file = File::open(path).map_err(|e| Error::io("opening file", e))?;
    let mmap = unsafe { Mmap::map(&file).map_err(|e| Error::io("memory mapping file", e))? };

    let num_blocks = (file_size as usize + block_size - 1) / block_size;

    // Parallel block processing with rayon
    let blocks: Vec<BlockChecksum> = (0..num_blocks)
        .into_par_iter()
        .map(|i| {
            let offset = (i * block_size) as u64;
            let end = std::cmp::min(offset + block_size as u64, file_size) as usize;
            let start = offset as usize;
            let chunk = &mmap[start..end];
            let size = chunk.len();

            let rolling = rolling_checksum(chunk);
            let strong = blake3::hash(chunk);

            BlockChecksum::new(i, offset, size, rolling, *strong.as_bytes())
        })
        .collect();

    // Compute file hash for etag (using BLAKE3 of entire file)
    let file_hash = blake3::hash(&mmap);
    let etag = hex::encode(&file_hash.as_bytes()[..16]); // Use first 16 bytes for shorter etag

    let mut sig = Signature::new(block_size, file_size);
    sig.blocks = blocks;
    sig.source_etag = Some(etag);

    Ok(sig)
}

/// Generate signature using standard file reading (for smaller files)
fn generate_signature_read(path: &Path, block_size: usize, file_size: u64) -> Result<Signature> {
    let mut file = File::open(path).map_err(|e| Error::io("opening file", e))?;
    let mut sig = Signature::new(block_size, file_size);

    let mut buffer = vec![0u8; block_size];
    let mut offset = 0u64;
    let mut index = 0usize;
    let mut file_hasher = blake3::Hasher::new();

    loop {
        let bytes_read = file.read(&mut buffer).map_err(|e| Error::io("reading file", e))?;

        if bytes_read == 0 {
            break;
        }

        let chunk = &buffer[..bytes_read];
        let rolling = rolling_checksum(chunk);
        let strong = blake3::hash(chunk);

        // Update file hash for etag
        file_hasher.update(chunk);

        sig.blocks.push(BlockChecksum::new(
            index,
            offset,
            bytes_read,
            rolling,
            *strong.as_bytes(),
        ));

        offset += bytes_read as u64;
        index += 1;
    }

    // Set etag from file hash
    let file_hash = file_hasher.finalize();
    sig.source_etag = Some(hex::encode(&file_hash.as_bytes()[..16]));

    Ok(sig)
}

/// Generate signature from a byte slice (for testing)
pub fn generate_signature_from_bytes(data: &[u8], block_size: usize) -> Signature {
    let file_size = data.len() as u64;
    let mut sig = Signature::new(block_size, file_size);

    for (i, chunk) in data.chunks(block_size).enumerate() {
        let offset = (i * block_size) as u64;
        let rolling = rolling_checksum(chunk);
        let strong = blake3::hash(chunk);

        sig.blocks.push(BlockChecksum::new(
            i,
            offset,
            chunk.len(),
            rolling,
            *strong.as_bytes(),
        ));
    }

    // Set etag from data hash
    let data_hash = blake3::hash(data);
    sig.source_etag = Some(hex::encode(&data_hash.as_bytes()[..16]));

    sig
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_rolling_checksum() {
        let data = b"hello world";
        let sum1 = rolling_checksum(data);
        let sum2 = rolling_checksum(data);
        assert_eq!(sum1, sum2);

        let other = b"different data";
        let sum3 = rolling_checksum(other);
        assert_ne!(sum1, sum3);
    }

    #[test]
    fn test_generate_signature_small_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content here").unwrap();

        let sig = generate_signature(file.path(), 8).unwrap();
        assert_eq!(sig.file_size, 17);
        assert_eq!(sig.block_size, 8);
        assert_eq!(sig.blocks.len(), 3); // 17 bytes / 8 = 3 blocks
    }

    #[test]
    fn test_generate_signature_from_bytes() {
        let data = b"hello world, this is a test";
        let sig = generate_signature_from_bytes(data, 10);

        assert_eq!(sig.file_size, 27);
        assert_eq!(sig.blocks.len(), 3);
        assert_eq!(sig.blocks[0].size, 10);
        assert_eq!(sig.blocks[1].size, 10);
        assert_eq!(sig.blocks[2].size, 7);
    }

    #[test]
    fn test_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let sig = generate_signature(file.path(), 5 * 1024 * 1024).unwrap();
        assert_eq!(sig.file_size, 0);
        assert!(sig.blocks.is_empty());
    }
}
