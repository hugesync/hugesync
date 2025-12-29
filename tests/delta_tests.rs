//! Delta algorithm tests

use hugesync::delta::{coalesce_operations, Delta};
use hugesync::signature::generate_signature_from_bytes;

#[test]
fn test_delta_operations_count() {
    let mut delta = Delta::new(100);
    delta.add_copy(0, 50);
    delta.add_insert(vec![1, 2, 3]);
    delta.add_copy(60, 40);
    
    assert_eq!(delta.operation_count(), 3);
    assert_eq!(delta.bytes_reused, 90);
    assert_eq!(delta.bytes_new, 3);
}

#[test]
fn test_coalesce_contiguous_copies() {
    let mut delta = Delta::new(100);
    delta.add_copy(0, 10);
    delta.add_copy(10, 20);
    delta.add_copy(30, 30);
    
    let coalesced = coalesce_operations(&delta, 5 * 1024 * 1024);
    
    // Should merge into one copy operation
    assert_eq!(coalesced.len(), 1);
}

#[test]
fn test_coalesce_non_contiguous_copies() {
    let mut delta = Delta::new(100);
    delta.add_copy(0, 10);
    delta.add_copy(50, 20); // Gap at 10-50
    
    let coalesced = coalesce_operations(&delta, 1);
    
    // Should remain as two separate operations
    assert_eq!(coalesced.len(), 2);
}

#[test]
fn test_coalesce_merges_inserts() {
    let mut delta = Delta::new(100);
    delta.add_insert(vec![1, 2, 3]);
    delta.add_insert(vec![4, 5, 6]);
    
    let coalesced = coalesce_operations(&delta, 1);
    
    // Should merge into one insert
    assert_eq!(coalesced.len(), 1);
}

#[test]
fn test_signature_block_count() {
    let data = vec![0u8; 1000];
    let sig = generate_signature_from_bytes(&data, 100);
    
    // 1000 bytes / 100 block size = 10 blocks
    assert_eq!(sig.block_count(), 10);
    assert_eq!(sig.file_size, 1000);
}

#[test]
fn test_signature_last_block_partial() {
    let data = vec![0u8; 150];
    let sig = generate_signature_from_bytes(&data, 100);
    
    // 150 bytes / 100 block size = 2 blocks (100 + 50)
    assert_eq!(sig.block_count(), 2);
    assert_eq!(sig.blocks[1].size, 50);
}

#[test]
fn test_rolling_hash_changes_with_content() {
    let data1 = vec![0xAAu8; 32];
    let data2 = vec![0xBBu8; 32];
    
    let sig1 = generate_signature_from_bytes(&data1, 32);
    let sig2 = generate_signature_from_bytes(&data2, 32);
    
    // Different content should have different hashes
    assert_ne!(sig1.blocks[0].rolling, sig2.blocks[0].rolling);
    assert_ne!(sig1.blocks[0].strong, sig2.blocks[0].strong);
}

#[test]
fn test_delta_savings_percent() {
    let mut delta = Delta::new(100);
    delta.add_copy(0, 80);
    delta.add_insert(vec![0; 20]);
    delta.target_size = 100;
    
    // 80% reuse
    assert!((delta.savings_percent() - 80.0).abs() < 1.0);
    assert!(delta.is_beneficial());
}

#[test]
fn test_delta_not_beneficial_all_new() {
    let mut delta = Delta::new(100);
    delta.add_insert(vec![0; 100]);
    delta.target_size = 100;
    
    // 0% reuse - not beneficial
    assert!(!delta.is_beneficial());
}

#[test]
fn test_signature_generation_basic() {
    let data = b"hello world test content";
    let sig = generate_signature_from_bytes(data, 8);
    
    assert_eq!(sig.file_size, data.len() as u64);
    assert_eq!(sig.block_size, 8);
    assert!(!sig.blocks.is_empty());
}

#[test]
fn test_signature_offsets() {
    let data = vec![0u8; 50];
    let block_size = 10;
    let sig = generate_signature_from_bytes(&data, block_size);
    
    // Check offsets are correct
    assert_eq!(sig.blocks[0].offset, 0);
    assert_eq!(sig.blocks[1].offset, 10);
    assert_eq!(sig.blocks[2].offset, 20);
}
