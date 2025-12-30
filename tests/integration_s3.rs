//! AWS S3 Integration Tests
//!
//! These tests are marked with #[ignore] and require real AWS credentials.
//! They will NOT run with normal `cargo test`.
//!
//! To run these tests manually:
//!   export AWS_ACCESS_KEY_ID="your_key"
//!   export AWS_SECRET_ACCESS_KEY="your_secret"
//!   export AWS_REGION="eu-central-1"
//!   cargo test --test integration_s3 -- --ignored --nocapture
//!
//! Test bucket: hsynctestawss3bucket

use std::fs;
use std::io::Write;
use tempfile::TempDir;

const TEST_BUCKET: &str = "hsynctestawss3bucket";
const TEST_FILE_SIZE: usize = 20 * 1024 * 1024; // 20MB

/// Helper to create S3 storage backend with default options
async fn create_s3_backend(prefix: String) -> hugesync::storage::StorageBackend {
    hugesync::storage::StorageBackend::s3(
        TEST_BUCKET.to_string(),
        prefix,
        None, // storage_class
        None, // endpoint (use default AWS)
    ).await.expect("Failed to create S3 backend")
}

/// Generate a random file of specified size
fn generate_random_file(path: &std::path::Path, size: usize) -> std::io::Result<()> {
    let mut file = fs::File::create(path)?;
    let mut rng_data = vec![0u8; 64 * 1024]; // 64KB buffer
    
    // Use a simple PRNG for reproducible "random" data
    let mut seed: u64 = 0xDEADBEEF;
    let mut written = 0;
    
    while written < size {
        // Fill buffer with pseudo-random data
        for byte in rng_data.iter_mut() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (seed >> 33) as u8;
        }
        
        let to_write = std::cmp::min(rng_data.len(), size - written);
        file.write_all(&rng_data[..to_write])?;
        written += to_write;
    }
    
    file.sync_all()?;
    Ok(())
}

/// Helper to create a unique test prefix for isolation
fn test_prefix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos(); // Use nanos for better uniqueness
    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();

    // Combine timestamp, counter, and PID for guaranteed uniqueness
    format!("hsync-test-{}-{}-{}", timestamp, pid, counter)
}

#[tokio::test]
#[ignore]
async fn test_s3_upload_20mb_file() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("large_file.bin");
    let prefix = test_prefix();
    
    // Generate 20MB random file
    println!("Generating 20MB test file...");
    generate_random_file(&test_file, TEST_FILE_SIZE).unwrap();
    
    let file_size = fs::metadata(&test_file).unwrap().len();
    assert_eq!(file_size, TEST_FILE_SIZE as u64);
    println!("Created test file: {} bytes", file_size);
    
    // Create S3 backend
    let storage = create_s3_backend(prefix.clone()).await;
    
    // Upload file
    let remote_path = "large_file.bin";
    println!("Uploading to s3://{}/{}/{}", TEST_BUCKET, prefix, remote_path);
    
    let data = fs::read(&test_file).unwrap();
    storage.put(remote_path, bytes::Bytes::from(data)).await
        .expect("Failed to upload file");
    
    println!("Upload successful!");
    
    // Verify file exists
    let head = storage.head(remote_path).await
        .expect("Failed to head file");
    assert!(head.is_some(), "File should exist after upload");
    assert_eq!(head.unwrap().size, TEST_FILE_SIZE as u64);
    
    // Cleanup
    println!("Cleaning up...");
    storage.delete(remote_path).await
        .expect("Failed to delete test file");
    
    println!("Test passed!");
}

#[tokio::test]
#[ignore]
async fn test_s3_download_20mb_file() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("large_file.bin");
    let download_file = temp_dir.path().join("downloaded.bin");
    let prefix = test_prefix();
    
    // Generate and upload file
    println!("Generating 20MB test file...");
    generate_random_file(&test_file, TEST_FILE_SIZE).unwrap();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    let remote_path = "large_file.bin";
    let original_data = fs::read(&test_file).unwrap();
    
    println!("Uploading...");
    storage.put(remote_path, bytes::Bytes::from(original_data.clone())).await
        .expect("Failed to upload file");
    
    // Download file
    println!("Downloading...");
    let downloaded_data = storage.get(remote_path).await
        .expect("Failed to download file");
    
    fs::write(&download_file, &downloaded_data).unwrap();
    
    // Verify
    assert_eq!(downloaded_data.len(), TEST_FILE_SIZE);
    assert_eq!(&downloaded_data[..], &original_data[..]);
    println!("Download verified: {} bytes match!", downloaded_data.len());
    
    // Cleanup
    storage.delete(remote_path).await.expect("Failed to cleanup");
    println!("Test passed!");
}

#[tokio::test]
#[ignore]
async fn test_s3_list_files() {
    let prefix = test_prefix();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    // Upload multiple small files
    println!("Uploading test files...");
    for i in 0..5 {
        let filename = format!("file_{}.txt", i);
        let content = format!("Content of file {}", i);
        storage.put(&filename, bytes::Bytes::from(content)).await
            .expect("Failed to upload file");
    }
    
    // List files
    println!("Listing files...");
    let files = storage.list("").await.expect("Failed to list files");
    
    println!("Found {} files:", files.len());
    for f in &files {
        println!("  - {} ({} bytes)", f.path.display(), f.size);
    }
    
    assert_eq!(files.len(), 5, "Should have 5 files");
    
    // Cleanup
    println!("Cleaning up...");
    for i in 0..5 {
        let filename = format!("file_{}.txt", i);
        storage.delete(&filename).await.expect("Failed to delete");
    }
    
    println!("Test passed!");
}

#[tokio::test]
#[ignore]
async fn test_s3_delta_upload() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("delta_test.bin");
    let prefix = test_prefix();
    
    // Generate 20MB file
    println!("Generating 20MB test file...");
    generate_random_file(&test_file, TEST_FILE_SIZE).unwrap();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    let remote_path = "delta_test.bin";
    
    // Initial upload
    println!("Initial upload...");
    let original_data = fs::read(&test_file).unwrap();
    storage.put(remote_path, bytes::Bytes::from(original_data.clone())).await
        .expect("Failed to initial upload");
    
    // Generate and upload signature
    println!("Generating signature...");
    let config = hugesync::config::Config::default();
    let sig = hugesync::signature::generate::generate_signature_from_bytes(&original_data, config.block_size);
    let sig_bytes = hugesync::signature::write_signature_to_bytes(&sig)
        .expect("Failed to serialize signature");
    
    let sig_path = format!("{}.hssig", remote_path);
    storage.put(&sig_path, bytes::Bytes::from(sig_bytes)).await
        .expect("Failed to upload signature");
    
    // Modify file (append 1MB)
    println!("Modifying file (append 1MB)...");
    let mut modified_data = original_data.clone();
    let extra_data = vec![0xAB; 1024 * 1024]; // 1MB
    modified_data.extend_from_slice(&extra_data);
    fs::write(&test_file, &modified_data).unwrap();
    
    // Perform delta upload
    println!("Performing delta upload...");
    let result = hugesync::sync::delta_upload::delta_upload(
        &test_file,
        remote_path,
        &storage,
        &config,
    ).await.expect("Delta upload failed");
    
    println!("Delta upload result:");
    println!("  Bytes transferred: {}", result.bytes_transferred);
    println!("  Bytes reused: {}", result.bytes_reused);
    println!("  Parts uploaded: {}", result.parts_uploaded);
    println!("  Parts copied: {}", result.parts_copied);
    
    // Verify savings (should transfer ~1MB, reuse ~20MB)
    assert!(result.bytes_reused > 0, "Should have reused some bytes");
    assert!(result.bytes_transferred < TEST_FILE_SIZE as u64, "Should transfer less than full file");
    
    // Verify file content
    println!("Verifying uploaded content...");
    let downloaded = storage.get(remote_path).await.expect("Failed to download");
    assert_eq!(downloaded.len(), modified_data.len());
    
    // Cleanup
    println!("Cleaning up...");
    storage.delete(remote_path).await.ok();
    storage.delete(&sig_path).await.ok();
    
    println!("Test passed! Delta saved {} bytes", result.bytes_reused);
}

#[tokio::test]
#[ignore]
async fn test_s3_streaming_upload() {
    use futures::stream;
    
    let prefix = test_prefix();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    let remote_path = "streamed_file.bin";
    
    // Create a stream of chunks
    println!("Creating 20MB stream...");
    let chunk_size = 1024 * 1024; // 1MB chunks
    let num_chunks = TEST_FILE_SIZE / chunk_size;
    
    let chunks: Vec<bytes::Bytes> = (0..num_chunks)
        .map(|i| {
            let mut data = vec![i as u8; chunk_size];
            // Add some variation
            data[0] = (i * 7) as u8;
            bytes::Bytes::from(data)
        })
        .collect();
    
    let byte_stream = Box::pin(stream::iter(
        chunks.into_iter().map(Ok::<_, hugesync::error::Error>)
    ));
    
    // Stream upload
    println!("Streaming upload...");
    storage.put_stream(remote_path, byte_stream, Some(TEST_FILE_SIZE as u64)).await
        .expect("Failed to stream upload");
    
    // Verify
    let head = storage.head(remote_path).await.expect("Failed to head");
    assert!(head.is_some());
    assert_eq!(head.unwrap().size, TEST_FILE_SIZE as u64);
    println!("Streaming upload verified: {} bytes", TEST_FILE_SIZE);
    
    // Cleanup
    storage.delete(remote_path).await.expect("Failed to cleanup");
    println!("Test passed!");
}

#[tokio::test]
#[ignore]
async fn test_s3_range_download() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("range_test.bin");
    let prefix = test_prefix();
    
    // Generate file with known pattern
    println!("Generating test file with known pattern...");
    let mut data = Vec::with_capacity(TEST_FILE_SIZE);
    for i in 0..TEST_FILE_SIZE {
        data.push((i % 256) as u8);
    }
    fs::write(&test_file, &data).unwrap();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    let remote_path = "range_test.bin";
    
    // Upload
    println!("Uploading...");
    storage.put(remote_path, bytes::Bytes::from(data.clone())).await
        .expect("Failed to upload");
    
    // Test range download at various positions
    let test_ranges = [
        (0, 1023),                           // First 1KB
        (1024 * 1024, 2 * 1024 * 1024 - 1),   // Second MB
        (TEST_FILE_SIZE as u64 - 1024, TEST_FILE_SIZE as u64 - 1), // Last 1KB
    ];
    
    for (start, end) in test_ranges {
        println!("Testing range {}-{}...", start, end);
        let range_data = storage.get_range(remote_path, start, end).await
            .expect("Failed to get range");
        
        let expected_len = (end - start + 1) as usize;
        assert_eq!(range_data.len(), expected_len, "Range length mismatch");
        
        // Verify content
        for (i, byte) in range_data.iter().enumerate() {
            let expected = ((start as usize + i) % 256) as u8;
            assert_eq!(*byte, expected, "Byte mismatch at offset {}", start as usize + i);
        }
        println!("  Range verified: {} bytes", range_data.len());
    }
    
    // Cleanup
    storage.delete(remote_path).await.expect("Failed to cleanup");
    println!("Test passed!");
}

#[tokio::test]
#[ignore]
async fn test_s3_multipart_upload() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("multipart_test.bin");
    let prefix = test_prefix();
    
    // Generate 20MB file
    println!("Generating 20MB test file...");
    generate_random_file(&test_file, TEST_FILE_SIZE).unwrap();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    let remote_path = "multipart_test.bin";
    
    // Read file data
    let data = fs::read(&test_file).unwrap();
    
    // Start multipart upload
    println!("Starting multipart upload...");
    let upload_id = storage.create_multipart_upload(remote_path).await
        .expect("Failed to create multipart upload");
    
    println!("Upload ID: {}", upload_id);
    
    // Upload in 5MB parts
    let part_size = 5 * 1024 * 1024;
    let mut parts = Vec::new();
    let mut offset = 0;
    let mut part_number = 1i32;
    
    while offset < data.len() {
        let end = std::cmp::min(offset + part_size, data.len());
        let chunk = bytes::Bytes::copy_from_slice(&data[offset..end]);
        
        println!("Uploading part {} ({} bytes)...", part_number, chunk.len());
        let completed = storage.upload_part(remote_path, &upload_id, part_number, chunk).await
            .expect("Failed to upload part");
        
        parts.push(completed);
        offset = end;
        part_number += 1;
    }
    
    // Complete multipart upload
    println!("Completing multipart upload with {} parts...", parts.len());
    storage.complete_multipart_upload(remote_path, &upload_id, parts).await
        .expect("Failed to complete multipart upload");
    
    // Verify
    let head = storage.head(remote_path).await.expect("Failed to head");
    assert!(head.is_some());
    assert_eq!(head.unwrap().size, TEST_FILE_SIZE as u64);
    
    // Verify content
    println!("Verifying content...");
    let downloaded = storage.get(remote_path).await.expect("Failed to download");
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(&downloaded[..1024], &data[..1024]); // Check first 1KB
    assert_eq!(&downloaded[data.len()-1024..], &data[data.len()-1024..]); // Check last 1KB
    
    // Cleanup
    storage.delete(remote_path).await.expect("Failed to cleanup");
    println!("Test passed!");
}

// =============================================================================
// LARGE FILE DELTA TESTS (200MB)
// =============================================================================

const LARGE_FILE_SIZE: usize = 200 * 1024 * 1024; // 200MB

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_s3_delta_200mb_middle_change() {
    // Initialize tracing for debug output
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("large_delta_test.bin");
    let prefix = test_prefix();

    // Generate 200MB file with pseudo-random data (not repetitive pattern!)
    // Using a simple but fast PRNG to avoid rolling hash collisions
    println!("Generating 200MB test file with random data...");
    let mut data = vec![0u8; LARGE_FILE_SIZE];
    let mut seed: u64 = 0x123456789ABCDEF0;
    for chunk in data.chunks_mut(8) {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let bytes = seed.to_le_bytes();
        for (i, byte) in chunk.iter_mut().enumerate() {
            if i < bytes.len() {
                *byte = bytes[i];
            }
        }
    }
    fs::write(&test_file, &data).unwrap();
    println!("Created 200MB test file");

    let storage = create_s3_backend(prefix.clone()).await;
    let remote_path = "large_delta_test.bin";

    // Initial upload using streaming
    println!("Initial upload (200MB)...");
    let start = std::time::Instant::now();
    storage.put_file(remote_path, &test_file).await
        .expect("Failed to initial upload");
    println!("Upload took {:?}", start.elapsed());

    // Generate and upload signature
    println!("Generating signature...");
    let config = hugesync::config::Config::default();
    let sig = hugesync::signature::generate::generate_signature_from_bytes(&data, config.block_size);
    println!("Signature has {} blocks", sig.blocks.len());
    let sig_bytes = hugesync::signature::write_signature_to_bytes(&sig)
        .expect("Failed to serialize signature");

    let sig_path = format!("{}.hssig", remote_path);
    storage.put(&sig_path, bytes::Bytes::from(sig_bytes)).await
        .expect("Failed to upload signature");

    // Modify file IN THE MIDDLE (at 100MB offset, change 2MB)
    println!("Modifying 2MB in the MIDDLE of the file (at 100MB offset)...");
    let middle_offset = 100 * 1024 * 1024; // 100MB
    let change_size = 2 * 1024 * 1024; // 2MB
    for i in 0..change_size {
        data[middle_offset + i] = 0xFF; // Replace with 0xFF pattern
    }
    fs::write(&test_file, &data).unwrap();

    // Perform delta upload with timeout
    println!("Performing delta upload...");
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(120), // 2 minute timeout
        hugesync::sync::delta_upload::delta_upload(
            &test_file,
            remote_path,
            &storage,
            &config,
        )
    ).await
        .expect("Delta upload timed out after 120 seconds")
        .expect("Delta upload failed");
    let delta_time = start.elapsed();
    
    println!("Delta upload result:");
    println!("  Time: {:?}", delta_time);
    println!("  Bytes transferred: {} ({:.2} MB)", result.bytes_transferred, result.bytes_transferred as f64 / 1024.0 / 1024.0);
    println!("  Bytes reused: {} ({:.2} MB)", result.bytes_reused, result.bytes_reused as f64 / 1024.0 / 1024.0);
    println!("  Savings: {:.1}%", (result.bytes_reused as f64 / LARGE_FILE_SIZE as f64) * 100.0);
    println!("  Parts uploaded: {}", result.parts_uploaded);
    println!("  Parts copied: {}", result.parts_copied);
    
    // Should reuse most of the file (expect ~95%+ savings)
    assert!(result.bytes_reused > (LARGE_FILE_SIZE as u64 * 90 / 100), 
        "Should reuse at least 90% of the file");
    assert!(result.bytes_transferred < (LARGE_FILE_SIZE as u64 / 10),
        "Should transfer less than 10% of the file");
    
    // Verify file content
    println!("Verifying uploaded content...");
    let downloaded = storage.get(remote_path).await.expect("Failed to download");
    assert_eq!(downloaded.len(), data.len());
    
    // Verify the middle section was correctly updated
    for i in 0..change_size {
        assert_eq!(downloaded[middle_offset + i], 0xFF, 
            "Middle section should be 0xFF at offset {}", middle_offset + i);
    }
    
    // Cleanup
    println!("Cleaning up...");
    storage.delete(remote_path).await.ok();
    storage.delete(&sig_path).await.ok();
    
    println!("Test passed! Delta saved {:.1}% bandwidth", 
        (result.bytes_reused as f64 / LARGE_FILE_SIZE as f64) * 100.0);
}

// =============================================================================
// FOLDER SYNC TESTS
// =============================================================================

const NUM_FOLDER_FILES: usize = 10;
const FOLDER_FILE_SIZE: usize = 2 * 1024 * 1024; // 2MB per file

/// Generate a folder with multiple files
fn generate_test_folder(path: &std::path::Path, num_files: usize, file_size: usize) {
    fs::create_dir_all(path).unwrap();
    
    for i in 0..num_files {
        let filename = format!("file_{:03}.bin", i);
        let filepath = path.join(&filename);
        
        // Generate file with unique content based on index
        let mut data = vec![0u8; file_size];
        for (j, byte) in data.iter_mut().enumerate() {
            *byte = ((i * 17 + j) % 256) as u8;
        }
        fs::write(&filepath, &data).unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn test_s3_folder_upload_10_files() {
    let temp_dir = TempDir::new().unwrap();
    let source_folder = temp_dir.path().join("source");
    let prefix = test_prefix();
    
    // Generate folder with 10 x 2MB files = 20MB total
    println!("Generating folder with {} x {}MB files...", NUM_FOLDER_FILES, FOLDER_FILE_SIZE / 1024 / 1024);
    generate_test_folder(&source_folder, NUM_FOLDER_FILES, FOLDER_FILE_SIZE);
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    // Upload each file
    println!("Uploading {} files to S3...", NUM_FOLDER_FILES);
    let start = std::time::Instant::now();
    
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let local_path = source_folder.join(&filename);
        let remote_path = format!("folder/{}", filename);
        
        let data = fs::read(&local_path).unwrap();
        storage.put(&remote_path, bytes::Bytes::from(data)).await
            .expect("Failed to upload file");
        println!("  Uploaded {}", filename);
    }
    
    println!("Upload took {:?}", start.elapsed());
    
    // List and verify
    println!("Listing files...");
    let files = storage.list("folder/").await.expect("Failed to list");
    println!("Found {} files", files.len());
    assert_eq!(files.len(), NUM_FOLDER_FILES, "Should have {} files", NUM_FOLDER_FILES);
    
    // Verify each file
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let remote_path = format!("folder/{}", filename);
        
        let head = storage.head(&remote_path).await.expect("Failed to head");
        assert!(head.is_some(), "File {} should exist", filename);
        assert_eq!(head.unwrap().size, FOLDER_FILE_SIZE as u64);
    }
    
    // Cleanup
    println!("Cleaning up...");
    for i in 0..NUM_FOLDER_FILES {
        let remote_path = format!("folder/file_{:03}.bin", i);
        storage.delete(&remote_path).await.ok();
    }
    
    println!("Test passed! Uploaded {} files ({} MB total)", 
        NUM_FOLDER_FILES, 
        (NUM_FOLDER_FILES * FOLDER_FILE_SIZE) / 1024 / 1024);
}

#[tokio::test]
#[ignore]
async fn test_s3_folder_download_10_files() {
    let temp_dir = TempDir::new().unwrap();
    let source_folder = temp_dir.path().join("source");
    let download_folder = temp_dir.path().join("downloaded");
    let prefix = test_prefix();
    
    // Generate and upload folder
    println!("Generating and uploading folder with {} files...", NUM_FOLDER_FILES);
    generate_test_folder(&source_folder, NUM_FOLDER_FILES, FOLDER_FILE_SIZE);
    fs::create_dir_all(&download_folder).unwrap();
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    // Upload
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let local_path = source_folder.join(&filename);
        let remote_path = format!("folder/{}", filename);
        let data = fs::read(&local_path).unwrap();
        storage.put(&remote_path, bytes::Bytes::from(data)).await.unwrap();
    }
    println!("Uploaded {} files", NUM_FOLDER_FILES);
    
    // Download all files
    println!("Downloading {} files from S3...", NUM_FOLDER_FILES);
    let start = std::time::Instant::now();
    
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let remote_path = format!("folder/{}", filename);
        let local_path = download_folder.join(&filename);
        
        let data = storage.get(&remote_path).await
            .expect("Failed to download");
        fs::write(&local_path, &data).unwrap();
        println!("  Downloaded {}", filename);
    }
    
    println!("Download took {:?}", start.elapsed());
    
    // Verify downloaded files match originals
    println!("Verifying downloaded files...");
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let original = fs::read(source_folder.join(&filename)).unwrap();
        let downloaded = fs::read(download_folder.join(&filename)).unwrap();
        
        assert_eq!(original.len(), downloaded.len(), "File {} size mismatch", filename);
        assert_eq!(original, downloaded, "File {} content mismatch", filename);
    }
    
    // Cleanup
    println!("Cleaning up...");
    for i in 0..NUM_FOLDER_FILES {
        let remote_path = format!("folder/file_{:03}.bin", i);
        storage.delete(&remote_path).await.ok();
    }
    
    println!("Test passed! Downloaded and verified {} files", NUM_FOLDER_FILES);
}

#[tokio::test]
#[ignore]
async fn test_s3_folder_sync_incremental() {
    let temp_dir = TempDir::new().unwrap();
    let source_folder = temp_dir.path().join("source");
    let prefix = test_prefix();
    
    // Generate initial folder with 10 files
    println!("Phase 1: Initial upload of {} files...", NUM_FOLDER_FILES);
    generate_test_folder(&source_folder, NUM_FOLDER_FILES, FOLDER_FILE_SIZE);
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    // Upload all files
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let local_path = source_folder.join(&filename);
        let remote_path = format!("folder/{}", filename);
        let data = fs::read(&local_path).unwrap();
        storage.put(&remote_path, bytes::Bytes::from(data)).await.unwrap();
    }
    
    // Modify only 2 files (simulate incremental sync)
    println!("Phase 2: Modifying 2 out of {} files...", NUM_FOLDER_FILES);
    let modified_files = [0, 5]; // Modify file_000 and file_005
    
    for &i in &modified_files {
        let filename = format!("file_{:03}.bin", i);
        let filepath = source_folder.join(&filename);
        
        // Read, modify, write
        let mut data = fs::read(&filepath).unwrap();
        for byte in data.iter_mut().take(1024) {
            *byte = 0xAB; // Modify first 1KB
        }
        fs::write(&filepath, &data).unwrap();
        println!("  Modified {}", filename);
    }
    
    // Upload only modified files (simulating what sync engine would do)
    println!("Phase 3: Uploading only modified files...");
    let start = std::time::Instant::now();
    
    for &i in &modified_files {
        let filename = format!("file_{:03}.bin", i);
        let local_path = source_folder.join(&filename);
        let remote_path = format!("folder/{}", filename);
        let data = fs::read(&local_path).unwrap();
        storage.put(&remote_path, bytes::Bytes::from(data)).await.unwrap();
        println!("  Uploaded {}", filename);
    }
    
    println!("Incremental upload took {:?}", start.elapsed());
    
    // Verify all files (including unmodified ones)
    println!("Phase 4: Verifying all files...");
    for i in 0..NUM_FOLDER_FILES {
        let filename = format!("file_{:03}.bin", i);
        let remote_path = format!("folder/{}", filename);
        let local_path = source_folder.join(&filename);

        // Retry on transient AWS errors
        let mut attempts = 0;
        let remote_data = loop {
            attempts += 1;
            match storage.get(&remote_path).await {
                Ok(data) => break data,
                Err(e) if attempts < 3 => {
                    eprintln!("  Retry {} for {}: {}", attempts, filename, e);
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => panic!("Failed to get {} after {} attempts: {}", filename, attempts, e),
            }
        };
        let local_data = fs::read(&local_path).unwrap();

        assert_eq!(remote_data.len(), local_data.len());
        assert_eq!(&remote_data[..], &local_data[..], "File {} should match", filename);
    }
    
    // Cleanup
    println!("Cleaning up...");
    for i in 0..NUM_FOLDER_FILES {
        let remote_path = format!("folder/file_{:03}.bin", i);
        storage.delete(&remote_path).await.ok();
    }
    
    println!("Test passed! Incremental sync of {} modified files verified", modified_files.len());
}

#[tokio::test]
#[ignore]
async fn test_s3_folder_with_subdirectories() {
    let temp_dir = TempDir::new().unwrap();
    let source_folder = temp_dir.path().join("source");
    let prefix = test_prefix();
    
    // Create folder structure with subdirectories
    println!("Creating folder structure with subdirectories...");
    fs::create_dir_all(source_folder.join("subdir_a")).unwrap();
    fs::create_dir_all(source_folder.join("subdir_b/nested")).unwrap();
    
    // Create files in each location
    let files = [
        ("root_file.txt", "Root content"),
        ("subdir_a/file_a1.txt", "Content in A"),
        ("subdir_a/file_a2.txt", "More content in A"),
        ("subdir_b/file_b1.txt", "Content in B"),
        ("subdir_b/nested/deep_file.txt", "Deep nested content"),
    ];
    
    for (path, content) in &files {
        let full_path = source_folder.join(path);
        fs::write(&full_path, content).unwrap();
    }
    
    let storage = create_s3_backend(prefix.clone()).await;
    
    // Upload all files preserving structure
    println!("Uploading {} files with directory structure...", files.len());
    for (path, content) in &files {
        let remote_path = format!("tree/{}", path);
        storage.put(&remote_path, bytes::Bytes::from(*content)).await.unwrap();
        println!("  Uploaded {}", path);
    }
    
    // List and verify
    println!("Listing all files...");
    let all_files = storage.list("tree/").await.expect("Failed to list");
    println!("Found {} files:", all_files.len());
    for f in &all_files {
        println!("  - {} ({} bytes)", f.path.display(), f.size);
    }
    
    assert_eq!(all_files.len(), files.len(), "Should have {} files", files.len());
    
    // Verify each file's content
    println!("Verifying content...");
    for (path, expected_content) in &files {
        let remote_path = format!("tree/{}", path);
        let data = storage.get(&remote_path).await.expect("Failed to get");
        let content = String::from_utf8(data.to_vec()).unwrap();
        assert_eq!(&content, *expected_content, "Content mismatch for {}", path);
    }
    
    // Cleanup
    println!("Cleaning up...");
    for (path, _) in &files {
        let remote_path = format!("tree/{}", path);
        storage.delete(&remote_path).await.ok();
    }
    
    println!("Test passed! Subdirectory structure verified");
}
