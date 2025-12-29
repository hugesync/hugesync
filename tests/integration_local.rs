//! Integration tests for local-to-local sync

use hugesync::config::Config;
use hugesync::storage::StorageBackend;
use hugesync::sync::{create_backends, SyncEngine};
use hugesync::uri::Location;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;

/// Create a test file with specified content
fn create_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.path().join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&path, content).unwrap();
    path
}

/// Get file content
fn read_file(dir: &TempDir, name: &str) -> Vec<u8> {
    fs::read(dir.path().join(name)).unwrap()
}

/// Check if file exists
fn file_exists(dir: &TempDir, name: &str) -> bool {
    dir.path().join(name).exists()
}

#[tokio::test]
async fn test_sync_new_files() {
    let source = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    // Create source files
    create_file(&source, "file1.txt", b"hello world");
    create_file(&source, "file2.txt", b"goodbye world");
    create_file(&source, "subdir/file3.txt", b"nested file");

    // Run sync
    let config = Config::default();
    let source_backend = StorageBackend::local(source.path().to_path_buf());
    let dest_backend = StorageBackend::local(dest.path().to_path_buf());

    let engine = SyncEngine::new(config, source_backend, dest_backend);
    let stats = engine.sync().await.unwrap();

    // Verify files were copied
    assert_eq!(read_file(&dest, "file1.txt"), b"hello world");
    assert_eq!(read_file(&dest, "file2.txt"), b"goodbye world");
    assert_eq!(read_file(&dest, "subdir/file3.txt"), b"nested file");

    // Check stats
    assert_eq!(stats.files_uploaded, 3);
    assert_eq!(stats.errors, 0);
}

#[tokio::test]
async fn test_sync_updates_modified_files() {
    let source = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    // Create files with DIFFERENT sizes so sync detects the change
    create_file(&source, "file.txt", b"this is the new updated content");
    create_file(&dest, "file.txt", b"old");

    // Run sync
    let config = Config::default();
    let source_backend = StorageBackend::local(source.path().to_path_buf());
    let dest_backend = StorageBackend::local(dest.path().to_path_buf());

    let engine = SyncEngine::new(config, source_backend, dest_backend);
    let stats = engine.sync().await.unwrap();

    // Verify file was updated
    assert_eq!(read_file(&dest, "file.txt"), b"this is the new updated content");
    assert_eq!(stats.files_uploaded, 1);
}

#[tokio::test]
async fn test_sync_skips_identical_files() {
    let source = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    // Create identical files
    create_file(&source, "file.txt", b"same content");
    create_file(&dest, "file.txt", b"same content");

    // Run sync
    let config = Config::default();
    let source_backend = StorageBackend::local(source.path().to_path_buf());
    let dest_backend = StorageBackend::local(dest.path().to_path_buf());

    let engine = SyncEngine::new(config, source_backend, dest_backend);
    let stats = engine.sync().await.unwrap();

    // Should skip identical file
    assert_eq!(stats.files_skipped, 1);
    assert_eq!(stats.files_uploaded, 0);
}

#[tokio::test]
async fn test_sync_dry_run() {
    let source = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    create_file(&source, "file.txt", b"content");

    // Run dry run sync
    let mut config = Config::default();
    config.dry_run = true;
    let source_backend = StorageBackend::local(source.path().to_path_buf());
    let dest_backend = StorageBackend::local(dest.path().to_path_buf());

    let engine = SyncEngine::new(config, source_backend, dest_backend);
    engine.sync().await.unwrap();

    // File should NOT exist in dest (dry run)
    assert!(!file_exists(&dest, "file.txt"));
}

#[tokio::test]
async fn test_sync_deletes_extraneous() {
    let source = TempDir::new().unwrap();
    let dest = TempDir::new().unwrap();

    // Create file only in dest
    create_file(&dest, "extraneous.txt", b"should be deleted");

    // Run sync with delete flag
    let mut config = Config::default();
    config.delete = true;
    let source_backend = StorageBackend::local(source.path().to_path_buf());
    let dest_backend = StorageBackend::local(dest.path().to_path_buf());

    let engine = SyncEngine::new(config, source_backend, dest_backend);
    let stats = engine.sync().await.unwrap();

    // File should be deleted
    assert!(!file_exists(&dest, "extraneous.txt"));
    assert_eq!(stats.files_deleted, 1);
}

#[tokio::test]
async fn test_local_backend_operations() {
    let temp = TempDir::new().unwrap();
    let backend = StorageBackend::local(temp.path().to_path_buf());

    // Test put
    backend.put("test.txt", bytes::Bytes::from("hello")).await.unwrap();
    assert!(file_exists(&temp, "test.txt"));

    // Test get
    let data = backend.get("test.txt").await.unwrap();
    assert_eq!(&data[..], b"hello");

    // Test head
    let entry = backend.head("test.txt").await.unwrap().unwrap();
    assert_eq!(entry.size, 5);

    // Test exists
    assert!(backend.exists("test.txt").await.unwrap());
    assert!(!backend.exists("nonexistent.txt").await.unwrap());

    // Test delete
    backend.delete("test.txt").await.unwrap();
    assert!(!file_exists(&temp, "test.txt"));
}

#[tokio::test]
async fn test_local_backend_list() {
    let temp = TempDir::new().unwrap();
    let backend = StorageBackend::local(temp.path().to_path_buf());

    // Create files
    create_file(&temp, "a.txt", b"a");
    create_file(&temp, "b.txt", b"bb");
    create_file(&temp, "sub/c.txt", b"ccc");

    // List all
    let files = backend.list("").await.unwrap();
    assert!(files.len() >= 3);

    // Check we found our files
    let paths: Vec<_> = files.iter().map(|f| f.path.to_string_lossy().to_string()).collect();
    assert!(paths.iter().any(|p| p.contains("a.txt")));
    assert!(paths.iter().any(|p| p.contains("b.txt")));
}
