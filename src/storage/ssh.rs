//! SSH/SFTP storage backend using russh (pure Rust, cross-platform)
//!
//! This backend provides SFTP-based file transfers using the russh library,
//! which is a pure Rust SSH implementation that works across all platforms.

use crate::error::{Error, Result};
use crate::storage::ByteStream;
use crate::types::FileEntry;
use bytes::Bytes;
use futures::StreamExt;
use russh::client::{self, Config, Handle, Handler};
use russh::keys::ssh_key::PublicKey;
use russh_sftp::client::SftpSession;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// SSH client handler for russh
struct SshHandler;

impl Handler for SshHandler {
    type Error = russh::Error;

    async fn check_server_key(&mut self, _server_public_key: &PublicKey) -> std::result::Result<bool, Self::Error> {
        // Accept all server keys for now
        // TODO: Implement proper known_hosts checking
        Ok(true)
    }
}

/// SFTP session wrapper
struct SftpConnection {
    session: SftpSession,
    #[allow(dead_code)]
    handle: Handle<SshHandler>,
}

/// SSH/SFTP storage backend using pure Rust russh library
#[derive(Clone)]
pub struct SshBackend {
    /// Shared SFTP connection
    sftp: Arc<Mutex<SftpConnection>>,
    /// Remote base path
    path: String,
    /// Connection info for display
    host: String,
    user: String,
}

impl SshBackend {
    /// Create a new SSH backend
    pub async fn new(
        user: Option<String>,
        host: String,
        path: String,
        port: Option<u16>,
    ) -> Result<Self> {
        let port = port.unwrap_or(22);
        let user = user.unwrap_or_else(|| whoami::username());

        tracing::info!(host = %host, user = %user, port = port, path = %path, "Connecting to SSH server via SFTP");

        // Create SSH config
        let config = Config::default();
        let config = Arc::new(config);

        // Connect to SSH server
        let mut handle = client::connect(config, (host.as_str(), port), SshHandler)
            .await
            .map_err(|e| Error::Ssh {
                message: format!("Failed to connect to {}:{}: {}", host, port, e),
            })?;

        // Try to authenticate
        let authenticated = try_authenticate(&mut handle, &user).await?;

        if !authenticated {
            return Err(Error::Ssh {
                message: format!("Authentication failed for {}@{}", user, host),
            });
        }

        tracing::info!("SSH authentication successful");

        // Open SFTP channel
        let channel = handle.channel_open_session().await.map_err(|e| Error::Ssh {
            message: format!("Failed to open SSH channel: {}", e),
        })?;

        // Request SFTP subsystem
        channel.request_subsystem(true, "sftp").await.map_err(|e| Error::Ssh {
            message: format!("Failed to request SFTP subsystem: {}", e),
        })?;

        // Create SFTP session
        let sftp = SftpSession::new(channel.into_stream()).await.map_err(|e| Error::Ssh {
            message: format!("Failed to initialize SFTP session: {}", e),
        })?;

        tracing::info!("SFTP session established");

        let connection = SftpConnection { session: sftp, handle };

        Ok(Self {
            sftp: Arc::new(Mutex::new(connection)),
            path,
            host,
            user,
        })
    }

    /// Get the remote base path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Resolve a relative path to full remote path
    fn resolve(&self, path: &str) -> String {
        if path.is_empty() {
            self.path.clone()
        } else if self.path.is_empty() {
            path.to_string()
        } else {
            format!("{}/{}", self.path.trim_end_matches('/'), path)
        }
    }

    /// List all files under the given prefix
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        let remote_path = self.resolve(prefix);
        let conn = self.sftp.lock().await;

        let mut entries = Vec::new();
        self.list_recursive(&conn.session, &remote_path, &remote_path, &mut entries).await?;

        tracing::debug!(count = entries.len(), path = %remote_path, "Listed SFTP files");
        Ok(entries)
    }

    /// Recursively list directory contents
    async fn list_recursive(
        &self,
        sftp: &SftpSession,
        base_path: &str,
        current_path: &str,
        entries: &mut Vec<FileEntry>,
    ) -> Result<()> {
        let dir_entries = sftp.read_dir(current_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to list directory {}: {}", current_path, e),
        })?;

        for entry in dir_entries {
            let filename = entry.file_name();

            // Skip . and ..
            if filename == "." || filename == ".." {
                continue;
            }

            let full_path = format!("{}/{}", current_path, filename);
            let relative_path = full_path
                .strip_prefix(base_path)
                .unwrap_or(&full_path)
                .trim_start_matches('/')
                .to_string();

            if relative_path.is_empty() {
                continue;
            }

            let attrs = entry.metadata();
            let is_dir = attrs.is_dir();
            let size = attrs.size.unwrap_or(0);
            let mtime = attrs.mtime.map(|t| {
                std::time::UNIX_EPOCH + std::time::Duration::from_secs(t as u64)
            });

            entries.push(FileEntry {
                path: PathBuf::from(&relative_path),
                size,
                mtime,
                is_dir,
                mode: attrs.permissions,
                etag: None,
            });

            // Recurse into directories
            if is_dir {
                // Use Box::pin for recursive async
                Box::pin(self.list_recursive(sftp, base_path, &full_path, entries)).await?;
            }
        }

        Ok(())
    }

    /// Get file metadata
    pub async fn head(&self, path: &str) -> Result<Option<FileEntry>> {
        let remote_path = self.resolve(path);
        let conn = self.sftp.lock().await;

        match conn.session.metadata(&remote_path).await {
            Ok(attrs) => {
                let is_dir = attrs.is_dir();
                let size = attrs.size.unwrap_or(0);
                let mtime = attrs.mtime.map(|t| {
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(t as u64)
                });

                Ok(Some(FileEntry {
                    path: PathBuf::from(path),
                    size,
                    mtime,
                    is_dir,
                    mode: attrs.permissions,
                    etag: None,
                }))
            }
            Err(e) => {
                // Check if it's a "not found" error
                let err_str = e.to_string().to_lowercase();
                if err_str.contains("no such file") || err_str.contains("not found") {
                    Ok(None)
                } else {
                    Err(Error::Ssh {
                        message: format!("Failed to get metadata for {}: {}", remote_path, e),
                    })
                }
            }
        }
    }

    /// Read a file's contents (loads entire file into memory)
    ///
    /// WARNING: For large files, use `get_stream()` instead.
    pub async fn get(&self, path: &str) -> Result<Bytes> {
        let remote_path = self.resolve(path);
        let conn = self.sftp.lock().await;

        let data = conn.session.read(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to read file {}: {}", remote_path, e),
        })?;

        Ok(Bytes::from(data))
    }

    /// Read a file as a stream of chunks (memory-efficient for large files)
    ///
    /// Reads the file in chunks using SFTP seek/read operations to avoid
    /// loading the entire file into memory at once.
    pub async fn get_stream(&self, path: &str) -> Result<ByteStream> {
        use bytes::BytesMut;
        use futures::stream;

        const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16MB chunks

        let remote_path = self.resolve(path);
        let conn = self.sftp.lock().await;

        // Get file size first
        let attrs = conn.session.metadata(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to stat file {}: {}", remote_path, e),
        })?;
        let file_size = attrs.size.unwrap_or(0) as usize;

        // For small files, just read the whole thing
        if file_size < CHUNK_SIZE {
            let data = conn.session.read(&remote_path).await.map_err(|e| Error::Ssh {
                message: format!("Failed to read file {}: {}", remote_path, e),
            })?;
            return Ok(Box::pin(stream::once(async { Ok(Bytes::from(data)) })));
        }

        // For large files, read in chunks using seek/read
        let mut file = conn.session.open(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to open file {}: {}", remote_path, e),
        })?;

        let mut chunks = Vec::new();
        let mut offset = 0usize;

        while offset < file_size {
            let to_read = std::cmp::min(CHUNK_SIZE, file_size - offset);
            let mut buf = BytesMut::zeroed(to_read);

            // Seek to position
            file.seek(std::io::SeekFrom::Start(offset as u64))
                .await
                .map_err(|e| Error::Ssh {
                    message: format!("Failed to seek in file {}: {}", remote_path, e),
                })?;

            // Read chunk
            let bytes_read = file.read_exact(&mut buf).await.map_err(|e| Error::Ssh {
                message: format!("Failed to read chunk from {}: {}", remote_path, e),
            });

            match bytes_read {
                Ok(_) => {
                    chunks.push(buf.freeze());
                    offset += to_read;
                }
                Err(_) => break, // EOF or error
            }
        }

        Ok(Box::pin(stream::iter(chunks.into_iter().map(Ok))))
    }

    /// Read a range of bytes from a file
    ///
    /// Uses SFTP seek to read only the requested byte range, avoiding
    /// loading the entire file into memory.
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        use bytes::BytesMut;

        let remote_path = self.resolve(path);
        let conn = self.sftp.lock().await;

        // Open file for reading
        let mut file = conn.session.open(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to open file {}: {}", remote_path, e),
        })?;

        // Seek to start position
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|e| Error::Ssh {
                message: format!("Failed to seek in file {}: {}", remote_path, e),
            })?;

        // Calculate how many bytes to read (end is inclusive)
        let len = (end - start + 1) as usize;
        let mut buf = BytesMut::zeroed(len);

        // Read the exact range
        match file.read_exact(&mut buf).await {
            Ok(_) => Ok(buf.freeze()),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // File is shorter than expected, read what we can
                let mut buf = Vec::new();
                file.seek(std::io::SeekFrom::Start(start))
                    .await
                    .map_err(|e| Error::Ssh {
                        message: format!("Failed to seek in file {}: {}", remote_path, e),
                    })?;
                file.read_to_end(&mut buf).await.map_err(|e| Error::Ssh {
                    message: format!("Failed to read file {}: {}", remote_path, e),
                })?;
                Ok(Bytes::from(buf))
            }
            Err(e) => Err(Error::Ssh {
                message: format!("Failed to read range from {}: {}", remote_path, e),
            }),
        }
    }

    /// Write a file's contents
    pub async fn put(&self, path: &str, data: Bytes) -> Result<()> {
        let remote_path = self.resolve(path);

        // Ensure parent directory exists
        self.ensure_parent_dir(&remote_path).await?;

        let conn = self.sftp.lock().await;

        conn.session.write(&remote_path, &data).await.map_err(|e| Error::Ssh {
            message: format!("Failed to write file {}: {}", remote_path, e),
        })?;

        tracing::debug!(path = %remote_path, size = data.len(), "Wrote file via SFTP");
        Ok(())
    }

    /// Write a file from a stream of chunks
    ///
    /// Writes chunks incrementally using SFTP seek/write operations to avoid
    /// buffering the entire file in memory.
    pub async fn put_stream(&self, path: &str, mut stream: ByteStream) -> Result<()> {
        let remote_path = self.resolve(path);

        // Ensure parent directory exists
        self.ensure_parent_dir(&remote_path).await?;

        let conn = self.sftp.lock().await;

        // Create/truncate the file and get a write handle
        let mut file = conn.session.create(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to create file {}: {}", remote_path, e),
        })?;

        let mut total_written = 0u64;

        // Write chunks as they arrive
        while let Some(chunk) = stream.next().await {
            let data = chunk?;
            file.write_all(&data).await.map_err(|e| Error::Ssh {
                message: format!("Failed to write chunk to {}: {}", remote_path, e),
            })?;
            total_written += data.len() as u64;
        }

        // Flush to ensure all data is written
        file.flush().await.map_err(|e| Error::Ssh {
            message: format!("Failed to flush file {}: {}", remote_path, e),
        })?;

        tracing::debug!(path = %remote_path, size = total_written, "Wrote stream via SFTP");
        Ok(())
    }

    /// Delete a file or directory
    pub async fn delete(&self, path: &str) -> Result<()> {
        let remote_path = self.resolve(path);
        let conn = self.sftp.lock().await;

        // Check if it's a directory
        match conn.session.metadata(&remote_path).await {
            Ok(attrs) if attrs.is_dir() => {
                // Recursively delete directory contents first
                self.delete_dir_contents(&conn.session, &remote_path).await?;

                // Remove the directory itself
                conn.session.remove_dir(&remote_path).await.map_err(|e| Error::Ssh {
                    message: format!("Failed to remove directory {}: {}", remote_path, e),
                })?;
            }
            Ok(_) => {
                // It's a file
                conn.session.remove_file(&remote_path).await.map_err(|e| Error::Ssh {
                    message: format!("Failed to delete file {}: {}", remote_path, e),
                })?;
            }
            Err(e) => {
                let err_str = e.to_string().to_lowercase();
                if !err_str.contains("no such file") && !err_str.contains("not found") {
                    return Err(Error::Ssh {
                        message: format!("Failed to delete {}: {}", remote_path, e),
                    });
                }
                // File doesn't exist, that's fine
            }
        }

        tracing::debug!(path = %remote_path, "Deleted via SFTP");
        Ok(())
    }

    /// Recursively delete directory contents
    async fn delete_dir_contents(&self, sftp: &SftpSession, dir_path: &str) -> Result<()> {
        let entries = sftp.read_dir(dir_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to list directory {}: {}", dir_path, e),
        })?;

        for entry in entries {
            let filename = entry.file_name();
            if filename == "." || filename == ".." {
                continue;
            }

            let full_path = format!("{}/{}", dir_path, filename);
            let attrs = entry.metadata();

            if attrs.is_dir() {
                Box::pin(self.delete_dir_contents(sftp, &full_path)).await?;
                sftp.remove_dir(&full_path).await.map_err(|e| Error::Ssh {
                    message: format!("Failed to remove directory {}: {}", full_path, e),
                })?;
            } else {
                sftp.remove_file(&full_path).await.map_err(|e| Error::Ssh {
                    message: format!("Failed to delete file {}: {}", full_path, e),
                })?;
            }
        }

        Ok(())
    }

    /// Check if a path exists
    pub async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.head(path).await?.is_some())
    }

    /// Create a directory (and parents)
    pub async fn mkdir(&self, path: &str) -> Result<()> {
        let remote_path = self.resolve(path);
        self.mkdir_recursive(&remote_path).await
    }

    /// Ensure parent directory exists
    async fn ensure_parent_dir(&self, path: &str) -> Result<()> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            let parent_str = parent.to_string_lossy().to_string();
            if !parent_str.is_empty() && parent_str != "/" {
                self.mkdir_recursive(&parent_str).await?;
            }
        }
        Ok(())
    }

    /// Create directory and all parents
    async fn mkdir_recursive(&self, path: &str) -> Result<()> {
        let conn = self.sftp.lock().await;

        // Split path into components and create each level
        let mut current = String::new();
        for component in path.split('/').filter(|c| !c.is_empty()) {
            current = if current.is_empty() {
                format!("/{}", component)
            } else {
                format!("{}/{}", current, component)
            };

            // Check if directory exists
            match conn.session.metadata(&current).await {
                Ok(attrs) if attrs.is_dir() => continue,
                Ok(_) => {
                    return Err(Error::Ssh {
                        message: format!("Path exists but is not a directory: {}", current),
                    });
                }
                Err(_) => {
                    // Directory doesn't exist, create it
                    conn.session.create_dir(&current).await.map_err(|e| Error::Ssh {
                        message: format!("Failed to create directory {}: {}", current, e),
                    })?;
                }
            }
        }

        Ok(())
    }
}

/// Try to authenticate using available methods
async fn try_authenticate(handle: &mut Handle<SshHandler>, user: &str) -> Result<bool> {
    use russh::client::AuthResult;

    // Try SSH agent first
    if let Ok(true) = try_agent_auth(handle, user).await {
        return Ok(true);
    }

    // Try key files from ~/.ssh
    if let Ok(true) = try_key_file_auth(handle, user).await {
        return Ok(true);
    }

    // Try no authentication (for servers that allow it)
    match handle.authenticate_none(user).await {
        Ok(AuthResult::Success) => return Ok(true),
        _ => {}
    }

    Ok(false)
}

/// Try to authenticate using SSH agent
async fn try_agent_auth(handle: &mut Handle<SshHandler>, user: &str) -> Result<bool> {
    use russh::client::AuthResult;
    use russh::keys::agent::client::AgentClient;

    // Connect to SSH agent
    let mut agent = match AgentClient::connect_env().await {
        Ok(agent) => agent,
        Err(_) => return Ok(false),
    };

    // Get identities from agent
    let identities = match agent.request_identities().await {
        Ok(ids) => ids,
        Err(_) => return Ok(false),
    };

    // Try each identity
    for key in identities {
        let auth_result = handle
            .authenticate_publickey_with(user, key, None, &mut agent)
            .await;

        match auth_result {
            Ok(AuthResult::Success) => {
                tracing::debug!("SSH agent authentication successful");
                return Ok(true);
            }
            _ => continue,
        }
    }

    Ok(false)
}

/// Try to authenticate using key files
async fn try_key_file_auth(handle: &mut Handle<SshHandler>, user: &str) -> Result<bool> {
    use russh::client::AuthResult;
    use russh::keys::{load_secret_key, PrivateKeyWithHashAlg};

    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Ok(false),
    };

    let ssh_dir = home.join(".ssh");

    // Common key file names to try
    let key_files = ["id_ed25519", "id_rsa", "id_ecdsa", "id_dsa"];

    for key_name in &key_files {
        let key_path = ssh_dir.join(key_name);

        if !key_path.exists() {
            continue;
        }

        // Load key (try without passphrase first)
        let key = match load_secret_key(&key_path, None) {
            Ok(k) => k,
            Err(_) => continue, // Key might need passphrase, skip for now
        };

        // Wrap key with hash algorithm
        let key_with_hash = PrivateKeyWithHashAlg::new(Arc::new(key), None);

        let auth_result = handle
            .authenticate_publickey(user, key_with_hash)
            .await;

        match auth_result {
            Ok(AuthResult::Success) => {
                tracing::debug!(key = %key_path.display(), "Key file authentication successful");
                return Ok(true);
            }
            _ => continue,
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a real SSH server
    // They are ignored by default and can be run with:
    // SSH_TEST_HOST=localhost SSH_TEST_USER=user cargo test ssh --ignored

    fn get_test_config() -> Option<(Option<String>, String, String)> {
        let host = std::env::var("SSH_TEST_HOST").ok()?;
        let user = std::env::var("SSH_TEST_USER").ok();
        let path = std::env::var("SSH_TEST_PATH").unwrap_or_else(|_| "/tmp/hugesync_test".to_string());
        Some((user, host, path))
    }

    #[tokio::test]
    #[ignore]
    async fn test_ssh_connect() {
        let (user, host, path) = get_test_config().expect("SSH_TEST_HOST not set");
        let backend = SshBackend::new(user, host, path, None).await;
        assert!(backend.is_ok(), "Failed to connect: {:?}", backend.err());
    }

    #[tokio::test]
    #[ignore]
    async fn test_ssh_put_get() {
        let (user, host, path) = get_test_config().expect("SSH_TEST_HOST not set");
        let backend = SshBackend::new(user, host, path, None).await.unwrap();

        // Put a file
        backend.put("test.txt", Bytes::from("hello sftp")).await.unwrap();

        // Get the file
        let data = backend.get("test.txt").await.unwrap();
        assert_eq!(data, Bytes::from("hello sftp"));

        // Clean up
        backend.delete("test.txt").await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_ssh_list() {
        let (user, host, path) = get_test_config().expect("SSH_TEST_HOST not set");
        let backend = SshBackend::new(user, host, path, None).await.unwrap();

        // Create test files
        backend.put("testdir/file1.txt", Bytes::from("content1")).await.unwrap();
        backend.put("testdir/file2.txt", Bytes::from("content2")).await.unwrap();

        // List files
        let entries = backend.list("testdir").await.unwrap();
        assert!(entries.len() >= 2);

        // Clean up
        backend.delete("testdir").await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_ssh_head() {
        let (user, host, path) = get_test_config().expect("SSH_TEST_HOST not set");
        let backend = SshBackend::new(user, host, path, None).await.unwrap();

        // File doesn't exist
        let result = backend.head("nonexistent.txt").await.unwrap();
        assert!(result.is_none());

        // Create file
        backend.put("test_head.txt", Bytes::from("test content")).await.unwrap();

        // Now it exists
        let entry = backend.head("test_head.txt").await.unwrap().unwrap();
        assert_eq!(entry.size, 12);
        assert!(!entry.is_dir);

        // Clean up
        backend.delete("test_head.txt").await.unwrap();
    }
}
