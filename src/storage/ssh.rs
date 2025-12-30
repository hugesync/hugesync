//! SSH/SFTP storage backend using russh (pure Rust, cross-platform)
//!
//! This backend provides SFTP-based file transfers using the russh library,
//! which is a pure Rust SSH implementation that works across all platforms.
//!
//! ## Connection Pooling
//!
//! To enable concurrent transfers with `-j/--jobs`, this backend uses a connection
//! pool. Each concurrent operation gets its own SFTP connection, allowing true
//! parallelism instead of serializing all operations through a single connection.

use crate::bufpool::global as bufpool;
use crate::error::{Error, Result};
use crate::storage::ByteStream;
use crate::types::FileEntry;
use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use futures::StreamExt;
use russh::client::{self, Config, Handle, Handler};
use russh::keys::ssh_key::PublicKey;
use russh::ChannelMsg;
use russh_sftp::client::SftpSession;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore};

/// Escape a string for safe use in shell commands
fn shell_escape(s: &str) -> String {
    // Use single quotes and escape any single quotes within
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Default maximum connections in the pool
const DEFAULT_MAX_CONNECTIONS: usize = 8;

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
    handle: Handle<SshHandler>,
}

/// Connection parameters for creating new connections
#[derive(Clone)]
struct ConnectionParams {
    user: String,
    host: String,
    port: u16,
}

/// A pooled SFTP connection that returns to the pool when dropped
pub struct PooledConnection {
    conn: Option<SftpConnection>,
    pool: Arc<SshConnectionPool>,
}

impl PooledConnection {
    /// Get the SFTP session
    pub fn session(&self) -> &SftpSession {
        &self.conn.as_ref().unwrap().session
    }

    /// Get the SSH handle for command execution
    fn handle(&self) -> &Handle<SshHandler> {
        &self.conn.as_ref().unwrap().handle
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.return_connection(conn);
        }
    }
}

/// SSH connection pool for concurrent transfers
struct SshConnectionPool {
    /// Available connections
    available: ArrayQueue<SftpConnection>,
    /// Semaphore to limit total connections
    semaphore: Semaphore,
    /// Connection parameters for creating new connections
    params: ConnectionParams,
    /// Number of active connections (created but not yet returned)
    active_count: AtomicUsize,
    /// Total connections created (for stats)
    total_created: AtomicUsize,
}

impl SshConnectionPool {
    /// Create a new connection pool
    fn new(params: ConnectionParams, max_connections: usize) -> Self {
        Self {
            available: ArrayQueue::new(max_connections),
            semaphore: Semaphore::new(max_connections),
            params,
            active_count: AtomicUsize::new(0),
            total_created: AtomicUsize::new(0),
        }
    }

    /// Acquire a connection from the pool
    async fn acquire(self: &Arc<Self>) -> Result<PooledConnection> {
        // Wait for a permit (limits total concurrent connections)
        let _permit = self.semaphore.acquire().await.map_err(|_| Error::Ssh {
            message: "Connection pool closed".to_string(),
        })?;

        // We have a permit, so we're allowed to have a connection
        // Try to get an existing connection first
        if let Some(conn) = self.available.pop() {
            return Ok(PooledConnection {
                conn: Some(conn),
                pool: Arc::clone(self),
            });
        }

        // No available connection, create a new one
        self.active_count.fetch_add(1, Ordering::Relaxed);
        self.total_created.fetch_add(1, Ordering::Relaxed);

        let conn = create_sftp_connection(&self.params).await?;

        tracing::debug!(
            total_created = self.total_created.load(Ordering::Relaxed),
            active = self.active_count.load(Ordering::Relaxed),
            "Created new SSH connection"
        );

        Ok(PooledConnection {
            conn: Some(conn),
            pool: Arc::clone(self),
        })
    }

    /// Return a connection to the pool
    fn return_connection(&self, conn: SftpConnection) {
        // Try to return to pool
        if self.available.push(conn).is_err() {
            // Pool is full, drop the connection
            tracing::debug!("SSH connection pool full, dropping connection");
        }
        self.active_count.fetch_sub(1, Ordering::Relaxed);

        // Release the semaphore permit
        self.semaphore.add_permits(1);
    }

    /// Get pool statistics
    #[allow(dead_code)]
    fn stats(&self) -> (usize, usize, usize) {
        (
            self.available.len(),
            self.active_count.load(Ordering::Relaxed),
            self.total_created.load(Ordering::Relaxed),
        )
    }
}

/// Create a new SFTP connection
async fn create_sftp_connection(params: &ConnectionParams) -> Result<SftpConnection> {
    let config = Arc::new(Config::default());

    // Connect to SSH server
    let mut handle = client::connect(config, (params.host.as_str(), params.port), SshHandler)
        .await
        .map_err(|e| Error::Ssh {
            message: format!("Failed to connect to {}:{}: {}", params.host, params.port, e),
        })?;

    // Authenticate
    let authenticated = try_authenticate(&mut handle, &params.user).await?;
    if !authenticated {
        return Err(Error::Ssh {
            message: format!("Authentication failed for {}@{}", params.user, params.host),
        });
    }

    // Open SFTP channel
    let channel = handle.channel_open_session().await.map_err(|e| Error::Ssh {
        message: format!("Failed to open SSH channel: {}", e),
    })?;

    channel.request_subsystem(true, "sftp").await.map_err(|e| Error::Ssh {
        message: format!("Failed to request SFTP subsystem: {}", e),
    })?;

    let session = SftpSession::new(channel.into_stream()).await.map_err(|e| Error::Ssh {
        message: format!("Failed to initialize SFTP session: {}", e),
    })?;

    Ok(SftpConnection { session, handle })
}

/// SSH/SFTP storage backend using pure Rust russh library
///
/// This backend uses a connection pool to support concurrent transfers.
/// The pool size can be configured to match the `-j/--jobs` parameter.
#[derive(Clone)]
pub struct SshBackend {
    /// Connection pool for concurrent operations
    pool: Arc<SshConnectionPool>,
    /// Remote base path
    path: String,
}

impl SshBackend {
    /// Create a new SSH backend with default pool size
    pub async fn new(
        user: Option<String>,
        host: String,
        path: String,
        port: Option<u16>,
    ) -> Result<Self> {
        Self::with_pool_size(user, host, path, port, DEFAULT_MAX_CONNECTIONS).await
    }

    /// Create a new SSH backend with custom pool size
    ///
    /// The pool size should match the `-j/--jobs` parameter for optimal concurrency.
    pub async fn with_pool_size(
        user: Option<String>,
        host: String,
        path: String,
        port: Option<u16>,
        max_connections: usize,
    ) -> Result<Self> {
        let port = port.unwrap_or(22);
        let user = user.unwrap_or_else(|| whoami::username());

        tracing::info!(
            host = %host,
            user = %user,
            port = port,
            path = %path,
            max_connections = max_connections,
            "Initializing SSH connection pool"
        );

        let params = ConnectionParams {
            user: user.clone(),
            host: host.clone(),
            port,
        };

        // Create the first connection to validate credentials
        let initial_conn = create_sftp_connection(&params).await?;
        tracing::info!("SSH authentication successful, SFTP session established");

        // Create the pool
        let pool = Arc::new(SshConnectionPool::new(params, max_connections));

        // Return the initial connection to the pool
        pool.return_connection(initial_conn);

        Ok(Self { pool, path })
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
    ///
    /// Uses a single remote `find` command to scan the entire directory tree,
    /// avoiding multiple SFTP round-trips. Falls back to SFTP READDIR if the
    /// remote command fails (e.g., on Windows SSH servers).
    pub async fn list(&self, prefix: &str) -> Result<Vec<FileEntry>> {
        let remote_path = self.resolve(prefix);

        // Try batch scan first (single remote command)
        match self.list_batch(&remote_path).await {
            Ok(entries) => {
                tracing::debug!(count = entries.len(), path = %remote_path, "Listed files via remote find");
                return Ok(entries);
            }
            Err(e) => {
                tracing::debug!(error = %e, "Remote find failed, falling back to SFTP readdir");
            }
        }

        // Fallback to SFTP-based listing (multiple round-trips)
        let conn = self.pool.acquire().await?;
        let mut entries = Vec::new();
        self.list_recursive(conn.session(), &remote_path, &remote_path, &mut entries).await?;

        tracing::debug!(count = entries.len(), path = %remote_path, "Listed files via SFTP readdir");
        Ok(entries)
    }

    /// Batch list using a single remote `find` command
    ///
    /// Executes `find` on the remote server and parses the output.
    /// This is much faster than SFTP READDIR for large directory trees
    /// as it requires only one network round-trip.
    async fn list_batch(&self, remote_path: &str) -> Result<Vec<FileEntry>> {
        // Use find with -printf to get all file info in one command
        // Format: type\tsize\tmtime\tmode\tpath
        // %y = file type (f=file, d=directory, l=symlink)
        // %s = size in bytes
        // %T@ = modification time as Unix timestamp
        // %m = permissions in octal
        // %P = path relative to starting point
        let cmd = format!(
            "find {} -printf '%y\\t%s\\t%T@\\t%m\\t%P\\n' 2>/dev/null",
            shell_escape(remote_path)
        );

        let output = self.exec_command(&cmd).await?;

        let mut entries = Vec::new();

        for line in output.lines() {
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.splitn(5, '\t').collect();
            if parts.len() < 5 {
                continue;
            }

            let file_type = parts[0];
            let size: u64 = parts[1].parse().unwrap_or(0);
            let mtime_secs: f64 = parts[2].parse().unwrap_or(0.0);
            let mode: u32 = u32::from_str_radix(parts[3], 8).unwrap_or(0);
            let path = parts[4];

            // Skip empty paths (the root directory itself)
            if path.is_empty() {
                continue;
            }

            let is_dir = file_type == "d";
            let mtime = if mtime_secs > 0.0 {
                Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(mtime_secs))
            } else {
                None
            };

            entries.push(FileEntry {
                path: path.into(),
                size,
                mtime,
                is_dir,
                mode: Some(mode),
                etag: None,
            });
        }

        Ok(entries)
    }

    /// Execute a command on the remote server and return stdout
    async fn exec_command(&self, cmd: &str) -> Result<String> {
        let conn = self.pool.acquire().await?;
        let handle = conn.handle();

        // Open a new channel for command execution
        let mut channel = handle.channel_open_session().await.map_err(|e| Error::Ssh {
            message: format!("Failed to open exec channel: {}", e),
        })?;

        // Execute the command
        channel.exec(true, cmd).await.map_err(|e| Error::Ssh {
            message: format!("Failed to execute command: {}", e),
        })?;

        // Read all output using channel.wait()
        let mut output = Vec::new();

        loop {
            match channel.wait().await {
                Some(ChannelMsg::Data { data }) => {
                    output.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExtendedData { data, ext: 1 }) => {
                    // stderr - log but don't include in output
                    tracing::trace!("Remote stderr: {}", String::from_utf8_lossy(&data));
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    if exit_status != 0 {
                        tracing::debug!(exit_status, "Remote command exited with non-zero status");
                    }
                }
                Some(ChannelMsg::Eof) => {
                    // No more data
                    break;
                }
                Some(ChannelMsg::Close) => {
                    break;
                }
                None => {
                    // Channel closed
                    break;
                }
                _ => {}
            }
        }

        String::from_utf8(output).map_err(|e| Error::Ssh {
            message: format!("Invalid UTF-8 in command output: {}", e),
        })
    }

    /// Recursively list directory contents via SFTP (fallback method)
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
                path: relative_path.as_str().into(),
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
        let conn = self.pool.acquire().await?;

        match conn.session().metadata(&remote_path).await {
            Ok(attrs) => {
                let is_dir = attrs.is_dir();
                let size = attrs.size.unwrap_or(0);
                let mtime = attrs.mtime.map(|t| {
                    std::time::UNIX_EPOCH + std::time::Duration::from_secs(t as u64)
                });

                Ok(Some(FileEntry {
                    path: path.into(),
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
        let conn = self.pool.acquire().await?;

        let data = conn.session().read(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to read file {}: {}", remote_path, e),
        })?;

        Ok(Bytes::from(data))
    }

    /// Read a file as a stream of chunks (memory-efficient for large files)
    ///
    /// Reads the file in chunks using SFTP seek/read operations to avoid
    /// loading the entire file into memory at once. The connection is held
    /// from the pool for the duration of the stream.
    pub async fn get_stream(&self, path: &str) -> Result<ByteStream> {
        use futures::stream;

        const CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16MB chunks

        let remote_path = self.resolve(path);
        let conn = self.pool.acquire().await?;

        // Get file size first
        let attrs = conn.session().metadata(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to stat file {}: {}", remote_path, e),
        })?;
        let file_size = attrs.size.unwrap_or(0) as usize;

        // For small files, just read the whole thing and release connection
        if file_size < CHUNK_SIZE {
            let data = conn.session().read(&remote_path).await.map_err(|e| Error::Ssh {
                message: format!("Failed to read file {}: {}", remote_path, e),
            })?;
            drop(conn); // Return connection to pool
            return Ok(Box::pin(stream::once(async { Ok(Bytes::from(data)) })));
        }

        // For large files, open the file and stream chunks on-demand
        let file = conn.session().open(&remote_path).await.map_err(|e| Error::Ssh {
            message: format!("Failed to open file {}: {}", remote_path, e),
        })?;

        // Wrap file in Arc<Mutex> for shared ownership with stream
        let file = Arc::new(Mutex::new(file));
        let remote_path_clone = remote_path.clone();

        // Include the pooled connection in the stream state so it's held for the duration
        // When the stream is dropped, the connection returns to the pool
        let stream = stream::unfold(
            (file, 0usize, file_size, remote_path_clone, Some(conn)),
            move |(file, offset, total_size, path, conn)| async move {
                if offset >= total_size {
                    return None;
                }

                let to_read = std::cmp::min(CHUNK_SIZE, total_size - offset);
                // Use buffer pool to reduce allocations
                let mut buf = bufpool::acquire_chunk_sized(to_read);

                let mut file_guard = file.lock().await;

                // Seek to position
                if let Err(e) = file_guard.seek(std::io::SeekFrom::Start(offset as u64)).await {
                    return Some((
                        Err(Error::Ssh {
                            message: format!("Failed to seek in file {}: {}", path, e),
                        }),
                        (file.clone(), total_size, total_size, path, conn), // Force end
                    ));
                }

                // Read chunk
                match file_guard.read_exact(&mut buf[..to_read]).await {
                    Ok(_) => {
                        drop(file_guard);
                        Some((Ok(buf.freeze()), (file, offset + to_read, total_size, path, conn)))
                    }
                    Err(_) => None, // EOF or error
                }
            },
        );

        Ok(Box::pin(stream))
    }

    /// Read a range of bytes from a file
    ///
    /// Uses SFTP seek to read only the requested byte range, avoiding
    /// loading the entire file into memory. Uses buffer pooling to reduce allocations.
    pub async fn get_range(&self, path: &str, start: u64, end: u64) -> Result<Bytes> {
        let remote_path = self.resolve(path);
        let conn = self.pool.acquire().await?;

        // Open file for reading
        let mut file = conn.session().open(&remote_path).await.map_err(|e| Error::Ssh {
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
        // Use buffer pool to reduce allocations
        let mut buf = bufpool::acquire_chunk_sized(len);

        // Read the exact range
        match file.read_exact(&mut buf[..len]).await {
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

        let conn = self.pool.acquire().await?;

        conn.session().write(&remote_path, &data).await.map_err(|e| Error::Ssh {
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

        let conn = self.pool.acquire().await?;

        // Create/truncate the file and get a write handle
        let mut file = conn.session().create(&remote_path).await.map_err(|e| Error::Ssh {
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
        let conn = self.pool.acquire().await?;

        // Check if it's a directory
        match conn.session().metadata(&remote_path).await {
            Ok(attrs) if attrs.is_dir() => {
                // Recursively delete directory contents first
                self.delete_dir_contents(conn.session(), &remote_path).await?;

                // Remove the directory itself
                conn.session().remove_dir(&remote_path).await.map_err(|e| Error::Ssh {
                    message: format!("Failed to remove directory {}: {}", remote_path, e),
                })?;
            }
            Ok(_) => {
                // It's a file
                conn.session().remove_file(&remote_path).await.map_err(|e| Error::Ssh {
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
        let conn = self.pool.acquire().await?;

        // Split path into components and create each level
        let mut current = String::new();
        for component in path.split('/').filter(|c| !c.is_empty()) {
            current = if current.is_empty() {
                format!("/{}", component)
            } else {
                format!("{}/{}", current, component)
            };

            // Check if directory exists
            match conn.session().metadata(&current).await {
                Ok(attrs) if attrs.is_dir() => continue,
                Ok(_) => {
                    return Err(Error::Ssh {
                        message: format!("Path exists but is not a directory: {}", current),
                    });
                }
                Err(_) => {
                    // Directory doesn't exist, create it
                    conn.session().create_dir(&current).await.map_err(|e| Error::Ssh {
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
