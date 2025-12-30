//! Buffer pooling for memory-efficient file transfers
//!
//! When transferring files, allocating a new buffer for every chunk causes allocator
//! pressure and fragmentation. This module provides a lock-free buffer pool that
//! recycles buffers to reduce allocations.
//!
//! # Example
//!
//! ```ignore
//! let pool = BufferPool::new(32, 16 * 1024 * 1024);  // 32 buffers of 16MB
//! let buf = pool.acquire();  // Get a buffer
//! // Use buffer...
//! drop(buf);  // Automatically returns to pool
//! ```

use bytes::{Bytes, BytesMut};
use crossbeam_queue::ArrayQueue;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default buffer size for chunk transfers (16MB)
pub const DEFAULT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// Default pool capacity (number of buffers)
pub const DEFAULT_POOL_CAPACITY: usize = 32;

/// A lock-free buffer pool for recycling byte buffers
///
/// The pool uses a bounded lock-free queue to store available buffers.
/// When the pool is empty, new buffers are allocated. When a buffer is
/// returned and the pool is full, it is dropped instead of being pooled.
#[derive(Clone)]
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
}

struct BufferPoolInner {
    /// Lock-free queue of available buffers
    queue: ArrayQueue<BytesMut>,
    /// Buffer size for this pool
    buffer_size: usize,
    /// Statistics: total buffers created
    created: AtomicUsize,
    /// Statistics: buffers reused from pool
    reused: AtomicUsize,
    /// Statistics: buffers returned to pool
    returned: AtomicUsize,
    /// Statistics: buffers dropped (pool full)
    dropped: AtomicUsize,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity and buffer size
    ///
    /// - `capacity`: Maximum number of buffers to keep in the pool
    /// - `buffer_size`: Size of each buffer in bytes
    pub fn new(capacity: usize, buffer_size: usize) -> Self {
        Self {
            inner: Arc::new(BufferPoolInner {
                queue: ArrayQueue::new(capacity),
                buffer_size,
                created: AtomicUsize::new(0),
                reused: AtomicUsize::new(0),
                returned: AtomicUsize::new(0),
                dropped: AtomicUsize::new(0),
            }),
        }
    }

    /// Create a buffer pool with default settings (32 x 16MB buffers)
    pub fn default_chunk_pool() -> Self {
        Self::new(DEFAULT_POOL_CAPACITY, DEFAULT_BUFFER_SIZE)
    }

    /// Acquire a buffer from the pool
    ///
    /// Returns a pooled buffer from the queue if available, otherwise allocates
    /// a new one. The buffer is zeroed and ready for use.
    pub fn acquire(&self) -> PooledBuffer {
        let buf = match self.inner.queue.pop() {
            Some(mut buf) => {
                self.inner.reused.fetch_add(1, Ordering::Relaxed);
                // Clear the buffer for reuse (keeps capacity)
                buf.clear();
                buf.resize(self.inner.buffer_size, 0);
                buf
            }
            None => {
                self.inner.created.fetch_add(1, Ordering::Relaxed);
                BytesMut::zeroed(self.inner.buffer_size)
            }
        };

        PooledBuffer {
            buf: Some(buf),
            pool: self.clone(),
        }
    }

    /// Acquire a buffer with a specific size
    ///
    /// If the requested size matches the pool's buffer size, uses the pool.
    /// Otherwise allocates a new buffer (not pooled).
    pub fn acquire_sized(&self, size: usize) -> PooledBuffer {
        if size == self.inner.buffer_size {
            self.acquire()
        } else if size <= self.inner.buffer_size {
            // Use a pooled buffer but only fill to requested size
            let mut pooled = self.acquire();
            pooled.truncate(size);
            pooled
        } else {
            // Size too large, allocate outside the pool
            self.inner.created.fetch_add(1, Ordering::Relaxed);
            PooledBuffer {
                buf: Some(BytesMut::zeroed(size)),
                pool: self.clone(),
            }
        }
    }

    /// Return a buffer to the pool
    fn return_buffer(&self, buf: BytesMut) {
        // Only pool buffers that match our size
        if buf.capacity() >= self.inner.buffer_size {
            match self.inner.queue.push(buf) {
                Ok(()) => {
                    self.inner.returned.fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    // Pool is full, drop the buffer
                    self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get the buffer size for this pool
    pub fn buffer_size(&self) -> usize {
        self.inner.buffer_size
    }

    /// Get pool statistics
    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            created: self.inner.created.load(Ordering::Relaxed),
            reused: self.inner.reused.load(Ordering::Relaxed),
            returned: self.inner.returned.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
            available: self.inner.queue.len(),
            capacity: self.inner.queue.capacity(),
        }
    }
}

/// Statistics about buffer pool usage
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    /// Total buffers created (allocated)
    pub created: usize,
    /// Buffers reused from pool
    pub reused: usize,
    /// Buffers returned to pool
    pub returned: usize,
    /// Buffers dropped (pool was full or wrong size)
    pub dropped: usize,
    /// Currently available buffers in pool
    pub available: usize,
    /// Maximum pool capacity
    pub capacity: usize,
}

impl BufferPoolStats {
    /// Calculate the reuse rate (0.0 to 1.0)
    pub fn reuse_rate(&self) -> f64 {
        let total = self.created + self.reused;
        if total == 0 {
            0.0
        } else {
            self.reused as f64 / total as f64
        }
    }

    /// Estimated memory saved by reusing buffers
    pub fn memory_saved(&self, buffer_size: usize) -> usize {
        self.reused * buffer_size
    }
}

/// A buffer acquired from a pool that returns to the pool on drop
pub struct PooledBuffer {
    buf: Option<BytesMut>,
    pool: BufferPool,
}

impl PooledBuffer {
    /// Freeze this buffer into immutable Bytes
    ///
    /// This consumes the pooled buffer and returns Bytes.
    /// The underlying memory is NOT returned to the pool.
    pub fn freeze(mut self) -> Bytes {
        self.buf.take().unwrap().freeze()
    }

    /// Freeze a portion of this buffer and return the rest to the pool
    ///
    /// Splits off the first `len` bytes as Bytes, returns the remainder to the pool.
    pub fn freeze_to(mut self, len: usize) -> Bytes {
        let mut buf = self.buf.take().unwrap();
        let frozen = buf.split_to(len).freeze();
        // Return remainder to pool
        self.pool.return_buffer(buf);
        frozen
    }

    /// Get the length of the buffer contents
    pub fn len(&self) -> usize {
        self.buf.as_ref().map(|b| b.len()).unwrap_or(0)
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Truncate the buffer to the given length
    pub fn truncate(&mut self, len: usize) {
        if let Some(ref mut buf) = self.buf {
            buf.truncate(len);
        }
    }

    /// Resize the buffer to the given length
    pub fn resize(&mut self, len: usize, value: u8) {
        if let Some(ref mut buf) = self.buf {
            buf.resize(len, value);
        }
    }
}

impl Deref for PooledBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().map(|b| b.as_ref()).unwrap_or(&[])
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().map(|b| b.as_mut()).unwrap_or(&mut [])
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buf) = self.buf.take() {
            self.pool.return_buffer(buf);
        }
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl AsMut<[u8]> for PooledBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

/// Global buffer pool for general chunk transfers
///
/// This provides a default pool that can be used throughout the application.
/// For specialized use cases, create custom BufferPool instances.
pub mod global {
    use super::*;
    use std::sync::OnceLock;

    static CHUNK_POOL: OnceLock<BufferPool> = OnceLock::new();

    /// Get the global chunk buffer pool (16MB buffers)
    pub fn chunk_pool() -> &'static BufferPool {
        CHUNK_POOL.get_or_init(BufferPool::default_chunk_pool)
    }

    /// Acquire a buffer from the global chunk pool
    pub fn acquire_chunk() -> PooledBuffer {
        chunk_pool().acquire()
    }

    /// Acquire a sized buffer from the global chunk pool
    pub fn acquire_chunk_sized(size: usize) -> PooledBuffer {
        chunk_pool().acquire_sized(size)
    }

    /// Get statistics from the global chunk pool
    pub fn chunk_pool_stats() -> BufferPoolStats {
        chunk_pool().stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new(4, 1024);

        // Acquire a buffer
        let buf1 = pool.acquire();
        assert_eq!(buf1.len(), 1024);

        let stats = pool.stats();
        assert_eq!(stats.created, 1);
        assert_eq!(stats.reused, 0);

        // Drop it to return to pool
        drop(buf1);

        let stats = pool.stats();
        assert_eq!(stats.returned, 1);
        assert_eq!(stats.available, 1);

        // Acquire again - should reuse
        let buf2 = pool.acquire();
        assert_eq!(buf2.len(), 1024);

        let stats = pool.stats();
        assert_eq!(stats.created, 1);
        assert_eq!(stats.reused, 1);
    }

    #[test]
    fn test_buffer_pool_full() {
        let pool = BufferPool::new(2, 1024);

        // Fill the pool
        let buf1 = pool.acquire();
        let buf2 = pool.acquire();
        let buf3 = pool.acquire();

        drop(buf1);
        drop(buf2);
        // Pool is now full (2 buffers)

        drop(buf3);
        // This one should be dropped

        let stats = pool.stats();
        assert_eq!(stats.returned, 2);
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.available, 2);
    }

    #[test]
    fn test_buffer_freeze() {
        let pool = BufferPool::new(4, 1024);

        let mut buf = pool.acquire();
        buf[0..5].copy_from_slice(b"hello");
        buf.truncate(5);

        let bytes = buf.freeze();
        assert_eq!(&bytes[..], b"hello");

        // Buffer was consumed, not returned to pool
        let stats = pool.stats();
        assert_eq!(stats.returned, 0);
    }

    #[test]
    fn test_sized_acquire() {
        let pool = BufferPool::new(4, 1024);

        // Smaller size uses pool buffer
        let buf = pool.acquire_sized(512);
        assert_eq!(buf.len(), 512);

        // Larger size allocates new buffer
        let big_buf = pool.acquire_sized(2048);
        assert_eq!(big_buf.len(), 2048);

        let stats = pool.stats();
        assert_eq!(stats.created, 2);
    }

    #[test]
    fn test_reuse_rate() {
        let pool = BufferPool::new(4, 1024);

        // 5 acquires, first creates, rest reuse
        for _ in 0..5 {
            let buf = pool.acquire();
            drop(buf);
        }

        let stats = pool.stats();
        assert_eq!(stats.created, 1);
        assert_eq!(stats.reused, 4);
        assert!((stats.reuse_rate() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_global_pool() {
        let buf = global::acquire_chunk();
        assert_eq!(buf.len(), DEFAULT_BUFFER_SIZE);

        drop(buf);

        let stats = global::chunk_pool_stats();
        assert!(stats.created >= 1);
    }

    #[test]
    fn test_concurrent_access() {
        use std::thread;

        let pool = BufferPool::new(8, 1024);
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let pool = pool.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let buf = pool.acquire();
                        // Simulate some work
                        std::hint::black_box(&buf[..]);
                        drop(buf);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let stats = pool.stats();
        // Total operations: 4 threads * 100 iterations = 400
        assert_eq!(stats.created + stats.reused, 400);
        // Should have significant reuse
        assert!(stats.reuse_rate() > 0.5);
    }
}
