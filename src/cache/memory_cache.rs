use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// L1 in-memory LRU block cache, shared across all connections.
#[derive(Clone)]
pub struct MemoryCache {
    inner: Arc<Mutex<LruCache<u64, Bytes>>>,
    capacity: usize,
    block_size: usize,
}

impl MemoryCache {
    /// Create a new memory cache.
    /// `max_bytes`: total byte budget for cached blocks.
    /// `block_size`: size of each block in bytes.
    pub fn new(max_bytes: usize, block_size: usize) -> Self {
        let max_blocks = (max_bytes / block_size).max(1);
        let capacity = NonZeroUsize::new(max_blocks).unwrap();
        Self {
            inner: Arc::new(Mutex::new(LruCache::new(capacity))),
            capacity: max_blocks,
            block_size,
        }
    }

    pub fn get(&self, block_id: u64) -> Option<Bytes> {
        self.inner.lock().get(&block_id).cloned()
    }

    pub fn put(&self, block_id: u64, data: Bytes) {
        self.inner.lock().put(block_id, data);
    }

    pub fn invalidate(&self, block_id: u64) {
        self.inner.lock().pop(&block_id);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn approx_bytes(&self) -> usize {
        self.inner.lock().len() * self.block_size
    }
}
