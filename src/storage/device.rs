use anyhow::{anyhow, Result};
use bytes::Bytes;
use dashmap::DashSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::cache::{DiskCache, MemoryCache};
use crate::config::DeviceConfig;
use crate::storage::s3_backend::S3Backend;

/// A single block device backed by S3 with two-level caching.
///
/// Write path (write-behind):
///   1. Write block to L1 (memory) cache
///   2. Write block to L2 (disk) cache with dirty marker
///   3. Send block_id on dirty_tx channel
///   4. Acknowledge write to NBD client immediately
///   Background: flush worker reads from dirty_tx, uploads to S3, clears dirty marker
///
/// Read path:
///   L1 hit → return immediately
///   L2 hit → promote to L1 → return
///   S3 fetch → cache in L1 + L2 (clean) → return
#[derive(Clone)]
pub struct BlockDevice {
    pub config: Arc<DeviceConfig>,
    s3: S3Backend,
    l1: MemoryCache,
    l2: Option<DiskCache>,
    /// Set of block IDs currently queued/in-flight to S3
    dirty: Arc<DashSet<u64>>,
    /// Channel to enqueue dirty blocks for background upload
    dirty_tx: mpsc::Sender<u64>,
}

impl BlockDevice {
    pub async fn new(
        config: DeviceConfig,
        s3: S3Backend,
        l1: MemoryCache,
        l2: Option<DiskCache>,
    ) -> Result<(Self, mpsc::Receiver<u64>)> {
        let (dirty_tx, dirty_rx) = mpsc::channel::<u64>(65_536);
        let dirty = Arc::new(DashSet::new());

        let device = Self {
            config: Arc::new(config),
            s3,
            l1,
            l2,
            dirty,
            dirty_tx,
        };

        Ok((device, dirty_rx))
    }

    /// Return the S3 object key for a given block ID.
    #[inline]
    pub fn block_key(&self, block_id: u64) -> String {
        format!("{}/{:016x}", self.config.s3_prefix, block_id)
    }

    /// Block ID for a given byte offset.
    #[inline]
    pub fn block_id(&self, offset: u64) -> u64 {
        offset / self.config.block_size as u64
    }

    /// Read `length` bytes starting at `offset`.
    /// Handles reads spanning multiple blocks.
    pub async fn read(&self, offset: u64, length: u32) -> Result<Bytes> {
        let block_size = self.config.block_size as u64;
        let total = length as usize;
        let mut buf = Vec::with_capacity(total);

        let first_block = offset / block_size;
        let last_block = (offset + length as u64 - 1) / block_size;

        for bid in first_block..=last_block {
            let block_data = self.read_block(bid).await?;

            // Determine slice within this block
            let block_start = bid * block_size;
            let in_block_off = if bid == first_block {
                (offset - block_start) as usize
            } else {
                0
            };
            let remaining = total - buf.len();
            let available = block_size as usize - in_block_off;
            let take = remaining.min(available);

            buf.extend_from_slice(&block_data[in_block_off..in_block_off + take]);
        }

        Ok(Bytes::from(buf))
    }

    /// Write `data` bytes starting at `offset`.
    /// Handles writes spanning multiple blocks.
    pub async fn write(&self, offset: u64, data: Bytes) -> Result<()> {
        if self.config.read_only {
            return Err(anyhow!("device is read-only"));
        }

        let block_size = self.config.block_size as u64;
        let length = data.len() as u64;
        let first_block = offset / block_size;
        let last_block = (offset + length - 1) / block_size;

        for bid in first_block..=last_block {
            let block_start = bid * block_size;

            // If write is partial, we need the existing block for RMW
            let in_block_off = if bid == first_block {
                (offset - block_start) as usize
            } else {
                0
            };
            let data_off = if bid == first_block {
                0usize
            } else {
                ((bid - first_block) * block_size - (offset - first_block * block_size)) as usize
            };
            let take = (block_size as usize - in_block_off)
                .min(data.len() - data_off);

            let is_full_block = in_block_off == 0 && take == block_size as usize;

            let block_bytes = if is_full_block {
                data.slice(data_off..data_off + take)
            } else {
                // Read-modify-write for partial block
                let mut existing = self.read_block(bid).await?.to_vec();
                existing[in_block_off..in_block_off + take]
                    .copy_from_slice(&data[data_off..data_off + take]);
                Bytes::from(existing)
            };

            self.write_block(bid, block_bytes).await?;
        }

        Ok(())
    }

    /// Write a zero-filled range (WRITE_ZEROES / TRIM).
    pub async fn write_zeroes(&self, offset: u64, length: u32) -> Result<()> {
        let data = Bytes::from(vec![0u8; length as usize]);
        self.write(offset, data).await
    }

    /// Flush: wait until all dirty blocks are uploaded.
    /// Not strictly required with write-behind, but useful for fsync semantics.
    pub async fn flush(&self) -> Result<()> {
        // Drain the dirty set: spin-wait until it's empty.
        // A production implementation could use a condvar/watch channel.
        let mut waited = 0u64;
        while !self.dirty.is_empty() {
            sleep(Duration::from_millis(10)).await;
            waited += 10;
            if waited % 5000 == 0 {
                info!("flush waiting on {} dirty blocks…", self.dirty.len());
            }
        }
        Ok(())
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /// Fetch a single block (checking caches in order).
    async fn read_block(&self, block_id: u64) -> Result<Bytes> {
        // L1 hit?
        if let Some(data) = self.l1.get(block_id) {
            debug!("L1 hit block {:016x}", block_id);
            return Ok(data);
        }

        // L2 hit?
        if let Some(ref l2) = self.l2 {
            if let Some(data) = l2.get(block_id).await {
                debug!("L2 hit block {:016x}", block_id);
                self.l1.put(block_id, data.clone());
                return Ok(data);
            }
        }

        // S3 fetch
        debug!("S3 fetch block {:016x}", block_id);
        let key = self.block_key(block_id);
        let data = match self.s3.get_object(&key).await? {
            Some(d) => {
                // Validate size – S3 object might be truncated in edge cases
                if d.len() != self.config.block_size as usize {
                    warn!(
                        "block {:016x} S3 size mismatch: got {} expected {}",
                        block_id,
                        d.len(),
                        self.config.block_size
                    );
                    let mut padded = d.to_vec();
                    padded.resize(self.config.block_size as usize, 0);
                    Bytes::from(padded)
                } else {
                    d
                }
            }
            // Block doesn't exist yet → return zeros (sparse device)
            None => Bytes::from(vec![0u8; self.config.block_size as usize]),
        };

        // Populate caches (clean, already in S3)
        self.l1.put(block_id, data.clone());
        if let Some(ref l2) = self.l2 {
            if let Err(e) = l2.put_clean(block_id, &data).await {
                warn!("L2 put_clean block {:016x}: {}", block_id, e);
            }
        }

        Ok(data)
    }

    /// Write a single full block to caches and enqueue for S3 upload.
    async fn write_block(&self, block_id: u64, data: Bytes) -> Result<()> {
        // Update L1
        self.l1.put(block_id, data.clone());

        // Update L2 with dirty marker
        if let Some(ref l2) = self.l2 {
            l2.put_dirty(block_id, &data).await?;
        }

        // Mark dirty and enqueue
        self.dirty.insert(block_id);
        let _ = self.dirty_tx.send(block_id).await;

        Ok(())
    }

    /// Recover dirty blocks from L2 cache on startup (enqueue them for upload).
    pub async fn recover_dirty(&self) -> Result<()> {
        if let Some(ref l2) = self.l2 {
            let dirty = l2.dirty_blocks().await?;
            let count = dirty.len();
            for bid in dirty {
                self.dirty.insert(bid);
                let _ = self.dirty_tx.send(bid).await;
            }
            if count > 0 {
                info!("Recovered {} dirty blocks from L2 cache for device '{}'", count, self.config.name);
            }
        }
        Ok(())
    }

    /// Background flush worker: drains dirty_rx and uploads blocks to S3.
    /// Call this in a dedicated tokio task.
    pub async fn flush_worker(
        device: BlockDevice,
        mut dirty_rx: mpsc::Receiver<u64>,
        workers: usize,
    ) {
        use tokio::task::JoinSet;

        let device = Arc::new(device);
        let mut set: JoinSet<()> = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(workers));

        loop {
            // Drain as many dirty blocks as there are worker slots
            let mut block_id = match dirty_rx.recv().await {
                Some(id) => id,
                None => {
                    // Channel closed: wait for in-flight tasks and exit
                    while set.join_next().await.is_some() {}
                    break;
                }
            };

            loop {
                // Skip if we just processed this block (coalesce writes)
                // We re-check dirty marker from L2 instead
                let dev = Arc::clone(&device);
                let sem = Arc::clone(&semaphore);

                set.spawn(async move {
                    let permit = sem.acquire_owned().await.unwrap();
                    dev.upload_block(block_id).await;
                    drop(permit);
                });

                // Try to drain more without blocking
                match dirty_rx.try_recv() {
                    Ok(id) => block_id = id,
                    Err(_) => break,
                }
            }

            // Reap finished tasks
            while let Some(res) = set.try_join_next() {
                if let Err(e) = res {
                    error!("flush worker task panicked: {}", e);
                }
            }
        }
    }

    /// Upload a single block to S3. Called by flush_worker.
    async fn upload_block(&self, block_id: u64) {
        // If block was already cleaned by a concurrent worker, skip
        if !self.dirty.contains(&block_id) {
            // Check L2 dirty marker for ground truth
            if let Some(ref l2) = self.l2 {
                if !l2.is_dirty(block_id).await {
                    return;
                }
            } else {
                return;
            }
        }

        // Read data from L2 (or L1 if L2 not available)
        let data = if let Some(ref l2) = self.l2 {
            match l2.get(block_id).await {
                Some(d) => d,
                None => {
                    // Data evicted from L2 but L1 might have it
                    match self.l1.get(block_id) {
                        Some(d) => d,
                        None => {
                            warn!("flush_worker: block {:016x} not in L1 or L2, skipping", block_id);
                            self.dirty.remove(&block_id);
                            return;
                        }
                    }
                }
            }
        } else {
            match self.l1.get(block_id) {
                Some(d) => d,
                None => {
                    warn!("flush_worker: block {:016x} not in L1 (no L2), skipping", block_id);
                    self.dirty.remove(&block_id);
                    return;
                }
            }
        };

        let key = self.block_key(block_id);
        // put_object retries indefinitely until success or max_retries
        if let Err(e) = self.s3.put_object(&key, data).await {
            error!("flush_worker: S3 upload block {:016x} failed permanently: {}", block_id, e);
            // Re-enqueue so we keep trying
            let _ = self.dirty_tx.send(block_id).await;
            sleep(Duration::from_secs(5)).await;
            return;
        }

        // Clear dirty marker
        if let Some(ref l2) = self.l2 {
            if let Err(e) = l2.mark_clean(block_id).await {
                warn!("flush_worker: mark_clean block {:016x}: {}", block_id, e);
            }
        }
        self.dirty.remove(&block_id);
        debug!("flush_worker: block {:016x} → S3 OK", block_id);
    }
}
