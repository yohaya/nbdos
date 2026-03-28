use anyhow::{Context, Result};
use bytes::Bytes;
use std::path::PathBuf;
use tokio::fs;

/// L2 on-disk block cache with dirty-tracking for write-behind.
///
/// Layout under `base_dir/{device_name}/`:
///   data/{block_id:016x}       — cached block data
///   dirty/{block_id:016x}      — zero-byte marker: block is dirty (not yet in S3)
#[derive(Clone, Debug)]
pub struct DiskCache {
    data_dir: PathBuf,
    dirty_dir: PathBuf,
    pub block_size: u32,
}

impl DiskCache {
    pub async fn new(base_dir: &str, device_name: &str, block_size: u32) -> Result<Self> {
        let base = PathBuf::from(base_dir).join(device_name);
        let data_dir = base.join("data");
        let dirty_dir = base.join("dirty");

        fs::create_dir_all(&data_dir)
            .await
            .with_context(|| format!("create disk cache data dir {:?}", data_dir))?;
        fs::create_dir_all(&dirty_dir)
            .await
            .with_context(|| format!("create disk cache dirty dir {:?}", dirty_dir))?;

        Ok(Self { data_dir, dirty_dir, block_size })
    }

    fn data_path(&self, block_id: u64) -> PathBuf {
        self.data_dir.join(format!("{:016x}", block_id))
    }

    fn dirty_path(&self, block_id: u64) -> PathBuf {
        self.dirty_dir.join(format!("{:016x}", block_id))
    }

    /// Read a block from L2 cache. Returns None if not cached.
    pub async fn get(&self, block_id: u64) -> Option<Bytes> {
        let path = self.data_path(block_id);
        match fs::read(&path).await {
            Ok(data) => Some(Bytes::from(data)),
            Err(_) => None,
        }
    }

    /// Write a block to L2 cache and mark it as dirty.
    pub async fn put_dirty(&self, block_id: u64, data: &Bytes) -> Result<()> {
        let data_path = self.data_path(block_id);
        let dirty_path = self.dirty_path(block_id);

        // Write data atomically via a temp file
        let tmp = data_path.with_extension("tmp");
        fs::write(&tmp, data.as_ref())
            .await
            .with_context(|| format!("write block {:016x} to disk cache", block_id))?;
        fs::rename(&tmp, &data_path)
            .await
            .with_context(|| format!("rename block {:016x} tmp file", block_id))?;

        // Create dirty marker (zero-byte file)
        fs::write(&dirty_path, b"")
            .await
            .with_context(|| format!("create dirty marker for block {:016x}", block_id))?;

        Ok(())
    }

    /// Write a block to L2 cache as clean (already in S3).
    pub async fn put_clean(&self, block_id: u64, data: &Bytes) -> Result<()> {
        let data_path = self.data_path(block_id);
        let tmp = data_path.with_extension("tmp");
        fs::write(&tmp, data.as_ref()).await?;
        fs::rename(&tmp, &data_path).await?;
        Ok(())
    }

    /// Mark a block as clean (successfully written to S3).
    pub async fn mark_clean(&self, block_id: u64) -> Result<()> {
        let dirty_path = self.dirty_path(block_id);
        if dirty_path.exists() {
            fs::remove_file(&dirty_path).await?;
        }
        Ok(())
    }

    /// Collect all dirty block IDs (for startup recovery and background flusher).
    pub async fn dirty_blocks(&self) -> Result<Vec<u64>> {
        let mut dirty = Vec::new();
        let mut entries = fs::read_dir(&self.dirty_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(name) = entry.file_name().into_string() {
                if let Ok(id) = u64::from_str_radix(&name, 16) {
                    dirty.push(id);
                }
            }
        }
        Ok(dirty)
    }

    /// Check whether a block has a dirty marker.
    pub async fn is_dirty(&self, block_id: u64) -> bool {
        self.dirty_path(block_id).exists()
    }

    /// Remove cached data for a block (eviction).
    pub async fn evict(&self, block_id: u64) -> Result<()> {
        let _ = fs::remove_file(self.data_path(block_id)).await;
        Ok(())
    }
}
