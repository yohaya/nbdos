use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub nbd: NbdConfig,
    pub s3: S3Config,
    pub cache: CacheConfig,
    pub devices: Vec<DeviceConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NbdConfig {
    /// Address to listen on, e.g. "0.0.0.0"
    pub listen_addr: String,
    /// TCP port for NBD (default 10809)
    pub listen_port: u16,
}

impl Default for NbdConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0".into(),
            listen_port: 10809,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct S3Config {
    /// Custom S3-compatible endpoint URL (e.g. MinIO, Ceph RGW)
    pub endpoint: Option<String>,
    /// AWS region or placeholder for S3-compatible storage
    pub region: String,
    /// Bucket name
    pub bucket: String,
    /// Access key (falls back to env AWS_ACCESS_KEY_ID)
    pub access_key: Option<String>,
    /// Secret key (falls back to env AWS_SECRET_ACCESS_KEY)
    pub secret_key: Option<String>,
    /// How long to wait for a write to S3 before giving up and retrying (seconds)
    pub write_timeout_secs: u64,
    /// How long to wait for a read from S3 (seconds)
    pub read_timeout_secs: u64,
    /// Max number of retries for S3 operations
    pub max_retries: u32,
    /// Base delay between retries (ms), doubles each retry up to max_retry_delay_ms
    pub retry_delay_ms: u64,
    /// Maximum retry delay cap (ms)
    pub max_retry_delay_ms: u64,
    /// Number of concurrent S3 upload workers
    pub upload_workers: usize,
    /// Use path-style S3 URLs (required for MinIO/Ceph)
    pub force_path_style: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: "us-east-1".into(),
            bucket: "nbdos".into(),
            access_key: None,
            secret_key: None,
            write_timeout_secs: 300,
            read_timeout_secs: 60,
            max_retries: u32::MAX,
            retry_delay_ms: 500,
            max_retry_delay_ms: 30_000,
            upload_workers: 8,
            force_path_style: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    /// L1 in-memory cache size in bytes
    pub l1_max_bytes: usize,
    /// L2 disk cache directory (local fast SSD)
    pub l2_dir: Option<String>,
    /// L2 disk cache max size in bytes (None = unlimited)
    pub l2_max_bytes: Option<u64>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            l1_max_bytes: 512 * 1024 * 1024, // 512 MB
            l2_dir: None,
            l2_max_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeviceConfig {
    /// Export name used by NBD clients (e.g. "myvolume")
    pub name: String,
    /// Total device size in bytes
    pub size_bytes: u64,
    /// Block size in bytes (default 4096)
    #[serde(default = "default_block_size")]
    pub block_size: u32,
    /// S3 key prefix for this device (e.g. "volumes/vol0")
    pub s3_prefix: String,
    /// Whether to mount read-only
    #[serde(default)]
    pub read_only: bool,
    /// Max seconds to wait for in-flight S3 writes before acknowledging to NBD client
    /// (0 = write-behind: always acknowledge immediately after L2 write)
    #[serde(default)]
    pub write_ack_timeout_secs: u64,
}

fn default_block_size() -> u32 {
    4096
}
