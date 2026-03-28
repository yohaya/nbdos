#![allow(unused_imports)]
mod cache;
mod config;
mod nbd;
mod storage;

use anyhow::{Context, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::{Credentials, Region};
use clap::Parser;
use std::collections::HashMap;
use tokio::task::JoinSet;
use tracing::info;

use cache::{DiskCache, MemoryCache};
use config::{CacheConfig, Config, DeviceConfig};
use nbd::NbdServer;
use storage::{BlockDevice, S3Backend};

#[derive(Parser, Debug)]
#[command(name = "nbdos", about = "NBD server backed by S3-compatible object storage")]
struct Cli {
    /// Path to config file (YAML)
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let config_text =
        std::fs::read_to_string(&cli.config).with_context(|| format!("read config file '{}'", cli.config))?;
    let config: Config =
        serde_yaml::from_str(&config_text).with_context(|| "parse config YAML")?;

    info!("Starting nbdos with {} device(s)", config.devices.len());

    // ─── Build S3 client ──────────────────────────────────────────────────────

    let s3_cfg = &config.s3;

    let mut aws_cfg_builder = aws_config::defaults(BehaviorVersion::latest());

    if let (Some(ak), Some(sk)) = (&s3_cfg.access_key, &s3_cfg.secret_key) {
        let creds = Credentials::new(ak, sk, None, None, "nbdos-config");
        aws_cfg_builder = aws_cfg_builder.credentials_provider(creds);
    }

    aws_cfg_builder = aws_cfg_builder.region(RegionProviderChain::first_try(Region::new(
        s3_cfg.region.clone(),
    )));

    let aws_cfg = aws_cfg_builder.load().await;

    let mut s3_builder = aws_sdk_s3::config::Builder::from(&aws_cfg);

    if let Some(ref endpoint) = s3_cfg.endpoint {
        s3_builder = s3_builder.endpoint_url(endpoint);
    }

    if s3_cfg.force_path_style {
        s3_builder = s3_builder.force_path_style(true);
    }

    let s3_client = aws_sdk_s3::Client::from_conf(s3_builder.build());

    let s3_backend = S3Backend::new(
        s3_client,
        s3_cfg.bucket.clone(),
        s3_cfg.write_timeout_secs,
        s3_cfg.read_timeout_secs,
        s3_cfg.max_retries,
        s3_cfg.retry_delay_ms,
        s3_cfg.max_retry_delay_ms,
    );

    // ─── Build devices ────────────────────────────────────────────────────────

    let cache_cfg = &config.cache;
    let upload_workers = config.s3.upload_workers;
    let mut devices: HashMap<String, BlockDevice> = HashMap::new();
    let mut flush_workers: JoinSet<()> = JoinSet::new();

    for dev_cfg in config.devices {
        let (device, dirty_rx) =
            build_device(dev_cfg, s3_backend.clone(), cache_cfg).await?;
        let name = device.config.name.clone();

        device.recover_dirty().await?;

        let flush_device = device.clone();
        flush_workers.spawn(async move {
            BlockDevice::flush_worker(flush_device, dirty_rx, upload_workers).await;
        });

        devices.insert(name, device);
    }

    // ─── Start NBD server ─────────────────────────────────────────────────────

    let nbd = NbdServer::bind(
        &config.nbd.listen_addr,
        config.nbd.listen_port,
        devices,
    )
    .await?;

    // Run NBD server in foreground; flush workers run in background
    nbd.serve().await?;

    Ok(())
}

async fn build_device(
    dev_cfg: DeviceConfig,
    s3: S3Backend,
    cache_cfg: &CacheConfig,
) -> Result<(BlockDevice, tokio::sync::mpsc::Receiver<u64>)> {
    let block_size = dev_cfg.block_size as usize;
    let l1 = MemoryCache::new(cache_cfg.l1_max_bytes, block_size);

    let l2 = if let Some(ref l2_dir) = cache_cfg.l2_dir {
        Some(
            DiskCache::new(l2_dir, &dev_cfg.name, dev_cfg.block_size)
                .await
                .with_context(|| format!("init L2 cache for device '{}'", dev_cfg.name))?,
        )
    } else {
        None
    };

    info!(
        "Device '{}': size={} bytes, block_size={}, prefix='{}', read_only={}, L2={}",
        dev_cfg.name,
        dev_cfg.size_bytes,
        dev_cfg.block_size,
        dev_cfg.s3_prefix,
        dev_cfg.read_only,
        cache_cfg.l2_dir.as_deref().unwrap_or("disabled"),
    );

    BlockDevice::new(dev_cfg, s3, l1, l2).await
}
