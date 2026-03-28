use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use super::protocol::*;
use crate::storage::BlockDevice;

/// Handles a single NBD client connection: negotiation + transmission.
pub struct NbdHandler {
    stream: TcpStream,
    devices: Arc<HashMap<String, BlockDevice>>,
}

impl NbdHandler {
    pub fn new(stream: TcpStream, devices: Arc<HashMap<String, BlockDevice>>) -> Self {
        Self { stream, devices }
    }

    pub async fn run(mut self) -> Result<()> {
        let peer = self.stream.peer_addr()?;
        info!("NBD client connected: {}", peer);

        let device = match self.negotiate().await {
            Ok(d) => d,
            Err(e) => {
                warn!("NBD negotiation failed for {}: {}", peer, e);
                return Err(e);
            }
        };

        info!(
            "NBD client {} entered transmission for device '{}'",
            peer, device.config.name
        );

        if let Err(e) = self.transmission(device).await {
            info!("NBD client {} disconnected: {}", peer, e);
        }

        Ok(())
    }

    // ─── Negotiation phase ────────────────────────────────────────────────────

    async fn negotiate(&mut self) -> Result<BlockDevice> {
        // Step 1: Send server greeting
        // NBD_MAGIC + IHAVEOPT + handshake flags
        let mut greeting = BytesMut::with_capacity(18);
        greeting.put_u64(NBD_MAGIC);
        greeting.put_u64(NBD_IHAVEOPT);
        greeting.put_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES);
        self.write_all(&greeting).await?;

        // Step 2: Read client flags
        let client_flags = self.read_u32().await?;
        if client_flags & NBD_FLAG_C_FIXED_NEWSTYLE == 0 {
            return Err(anyhow!("client does not support fixed newstyle negotiation"));
        }
        let no_zeroes = client_flags & NBD_FLAG_C_NO_ZEROES != 0;

        // Step 3: Option negotiation loop
        loop {
            // Read: option magic (8), option type (4), data length (4)
            let magic = self.read_u64().await?;
            if magic != NBD_IHAVEOPT {
                return Err(anyhow!("expected IHAVEOPT magic, got {:016x}", magic));
            }
            let opt = self.read_u32().await?;
            let data_len = self.read_u32().await?;

            if data_len > 64 * 1024 {
                return Err(anyhow!("option data too large: {} bytes", data_len));
            }
            let opt_data = self.read_exact(data_len as usize).await?;

            match opt {
                NBD_OPT_EXPORT_NAME => {
                    let name = String::from_utf8_lossy(&opt_data).to_string();
                    return self
                        .handle_opt_export_name(&name, no_zeroes)
                        .await;
                }
                NBD_OPT_GO | NBD_OPT_INFO => {
                    match self.handle_opt_go(opt, &opt_data).await {
                        Ok(Some(device)) => return Ok(device),
                        Ok(None) => continue, // info only, continue loop
                        Err(e) => return Err(e),
                    }
                }
                NBD_OPT_LIST => {
                    self.handle_opt_list(opt).await?;
                }
                NBD_OPT_ABORT => {
                    self.send_option_reply(opt, NBD_REP_ACK, &[]).await?;
                    return Err(anyhow!("client sent NBD_OPT_ABORT"));
                }
                _ => {
                    // Unsupported option
                    self.send_option_reply(opt, NBD_REP_ERR_UNSUP, b"unsupported option")
                        .await?;
                }
            }
        }
    }

    async fn handle_opt_export_name(
        &mut self,
        name: &str,
        no_zeroes: bool,
    ) -> Result<BlockDevice> {
        let device = self.find_device(name)?;

        // Send: export size (8) + transmission flags (2)
        let tx_flags = self.transmission_flags(&device);
        let mut resp = BytesMut::with_capacity(10 + if no_zeroes { 0 } else { 124 });
        resp.put_u64(device.config.size_bytes);
        resp.put_u16(tx_flags);
        if !no_zeroes {
            resp.put_bytes(0, 124);
        }
        self.write_all(&resp).await?;

        Ok(device)
    }

    async fn handle_opt_go(
        &mut self,
        opt: u32,
        data: &[u8],
    ) -> Result<Option<BlockDevice>> {
        if data.len() < 4 {
            self.send_option_reply(opt, NBD_REP_ERR_INVALID, b"bad option data").await?;
            return Ok(None);
        }
        let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + name_len {
            self.send_option_reply(opt, NBD_REP_ERR_INVALID, b"name truncated").await?;
            return Ok(None);
        }
        let name = String::from_utf8_lossy(&data[4..4 + name_len]).to_string();

        // info_requests follow: u16 count + u16 list — we ignore them for now
        let name = if name.is_empty() {
            // Default export
            self.devices.keys().next().cloned().unwrap_or_default()
        } else {
            name
        };

        let device = match self.find_device(&name) {
            Ok(d) => d,
            Err(_) => {
                self.send_option_reply(opt, NBD_REP_ERR_UNKNOWN, b"unknown export").await?;
                return Ok(None);
            }
        };

        let tx_flags = self.transmission_flags(&device);

        // Send NBD_INFO_EXPORT
        let mut info = BytesMut::with_capacity(12);
        info.put_u16(NBD_INFO_EXPORT);
        info.put_u64(device.config.size_bytes);
        info.put_u16(tx_flags);
        self.send_option_reply(opt, NBD_REP_INFO, &info).await?;

        // Send NBD_INFO_BLOCK_SIZE
        let mut bs_info = BytesMut::with_capacity(14);
        bs_info.put_u16(NBD_INFO_BLOCK_SIZE);
        bs_info.put_u32(512);                            // minimum block size
        bs_info.put_u32(device.config.block_size);       // preferred block size
        bs_info.put_u32(device.config.block_size);       // maximum block size
        self.send_option_reply(opt, NBD_REP_INFO, &bs_info).await?;

        // ACK
        self.send_option_reply(opt, NBD_REP_ACK, &[]).await?;

        if opt == NBD_OPT_GO {
            Ok(Some(device))
        } else {
            Ok(None) // NBD_OPT_INFO: just info, don't start transmission
        }
    }

    async fn handle_opt_list(&mut self, opt: u32) -> Result<()> {
        // Collect names first to avoid holding an immutable borrow while calling &mut self methods
        let names: Vec<String> = self.devices.keys().cloned().collect();
        for name in names {
            let name_bytes = name.as_bytes();
            let mut entry = BytesMut::with_capacity(4 + name_bytes.len());
            entry.put_u32(name_bytes.len() as u32);
            entry.put_slice(name_bytes);
            self.send_option_reply(opt, NBD_REP_SERVER, &entry).await?;
        }
        self.send_option_reply(opt, NBD_REP_ACK, &[]).await?;
        Ok(())
    }

    async fn send_option_reply(&mut self, opt: u32, reply_type: u32, data: &[u8]) -> Result<()> {
        let mut buf = BytesMut::with_capacity(20 + data.len());
        buf.put_u64(NBD_REP_MAGIC);
        buf.put_u32(opt);
        buf.put_u32(reply_type);
        buf.put_u32(data.len() as u32);
        buf.put_slice(data);
        self.write_all(&buf).await
    }

    fn find_device(&self, name: &str) -> Result<BlockDevice> {
        self.devices
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("unknown device '{}'", name))
    }

    fn transmission_flags(&self, device: &BlockDevice) -> u16 {
        let mut flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM;
        if device.config.read_only {
            flags |= NBD_FLAG_READ_ONLY;
        }
        flags
    }

    // ─── Transmission phase ───────────────────────────────────────────────────

    async fn transmission(&mut self, device: BlockDevice) -> Result<()> {
        loop {
            // Read request header: magic(4) + flags(2) + type(2) + handle(8) + offset(8) + length(4) = 28 bytes
            let hdr = self.read_exact(28).await?;
            let mut cur = hdr.as_ref();

            let magic = cur.get_u32();
            if magic != NBD_REQUEST_MAGIC {
                return Err(anyhow!("bad request magic: {:08x}", magic));
            }
            let flags = cur.get_u16();
            let cmd = cur.get_u16();
            let handle = cur.get_u64();
            let offset = cur.get_u64();
            let length = cur.get_u32();

            debug!(
                "NBD cmd={} flags={:#06x} handle={} offset={} length={}",
                cmd, flags, handle, offset, length
            );

            match cmd {
                NBD_CMD_READ => {
                    let result = device.read(offset, length).await;
                    match result {
                        Ok(data) => {
                            self.send_reply(handle, NBD_E_OK, Some(data)).await?;
                        }
                        Err(e) => {
                            error!("read error at offset {}: {}", offset, e);
                            self.send_reply(handle, NBD_E_IO, None).await?;
                        }
                    }
                }
                NBD_CMD_WRITE => {
                    let data = self.read_exact(length as usize).await?;
                    let result = device.write(offset, data).await;
                    match result {
                        Ok(()) => self.send_reply(handle, NBD_E_OK, None).await?,
                        Err(e) => {
                            error!("write error at offset {}: {}", offset, e);
                            self.send_reply(handle, NBD_E_IO, None).await?;
                        }
                    }
                }
                NBD_CMD_FLUSH => {
                    let result = device.flush().await;
                    match result {
                        Ok(()) => self.send_reply(handle, NBD_E_OK, None).await?,
                        Err(e) => {
                            error!("flush error: {}", e);
                            self.send_reply(handle, NBD_E_IO, None).await?;
                        }
                    }
                }
                NBD_CMD_TRIM | NBD_CMD_WRITE_ZEROES => {
                    let result = device.write_zeroes(offset, length).await;
                    match result {
                        Ok(()) => self.send_reply(handle, NBD_E_OK, None).await?,
                        Err(e) => {
                            error!("write_zeroes error: {}", e);
                            self.send_reply(handle, NBD_E_IO, None).await?;
                        }
                    }
                }
                NBD_CMD_DISC => {
                    info!("client requested disconnect");
                    return Ok(());
                }
                _ => {
                    warn!("unknown NBD command: {}", cmd);
                    self.send_reply(handle, NBD_E_NOTSUP, None).await?;
                }
            }
        }
    }

    /// Send a simple reply (and optional data payload for reads).
    async fn send_reply(&mut self, handle: u64, error: u32, data: Option<Bytes>) -> Result<()> {
        let data_len = data.as_ref().map(|d| d.len()).unwrap_or(0);
        let mut buf = BytesMut::with_capacity(16 + data_len);
        buf.put_u32(NBD_SIMPLE_REPLY_MAGIC);
        buf.put_u32(error);
        buf.put_u64(handle);
        if let Some(d) = data {
            buf.put(d);
        }
        self.write_all(&buf).await
    }

    // ─── I/O helpers ──────────────────────────────────────────────────────────

    async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.stream
            .write_all(buf)
            .await
            .map_err(|e| anyhow!("write_all: {}", e))
    }

    async fn read_u32(&mut self) -> Result<u32> {
        let b = self.read_exact(4).await?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    async fn read_u64(&mut self) -> Result<u64> {
        let b = self.read_exact(8).await?;
        Ok(u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    async fn read_exact(&mut self, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let mut buf = BytesMut::zeroed(len);
        self.stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| anyhow!("read_exact({} bytes): {}", len, e))?;
        Ok(buf.freeze())
    }
}
