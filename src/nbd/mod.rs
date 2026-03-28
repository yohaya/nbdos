pub mod handler;
pub mod protocol;

use anyhow::Result;
use handler::NbdHandler;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::storage::BlockDevice;

/// TCP server that accepts NBD connections and dispatches them to handlers.
pub struct NbdServer {
    listener: TcpListener,
    devices: Arc<HashMap<String, BlockDevice>>,
}

impl NbdServer {
    pub async fn bind(
        addr: &str,
        port: u16,
        devices: HashMap<String, BlockDevice>,
    ) -> Result<Self> {
        let bind_addr = format!("{}:{}", addr, port);
        let listener = TcpListener::bind(&bind_addr).await?;
        info!("NBD server listening on {}", bind_addr);
        Ok(Self {
            listener,
            devices: Arc::new(devices),
        })
    }

    pub async fn serve(self) -> Result<()> {
        loop {
            let (stream, peer) = self.listener.accept().await?;

            // Disable Nagle for lower latency
            let _ = stream.set_nodelay(true);

            let devices = Arc::clone(&self.devices);
            tokio::spawn(async move {
                let handler = NbdHandler::new(stream, devices);
                if let Err(e) = handler.run().await {
                    error!("NBD handler error for {}: {}", peer, e);
                }
            });
        }
    }
}
