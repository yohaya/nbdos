use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::Client;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Thin wrapper around the AWS S3 SDK client providing retry logic and timeout handling.
#[derive(Clone)]
pub struct S3Backend {
    client: Client,
    bucket: String,
    write_timeout: Duration,
    read_timeout: Duration,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
}

impl S3Backend {
    pub fn new(
        client: Client,
        bucket: String,
        write_timeout_secs: u64,
        read_timeout_secs: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        max_retry_delay_ms: u64,
    ) -> Self {
        Self {
            client,
            bucket,
            write_timeout: Duration::from_secs(write_timeout_secs),
            read_timeout: Duration::from_secs(read_timeout_secs),
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
            max_retry_delay: Duration::from_millis(max_retry_delay_ms),
        }
    }

    /// Download an object from S3 and return its body as Bytes.
    /// Returns Ok(None) if the object does not exist (404).
    pub async fn get_object(&self, key: &str) -> Result<Option<Bytes>> {
        let mut delay = self.retry_delay;
        let mut attempt = 0u32;

        loop {
            let result = tokio::time::timeout(
                self.read_timeout,
                self.client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send(),
            )
            .await;

            match result {
                Ok(Ok(resp)) => {
                    let body = resp
                        .body
                        .collect()
                        .await
                        .context("collect S3 object body")?;
                    return Ok(Some(body.into_bytes()));
                }
                Ok(Err(err)) => {
                    // Check if it's a NoSuchKey (object doesn't exist → block is zero)
                    let service_err = err.as_service_error();
                    if service_err.map(|e| e.is_no_such_key()).unwrap_or(false) {
                        return Ok(None);
                    }

                    if attempt >= self.max_retries {
                        return Err(anyhow!("S3 get_object '{}' failed after {} attempts: {}", key, attempt, err));
                    }
                    warn!("S3 get_object '{}' attempt {}/{} failed: {}", key, attempt + 1, self.max_retries, err);
                }
                Err(_elapsed) => {
                    if attempt >= self.max_retries {
                        return Err(anyhow!("S3 get_object '{}' timed out after {} attempts", key, attempt));
                    }
                    warn!("S3 get_object '{}' attempt {}/{} timed out after {:?}", key, attempt + 1, self.max_retries, self.read_timeout);
                }
            }

            sleep(delay).await;
            delay = (delay * 2).min(self.max_retry_delay);
            attempt += 1;
        }
    }

    /// Upload data to S3 with retry and timeout.
    /// Retries indefinitely up to `max_retries` (u32::MAX means forever).
    pub async fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        let mut delay = self.retry_delay;
        let mut attempt = 0u32;

        loop {
            let body = aws_sdk_s3::primitives::ByteStream::from(data.clone());

            let result = tokio::time::timeout(
                self.write_timeout,
                self.client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .body(body)
                    .send(),
            )
            .await;

            match result {
                Ok(Ok(_)) => {
                    debug!("S3 put_object '{}' succeeded (attempt {})", key, attempt + 1);
                    return Ok(());
                }
                Ok(Err(err)) => {
                    if attempt >= self.max_retries {
                        return Err(anyhow!("S3 put_object '{}' failed after {} attempts: {}", key, attempt, err));
                    }
                    warn!("S3 put_object '{}' attempt {}/{} error: {}", key, attempt + 1, self.max_retries, err);
                }
                Err(_elapsed) => {
                    // Timeout: always retry (S3 may be slow but we must not lose data)
                    warn!(
                        "S3 put_object '{}' attempt {}/{} timed out after {:?}, retrying…",
                        key, attempt + 1, self.max_retries, self.write_timeout
                    );
                }
            }

            sleep(delay).await;
            delay = (delay * 2).min(self.max_retry_delay);
            attempt += 1;
        }
    }

    /// Delete an object from S3 (used for TRIM/discard).
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        let mut delay = self.retry_delay;
        let mut attempt = 0u32;

        loop {
            let result = tokio::time::timeout(
                self.write_timeout,
                self.client
                    .delete_object()
                    .bucket(&self.bucket)
                    .key(key)
                    .send(),
            )
            .await;

            match result {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(err)) => {
                    if attempt >= self.max_retries {
                        return Err(anyhow!("S3 delete_object '{}' failed: {}", key, err));
                    }
                    warn!("S3 delete_object '{}' attempt {} error: {}", key, attempt + 1, err);
                }
                Err(_) => {
                    warn!("S3 delete_object '{}' attempt {} timed out", key, attempt + 1);
                }
            }

            sleep(delay).await;
            delay = (delay * 2).min(self.max_retry_delay);
            attempt += 1;
        }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}
