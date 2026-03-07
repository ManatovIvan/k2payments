// Copyright (C) 2026 mx20022-runtime contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use mx20022_channels::{
    ChannelError, ChannelHealth, DeliveryReceipt, InboundChannel, InboundMessage, OutboundChannel,
    OutboundMessage,
};
use tokio::fs;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct FileInboundChannel {
    name: String,
    directory: PathBuf,
    pattern: String,
    poll_interval: Duration,
    move_processed_to: Option<PathBuf>,
    move_failed_to: Option<PathBuf>,
    paused: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
}

impl FileInboundChannel {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: impl Into<String>,
        directory: impl Into<PathBuf>,
        pattern: impl Into<String>,
        poll_interval: Duration,
        move_processed_to: Option<PathBuf>,
        move_failed_to: Option<PathBuf>,
    ) -> Self {
        Self {
            name: name.into(),
            directory: directory.into(),
            pattern: pattern.into(),
            poll_interval,
            move_processed_to,
            move_failed_to,
            paused: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn matches_pattern(&self, file_name: &str) -> bool {
        if self.pattern == "*" {
            return true;
        }
        if let Some(suffix) = self.pattern.strip_prefix("*.") {
            return file_name.ends_with(&format!(".{suffix}"));
        }
        file_name == self.pattern
    }

    async fn process_file(
        &self,
        sender: &mpsc::Sender<InboundMessage>,
        path: &Path,
    ) -> Result<(), ChannelError> {
        let content = fs::read_to_string(path).await.map_err(|e| {
            ChannelError::new(format!("failed reading file {}: {e}", path.display()))
        })?;
        sender
            .send(InboundMessage {
                raw: content,
                content_type: "application/xml".to_string(),
            })
            .await
            .map_err(|e| ChannelError::new(format!("failed to enqueue inbound file: {e}")))?;

        if let Some(target_dir) = &self.move_processed_to {
            move_file(path, target_dir).await?;
        } else {
            let processed = path.with_extension(format!(
                "{}.processed",
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or_default()
            ));
            fs::rename(path, processed).await.map_err(|e| {
                ChannelError::new(format!(
                    "failed moving processed file {}: {e}",
                    path.display()
                ))
            })?;
        }
        Ok(())
    }

    async fn handle_error_file(&self, path: &Path) -> Result<(), ChannelError> {
        if let Some(target_dir) = &self.move_failed_to {
            move_file(path, target_dir).await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl InboundChannel for FileInboundChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(&self, sender: mpsc::Sender<InboundMessage>) -> Result<(), ChannelError> {
        while !self.shutdown.load(Ordering::Relaxed) {
            if self.paused.load(Ordering::Relaxed) {
                tokio::time::sleep(self.poll_interval).await;
                continue;
            }

            let mut entries = fs::read_dir(&self.directory).await.map_err(|e| {
                ChannelError::new(format!(
                    "failed reading directory {}: {e}",
                    self.directory.display()
                ))
            })?;

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| ChannelError::new(format!("failed scanning directory: {e}")))?
            {
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let Some(file_name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !self.matches_pattern(file_name) {
                    continue;
                }

                if let Err(err) = self.process_file(&sender, &path).await {
                    tracing::error!(channel = %self.name, file = %path.display(), error = %err, "file inbound processing failed");
                    self.handle_error_file(&path).await?;
                }
            }

            tokio::time::sleep(self.poll_interval).await;
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<(), ChannelError> {
        self.shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth, ChannelError> {
        let exists = self.directory.is_dir();
        Ok(ChannelHealth {
            ok: exists,
            message: if exists {
                Some("ok".to_string())
            } else {
                Some(format!(
                    "watch directory is missing: {}",
                    self.directory.display()
                ))
            },
        })
    }

    async fn pause(&self) -> Result<(), ChannelError> {
        self.paused.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn resume(&self) -> Result<(), ChannelError> {
        self.paused.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone)]
pub struct FileOutboundChannel {
    name: String,
    directory: PathBuf,
    content_type_extension: String,
    counter: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
}

impl FileOutboundChannel {
    pub fn new(
        name: impl Into<String>,
        directory: impl Into<PathBuf>,
        content_type_extension: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            directory: directory.into(),
            content_type_extension: content_type_extension.into(),
            counter: Arc::new(AtomicU64::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl OutboundChannel for FileOutboundChannel {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, msg: OutboundMessage) -> Result<DeliveryReceipt, ChannelError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(ChannelError::new("channel is shut down"));
        }
        fs::create_dir_all(&self.directory)
            .await
            .map_err(|e| ChannelError::new(format!("failed creating output directory: {e}")))?;

        let now_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis();
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let ext = extension_for_content_type(&msg.content_type, &self.content_type_extension);
        let file_name = format!("mxout-{now_millis}-{counter}.{ext}");
        let path = self.directory.join(file_name);

        fs::write(&path, msg.raw)
            .await
            .map_err(|e| ChannelError::new(format!("failed writing outbound file: {e}")))?;

        Ok(DeliveryReceipt {
            id: path.display().to_string(),
        })
    }

    async fn shutdown(&self) -> Result<(), ChannelError> {
        self.shutdown.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn health(&self) -> Result<ChannelHealth, ChannelError> {
        Ok(ChannelHealth {
            ok: !self.shutdown.load(Ordering::Relaxed),
            message: Some("ok".to_string()),
        })
    }
}

fn extension_for_content_type(content_type: &str, default_extension: &str) -> String {
    if content_type.contains("xml") {
        "xml".to_string()
    } else if content_type.contains("json") {
        "json".to_string()
    } else {
        default_extension.to_string()
    }
}

async fn move_file(path: &Path, target_dir: &Path) -> Result<(), ChannelError> {
    fs::create_dir_all(target_dir)
        .await
        .map_err(|e| ChannelError::new(format!("failed creating move target directory: {e}")))?;
    let file_name = path
        .file_name()
        .ok_or_else(|| ChannelError::new(format!("file has no name: {}", path.display())))?;
    let target = target_dir.join(file_name);
    fs::rename(path, target)
        .await
        .map_err(|e| ChannelError::new(format!("failed moving file {}: {e}", path.display())))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mx20022_channels::{InboundChannel, OutboundChannel, OutboundMessage};

    use super::{FileInboundChannel, FileOutboundChannel};

    #[tokio::test]
    async fn outbound_writes_file() {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let channel = FileOutboundChannel::new("file-out", dir.path(), "txt");

        let receipt = channel
            .send(OutboundMessage {
                raw: "<Document/>".to_string(),
                content_type: "application/xml".to_string(),
            })
            .await
            .expect("send should write file");

        assert!(receipt.id.contains(".xml"));
    }

    #[tokio::test]
    async fn inbound_reads_and_moves_file() {
        let in_dir = tempfile::tempdir().expect("input tempdir should be created");
        let processed_dir = tempfile::tempdir().expect("processed tempdir should be created");
        let source = in_dir.path().join("sample.xml");
        tokio::fs::write(&source, "<Document/>")
            .await
            .expect("fixture file should be written");

        let channel = FileInboundChannel::new(
            "file-in",
            in_dir.path(),
            "*.xml",
            Duration::from_millis(20),
            Some(processed_dir.path().to_path_buf()),
            None,
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        let runner = channel.clone();
        let task = tokio::spawn(async move { runner.run(tx).await });

        let inbound = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("file should be ingested in time")
            .expect("message should exist");
        assert_eq!(inbound.raw, "<Document/>");

        channel.shutdown().await.expect("shutdown should work");
        task.await
            .expect("task should complete")
            .expect("run should stop cleanly");

        let moved = processed_dir.path().join("sample.xml");
        assert!(moved.exists());
    }
}
