//! HTTP 转发 hook：将匹配主题的入站 PUBLISH 异步转发到外部 HTTP 端点
//!
//! 设计要点：
//! - 异步非阻塞：broker 主循环仅 `try_send` 到有界通道，由独立后台任务消费并 POST
//! - 背压保护：通道满时丢弃最旧消息并计数（防 OOM）
//! - 失败重试：HTTP 请求失败仅记日志，不重试（工业场景下游短暂故障可接受丢消息；
//!   如需可靠投递应配合离线消息存储 + 重放）
//!
//! 转发报文 JSON 格式：
//! ```json
//! { "topic": "sensor/temp", "qos": 1, "retain": false, "payload_b64": "...", "client_id": "..." }
//! ```

use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose, Engine as _};
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::broker::subscription::topic_matches_filter;
use crate::codec::Publish;
use crate::config::ForwardConfig;
use crate::utils::{BrokerError, BrokerResult};

/// 转发到 HTTP 端点的报文 JSON 结构
#[derive(Debug, Serialize)]
struct ForwardPayload {
    topic: String,
    qos: u8,
    retain: bool,
    payload_b64: String,
    client_id: Option<String>,
}

/// HTTP 转发器句柄
///
/// 通过 `try_send` 投递消息到后台任务；通道满时丢弃最旧消息。
pub struct Forwarder {
    tx: Option<mpsc::Sender<ForwardPayload>>,
    topic_filter: Option<String>,
    dropped: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for Forwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forwarder")
            .field("topic_filter", &self.topic_filter)
            .field("has_tx", &self.tx.is_some())
            .finish()
    }
}

impl Forwarder {
    /// 从配置构建转发器：若启用则启动后台 POST 任务
    ///
    /// 注意：需在 tokio 运行时上下文中调用（会 spawn 后台任务）。
    pub fn new(cfg: &ForwardConfig) -> BrokerResult<Self> {
        if !cfg.enabled {
            return Ok(Self {
                tx: None,
                topic_filter: None,
                dropped: std::sync::atomic::AtomicU64::new(0),
            });
        }
        if cfg.url.is_empty() {
            return Err(BrokerError::Config("plugin.forward.url must be set when enabled".into()));
        }
        if !cfg.url.starts_with("http://") && !cfg.url.starts_with("https://") {
            return Err(BrokerError::Config(format!(
                "plugin.forward.url invalid scheme: {} (must be http:// or https://)",
                cfg.url
            )));
        }

        let max_queue = if cfg.max_queue == 0 { 1024 } else { cfg.max_queue };
        let (tx, rx) = mpsc::channel::<ForwardPayload>(max_queue);
        let topic_filter = if cfg.topic_filter.is_empty() {
            None
        } else {
            Some(cfg.topic_filter.clone())
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(if cfg.timeout_secs == 0 { 5 } else { cfg.timeout_secs }))
            .build()
            .map_err(|e| BrokerError::Config(format!("build http client failed: {e}")))?;

        let url = cfg.url.clone();
        tokio::spawn(forward_loop(client, url, rx));

        info!(url = %cfg.url, filter = ?topic_filter, max_queue, "http forwarder started");

        Ok(Self {
            tx: Some(tx),
            topic_filter,
            dropped: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// 禁用状态构建（不转发任何消息）
    pub fn disabled() -> Self {
        Self {
            tx: None,
            topic_filter: None,
            dropped: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 是否已启用
    pub fn enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// 尝试转发一条 PUBLISH（非阻塞）
    ///
    /// - 主题不匹配过滤器 → 跳过
    /// - 通道满 → 丢弃并计数
    /// - 通道关闭 → 静默
    pub fn try_forward(&self, p: &Publish, client_id: Option<&str>) {
        let Some(tx) = &self.tx else { return };

        // 主题过滤
        if let Some(filter) = &self.topic_filter {
            if !topic_matches_filter(&p.topic, filter) {
                return;
            }
        }

        let payload = ForwardPayload {
            topic: p.topic.clone(),
            qos: p.qos.as_u8(),
            retain: p.retain,
            payload_b64: general_purpose::STANDARD.encode(&p.payload),
            client_id: client_id.map(|s| s.to_string()),
        };

        match tx.try_send(payload) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let prev = self.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                crate::monitor::METRICS.inc_forward_dropped();
                if prev % 100 == 0 {
                    warn!(dropped = prev + 1, "forward queue full, dropping oldest messages");
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("forward channel closed");
            }
        }
    }

    /// 累计丢弃的消息数（运维观测）
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// 后台消费循环：从通道取消息并 POST
async fn forward_loop(
    client: reqwest::Client,
    url: String,
    mut rx: mpsc::Receiver<ForwardPayload>,
) {
    while let Some(msg) = rx.recv().await {
        match client.post(&url).json(&msg).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    debug!(status = %resp.status(), topic = %msg.topic, "forward response non-2xx");
                }
            }
            Err(e) => {
                debug!(error = %e, topic = %msg.topic, "forward http request failed");
            }
        }
    }
    info!("http forwarder stopped");
}

/// Arc 别名，便于在 BrokerState 中共享
pub type SharedForwarder = Arc<Forwarder>;
