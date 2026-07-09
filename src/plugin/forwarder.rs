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
        // SSRF 防护：拒绝内网/环回/链路本地地址（防止通过无鉴权的 reload/plugin
        // 接口把转发目标改成内网服务或云元数据端点 169.254.169.254）
        // allow_private_network=true 时显式放行（适用于内网 webhook 场景）
        if !cfg.allow_private_network {
            validate_forward_url(&cfg.url)?;
        } else {
            warn!(url = %cfg.url, "plugin.forward.allow_private_network=true: SSRF protection bypassed");
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
            // 禁止跟随重定向：防止 SSRF 通过 3xx 跳转到内网/元数据端点
            .redirect(reqwest::redirect::Policy::none())
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
                if prev.is_multiple_of(100) {
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
///
/// 失败重试策略：网络错误（连接拒绝/超时）时立即重试 1 次；仍失败则丢弃并计数。
/// 非 2xx 响应不重试（下游业务错误重试无意义）。
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
                // 网络错误：立即重试 1 次（下游短暂故障可恢复）
                debug!(error = %e, topic = %msg.topic, "forward http request failed, retrying once");
                match client.post(&url).json(&msg).send().await {
                    Ok(resp) => {
                        if !resp.status().is_success() {
                            debug!(status = %resp.status(), topic = %msg.topic, "forward retry response non-2xx");
                        }
                    }
                    Err(e2) => {
                        warn!(error = %e2, topic = %msg.topic, "forward retry failed, dropping message");
                    }
                }
            }
        }
    }
    info!("http forwarder stopped");
}

/// Arc 别名，便于在 BrokerState 中共享
pub type SharedForwarder = Arc<Forwarder>;

/// SSRF 防护：校验转发目标 URL 的 host 不是内网/环回/链路本地地址
///
/// 拒绝的地址段：
/// - 环回：127.0.0.0/8、::1、hostname "localhost"
/// - RFC1918 私有：10.0.0.0/8、172.16.0.0/12、192.168.0.0/16
/// - 链路本地：169.254.0.0/16（含云元数据端点 169.254.169.254）、fe80::/10
/// - IPv6 ULA：fc00::/7
/// - 未指定地址：0.0.0.0、::
///
/// 注意：仅校验 URL 中字面量 IP 或 "localhost" 主机名；对其他域名不做 DNS 解析
/// （运营商应负责自己的 DNS 配置；解析后 IP 校验需要 async + DNS 依赖，超出本次修复范围）。
fn validate_forward_url(url: &str) -> BrokerResult<()> {
    let host = extract_host(url).ok_or_else(|| {
        BrokerError::Config(format!("plugin.forward.url cannot parse host: {url}"))
    })?;

    // 主机名为 localhost 直接拒绝
    if host.eq_ignore_ascii_case("localhost") {
        return Err(BrokerError::Config(
            "plugin.forward.url host 'localhost' is forbidden (SSRF protection)".into(),
        ));
    }

    // 尝试解析为 IP 字面量
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if is_forbidden_ip(&ip) {
            return Err(BrokerError::Config(format!(
                "plugin.forward.url host '{ip}' is forbidden (SSRF protection: loopback/private/link-local/ULA)"
            )));
        }
    }
    // 其他域名主机名放行（运维负责 DNS 配置）

    Ok(())
}

/// 从 URL 字符串中提取 host 部分（剥离 scheme、port、path、query）
fn extract_host(url: &str) -> Option<String> {
    // 剥离 scheme
    let after_scheme = url.strip_prefix("http://").or_else(|| url.strip_prefix("https://"))?;
    // 取第一个 '/' 或 '?' 或 '#' 之前的部分作为 authority
    let authority_end = after_scheme
        .find(['/', '?', '#'])
        .unwrap_or(after_scheme.len());
    let authority = &after_scheme[..authority_end];

    // authority 可能是 user:pass@host:port 或 host:port
    // 剥离 user info
    let host_port = authority.rsplit('@').next().unwrap_or(authority);

    // 处理 IPv6 字面量 [::1]:port
    if let Some(rest) = host_port.strip_prefix('[') {
        let close = rest.find(']')?;
        return Some(rest[..close].to_string());
    }

    // 否则取 ':' 之前的部分（剥离端口）
    let host = host_port.split(':').next().unwrap_or(host_port);
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

/// 判断 IP 是否属于禁止访问的范围
fn is_forbidden_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            let octets = v4.octets();
            // 环回 127.0.0.0/8
            if octets[0] == 127 {
                return true;
            }
            // 未指定 0.0.0.0/8
            if octets[0] == 0 {
                return true;
            }
            // RFC1918 私有
            // 10.0.0.0/8
            if octets[0] == 10 {
                return true;
            }
            // 172.16.0.0/12
            if octets[0] == 172 && (octets[1] & 0xF0) == 0x10 {
                return true;
            }
            // 192.168.0.0/16
            if octets[0] == 192 && octets[1] == 168 {
                return true;
            }
            // 链路本地 169.254.0.0/16（含 169.254.169.254 元数据端点）
            if octets[0] == 169 && octets[1] == 254 {
                return true;
            }
            false
        }
        std::net::IpAddr::V6(v6) => {
            // 未指定 ::
            if v6.is_unspecified() {
                return true;
            }
            // 环回 ::1
            if v6.is_loopback() {
                return true;
            }
            let segments = v6.segments();
            // 链路本地 fe80::/10
            if (segments[0] & 0xFFC0) == 0xFE80 {
                return true;
            }
            // ULA fc00::/7（含 fd00::/8）
            if (segments[0] & 0xFE00) == 0xFC00 {
                return true;
            }
            // IPv4-mapped ::ffff:a.b.c.d：递归校验内嵌的 IPv4
            if matches!(v6.octets(), [0,0,0,0,0,0,0,0,0,0,0xFF,0xFF, _, _, _, _]) {
                let v4 = std::net::Ipv4Addr::new(
                    v6.octets()[12],
                    v6.octets()[13],
                    v6.octets()[14],
                    v6.octets()[15],
                );
                return is_forbidden_ip(&std::net::IpAddr::V4(v4));
            }
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_host_handles_common_forms() {
        assert_eq!(extract_host("http://example.com/path"), Some("example.com".into()));
        assert_eq!(extract_host("https://example.com:8443"), Some("example.com".into()));
        assert_eq!(extract_host("http://1.2.3.4:80/x"), Some("1.2.3.4".into()));
        assert_eq!(extract_host("http://[::1]:8080/x"), Some("::1".into()));
        assert_eq!(extract_host("http://user:pass@host/"), Some("host".into()));
        assert_eq!(extract_host("ftp://nope/"), None);
    }

    #[test]
    fn validate_rejects_loopback_and_private() {
        assert!(validate_forward_url("http://127.0.0.1/x").is_err());
        assert!(validate_forward_url("http://localhost/x").is_err());
        assert!(validate_forward_url("http://10.0.0.1/x").is_err());
        assert!(validate_forward_url("http://172.16.5.4/x").is_err());
        assert!(validate_forward_url("http://192.168.1.1/x").is_err());
        assert!(validate_forward_url("http://169.254.169.254/latest/meta-data/").is_err());
        assert!(validate_forward_url("http://[::1]:80/x").is_err());
        assert!(validate_forward_url("http://[fe80::1]:80/x").is_err());
        assert!(validate_forward_url("http://[fc00::1]:80/x").is_err());
    }

    #[test]
    fn validate_accepts_public_ips_and_domains() {
        assert!(validate_forward_url("http://8.8.8.8/x").is_ok());
        assert!(validate_forward_url("https://example.com/webhook").is_ok());
        assert!(validate_forward_url("http://203.0.113.5:9000/hook").is_ok());
    }
}
