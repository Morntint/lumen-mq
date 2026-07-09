use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 顶层配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Settings {
    pub broker: BrokerConfig,
    pub tcp: TcpConfig,
    pub tls: TlsConfig,
    pub websocket: WebSocketConfig,
    pub mqtt_sn: MqttSnConfig,
    pub auth: AuthConfig,
    pub storage: StorageConfig,
    pub log: LogConfig,
    pub monitor: MonitorConfig,
    pub admin: AdminConfig,
    pub security: SecurityConfig,
    pub plugin: PluginConfig,
}


/// Broker 全局参数
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BrokerConfig {
    pub node_id: String,
    pub max_connections: usize,
    pub max_packet_size: usize,
    pub default_keep_alive: u16,
    pub max_subscriptions_per_client: usize,
    pub max_inflight: usize,
    /// 出站 QoS1/QoS2 inflight 重传检查间隔（秒）
    pub retry_interval_secs: Option<u64>,
    /// 出站 inflight 最大重传次数；超过即丢弃
    pub max_retries: Option<u32>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            node_id: "lumenmq@127.0.0.1".into(),
            max_connections: 100_000,
            max_packet_size: 1024 * 1024,
            default_keep_alive: 120,
            max_subscriptions_per_client: 256,
            max_inflight: 1024,
            retry_interval_secs: Some(10),
            max_retries: Some(3),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TcpConfig {
    pub enabled: bool,
    pub bind: String,
}
impl Default for TcpConfig {
    fn default() -> Self {
        Self { enabled: true, bind: "0.0.0.0:1883".into() }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    pub enabled: bool,
    pub bind: String,
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
    pub mutual: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub bind: String,
    pub path: String,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "0.0.0.0:8083".into(),
            path: "/mqtt".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MqttSnConfig {
    pub enabled: bool,
    pub bind: String,
}

impl Default for MqttSnConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "0.0.0.0:1884".into(),
        }
    }
}

/// 鉴权配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    pub mode: AuthMode,
    pub allow_anonymous: bool,
    pub users: Vec<UserConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        // 默认匿名模式（开发友好）；生产环境必须显式配置用户名密码
        // 不再使用硬编码的 admin/public 默认密码（安全风险）
        Self {
            mode: AuthMode::Anonymous,
            allow_anonymous: true,
            users: vec![],
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthMode {
    Anonymous,
    UsernamePassword,
    Token,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UserConfig {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub publish_acl: Vec<String>,
    #[serde(default)]
    pub subscribe_acl: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub max_offline_messages: usize,
    pub offline_message_ttl: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: PathBuf::from("./data/lumenmq"),
            max_offline_messages: 1000,
            offline_message_ttl: 86400,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LogConfig {
    pub level: String,
    pub format: LogFormat,
    pub dir: String,
    pub rotation: LogRotation,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: LogFormat::Compact,
            dir: String::new(),
            rotation: LogRotation::Daily,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Compact,
    Json,
    Full,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogRotation {
    Hourly,
    Daily,
    Never,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct MonitorConfig {
    pub metrics_enabled: bool,
    pub health_check_interval: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    pub enabled: bool,
    pub bind: String,
    /// 鉴权 Token（为空时仅允许环回地址访问；非空时要求请求头
    /// `Authorization: Bearer <token>` 匹配）
    pub token: String,
}

/// 安全中间件配置（阶段四：IP 黑白名单 + 连接/消息限流）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct SecurityConfig {
    /// 是否启用安全中间件
    pub enabled: bool,
    /// IP 黑名单（CIDR 或单 IP，如 "10.0.0.0/8"、"192.168.1.5"）
    pub ip_blacklist: Vec<String>,
    /// IP 白名单（CIDR 或单 IP）；非空时仅允许白名单内 IP 连接
    pub ip_whitelist: Vec<String>,
    /// 单 IP 最大并发连接数；0 表示不限制
    pub max_connections_per_ip: usize,
    /// 单客户端每秒最大入站 PUBLISH 数；0 表示不限制
    pub publish_rate_per_second: u32,
    /// 单客户端最大 PUBLISH 载荷字节数；0 表示使用 broker.max_packet_size
    pub max_payload_bytes: usize,
}


/// 消息插件配置（阶段四：主题 ACL + 载荷黑白名单 + HTTP 转发 hook）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct PluginConfig {
    /// 是否启用插件中间件
    pub enabled: bool,
    /// 主题 ACL 规则
    pub topic_acl: TopicAclConfig,
    /// 载荷内容过滤
    pub payload_filter: PayloadFilterConfig,
    /// HTTP 转发 hook
    pub forward: ForwardConfig,
}


/// 主题 ACL 配置（黑名单优先于白名单）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TopicAclConfig {
    /// 禁止发布的主题过滤器列表（如 ["cmd/#", "internal/+"]）
    pub publish_blacklist: Vec<String>,
    /// 允许发布的主题过滤器列表；非空时仅允许匹配的消息发布
    pub publish_whitelist: Vec<String>,
    /// 禁止订阅的主题过滤器列表
    pub subscribe_blacklist: Vec<String>,
    /// 允许订阅的主题过滤器列表；非空时仅允许匹配的订阅
    pub subscribe_whitelist: Vec<String>,
}

/// 载荷内容过滤配置（关键字级，黑名单优先）
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PayloadFilterConfig {
    /// 是否启用载荷过滤
    pub enabled: bool,
    /// 载荷包含任一关键字则拒绝（按字节子串匹配，兼容非 UTF-8）
    pub blacklist_keywords: Vec<String>,
    /// 载荷必须包含其中任一关键字才放行；非空时启用白名单模式
    pub whitelist_keywords: Vec<String>,
}

/// HTTP 转发 hook 配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ForwardConfig {
    /// 是否启用 HTTP 转发
    pub enabled: bool,
    /// 转发目标 URL（如 "http://127.0.0.1:8080/mqtt"）
    pub url: String,
    /// 仅转发匹配此过滤器的主题（如 "sensor/#"）；为空则转发全部
    pub topic_filter: String,
    /// HTTP 请求超时（秒）
    pub timeout_secs: u64,
    /// 转发队列最大长度（溢出时丢弃最旧消息，防 OOM）
    pub max_queue: usize,
    /// 允许转发到私有/环回/链路本地地址（SSRF 防护豁免）
    ///
    /// 默认 false：拒绝 127.0.0.0/8、RFC1918、169.254.0.0/16、fc00::/7 等内网地址。
    /// 工业场景下若转发目标确实是内网 webhook 服务，可显式置 true 放行。
    /// 任何通过无鉴权 reload/plugin 接口修改 url 的操作仍受此开关约束。
    #[serde(default)]
    pub allow_private_network: bool,
}
