//! 消息插件中间件（阶段四）
//!
//! 提供能力：
//! - `PluginGuard`：统一插件检查入口，组合主题 ACL + 载荷黑白名单 + HTTP 转发 hook
//! - 热更新：`reload()` 原子替换规则配置；HTTP 转发器需重建后台任务（较重，建议少变更）
//! - 接入点：
//!   - broker `handle_publish`：调用 `check_publish`（主题 ACL + 载荷过滤），通过后调用 `try_forward`
//!   - broker `handle_subscribe`：调用 `check_subscribe`（主题 ACL）
//!
//! 设计原则：
//! - 配置禁用时所有检查直接放行（零成本）
//! - 转发路径非阻塞：仅 try_send 到有界通道，不阻塞 broker 主循环
//! - 规则匹配复用 `broker::subscription::topic_matches_filter`，无重复实现

pub mod forwarder;
pub mod payload_filter;
pub mod topic_acl;

use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::{info, warn};

use crate::config::PluginConfig;
use crate::codec::Publish;
use crate::utils::{BrokerError, BrokerResult};

pub use forwarder::{Forwarder, SharedForwarder};
pub use payload_filter::{PayloadFilter, PayloadVerdict};
pub use topic_acl::{AclVerdict, TopicAcl};

/// 已编译的插件策略（不可变，热更新时整体替换）
#[derive(Debug)]
#[derive(Default)]
struct PluginPolicy {
    enabled: bool,
    topic_acl: TopicAcl,
    payload_filter: PayloadFilter,
}


/// 插件中间件守卫（共享句柄）
///
/// - `policy` 通过 `ArcSwap` 持有，`reload()` 时原子替换主题 ACL + 载荷过滤规则
/// - `forwarder` 持有 HTTP 转发后台任务句柄；热更新时若配置变更则重建
pub struct PluginGuard {
    policy: ArcSwap<PluginPolicy>,
    forwarder: ArcSwap<Forwarder>,
}

impl std::fmt::Debug for PluginGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let p = self.policy.load();
        f.debug_struct("PluginGuard")
            .field("enabled", &p.enabled)
            .field("topic_acl_empty", &p.topic_acl.is_empty())
            .field("payload_filter_empty", &p.payload_filter.is_empty())
            .field("forwarder_enabled", &self.forwarder.load().enabled())
            .finish()
    }
}

impl PluginGuard {
    /// 从配置构建守卫
    ///
    /// 注意：若启用 HTTP 转发，需在 tokio 运行时上下文中调用（会 spawn 后台任务）。
    pub fn new(cfg: &PluginConfig) -> BrokerResult<Arc<Self>> {
        let policy = Self::build_policy(cfg);
        let forwarder = match Self::build_forwarder(cfg) {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "plugin.forward config invalid, forwarder disabled");
                Forwarder::disabled()
            }
        };
        Ok(Arc::new(Self {
            policy: ArcSwap::from(Arc::new(policy)),
            forwarder: ArcSwap::from(Arc::new(forwarder)),
        }))
    }

    /// 禁用状态构建（所有检查放行，不转发）
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            policy: ArcSwap::from(Arc::new(PluginPolicy::default())),
            forwarder: ArcSwap::from(Arc::new(Forwarder::disabled())),
        })
    }

    fn build_policy(cfg: &PluginConfig) -> PluginPolicy {
        PluginPolicy {
            enabled: cfg.enabled,
            topic_acl: TopicAcl::from_config(&cfg.topic_acl),
            payload_filter: if cfg.payload_filter.enabled {
                PayloadFilter::from_config(&cfg.payload_filter)
            } else {
                PayloadFilter::default()
            },
        }
    }

    fn build_forwarder(cfg: &PluginConfig) -> BrokerResult<Forwarder> {
        Forwarder::new(&cfg.forward)
    }

    /// 热更新：原子替换主题 ACL + 载荷过滤规则；转发器配置变更时重建
    pub fn reload(&self, cfg: &PluginConfig) -> BrokerResult<()> {
        let policy = Self::build_policy(cfg);
        let topic_acl_empty = policy.topic_acl.is_empty();
        let payload_filter_enabled = cfg.payload_filter.enabled;
        self.policy.store(Arc::new(policy));

        // 转发器：仅在配置启用且发生变更时重建（重建会 spawn 新后台任务）
        // 简化策略：每次 reload 都尝试重建（旧后台任务因通道关闭自动退出）
        match Self::build_forwarder(cfg) {
            Ok(f) => {
                self.forwarder.store(Arc::new(f));
            }
            Err(e) => {
                warn!(error = %e, "reload forwarder failed, keeping old instance");
            }
        }

        info!(
            enabled = cfg.enabled,
            topic_acl_empty,
            payload_filter_enabled,
            forward_enabled = cfg.forward.enabled,
            "plugin policy reloaded"
        );
        Ok(())
    }

    /// 当前策略是否启用
    pub fn enabled(&self) -> bool {
        self.policy.load().enabled
    }

    /// 转发器是否启用
    pub fn forwarder_enabled(&self) -> bool {
        self.forwarder.load().enabled()
    }

    // ---------- 入站 PUBLISH 检查 ----------

    /// 检查入站 PUBLISH：主题 ACL + 载荷内容过滤
    ///
    /// 返回 Ok(()) 表示放行；Err 表示拒绝该消息（不断开连接）。
    pub fn check_publish(&self, p: &Publish) -> BrokerResult<()> {
        let policy = self.policy.load();
        if !policy.enabled {
            return Ok(());
        }

        // 1. 主题 ACL
        match policy.topic_acl.check_publish(&p.topic) {
            AclVerdict::Allow => {}
            AclVerdict::DeniedByBlacklist => {
                warn!(topic = %p.topic, "PUBLISH rejected by topic blacklist");
                return Err(BrokerError::InvalidTopic(format!(
                    "topic '{}' denied by publish blacklist"
                , p.topic)));
            }
            AclVerdict::DeniedByWhitelist => {
                warn!(topic = %p.topic, "PUBLISH rejected by topic whitelist");
                return Err(BrokerError::InvalidTopic(format!(
                    "topic '{}' not in publish whitelist"
                , p.topic)));
            }
        }

        // 2. 载荷过滤
        if !policy.payload_filter.is_empty() {
            match policy.payload_filter.check(&p.payload) {
                PayloadVerdict::Allow => {}
                PayloadVerdict::DeniedByBlacklist => {
                    warn!(topic = %p.topic, "PUBLISH rejected by payload blacklist");
                    return Err(BrokerError::Other(format!(
                        "payload of '{}' denied by blacklist keyword"
                    , p.topic)));
                }
                PayloadVerdict::DeniedByWhitelist => {
                    warn!(topic = %p.topic, "PUBLISH rejected by payload whitelist");
                    return Err(BrokerError::Other(format!(
                        "payload of '{}' denied by whitelist keyword"
                    , p.topic)));
                }
            }
        }
        Ok(())
    }

    /// 检查订阅权限
    pub fn check_subscribe(&self, filter: &str) -> BrokerResult<()> {
        let policy = self.policy.load();
        if !policy.enabled {
            return Ok(());
        }
        match policy.topic_acl.check_subscribe(filter) {
            AclVerdict::Allow => Ok(()),
            AclVerdict::DeniedByBlacklist => {
                warn!(filter = %filter, "SUBSCRIBE rejected by topic blacklist");
                Err(BrokerError::InvalidTopic(format!(
                    "filter '{}' denied by subscribe blacklist", filter
                )))
            }
            AclVerdict::DeniedByWhitelist => {
                warn!(filter = %filter, "SUBSCRIBE rejected by topic whitelist");
                Err(BrokerError::InvalidTopic(format!(
                    "filter '{}' not in subscribe whitelist", filter
                )))
            }
        }
    }

    /// 尝试异步转发一条已通过检查的 PUBLISH（非阻塞）
    pub fn try_forward(&self, p: &Publish, client_id: Option<&str>) {
        self.forwarder.load().try_forward(p, client_id);
    }

    /// 累计因队列满而丢弃的转发消息数
    pub fn forward_dropped_count(&self) -> u64 {
        self.forwarder.load().dropped_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Publish, QoS};

    fn publish(topic: &str, payload: &[u8]) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.to_string(),
            packet_id: None,
            payload: bytes::Bytes::from(payload.to_vec()),
        }
    }

    fn cfg_with_topic_acl() -> PluginConfig {
        PluginConfig {
            enabled: true,
            topic_acl: TopicAclConfig {
                publish_blacklist: vec!["cmd/#".into()],
                publish_whitelist: vec![],
                subscribe_blacklist: vec!["internal/#".into()],
                subscribe_whitelist: vec![],
            },
            payload_filter: PayloadFilterConfig {
                enabled: true,
                blacklist_keywords: vec!["forbidden".into()],
                whitelist_keywords: vec![],
            },
            forward: ForwardConfig::default(),
        }
    }

    use crate::config::{ForwardConfig, PayloadFilterConfig, TopicAclConfig};

    #[test]
    fn disabled_guard_allows_all() {
        let g = PluginGuard::disabled();
        let p = publish("cmd/reboot", b"forbidden");
        assert!(g.check_publish(&p).is_ok());
        assert!(g.check_subscribe("internal/secret").is_ok());
    }

    #[test]
    fn topic_blacklist_rejects_publish() {
        let g = PluginGuard::new(&cfg_with_topic_acl()).unwrap();
        assert!(g.check_publish(&publish("cmd/reboot", b"ok")).is_err());
        assert!(g.check_publish(&publish("sensor/temp", b"ok")).is_ok());
    }

    #[test]
    fn payload_blacklist_rejects_publish() {
        let g = PluginGuard::new(&cfg_with_topic_acl()).unwrap();
        assert!(g.check_publish(&publish("sensor/temp", b"this is forbidden")).is_err());
        assert!(g.check_publish(&publish("sensor/temp", b"clean data")).is_ok());
    }

    #[test]
    fn subscribe_blacklist_rejects() {
        let g = PluginGuard::new(&cfg_with_topic_acl()).unwrap();
        assert!(g.check_subscribe("internal/stats").is_err());
        assert!(g.check_subscribe("sensor/#").is_ok());
    }

    #[test]
    fn reload_updates_rules() {
        let g = PluginGuard::new(&cfg_with_topic_acl()).unwrap();
        assert!(g.check_publish(&publish("cmd/reboot", b"ok")).is_err());

        // 热更新：清空黑名单
        let mut cfg2 = cfg_with_topic_acl();
        cfg2.topic_acl.publish_blacklist.clear();
        g.reload(&cfg2).unwrap();
        assert!(g.check_publish(&publish("cmd/reboot", b"ok")).is_ok());
    }

    #[tokio::test]
    async fn forwarder_disabled_by_default() {
        let g = PluginGuard::new(&cfg_with_topic_acl()).unwrap();
        assert!(!g.forwarder_enabled());
        // try_forward 在禁用时应为空操作
        g.try_forward(&publish("sensor/temp", b"data"), Some("c1"));
        assert_eq!(g.forward_dropped_count(), 0);
    }
}
