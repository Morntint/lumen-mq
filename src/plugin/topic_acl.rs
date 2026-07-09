//! 主题 ACL：基于 MQTT 主题过滤器的发布/订阅权限控制
//!
//! 规则匹配使用标准 MQTT 通配符（`+` / `#`），复用 `broker::subscription::topic_matches_filter`。
//! 决策语义：黑名单优先于白名单。
//! - 命中黑名单 → 拒绝
//! - 否则若白名单非空 → 必须命中白名单才放行
//! - 否则 → 放行

use crate::broker::subscription::topic_matches_filter;
use crate::config::TopicAclConfig;

/// 主题 ACL 决策结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclVerdict {
    Allow,
    DeniedByBlacklist,
    DeniedByWhitelist,
}

/// 已编译的主题 ACL 规则集（不可变，热更新时整体替换）
#[derive(Debug, Clone, Default)]
pub struct TopicAcl {
    publish_blacklist: Vec<String>,
    publish_whitelist: Vec<String>,
    subscribe_blacklist: Vec<String>,
    subscribe_whitelist: Vec<String>,
}

impl TopicAcl {
    /// 从配置编译规则集
    pub fn from_config(cfg: &TopicAclConfig) -> Self {
        Self {
            publish_blacklist: cfg.publish_blacklist.clone(),
            publish_whitelist: cfg.publish_whitelist.clone(),
            subscribe_blacklist: cfg.subscribe_blacklist.clone(),
            subscribe_whitelist: cfg.subscribe_whitelist.clone(),
        }
    }

    /// 检查发布权限
    pub fn check_publish(&self, topic: &str) -> AclVerdict {
        Self::decide(topic, &self.publish_blacklist, &self.publish_whitelist)
    }

    /// 检查订阅权限
    pub fn check_subscribe(&self, filter: &str) -> AclVerdict {
        Self::decide(filter, &self.subscribe_blacklist, &self.subscribe_whitelist)
    }

    /// 统一决策逻辑：黑名单优先
    fn decide(topic: &str, blacklist: &[String], whitelist: &[String]) -> AclVerdict {
        // 1. 黑名单：任一命中即拒绝
        for f in blacklist {
            if topic_matches_filter(topic, f) {
                return AclVerdict::DeniedByBlacklist;
            }
        }
        // 2. 白名单非空：必须命中其一
        if !whitelist.is_empty() {
            for f in whitelist {
                if topic_matches_filter(topic, f) {
                    return AclVerdict::Allow;
                }
            }
            return AclVerdict::DeniedByWhitelist;
        }
        AclVerdict::Allow
    }

    /// 是否配置了任何规则（用于快速短路）
    pub fn is_empty(&self) -> bool {
        self.publish_blacklist.is_empty()
            && self.publish_whitelist.is_empty()
            && self.subscribe_blacklist.is_empty()
            && self.subscribe_whitelist.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(
        pb: &[&str],
        pw: &[&str],
        sb: &[&str],
        sw: &[&str],
    ) -> TopicAclConfig {
        TopicAclConfig {
            publish_blacklist: pb.iter().map(|s| s.to_string()).collect(),
            publish_whitelist: pw.iter().map(|s| s.to_string()).collect(),
            subscribe_blacklist: sb.iter().map(|s| s.to_string()).collect(),
            subscribe_whitelist: sw.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn empty_acl_allows_all() {
        let acl = TopicAcl::from_config(&cfg(&[], &[], &[], &[]));
        assert_eq!(acl.check_publish("any/topic"), AclVerdict::Allow);
        assert_eq!(acl.check_subscribe("any/#"), AclVerdict::Allow);
        assert!(acl.is_empty());
    }

    #[test]
    fn publish_blacklist_denies() {
        let acl = TopicAcl::from_config(&cfg(&["cmd/#"], &[], &[], &[]));
        // "cmd/#" 匹配 "cmd" 自身及所有子主题（MQTT 语义）
        assert_eq!(acl.check_publish("cmd/reboot"), AclVerdict::DeniedByBlacklist);
        assert_eq!(acl.check_publish("cmd"), AclVerdict::DeniedByBlacklist);
        assert_eq!(acl.check_publish("sensor/temp"), AclVerdict::Allow);
    }

    #[test]
    fn publish_whitelist_restricts() {
        let acl = TopicAcl::from_config(&cfg(&[], &["sensor/#"], &[], &[]));
        assert_eq!(acl.check_publish("sensor/temp"), AclVerdict::Allow);
        assert_eq!(acl.check_publish("cmd/reboot"), AclVerdict::DeniedByWhitelist);
    }

    #[test]
    fn blacklist_overrides_whitelist() {
        // 同时在白名单和黑名单：黑名单优先
        let acl = TopicAcl::from_config(&cfg(&["sensor/secret"], &["sensor/#"], &[], &[]));
        assert_eq!(acl.check_publish("sensor/secret"), AclVerdict::DeniedByBlacklist);
        assert_eq!(acl.check_publish("sensor/temp"), AclVerdict::Allow);
    }

    #[test]
    fn subscribe_acl_independent() {
        let acl = TopicAcl::from_config(&cfg(&[], &[], &["internal/#"], &[]));
        assert_eq!(acl.check_subscribe("internal/stats"), AclVerdict::DeniedByBlacklist);
        assert_eq!(acl.check_publish("internal/stats"), AclVerdict::Allow);
    }

    #[test]
    fn wildcard_plus_matches_single_level() {
        let acl = TopicAcl::from_config(&cfg(&[], &["sensor/+/temp"], &[], &[]));
        assert_eq!(acl.check_publish("sensor/room1/temp"), AclVerdict::Allow);
        assert_eq!(acl.check_publish("sensor/room1/humidity"), AclVerdict::DeniedByWhitelist);
        assert_eq!(acl.check_publish("sensor/room1/zone/temp"), AclVerdict::DeniedByWhitelist);
    }
}
