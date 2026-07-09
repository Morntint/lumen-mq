//! 载荷内容过滤：基于关键字的黑白名单
//!
//! 决策语义：黑名单优先于白名单。
//! - 载荷包含任一黑名单关键字 → 拒绝
//! - 否则若白名单非空 → 载荷必须包含其中任一关键字才放行
//! - 否则 → 放行
//!
//! 匹配按字节子串进行（兼容非 UTF-8 载荷）；关键字本身以 String 表达，
//! 编译期转为其 UTF-8 字节序列参与匹配。

use crate::config::PayloadFilterConfig;

/// 载荷过滤决策结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadVerdict {
    Allow,
    DeniedByBlacklist,
    DeniedByWhitelist,
}

/// 已编译的载荷过滤规则集（不可变，热更新时整体替换）
#[derive(Debug, Clone, Default)]
pub struct PayloadFilter {
    blacklist: Vec<Vec<u8>>,
    whitelist: Vec<Vec<u8>>,
}

impl PayloadFilter {
    /// 从配置编译规则集
    pub fn from_config(cfg: &PayloadFilterConfig) -> Self {
        Self {
            blacklist: cfg.blacklist_keywords.iter().map(|s| s.as_bytes().to_vec()).collect(),
            whitelist: cfg.whitelist_keywords.iter().map(|s| s.as_bytes().to_vec()).collect(),
        }
    }

    /// 检查载荷是否放行
    pub fn check(&self, payload: &[u8]) -> PayloadVerdict {
        // 1. 黑名单：任一关键字命中即拒绝
        for kw in &self.blacklist {
            if contains_subslice(payload, kw) {
                return PayloadVerdict::DeniedByBlacklist;
            }
        }
        // 2. 白名单非空：必须命中其一
        if !self.whitelist.is_empty() {
            for kw in &self.whitelist {
                if contains_subslice(payload, kw) {
                    return PayloadVerdict::Allow;
                }
            }
            return PayloadVerdict::DeniedByWhitelist;
        }
        PayloadVerdict::Allow
    }

    /// 是否配置了任何规则
    pub fn is_empty(&self) -> bool {
        self.blacklist.is_empty() && self.whitelist.is_empty()
    }
}

/// 标准库暂未稳定 `Vec::contains_slice`，这里手写一个字节子串搜索
fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(black: &[&str], white: &[&str]) -> PayloadFilterConfig {
        PayloadFilterConfig {
            enabled: true,
            blacklist_keywords: black.iter().map(|s| s.to_string()).collect(),
            whitelist_keywords: white.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn empty_filter_allows_all() {
        let f = PayloadFilter::from_config(&cfg(&[], &[]));
        assert_eq!(f.check(b"anything"), PayloadVerdict::Allow);
        assert!(f.is_empty());
    }

    #[test]
    fn blacklist_denies() {
        let f = PayloadFilter::from_config(&cfg(&["forbidden", "secret"], &[]));
        assert_eq!(f.check(b"this is forbidden data"), PayloadVerdict::DeniedByBlacklist);
        assert_eq!(f.check(b"top secret info"), PayloadVerdict::DeniedByBlacklist);
        assert_eq!(f.check(b"clean data"), PayloadVerdict::Allow);
    }

    #[test]
    fn whitelist_restricts() {
        let f = PayloadFilter::from_config(&cfg(&[], &["temperature", "humidity"]));
        assert_eq!(f.check(b"temperature=25"), PayloadVerdict::Allow);
        assert_eq!(f.check(b"humidity=60"), PayloadVerdict::Allow);
        assert_eq!(f.check(b"pressure=1013"), PayloadVerdict::DeniedByWhitelist);
    }

    #[test]
    fn blacklist_overrides_whitelist() {
        let f = PayloadFilter::from_config(&cfg(&["bad"], &["ok", "good"]));
        assert_eq!(f.check(b"ok and bad"), PayloadVerdict::DeniedByBlacklist);
        assert_eq!(f.check(b"ok"), PayloadVerdict::Allow);
        assert_eq!(f.check(b"neither"), PayloadVerdict::DeniedByWhitelist);
    }

    #[test]
    fn binary_payload_supported() {
        // 非 UTF-8 载荷也能匹配关键字（关键字本身需为合法 UTF-8 String）
        let f = PayloadFilter::from_config(&cfg(&["MAGIC"], &[]));
        assert_eq!(f.check(&[0x00, b'M', b'A', b'G', b'I', b'C', 0xff, 0x01]), PayloadVerdict::DeniedByBlacklist);
        assert_eq!(f.check(&[0x00, 0xff, 0xfe, 0x01]), PayloadVerdict::Allow);
    }

    #[test]
    fn case_sensitive() {
        let f = PayloadFilter::from_config(&cfg(&["ERROR"], &[]));
        assert_eq!(f.check(b"ERROR code"), PayloadVerdict::DeniedByBlacklist);
        assert_eq!(f.check(b"error code"), PayloadVerdict::Allow);
    }
}
