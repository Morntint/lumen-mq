//! IP 黑白名单过滤（支持 IPv4/IPv6 单 IP 与 CIDR 网段）
//!
//! 设计要点：
//! - 解析期一次性把字符串配置编译为 `IpRule` 列表，运行期匹配仅做位运算
//! - 黑名单优先级高于白名单（先查黑名单，命中即拒；再查白名单，未命中才拒）
//! - 单 IP 视作 /32（IPv4）或 /128（IPv6）的 CIDR

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::utils::{BrokerError, BrokerResult};

/// 一条 IP 匹配规则（CIDR 网段）
#[derive(Debug, Clone)]
struct IpRule {
    /// 网络地址（已按前缀长度掩码）
    network: IpAddr,
    /// 前缀长度
    prefix: u8,
}

impl IpRule {
    /// 解析 "192.168.1.0/24" / "::1/128" / "10.0.0.5" 为 IpRule
    fn parse(s: &str) -> BrokerResult<Self> {
        let s = s.trim();
        let (addr_str, prefix) = match s.rsplit_once('/') {
            Some((a, p)) => {
                let p: u8 = p
                    .parse()
                    .map_err(|_| BrokerError::Config(format!("invalid CIDR prefix in '{s}'")))?;
                (a, p)
            }
            None => (s, 0), // 无前缀，后续按单 IP 补全
        };
        let addr: IpAddr = addr_str
            .parse()
            .map_err(|_| BrokerError::Config(format!("invalid IP address in '{s}'")))?;
        // 无前缀时按单 IP 补全
        let prefix = if s.rsplit_once('/').is_none() {
            match addr {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            }
        } else {
            prefix
        };
        // 校验前缀长度合法性
        let max = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max {
            return Err(BrokerError::Config(format!(
                "CIDR prefix {prefix} exceeds max {max} for '{s}'"
            )));
        }
        // 把网络地址按前缀掩码，规范化
        let network = apply_mask(addr, prefix);
        Ok(Self { network, prefix })
    }

    /// 判断目标 IP 是否命中本规则
    fn matches(&self, ip: IpAddr) -> bool {
        // 版本不匹配直接放行，避免 apply_mask 跨版本算术下溢 panic
        match (&self.network, ip) {
            (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_)) => {}
            _ => return false,
        }
        let masked = apply_mask(ip, self.prefix);
        self.network == masked
    }
}

/// 按 CIDR 前缀掩码一个 IP
fn apply_mask(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => {
            let prefix = prefix.min(32);
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                (!0u32) << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(bits & mask))
        }
        IpAddr::V6(v6) => {
            let prefix = prefix.min(128);
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                (!0u128) << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(bits & mask))
        }
    }
}

/// IP 黑白名单过滤器（不可变，热更新时整体替换）
#[derive(Debug, Clone, Default)]
pub struct IpFilter {
    rules: Vec<IpRule>,
}

impl IpFilter {
    /// 从配置字符串列表构建过滤器
    pub fn new(entries: &[String]) -> BrokerResult<Self> {
        let mut rules = Vec::with_capacity(entries.len());
        for e in entries {
            rules.push(IpRule::parse(e)?);
        }
        Ok(Self { rules })
    }

    /// 空过滤器
    pub fn empty() -> Self {
        Self { rules: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// 判断 IP 是否命中任一规则
    pub fn contains(&self, ip: IpAddr) -> bool {
        self.rules.iter().any(|r| r.matches(ip))
    }
}

/// IP 准入决策结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpVerdict {
    /// 允许连接
    Allow,
    /// 命中黑名单
    Blacklisted,
    /// 不在白名单内
    NotWhitelisted,
}

/// 黑名单 + 白名单组合的准入检查
///
/// 决策顺序：先查黑名单（命中即拒），再查白名单（非空且未命中即拒）。
/// 两者均为空时允许所有。
#[derive(Debug, Clone, Default)]
pub struct IpAcl {
    pub blacklist: IpFilter,
    pub whitelist: IpFilter,
}

impl IpAcl {
    pub fn new(blacklist: IpFilter, whitelist: IpFilter) -> Self {
        Self { blacklist, whitelist }
    }

    /// 检查给定 IP 是否允许接入
    pub fn check(&self, ip: IpAddr) -> IpVerdict {
        if self.blacklist.contains(ip) {
            return IpVerdict::Blacklisted;
        }
        if !self.whitelist.is_empty() && !self.whitelist.contains(ip) {
            return IpVerdict::NotWhitelisted;
        }
        IpVerdict::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_ipv4() {
        let f = IpFilter::new(&["192.168.1.5".to_string()]).unwrap();
        assert!(f.contains("192.168.1.5".parse().unwrap()));
        assert!(!f.contains("192.168.1.6".parse().unwrap()));
    }

    #[test]
    fn parse_cidr_ipv4() {
        let f = IpFilter::new(&["10.0.0.0/8".to_string()]).unwrap();
        assert!(f.contains("10.1.2.3".parse().unwrap()));
        assert!(f.contains("10.255.255.255".parse().unwrap()));
        assert!(!f.contains("11.0.0.0".parse().unwrap()));
    }

    #[test]
    fn parse_cidr_ipv6() {
        let f = IpFilter::new(&["2001:db8::/32".to_string()]).unwrap();
        assert!(f.contains("2001:db8:1::1".parse().unwrap()));
        assert!(!f.contains("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn parse_zero_prefix() {
        let f = IpFilter::new(&["0.0.0.0/0".to_string()]).unwrap();
        assert!(f.contains("1.2.3.4".parse().unwrap()));
        assert!(f.contains("255.255.255.255".parse().unwrap()));
    }

    #[test]
    fn acl_blacklist_priority() {
        let acl = IpAcl::new(
            IpFilter::new(&["10.0.0.1".to_string()]).unwrap(),
            IpFilter::new(&["10.0.0.0/8".to_string()]).unwrap(),
        );
        // 10.0.0.1 在白名单网段内但命中黑名单 → 拒
        assert_eq!(acl.check("10.0.0.1".parse().unwrap()), IpVerdict::Blacklisted);
        // 10.0.0.2 在白名单网段内且不在黑名单 → 允许
        assert_eq!(acl.check("10.0.0.2".parse().unwrap()), IpVerdict::Allow);
        // 192.168.1.1 不在白名单 → 拒
        assert_eq!(
            acl.check("192.168.1.1".parse().unwrap()),
            IpVerdict::NotWhitelisted
        );
    }

    #[test]
    fn acl_empty_allows_all() {
        let acl = IpAcl::default();
        assert_eq!(acl.check("1.2.3.4".parse().unwrap()), IpVerdict::Allow);
    }

    #[test]
    fn invalid_prefix_rejected() {
        assert!(IpFilter::new(&["10.0.0.0/33".to_string()]).is_err());
        assert!(IpFilter::new(&["::1/129".to_string()]).is_err());
    }
}
