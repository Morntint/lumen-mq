//! IP 黑白名单过滤（支持 IPv4/IPv6 单 IP 与 CIDR 网段）
//!
//! 设计要点：
//! - 解析期一次性把字符串配置编译为按位二叉前缀树（radix trie），
//!   IPv4 / IPv6 各一棵；运行期 contains 为 O(prefix_length) 常数时间，
//!   即使配置数千条 CIDR 也不会退化成线性扫描
//! - 黑名单优先级高于白名单（先查黑名单，命中即拒；再查白名单，未命中才拒）
//! - 单 IP 视作 /32（IPv4）或 /128（IPv6）的 CIDR

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::utils::{BrokerError, BrokerResult};

/// 一条 IP 匹配规则（CIDR 网段），仅在解析期使用；编译后插入 trie
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

/// 按位二叉前缀树节点
///
/// 每个节点有两个子节点（bit 0 / bit 1），`terminal=true` 表示从根到本节点
/// 的路径构成一条已注册的 CIDR 前缀。查询时沿 IP 的位从高到低走，沿途
/// 遇到任一 terminal 即命中（包含关系：更短前缀覆盖更长前缀）。
struct TrieNode {
    children: [Option<Box<TrieNode>>; 2],
    terminal: bool,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: [None, None],
            terminal: false,
        }
    }
}

impl Clone for TrieNode {
    fn clone(&self) -> Self {
        Self {
            children: [self.children[0].clone(), self.children[1].clone()],
            terminal: self.terminal,
        }
    }
}

/// 通用按位前缀树，按 KEY 位数区分 IPv4(u32) / IPv6(u128)
struct BitTrie {
    root: Option<Box<TrieNode>>,
    max_bits: u8,
}

impl BitTrie {
    fn new(max_bits: u8) -> Self {
        Self {
            root: None,
            max_bits,
        }
    }

    /// 插入一条 CIDR 规则：按 network 的高 prefix 位走路径，终点标记 terminal
    fn insert(&mut self, network: u128, prefix: u8) {
        let prefix = prefix.min(self.max_bits);
        if self.root.is_none() {
            self.root = Some(Box::new(TrieNode::new()));
        }
        let mut node: &mut TrieNode = self.root.as_mut().unwrap();
        // prefix=0 表示匹配所有 IP，直接标记根节点
        if prefix == 0 {
            node.terminal = true;
            return;
        }
        for i in 0..prefix {
            // 从最高位开始取位
            let shift = (self.max_bits - 1 - i) as u32;
            let bit = ((network >> shift) & 1) as usize;
            if node.children[bit].is_none() {
                node.children[bit] = Some(Box::new(TrieNode::new()));
            }
            node = node.children[bit].as_mut().unwrap();
        }
        node.terminal = true;
    }

    /// 查询 IP 是否命中任一规则：沿 IP 位走，沿途遇到 terminal 即命中
    fn contains(&self, ip: u128) -> bool {
        let Some(root) = self.root.as_deref() else {
            return false;
        };
        // 根节点 terminal（prefix=0 规则）直接命中
        if root.terminal {
            return true;
        }
        let mut node: &TrieNode = root;
        for i in 0..self.max_bits {
            let shift = (self.max_bits - 1 - i) as u32;
            let bit = ((ip >> shift) & 1) as usize;
            match node.children[bit].as_deref() {
                Some(child) => {
                    node = child;
                    if node.terminal {
                        return true;
                    }
                }
                None => return false,
            }
        }
        false
    }
}

impl Clone for BitTrie {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            max_bits: self.max_bits,
        }
    }
}

/// IP 黑白名单过滤器（不可变，热更新时整体替换）
///
/// 内部使用两棵按位前缀树（IPv4 / IPv6），contains 为 O(32/128) 常数时间，
/// 即使配置数千条 CIDR 规则也不会退化成线性扫描。
#[derive(Clone)]
pub struct IpFilter {
    /// IPv4 规则树（max_bits=32）
    v4_trie: BitTrie,
    /// IPv6 规则树（max_bits=128）
    v6_trie: BitTrie,
    /// 规则数（仅用于 is_empty 判断与调试）
    rule_count: usize,
}

impl std::fmt::Debug for IpFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpFilter")
            .field("rule_count", &self.rule_count)
            .finish()
    }
}

impl Default for IpFilter {
    fn default() -> Self {
        Self::empty()
    }
}

impl IpFilter {
    /// 从配置字符串列表构建过滤器
    pub fn new(entries: &[String]) -> BrokerResult<Self> {
        let mut filter = Self::empty();
        for e in entries {
            let rule = IpRule::parse(e)?;
            filter.add_rule(&rule);
        }
        Ok(filter)
    }

    /// 空过滤器
    pub fn empty() -> Self {
        Self {
            v4_trie: BitTrie::new(32),
            v6_trie: BitTrie::new(128),
            rule_count: 0,
        }
    }

    /// 插入一条规则到对应版本的前缀树
    fn add_rule(&mut self, rule: &IpRule) {
        match rule.network {
            IpAddr::V4(v4) => {
                let bits = u32::from(v4) as u128;
                self.v4_trie.insert(bits, rule.prefix);
            }
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                self.v6_trie.insert(bits, rule.prefix);
            }
        }
        self.rule_count += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.rule_count == 0
    }

    /// 判断 IP 是否命中任一规则（O(32) for IPv4 / O(128) for IPv6）
    pub fn contains(&self, ip: IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => self.v4_trie.contains(u32::from(v4) as u128),
            IpAddr::V6(v6) => self.v6_trie.contains(u128::from(v6)),
        }
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

    #[test]
    fn trie_handles_many_rules_without_degradation() {
        // 构建大量 CIDR 规则验证 trie 正确性
        let entries: Vec<String> = (0..256)
            .map(|i| format!("10.{i}.0.0/16"))
            .collect();
        let f = IpFilter::new(&entries).unwrap();
        // 命中
        assert!(f.contains("10.50.1.2".parse().unwrap()));
        assert!(f.contains("10.255.255.255".parse().unwrap()));
        // 未命中
        assert!(!f.contains("11.0.0.1".parse().unwrap()));
        assert!(!f.contains("192.168.1.1".parse().unwrap()));
    }

    #[test]
    fn trie_longest_prefix_semantics() {
        // 更具体前缀（更长）与更宽前缀（更短）共存：都应命中
        let f = IpFilter::new(&[
            "10.0.0.0/8".to_string(),
            "10.1.2.0/24".to_string(),
        ])
        .unwrap();
        // 10.1.2.3 命中 /24（也命中 /8）
        assert!(f.contains("10.1.2.3".parse().unwrap()));
        // 10.5.0.1 只命中 /8
        assert!(f.contains("10.5.0.1".parse().unwrap()));
        // 11.0.0.1 都不命中
        assert!(!f.contains("11.0.0.1".parse().unwrap()));
    }

    #[test]
    fn trie_ipv6_zero_prefix_matches_all() {
        let f = IpFilter::new(&["::/0".to_string()]).unwrap();
        assert!(f.contains("2001:db8::1".parse().unwrap()));
        assert!(f.contains("::1".parse().unwrap()));
        assert!(f.contains("ff::".parse().unwrap()));
    }
}
