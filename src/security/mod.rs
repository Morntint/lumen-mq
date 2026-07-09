//! 安全中间件（阶段四）
//!
//! 提供能力：
//! - `SecurityGuard`：统一安全检查入口，组合 IP 黑白名单 + 单 IP 连接限流 + 消息速率限流
//! - 热更新：`reload()` 原子替换规则配置，无需重启即生效
//! - 接入点：TCP/TLS/WS server 在 accept 阶段调用 `check_connection`；
//!   broker 主循环在处理 PUBLISH 时调用 `check_publish`
//!
//! 设计原则：
//! - 检查路径无锁或细粒度锁，避免成为热路径瓶颈
//! - 配置禁用时所有检查直接放行（零成本）
//! - 限流配置变更时整体重建限流表（简单可靠，工业场景限流规则不频繁变更）

pub mod ip_filter;
pub mod limiter;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tracing::{debug, info, warn};

use crate::config::SecurityConfig;
use crate::monitor::METRICS;
use crate::utils::{BrokerError, BrokerResult};

pub use ip_filter::{IpAcl, IpFilter, IpVerdict};
pub use limiter::{ClientRateLimiter, IpConnectionCounter, TokenBucket};

/// 已编译的安全规则（不可变，热更新时整体替换）
#[derive(Debug)]
#[derive(Default)]
struct SecurityPolicy {
    enabled: bool,
    acl: IpAcl,
    max_connections_per_ip: usize,
    publish_rate_per_second: u32,
    max_payload_bytes: usize,
}


/// 安全中间件守卫（共享句柄）
///
/// 通过 `ArcSwap` 持有当前策略，`reload()` 时原子替换。
/// 限流器（连接计数 + 客户端令牌桶）单独持有，配置变更时 reset。
#[derive(Debug)]
pub struct SecurityGuard {
    /// 当前策略（热更新原子替换）
    policy: ArcSwap<SecurityPolicy>,
    /// 单 IP 连接计数器（运行期状态，跨策略保留）
    ip_counter: IpConnectionCounter,
    /// 每客户端 PUBLISH 速率限流器（运行期状态）
    /// 自身已含细粒度内锁，无需外层再套 Mutex（避免双重加锁）
    rate_limiter: ClientRateLimiter,
}

impl SecurityGuard {
    /// 从配置构建守卫
    pub fn new(cfg: &SecurityConfig) -> BrokerResult<Arc<Self>> {
        let policy = Self::build_policy(cfg)?;
        Ok(Arc::new(Self {
            policy: ArcSwap::from(Arc::new(policy)),
            ip_counter: IpConnectionCounter::new(),
            rate_limiter: ClientRateLimiter::new(cfg.publish_rate_per_second),
        }))
    }

    /// 禁用状态构建（所有检查放行）
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            policy: ArcSwap::from(Arc::new(SecurityPolicy::default())),
            ip_counter: IpConnectionCounter::new(),
            rate_limiter: ClientRateLimiter::new(0),
        })
    }

    /// 把配置编译为不可变策略
    fn build_policy(cfg: &SecurityConfig) -> BrokerResult<SecurityPolicy> {
        let blacklist = IpFilter::new(&cfg.ip_blacklist)?;
        let whitelist = IpFilter::new(&cfg.ip_whitelist)?;
        Ok(SecurityPolicy {
            enabled: cfg.enabled,
            acl: IpAcl::new(blacklist, whitelist),
            max_connections_per_ip: cfg.max_connections_per_ip,
            publish_rate_per_second: cfg.publish_rate_per_second,
            max_payload_bytes: cfg.max_payload_bytes,
        })
    }

    /// 热更新：原子替换策略 + 重置限流器
    pub fn reload(&self, cfg: &SecurityConfig) -> BrokerResult<()> {
        let policy = Self::build_policy(cfg)?;
        self.policy.store(Arc::new(policy));
        // 限流速率变更时重建令牌桶表
        self.rate_limiter.reset(cfg.publish_rate_per_second);
        info!(
            enabled = cfg.enabled,
            blacklist = cfg.ip_blacklist.len(),
            whitelist = cfg.ip_whitelist.len(),
            max_conn_per_ip = cfg.max_connections_per_ip,
            publish_rate = cfg.publish_rate_per_second,
            "security policy reloaded"
        );
        Ok(())
    }

    /// 当前策略是否启用
    pub fn enabled(&self) -> bool {
        self.policy.load().enabled
    }

    // ---------- 连接准入检查 ----------

    /// 连接建立前检查：IP 黑白名单 + 单 IP 连接数限制
    ///
    /// 返回 Ok(()) 表示放行；Err 表示拒绝（含拒绝原因）。
    /// 通过后调用方应在连接生命周期内持有 guard，断开时调用 `on_disconnect`。
    pub fn check_connection(&self, peer: SocketAddr) -> BrokerResult<()> {
        let policy = self.policy.load();
        if !policy.enabled {
            return Ok(());
        }
        let ip = peer.ip();

        // 1. IP 黑白名单
        match policy.acl.check(ip) {
            IpVerdict::Allow => {}
            IpVerdict::Blacklisted => {
                warn!(%ip, "connection rejected: IP blacklisted");
                METRICS.inc_disconnect();
                return Err(BrokerError::RateLimited(format!(
                    "IP {ip} blacklisted"
                )));
            }
            IpVerdict::NotWhitelisted => {
                warn!(%ip, "connection rejected: IP not in whitelist");
                METRICS.inc_disconnect();
                return Err(BrokerError::RateLimited(format!(
                    "IP {ip} not in whitelist"
                )));
            }
        }

        // 2. 单 IP 连接数限制
        if policy.max_connections_per_ip > 0 {
            let current = self.ip_counter.count(ip);
            if current as usize >= policy.max_connections_per_ip {
                warn!(%ip, current, max = policy.max_connections_per_ip, "connection rejected: per-ip limit reached");
                METRICS.inc_disconnect();
                return Err(BrokerError::RateLimited(format!(
                    "IP {ip} exceeded max connections per ip ({current})"
                )));
            }
        }
        Ok(())
    }

    /// 连接已建立（通过 check_connection 后调用）：登记 IP 计数
    pub fn on_connect(&self, peer: SocketAddr) {
        let policy = self.policy.load();
        if !policy.enabled || policy.max_connections_per_ip == 0 {
            return;
        }
        self.ip_counter.inc(peer.ip());
    }

    /// 连接断开：扣减 IP 计数 + 清理客户端令牌桶
    pub fn on_disconnect(&self, peer: SocketAddr, client_id: Option<&str>) {
        let policy = self.policy.load();
        if policy.enabled && policy.max_connections_per_ip > 0 {
            self.ip_counter.dec(peer.ip());
        }
        if let Some(cid) = client_id {
            self.rate_limiter.remove(cid);
        }
    }

    // ---------- 入站 PUBLISH 检查 ----------

    /// 检查入站 PUBLISH：速率限流 + 载荷长度
    ///
    /// 返回 Ok(()) 表示放行；Err 表示拒绝该消息（不断开连接）。
    pub fn check_publish(
        &self,
        client_id: &str,
        payload_len: usize,
        broker_max_packet: usize,
    ) -> BrokerResult<()> {
        let policy = self.policy.load();
        if !policy.enabled {
            return Ok(());
        }

        // 1. 载荷长度检查
        let max = if policy.max_payload_bytes > 0 {
            policy.max_payload_bytes
        } else {
            broker_max_packet
        };
        if payload_len > max {
            warn!(client = %client_id, payload_len, max, "PUBLISH rejected: payload too large");
            return Err(BrokerError::PacketTooLarge(payload_len, max));
        }

        // 2. 速率限流
        if policy.publish_rate_per_second > 0
            && !self.rate_limiter.try_consume(client_id)
        {
            debug!(client = %client_id, "PUBLISH rejected: rate limited");
            return Err(BrokerError::RateLimited(format!(
                "client {client_id} publish rate exceeded"
            )));
        }
        Ok(())
    }

    // ---------- 观测 ----------

    /// 当前跟踪的 IP 数量（用于运维观测）
    pub fn tracked_ip_count(&self) -> usize {
        self.ip_counter.tracked_ips()
    }

    /// 查询某 IP 当前连接数
    pub fn ip_connection_count(&self, ip: IpAddr) -> u64 {
        self.ip_counter.count(ip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_blacklist(ips: &[&str]) -> SecurityConfig {
        SecurityConfig {
            enabled: true,
            ip_blacklist: ips.iter().map(|s| s.to_string()).collect(),
            ip_whitelist: Vec::new(),
            max_connections_per_ip: 0,
            publish_rate_per_second: 0,
            max_payload_bytes: 0,
        }
    }

    #[test]
    fn disabled_guard_allows_all() {
        let g = SecurityGuard::disabled();
        let peer: SocketAddr = "1.2.3.4:1234".parse().unwrap();
        assert!(g.check_connection(peer).is_ok());
        g.on_connect(peer);
        g.on_disconnect(peer, Some("c1"));
        assert!(g.check_publish("c1", 9999, 1024).is_ok());
    }

    #[test]
    fn blacklist_rejects() {
        let g = SecurityGuard::new(&cfg_with_blacklist(&["10.0.0.1"])).unwrap();
        let peer_blocked: SocketAddr = "10.0.0.1:1234".parse().unwrap();
        let peer_ok: SocketAddr = "10.0.0.2:1234".parse().unwrap();
        assert!(g.check_connection(peer_blocked).is_err());
        assert!(g.check_connection(peer_ok).is_ok());
    }

    #[test]
    fn per_ip_connection_limit() {
        let mut cfg = cfg_with_blacklist(&[]);
        cfg.max_connections_per_ip = 2;
        let g = SecurityGuard::new(&cfg).unwrap();
        let p1: SocketAddr = "1.1.1.1:1000".parse().unwrap();
        let p2: SocketAddr = "1.1.1.1:1001".parse().unwrap();
        let p3: SocketAddr = "1.1.1.1:1002".parse().unwrap();

        // 前两个连接允许
        assert!(g.check_connection(p1).is_ok());
        g.on_connect(p1);
        assert!(g.check_connection(p2).is_ok());
        g.on_connect(p2);
        // 第三个应被拒
        assert!(g.check_connection(p3).is_err());
        // 断开一个后又能连
        g.on_disconnect(p1, None);
        assert!(g.check_connection(p3).is_ok());
    }

    #[test]
    fn payload_size_limit() {
        let mut cfg = cfg_with_blacklist(&[]);
        cfg.max_payload_bytes = 100;
        let g = SecurityGuard::new(&cfg).unwrap();
        assert!(g.check_publish("c1", 50, 1024).is_ok());
        assert!(g.check_publish("c1", 101, 1024).is_err());
        // 0 表示用 broker 上限
        cfg.max_payload_bytes = 0;
        let g2 = SecurityGuard::new(&cfg).unwrap();
        assert!(g2.check_publish("c1", 500, 1024).is_ok());
        assert!(g2.check_publish("c1", 2000, 1024).is_err());
    }

    #[test]
    fn reload_updates_blacklist() {
        let g = SecurityGuard::new(&cfg_with_blacklist(&["10.0.0.1"])).unwrap();
        let p: SocketAddr = "10.0.0.1:1".parse().unwrap();
        assert!(g.check_connection(p).is_err());

        // 热更新：清空黑名单
        let cfg2 = cfg_with_blacklist(&[]);
        g.reload(&cfg2).unwrap();
        assert!(g.check_connection(p).is_ok());
    }
}
