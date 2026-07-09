//! 限流器（令牌桶 + 单 IP 连接计数）
//!
//! 设计要点：
//! - `TokenBucket`：单客户端消息速率限流，懒填充（按时间差补令牌）
//! - `IpConnectionCounter`：单 IP 并发连接数计数，连接建立 +1、断开 -1
//! - 全部无锁或细粒度锁，适合高并发接入

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Instant;

use parking_lot::Mutex;

// ---------- 令牌桶 ----------

/// 单客户端令牌桶（用于 PUBLISH 速率限流）
///
/// 算法：每次消费时按距离上次填充的时长补令牌（上限 capacity），
/// 然后尝试扣减 1 个令牌；不足则拒绝。
#[derive(Debug, Clone)]
pub struct TokenBucket {
    /// 桶容量（最大令牌数）
    capacity: u32,
    /// 每秒补充令牌数
    refill_per_second: u32,
    /// 当前令牌数（浮点避免整数除法误差）
    tokens: f64,
    /// 上次填充时间
    last_refill: Instant,
}

impl TokenBucket {
    /// 创建一个已填满的令牌桶
    pub fn new(capacity: u32, refill_per_second: u32) -> Self {
        Self {
            capacity,
            refill_per_second,
            tokens: capacity as f64,
            last_refill: Instant::now(),
        }
    }

    /// 尝试消费 n 个令牌；成功返回 true，不足返回 false
    pub fn try_consume(&mut self, n: u32) -> bool {
        self.refill();
        if self.tokens >= n as f64 {
            self.tokens -= n as f64;
            true
        } else {
            false
        }
    }

    /// 按时间差补充令牌
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        let added = elapsed * self.refill_per_second as f64;
        self.tokens = (self.tokens + added).min(self.capacity as f64);
        self.last_refill = now;
    }

    /// 当前可用令牌数（仅用于观测）
    pub fn available_tokens(&self) -> u32 {
        self.tokens as u32
    }
}

// ---------- 单 IP 连接计数器 ----------

/// 单 IP 并发连接计数器
///
/// 连接建立时 `inc`，断开时 `dec`；计数归零时清理条目，避免长期内存增长。
#[derive(Debug, Default)]
pub struct IpConnectionCounter {
    inner: Mutex<HashMap<IpAddr, u64>>,
}

impl IpConnectionCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 增加某 IP 的连接计数，返回增加后的值
    pub fn inc(&self, ip: IpAddr) -> u64 {
        let mut m = self.inner.lock();
        let v = m.entry(ip).or_insert(0);
        *v += 1;
        *v
    }

    /// 减少某 IP 的连接计数，返回减少后的值；归零时移除条目
    pub fn dec(&self, ip: IpAddr) {
        let mut m = self.inner.lock();
        if let Some(v) = m.get_mut(&ip) {
            if *v > 0 {
                *v -= 1;
            }
            if *v == 0 {
                m.remove(&ip);
            }
        }
    }

    /// 查询某 IP 当前连接数
    pub fn count(&self, ip: IpAddr) -> u64 {
        self.inner.lock().get(&ip).copied().unwrap_or(0)
    }

    /// 当前已跟踪的 IP 数量（用于观测）
    pub fn tracked_ips(&self) -> usize {
        self.inner.lock().len()
    }
}

// ---------- 每客户端限流表 ----------

/// 每客户端令牌桶表（用于 PUBLISH 速率限流）
///
/// 懒初始化：客户端首次 PUBLISH 时创建其桶。桶配置变更时整体重建。
#[derive(Debug, Default)]
pub struct ClientRateLimiter {
    /// 桶容量 = 每秒补充量（即允许小突发）
    capacity: u32,
    refill_per_second: u32,
    inner: Mutex<HashMap<String, TokenBucket>>,
}

impl ClientRateLimiter {
    pub fn new(refill_per_second: u32) -> Self {
        Self {
            // 容量设为 2 倍速率，允许短时小突发
            capacity: refill_per_second.saturating_mul(2).max(1),
            refill_per_second,
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// 尝试为某客户端消费 1 个令牌；成功返回 true
    pub fn try_consume(&self, client_id: &str) -> bool {
        if self.refill_per_second == 0 {
            return true; // 0 表示不限流
        }
        let mut m = self.inner.lock();
        let bucket = m
            .entry(client_id.to_string())
            .or_insert_with(|| TokenBucket::new(self.capacity, self.refill_per_second));
        bucket.try_consume(1)
    }

    /// 客户端断开时清理其桶，释放内存
    pub fn remove(&self, client_id: &str) {
        self.inner.lock().remove(client_id);
    }

    /// 重置配置（热更新时调用）：清空所有桶
    pub fn reset(&mut self, refill_per_second: u32) {
        self.capacity = refill_per_second.saturating_mul(2).max(1);
        self.refill_per_second = refill_per_second;
        self.inner.get_mut().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_bucket_initial_full() {
        let mut b = TokenBucket::new(10, 5);
        // 初始满桶，可消费 10 次
        for _ in 0..10 {
            assert!(b.try_consume(1));
        }
        // 第 11 次应失败
        assert!(!b.try_consume(1));
    }

    #[test]
    fn token_bucket_refill() {
        let mut b = TokenBucket::new(10, 100);
        // 清空
        for _ in 0..10 {
            b.try_consume(1);
        }
        assert!(!b.try_consume(1));
        // 等 50ms，应补充约 5 个令牌
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(b.try_consume(1));
        assert!(b.try_consume(1));
    }

    #[test]
    fn ip_counter_basic() {
        let c = IpConnectionCounter::new();
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        assert_eq!(c.count(ip), 0);
        assert_eq!(c.inc(ip), 1);
        assert_eq!(c.inc(ip), 2);
        assert_eq!(c.count(ip), 2);
        c.dec(ip);
        assert_eq!(c.count(ip), 1);
        c.dec(ip);
        assert_eq!(c.count(ip), 0);
        // 归零后条目应被清理
        assert_eq!(c.tracked_ips(), 0);
    }

    #[test]
    fn client_rate_limiter_zero_means_unlimited() {
        let l = ClientRateLimiter::new(0);
        for _ in 0..1000 {
            assert!(l.try_consume("c1"));
        }
    }

    #[test]
    fn client_rate_limiter_throttles() {
        let l = ClientRateLimiter::new(2);
        // 容量 = 4，可消费 4 次
        for _ in 0..4 {
            assert!(l.try_consume("c1"));
        }
        // 第 5 次应被限流
        assert!(!l.try_consume("c1"));
        // 其它客户端不受影响
        assert!(l.try_consume("c2"));
    }
}
