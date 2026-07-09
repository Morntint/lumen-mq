//! 内存级离线消息缓存
//!
//! 设计要点：
//! - 每个 clean_session=false 的会话在离线时持有 `OfflineQueue`
//! - 队列容量受限（`max_messages`），超限时从最旧开始淘汰
//! - 每条消息带时间戳，TTL 过期项在入队/出队时被动淘汰（避免后台扫描开销）
//! - 阶段三将扩展为 sled 持久化

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::broker::router::OutboundPublish;

/// 单条离线消息
#[derive(Debug, Clone)]
pub struct OfflineMessage {
    pub payload: OutboundPublish,
    pub enqueued_at: Instant,
}

/// 单会话离线队列
pub struct OfflineQueue {
    inner: Mutex<VecDeque<OfflineMessage>>,
    max_messages: usize,
    ttl: Duration,
}

impl OfflineQueue {
    pub fn new(max_messages: usize, ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(32.min(max_messages))),
            max_messages,
            ttl,
        }
    }

    /// 入队一条离线消息
    /// - 先淘汰过期项
    /// - 若达容量上限，从最旧开始淘汰（FIFO drop）
    pub fn enqueue(&self, msg: OutboundPublish) {
        let now = Instant::now();
        let mut q = self.inner.lock();
        // 淘汰过期
        self.evict_expired_locked(&mut q, now);
        // 容量淘汰
        while q.len() >= self.max_messages {
            q.pop_front();
        }
        q.push_back(OfflineMessage { payload: msg, enqueued_at: now });
    }

    /// 取出全部未过期消息（清空队列），用于重连恢复
    pub fn drain(&self) -> Vec<OutboundPublish> {
        let now = Instant::now();
        let mut q = self.inner.lock();
        self.evict_expired_locked(&mut q, now);
        std::mem::take(&mut *q).into_iter().map(|m| m.payload).collect()
    }

    /// 当前队列长度（已淘汰过期）
    pub fn len(&self) -> usize {
        let now = Instant::now();
        let mut q = self.inner.lock();
        self.evict_expired_locked(&mut q, now);
        q.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 清空
    pub fn clear(&self) {
        self.inner.lock().clear();
    }

    fn evict_expired_locked(&self, q: &mut VecDeque<OfflineMessage>, now: Instant) {
        while let Some(front) = q.front() {
            if now.duration_since(front.enqueued_at) >= self.ttl {
                q.pop_front();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::QoS;

    fn mk(topic: &str) -> OutboundPublish {
        OutboundPublish {
            topic: topic.into(),
            payload: bytes::Bytes::from(vec![1]),
            qos: QoS::AtLeastOnce,
            retain: false,
        }
    }

    #[test]
    fn capacity_eviction() {
        let q = OfflineQueue::new(2, Duration::from_secs(3600));
        q.enqueue(mk("a"));
        q.enqueue(mk("b"));
        q.enqueue(mk("c"));
        let drained = q.drain();
        // 第一条 a 应被淘汰
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].topic, "b");
        assert_eq!(drained[1].topic, "c");
    }

    #[test]
    fn ttl_eviction() {
        let q = OfflineQueue::new(10, Duration::from_millis(10));
        q.enqueue(mk("old"));
        std::thread::sleep(Duration::from_millis(30));
        q.enqueue(mk("new"));
        let drained = q.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].topic, "new");
    }
}
