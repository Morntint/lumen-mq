use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::broker::auth::WillMessage;
use crate::broker::router::OutboundPublish;
use crate::broker::store_msg::OfflineQueue;
use crate::codec::QoS;

/// 单个会话条目
pub struct SessionEntry {
    pub client_id: String,
    pub clean_session: bool,
    pub tx: mpsc::Sender<OutboundPublish>,
    pub will: Option<WillMessage>,
    pub protocol_level: u8,
    pub peer_addr: SocketAddr,
    pub connected_at: Instant,
    /// 连接代际，用于识别会话被新连接接管
    pub epoch: u64,
    pub connected: bool,
    /// 离线消息队列（仅 clean_session=false 会话使用，clean=true 时为 None）
    pub offline_queue: Option<Arc<OfflineQueue>>,
    /// MQTT 5.0 Session Expiry Interval（秒）；None 表示无过期（永久保留）
    /// 仅对 clean_session=false 生效；0 表示会话立即过期（等同 clean=true）
    pub session_expiry: Option<u32>,
    /// 离线时间戳；mark_offline 时设置，重连时清零
    pub offline_at: Option<Instant>,
}

impl SessionEntry {
    /// 创建 clean_session=false 的会话 entry（带离线队列）
    pub fn persistent(
        client_id: String,
        tx: mpsc::Sender<OutboundPublish>,
        will: Option<WillMessage>,
        protocol_level: u8,
        peer_addr: SocketAddr,
        epoch: u64,
        max_offline: usize,
        offline_ttl: Duration,
        session_expiry: Option<u32>,
    ) -> Self {
        Self {
            client_id,
            clean_session: false,
            tx,
            will,
            protocol_level,
            peer_addr,
            connected_at: Instant::now(),
            epoch,
            connected: true,
            offline_queue: Some(Arc::new(OfflineQueue::new(max_offline, offline_ttl))),
            session_expiry,
            offline_at: None,
        }
    }

    /// 创建 clean_session=true 的会话 entry（无离线队列）
    pub fn clean(
        client_id: String,
        tx: mpsc::Sender<OutboundPublish>,
        will: Option<WillMessage>,
        protocol_level: u8,
        peer_addr: SocketAddr,
        epoch: u64,
    ) -> Self {
        Self {
            client_id,
            clean_session: true,
            tx,
            will,
            protocol_level,
            peer_addr,
            connected_at: Instant::now(),
            epoch,
            connected: true,
            offline_queue: None,
            session_expiry: None,
            offline_at: None,
        }
    }

    /// 会话是否已过期（基于 session_expiry + offline_at）
    pub fn is_expired(&self) -> bool {
        if self.connected {
            return false;
        }
        match (self.session_expiry, self.offline_at) {
            (Some(0), _) => true, // expiry=0 立即过期
            (Some(secs), Some(at)) => at.elapsed().as_secs() >= secs as u64,
            _ => false, // None 表示永久保留
        }
    }
}

/// 会话管理器：在线会话 + 内存级离线会话保留（阶段三接入持久化）
pub struct SessionManager {
    sessions: DashMap<String, SessionEntry>,
    epoch_counter: AtomicU64,
    max_offline_messages: usize,
    offline_message_ttl: Duration,
}

impl SessionManager {
    pub fn new() -> Self {
        Self::with_limits(1000, Duration::from_secs(86400))
    }

    pub fn with_limits(max_offline: usize, ttl: Duration) -> Self {
        Self {
            sessions: DashMap::new(),
            epoch_counter: AtomicU64::new(0),
            max_offline_messages: max_offline,
            offline_message_ttl: ttl,
        }
    }

    /// 注册/接管会话，返回 (epoch, session_present, previous_offline_messages)
    /// 若已存在同 client_id 会话：
    /// - 旧会话若 clean_session=false，其离线队列会被取出回放给新连接
    /// - 旧连接将被其自身循环通过 epoch 比对清理
    /// `session_expiry`：MQTT 5.0 Session Expiry Interval（秒）；None/Some(0) 仅对 clean=false 生效
    pub fn register(
        &self,
        client_id: String,
        clean_session: bool,
        tx: mpsc::Sender<OutboundPublish>,
        will: Option<WillMessage>,
        protocol_level: u8,
        peer_addr: SocketAddr,
        session_expiry: Option<u32>,
    ) -> (u64, bool, Vec<OutboundPublish>) {
        let epoch = self.epoch_counter.fetch_add(1, Ordering::Relaxed) + 1;
        // 取出旧会话的离线消息（若有）
        let mut drained: Vec<OutboundPublish> = Vec::new();
        let mut session_present = false;
        if let Some(old) = self.sessions.get(&client_id) {
            if !old.clean_session {
                session_present = true;
                if let Some(q) = &old.offline_queue {
                    drained = q.drain();
                }
            }
        }

        let entry = if clean_session {
            SessionEntry::clean(
                client_id.clone(),
                tx,
                will,
                protocol_level,
                peer_addr,
                epoch,
            )
        } else {
            SessionEntry::persistent(
                client_id.clone(),
                tx,
                will,
                protocol_level,
                peer_addr,
                epoch,
                self.max_offline_messages,
                self.offline_message_ttl,
                session_expiry,
            )
        };
        self.sessions.insert(client_id, entry);
        (epoch, session_present, drained)
    }

    /// 当前连接是否仍持有该会话（未被接管）
    pub fn owns(&self, client_id: &str, epoch: u64) -> bool {
        match self.sessions.get(client_id) {
            Some(e) => e.epoch == epoch,
            None => false,
        }
    }

    pub fn get_tx(&self, client_id: &str) -> Option<mpsc::Sender<OutboundPublish>> {
        self.sessions.get(client_id).map(|e| e.tx.clone())
    }

    /// 投递一条出站消息：在线则发送，离线则入队（仅 clean_session=false 会话）
    /// 返回 true 表示已成功处理（在线发送成功或离线入队）
    pub fn deliver_or_enqueue(&self, client_id: &str, msg: OutboundPublish) -> DeliveryOutcome {
        let Some(ref_entry) = self.sessions.get(client_id) else {
            return DeliveryOutcome::NoSession;
        };
        if ref_entry.connected {
            match ref_entry.tx.try_send(msg) {
                Ok(()) => DeliveryOutcome::Sent,
                Err(mpsc::error::TrySendError::Full(msg)) => {
                    // 通道已满；若有离线队列则入队，否则丢弃
                    if let Some(q) = &ref_entry.offline_queue {
                        q.enqueue(msg);
                        DeliveryOutcome::Enqueued
                    } else {
                        DeliveryOutcome::ChannelFull
                    }
                }
                Err(mpsc::error::TrySendError::Closed(msg)) => {
                    // 通道已关闭但会话仍存在；走离线入队
                    if let Some(q) = &ref_entry.offline_queue {
                        q.enqueue(msg);
                        DeliveryOutcome::Enqueued
                    } else {
                        DeliveryOutcome::Dropped
                    }
                }
            }
        } else if let Some(q) = &ref_entry.offline_queue {
            q.enqueue(msg);
            DeliveryOutcome::Enqueued
        } else {
            // clean_session=true 的离线会话不应存在；防御性丢弃
            DeliveryOutcome::Dropped
        }
    }

    pub fn remove(&self, client_id: &str) -> Option<SessionEntry> {
        self.sessions.remove(client_id).map(|(_, v)| v)
    }

    /// 标记离线但保留（用于 clean_session=false 的会话恢复）
    /// 记录 offline_at 时间戳，供 session_expiry 过期判断使用
    pub fn mark_offline(&self, client_id: &str, epoch: u64) {
        if let Some(mut e) = self.sessions.get_mut(client_id) {
            if e.epoch == epoch {
                e.connected = false;
                e.offline_at = Some(Instant::now());
            }
        }
    }

    /// 重连时清除离线时间戳（由 register 隐式完成，这里供特殊场景显式调用）
    #[allow(dead_code)]
    pub fn mark_online(&self, client_id: &str, epoch: u64) {
        if let Some(mut e) = self.sessions.get_mut(client_id) {
            if e.epoch == epoch {
                e.connected = true;
                e.offline_at = None;
            }
        }
    }

    /// 扫描并移除所有已过期的离线会话（session_expiry 到期）
    /// 返回被移除的 client_id 列表（供上层清理订阅树、磁盘快照等）
    pub fn cleanup_expired(&self) -> Vec<String> {
        let mut expired_ids: Vec<String> = Vec::new();
        let now = Instant::now();
        for entry in self.sessions.iter() {
            if entry.connected {
                continue;
            }
            let is_expired = match (entry.session_expiry, entry.offline_at) {
                (Some(0), _) => true,
                (Some(secs), Some(at)) => now.duration_since(at).as_secs() >= secs as u64,
                _ => false,
            };
            if is_expired {
                expired_ids.push(entry.client_id.clone());
            }
        }
        // 移除已过期的会话
        for id in &expired_ids {
            self.sessions.remove(id);
        }
        expired_ids
    }

    /// 检查某个 client_id 的会话是否已因 session_expiry 过期
    /// 供 cleanup 逻辑判断是否应保留离线会话
    pub fn is_session_expired(&self, client_id: &str) -> bool {
        if let Some(e) = self.sessions.get(client_id) {
            e.is_expired()
        } else {
            false
        }
    }

    /// 取出遗嘱消息（仅在当前连接仍持有时）
    pub fn take_will(&self, client_id: &str, epoch: u64) -> Option<WillMessage> {
        // 取走（替换为 None）以避免重复触发
        if let Some(mut e) = self.sessions.get_mut(client_id) {
            if e.epoch == epoch {
                return e.will.take();
            }
        }
        None
    }

    /// 清除遗嘱（用于客户端主动 DISCONNECT 时，不应触发遗嘱）
    pub fn clear_will(&self, client_id: &str, epoch: u64) {
        if let Some(mut e) = self.sessions.get_mut(client_id) {
            if e.epoch == epoch {
                e.will = None;
            }
        }
    }

    pub fn online_count(&self) -> usize {
        self.sessions.iter().filter(|e| e.connected).count()
    }

    pub fn total_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn iter_snapshot(&self) -> Vec<(String, bool, SocketAddr, Instant)> {
        self.sessions
            .iter()
            .map(|e| (e.client_id.clone(), e.connected, e.peer_addr, e.connected_at))
            .collect()
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// 投递结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// 在线成功发送
    Sent,
    /// 离线入队
    Enqueued,
    /// 通道已满（背压）
    ChannelFull,
    /// 队列已满或会话为 clean 但已离线，消息被丢弃
    Dropped,
    /// 无此会话
    NoSession,
}

/// 投递 QoS 计算：取发布 QoS 与订阅 QoS 的较小值
pub fn delivery_qos(pub_qos: QoS, sub_qos: QoS) -> QoS {
    match (pub_qos, sub_qos) {
        (QoS::AtMostOnce, _) | (_, QoS::AtMostOnce) => QoS::AtMostOnce,
        (QoS::AtLeastOnce, _) | (_, QoS::AtLeastOnce) => QoS::AtLeastOnce,
        (QoS::ExactlyOnce, QoS::ExactlyOnce) => QoS::ExactlyOnce,
    }
}

pub type SharedSessionManager = Arc<SessionManager>;

// 抑制未使用警告（保留 Mutex 供未来扩展使用）
#[allow(dead_code)]
type _Unused = Mutex<()>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn addr() -> SocketAddr {
        "127.0.0.1:1234".parse().unwrap()
    }

    fn mk_msg(topic: &str) -> OutboundPublish {
        OutboundPublish {
            topic: topic.into(),
            payload: vec![1],
            qos: QoS::AtLeastOnce,
            retain: false,
        }
    }

    #[test]
    fn persistent_session_offline_enqueue_and_drain() {
        let mgr = SessionManager::with_limits(10, Duration::from_secs(3600));
        let (tx, _rx) = mpsc::channel::<OutboundPublish>(8);

        // 首次注册（clean=false）
        let (epoch1, present1, drained1) = mgr.register(
            "c1".into(),
            false,
            tx,
            None,
            4,
            addr(),
            None,
        );
        assert!(!present1);
        assert!(drained1.is_empty());

        // 标记离线
        mgr.mark_offline("c1", epoch1);

        // 投递应入队
        let r = mgr.deliver_or_enqueue("c1", mk_msg("a"));
        assert_eq!(r, DeliveryOutcome::Enqueued);

        // 重新注册（clean=false），应取回离线消息
        let (tx2, _rx2) = mpsc::channel::<OutboundPublish>(8);
        let (_epoch2, present2, drained2) = mgr.register("c1".into(), false, tx2, None, 4, addr(), None);
        assert!(present2);
        assert_eq!(drained2.len(), 1);
        assert_eq!(drained2[0].topic, "a");
    }

    #[test]
    fn clean_session_drops_offline() {
        let mgr = SessionManager::with_limits(10, Duration::from_secs(3600));
        let (tx, _rx) = mpsc::channel::<OutboundPublish>(8);
        let (epoch, _, _) = mgr.register("c2".into(), true, tx, None, 4, addr(), None);
        mgr.mark_offline("c2", epoch);

        // clean session 离线时投递应被丢弃
        let r = mgr.deliver_or_enqueue("c2", mk_msg("x"));
        assert_eq!(r, DeliveryOutcome::Dropped);
    }

    #[test]
    fn session_expiry_zero_treats_as_clean() {
        // session_expiry=0 的持久会话，离线后应被 cleanup_expired 立即移除
        let mgr = SessionManager::with_limits(10, Duration::from_secs(3600));
        let (tx, _rx) = mpsc::channel::<OutboundPublish>(8);
        let (epoch, _, _) = mgr.register("c3".into(), false, tx, None, 5, addr(), Some(0));
        mgr.mark_offline("c3", epoch);

        let expired = mgr.cleanup_expired();
        assert_eq!(expired, vec!["c3".to_string()]);
        assert!(mgr.total_count() == 0);
    }

    #[test]
    fn session_expiry_none_keeps_session() {
        // session_expiry=None 的持久会话，离线后不应被清理
        let mgr = SessionManager::with_limits(10, Duration::from_secs(3600));
        let (tx, _rx) = mpsc::channel::<OutboundPublish>(8);
        let (epoch, _, _) = mgr.register("c4".into(), false, tx, None, 4, addr(), None);
        mgr.mark_offline("c4", epoch);

        let expired = mgr.cleanup_expired();
        assert!(expired.is_empty());
        assert_eq!(mgr.total_count(), 1);
    }

    #[test]
    fn session_expiry_keeps_within_window() {
        // session_expiry=3600 的持久会话，刚离线不应被清理
        let mgr = SessionManager::with_limits(10, Duration::from_secs(3600));
        let (tx, _rx) = mpsc::channel::<OutboundPublish>(8);
        let (epoch, _, _) = mgr.register("c5".into(), false, tx, None, 5, addr(), Some(3600));
        mgr.mark_offline("c5", epoch);

        let expired = mgr.cleanup_expired();
        assert!(expired.is_empty());
        assert_eq!(mgr.total_count(), 1);
    }
}
