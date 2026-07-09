//! Retain 保留消息存储
//!
//! 设计要点：
//! - 仅"精确主题"（不含 `+`/`#`）的 PUBLISH 才可被 retain
//! - 入站 PUBLISH retain=1 且 payload 非空 → 写入；payload 空 → 删除该主题的 retained
//! - 新订阅者订阅某 filter 时，扫描全部 retained，匹配 filter 的按 min(pub_qos, sub_qos) 投递
//! - 全局单实例，跨连接共享，使用 `parking_lot::RwLock<HashMap<...>>`
//! - 可选 sled 持久化：开启时所有 set/remove 同步落盘，启动时由 BrokerState 调用
//!   `load_from_storage` 恢复

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::broker::router::OutboundPublish;
use crate::broker::subscription::topic_matches_filter;
use crate::codec::QoS;
use crate::storage::SharedStorage;

/// 保留消息条目
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetainedMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
}

/// 全局 Retain 存储
#[derive(Default)]
pub struct RetainStore {
    map: RwLock<HashMap<String, RetainedMessage>>,
    storage: Option<SharedStorage>,
    /// retained payload 最大字节数；0 表示不限制（用 broker.max_packet_size 兜底）
    max_payload_bytes: usize,
}

impl RetainStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 附着 sled 持久化后端
    pub fn with_storage(storage: SharedStorage) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            storage: Some(storage),
            max_payload_bytes: 0,
        }
    }

    /// 设置 retained payload 最大字节数（通常来自 broker.max_packet_size）
    pub fn with_max_payload_bytes(mut self, max: usize) -> Self {
        self.max_payload_bytes = max;
        self
    }

    /// 从 sled 加载全部 retained 到内存（启动时调用）
    pub fn load_from_storage(&self, storage: &SharedStorage) -> Result<(), crate::utils::BrokerError> {
        let msgs = storage.load_all_retained()?;
        let mut map = self.map.write();
        for m in msgs {
            map.insert(m.topic.clone(), m);
        }
        Ok(())
    }

    /// 写入或删除一条 retained 消息
    /// - `payload` 为空 → 删除该 topic 的 retained
    /// - `payload` 非空 → 写入/覆盖
    /// - 主题含通配符则忽略（仅精确主题可被 retain）
    ///
    /// 接受 `impl Into<Bytes>`：调用方可传 `Vec<u8>`（零拷贝转换为 Bytes）或现成的 `Bytes`，
    /// 避免在路由热路径上重复分配。
    pub fn set(&self, topic: &str, payload: impl Into<Bytes>, qos: QoS) {
        if topic.is_empty() || topic.contains('+') || topic.contains('#') {
            return;
        }
        let payload = payload.into();
        // payload 上限校验：防止 admin /publish 或其他绕过 broker.max_packet_size
        // 的路径写入超大 retained 消息，导致内存单调增长
        if self.max_payload_bytes > 0 && payload.len() > self.max_payload_bytes {
            tracing::warn!(
                %topic,
                payload_len = payload.len(),
                max = self.max_payload_bytes,
                "retained payload exceeds limit, rejected"
            );
            return;
        }
        crate::monitor::METRICS.inc_retained_stored();
        let mut map = self.map.write();
        if payload.is_empty() {
            // 先落盘删除（与 insert 分支对称：失败时不改内存，避免内存与磁盘不一致）
            if let Some(s) = &self.storage {
                if let Err(e) = s.delete_retained(topic) {
                    tracing::warn!(error = %e, %topic, "persist delete_retained failed, skipping memory delete");
                    return;
                }
            }
            map.remove(topic);
        } else {
            let msg = RetainedMessage {
                topic: topic.to_string(),
                payload,
                qos,
            };
            // 落盘写入（先落盘后写内存：失败时不污染内存，避免内存与磁盘不一致）
            if let Some(s) = &self.storage {
                if let Err(e) = s.save_retained(topic, &msg) {
                    tracing::warn!(error = %e, %topic, "persist save_retained failed, skipping memory write");
                    return;
                }
            }
            map.insert(topic.to_string(), msg);
        }
    }

    pub fn remove(&self, topic: &str) {
        let mut map = self.map.write();
        map.remove(topic);
        if let Some(s) = &self.storage {
            if let Err(e) = s.delete_retained(topic) {
                tracing::warn!(error = %e, %topic, "persist delete_retained failed");
            }
        }
    }

    pub fn get(&self, topic: &str) -> Option<RetainedMessage> {
        self.map.read().get(topic).cloned()
    }

    pub fn len(&self) -> usize {
        self.map.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.read().is_empty()
    }

    /// 返回所有匹配订阅过滤器的 retained 消息
    pub fn matches(&self, filter: &str) -> Vec<RetainedMessage> {
        let map = self.map.read();
        map.values()
            .filter(|m| topic_matches_filter(&m.topic, filter))
            .cloned()
            .collect()
    }

    /// 构造投递给某订阅者的 OutboundPublish（按 min(pub_qos, sub_qos) 降级）
    pub fn build_outbound(msg: &RetainedMessage, sub_qos: QoS) -> OutboundPublish {
        let qos = std::cmp::min(msg.qos, sub_qos);
        OutboundPublish {
            topic: msg.topic.clone(),
            payload: msg.payload.clone(),
            qos,
            // 投递给新订阅者时 retain=1（MQTT 3.1.1 规定）
            retain: true,
        }
    }

    /// 清空所有 retained（不落盘，仅用于测试）
    pub fn clear(&self) {
        self.map.write().clear();
    }
}

pub type SharedRetainStore = Arc<RetainStore>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_remove() {
        let store = RetainStore::new();
        store.set("a/b", b"hello".to_vec(), QoS::AtLeastOnce);
        assert_eq!(store.len(), 1);
        assert!(store.get("a/b").is_some());

        // 空载荷 → 删除
        store.set("a/b", vec![], QoS::AtMostOnce);
        assert!(store.get("a/b").is_none());
        assert!(store.is_empty());
    }

    #[test]
    fn wildcard_topic_ignored() {
        let store = RetainStore::new();
        store.set("a/+", b"x".to_vec(), QoS::AtMostOnce);
        store.set("#", b"y".to_vec(), QoS::AtMostOnce);
        assert!(store.is_empty());
    }

    #[test]
    fn matches_filter() {
        let store = RetainStore::new();
        store.set("sensor/temp", b"23.5".to_vec(), QoS::AtLeastOnce);
        store.set("sensor/humi", b"40".to_vec(), QoS::AtMostOnce);
        store.set("status/on", b"1".to_vec(), QoS::AtMostOnce);

        let m = store.matches("sensor/+");
        assert_eq!(m.len(), 2);
        let m = store.matches("#");
        assert_eq!(m.len(), 3);
        let m = store.matches("sensor/temp");
        assert_eq!(m.len(), 1);
        let m = store.matches("foo/#");
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn build_outbound_qos_downgrade() {
        let msg = RetainedMessage {
            topic: "t".into(),
            payload: Bytes::from_static(b"p"),
            qos: QoS::ExactlyOnce,
        };
        let out = RetainStore::build_outbound(&msg, QoS::AtLeastOnce);
        assert_eq!(out.qos, QoS::AtLeastOnce);
        assert!(out.retain);
    }
}
