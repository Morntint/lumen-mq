//! 持久化存储层（基于 sled 嵌入式 KV）
//!
//! 设计要点：
//! - 三个独立 Tree：retained（保留消息）、sessions（会话订阅元数据）、offline（离线消息队列）
//! - 所有结构 serde_json 序列化为 value，key 为字符串/字节
//! - 容错策略：磁盘 IO 错误返回 `BrokerError::Storage`，不阻塞主流程
//! - 启动时调用 `load_*` 系列方法恢复到内存结构

use std::path::Path;
use std::sync::Arc;

use sled::{Db, Tree};
use tracing::warn;

use crate::broker::retain::RetainedMessage;
use crate::broker::router::OutboundPublish;
use crate::codec::QoS;
use crate::utils::BrokerError;

/// 持久化存储句柄
pub struct Storage {
    db: Db,
    retained: Tree,
    sessions: Tree,
    offline: Tree,
}

impl Storage {
    /// 打开/创建 sled 数据库
    pub fn open(path: &Path) -> Result<Self, BrokerError> {
        let db = sled::open(path).map_err(|e| BrokerError::Storage(format!("open sled: {e}")))?;
        let retained = db
            .open_tree("retained")
            .map_err(|e| BrokerError::Storage(format!("open retained tree: {e}")))?;
        let sessions = db
            .open_tree("sessions")
            .map_err(|e| BrokerError::Storage(format!("open sessions tree: {e}")))?;
        let offline = db
            .open_tree("offline")
            .map_err(|e| BrokerError::Storage(format!("open offline tree: {e}")))?;
        Ok(Self { db, retained, sessions, offline })
    }

    pub fn flush(&self) -> Result<(), BrokerError> {
        self.db.flush().map_err(|e| BrokerError::Storage(format!("flush: {e}")))?;
        Ok(())
    }

    // ---------- Retained 消息 ----------

    pub fn save_retained(&self, topic: &str, msg: &RetainedMessage) -> Result<(), BrokerError> {
        let bytes = serde_json::to_vec(msg)
            .map_err(|e| BrokerError::Storage(format!("serialize retained: {e}")))?;
        self.retained
            .insert(topic, bytes)
            .map_err(|e| BrokerError::Storage(format!("write retained: {e}")))?;
        Ok(())
    }

    pub fn delete_retained(&self, topic: &str) -> Result<(), BrokerError> {
        self.retained
            .remove(topic)
            .map_err(|e| BrokerError::Storage(format!("remove retained: {e}")))?;
        Ok(())
    }

    pub fn load_all_retained(&self) -> Result<Vec<RetainedMessage>, BrokerError> {
        let mut out = Vec::new();
        for item in self.retained.iter() {
            let (_k, v) = item.map_err(|e| BrokerError::Storage(format!("scan retained: {e}")))?;
            match serde_json::from_slice::<RetainedMessage>(&v) {
                Ok(m) => out.push(m),
                Err(e) => warn!(error = %e, "skip corrupted retained entry"),
            }
        }
        Ok(out)
    }

    // ---------- 会话元数据（用于重启后恢复订阅） ----------

    pub fn save_session(&self, client_id: &str, snap: &SessionSnapshot) -> Result<(), BrokerError> {
        let bytes = serde_json::to_vec(snap)
            .map_err(|e| BrokerError::Storage(format!("serialize session: {e}")))?;
        self.sessions
            .insert(client_id, bytes)
            .map_err(|e| BrokerError::Storage(format!("write session: {e}")))?;
        Ok(())
    }

    pub fn delete_session(&self, client_id: &str) -> Result<(), BrokerError> {
        self.sessions
            .remove(client_id)
            .map_err(|e| BrokerError::Storage(format!("remove session: {e}")))?;
        Ok(())
    }

    pub fn load_all_sessions(&self) -> Result<Vec<SessionSnapshot>, BrokerError> {
        let mut out = Vec::new();
        for item in self.sessions.iter() {
            let (_k, v) = item.map_err(|e| BrokerError::Storage(format!("scan sessions: {e}")))?;
            match serde_json::from_slice::<SessionSnapshot>(&v) {
                Ok(s) => out.push(s),
                Err(e) => warn!(error = %e, "skip corrupted session entry"),
            }
        }
        Ok(out)
    }

    // ---------- 离线消息 ----------

    /// 追加一条离线消息到客户端的队列（按 client_id 作 key，value 是 JSON 数组中的一个元素）
    /// 简化实现：把所有消息作为一个数组序列化整体写入；阶段五可优化为 sled 批量增量
    pub fn push_offline(&self, client_id: &str, msg: &OutboundPublish) -> Result<(), BrokerError> {
        crate::monitor::METRICS.inc_storage_write();
        let mut current: Vec<OutboundPublish> = match self.offline.get(client_id) {
            Ok(Some(v)) => serde_json::from_slice(&v).unwrap_or_default(),
            Ok(None) => Vec::new(),
            Err(e) => return Err(BrokerError::Storage(format!("read offline: {e}"))),
        };
        current.push(msg.clone());
        let bytes = serde_json::to_vec(&current)
            .map_err(|e| BrokerError::Storage(format!("serialize offline: {e}")))?;
        self.offline
            .insert(client_id, bytes)
            .map_err(|e| BrokerError::Storage(format!("write offline: {e}")))?;
        Ok(())
    }

    /// 取出并清空某客户端的所有离线消息
    pub fn drain_offline(&self, client_id: &str) -> Result<Vec<OutboundPublish>, BrokerError> {
        crate::monitor::METRICS.inc_storage_read();
        let v = self
            .offline
            .remove(client_id)
            .map_err(|e| BrokerError::Storage(format!("drain offline: {e}")))?;
        match v {
            Some(bytes) => Ok(serde_json::from_slice(&bytes).unwrap_or_default()),
            None => Ok(Vec::new()),
        }
    }
}

pub type SharedStorage = Arc<Storage>;

/// 会话元数据快照（持久化用）
/// 仅 clean_session=false 的会话才需要持久化；包含其订阅列表以便重启后恢复
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SessionSnapshot {
    pub client_id: String,
    pub subscriptions: Vec<(String, QoS)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn mk_retained(topic: &str) -> RetainedMessage {
        RetainedMessage {
            topic: topic.into(),
            payload: b"hello".to_vec(),
            qos: QoS::AtLeastOnce,
        }
    }

    fn mk_outbound(topic: &str) -> OutboundPublish {
        OutboundPublish {
            topic: topic.into(),
            payload: b"data".to_vec(),
            qos: QoS::AtLeastOnce,
            retain: false,
        }
    }

    #[test]
    fn retained_roundtrip() {
        let dir = tempdir().unwrap();
        let store = Storage::open(dir.path()).unwrap();
        store.save_retained("a/b", &mk_retained("a/b")).unwrap();
        store.save_retained("c/d", &mk_retained("c/d")).unwrap();
        let loaded = store.load_all_retained().unwrap();
        assert_eq!(loaded.len(), 2);

        store.delete_retained("a/b").unwrap();
        let loaded = store.load_all_retained().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].topic, "c/d");
    }

    #[test]
    fn session_roundtrip() {
        let dir = tempdir().unwrap();
        let store = Storage::open(dir.path()).unwrap();
        let snap = SessionSnapshot {
            client_id: "dev-1".into(),
            subscriptions: vec![("a/+".into(), QoS::AtLeastOnce), ("#".into(), QoS::AtMostOnce)],
        };
        store.save_session("dev-1", &snap).unwrap();
        let loaded = store.load_all_sessions().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].client_id, "dev-1");
        assert_eq!(loaded[0].subscriptions.len(), 2);

        store.delete_session("dev-1").unwrap();
        let loaded = store.load_all_sessions().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn offline_push_and_drain() {
        let dir = tempdir().unwrap();
        let store = Storage::open(dir.path()).unwrap();
        store.push_offline("c1", &mk_outbound("a")).unwrap();
        store.push_offline("c1", &mk_outbound("b")).unwrap();
        let drained = store.drain_offline("c1").unwrap();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].topic, "a");
        assert_eq!(drained[1].topic, "b");

        // 二次取应为空
        let drained = store.drain_offline("c1").unwrap();
        assert!(drained.is_empty());
    }
}
