//! QoS1/QoS2 出入站 inflight 跟踪与重传控制
//!
//! 设计要点：
//! - 出站 inflight：`HashMap<packet_id, OutboundInflight>`，每条记录携带报文与所处握手阶段
//! - 入站 QoS2 inflight：`HashSet<packet_id>`，记录已收 PUBLISH 但未收 PUBREL 的 id，用于去重
//! - 重传：由连接循环周期性 tick 调用 `retry_expired`，对超时未应答的项重发并计数
//! - 达到 max_retries 仍未应答：丢弃并返回失败列表，供上层记录告警

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::codec::{Packet, Publish, QoS};

/// 出站 inflight 项的握手阶段
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStage {
    /// QoS1: 等待 PUBACK; QoS2: 等待 PUBREC
    WaitFirstAck,
    /// QoS2 第二阶段: 已发 PUBREL, 等待 PUBCOMP
    WaitPubcomp,
}

/// 出站 inflight 项
#[derive(Debug, Clone)]
pub struct OutboundInflight {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
    pub stage: AckStage,
    pub last_sent_at: Instant,
    pub retry_count: u32,
}

impl OutboundInflight {
    pub fn new(topic: String, payload: Vec<u8>, qos: QoS, retain: bool) -> Self {
        Self {
            topic,
            payload,
            qos,
            retain,
            stage: AckStage::WaitFirstAck,
            last_sent_at: Instant::now(),
            retry_count: 0,
        }
    }

    /// 构造当前阶段需要重发或推进的报文
    pub fn build_packet(&self, packet_id: u16, dup: bool) -> Packet {
        match self.stage {
            AckStage::WaitFirstAck => Packet::Publish(Publish {
                dup,
                qos: self.qos,
                retain: self.retain,
                topic: self.topic.clone(),
                packet_id: Some(packet_id),
                payload: self.payload.clone(),
            }),
            // PUBREL 固定 DUP=0（MQTT 3.1.1 中 PUBREL 报文保留位固定为 0010）
            AckStage::WaitPubcomp => Packet::Pubrel(packet_id),
        }
    }
}

/// 出站 inflight 表（每连接独立）
#[derive(Default)]
pub struct OutboundInflightTable {
    map: HashMap<u16, OutboundInflight>,
}

impl OutboundInflightTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, packet_id: u16, entry: OutboundInflight) {
        self.map.insert(packet_id, entry);
    }

    pub fn get(&self, packet_id: u16) -> Option<&OutboundInflight> {
        self.map.get(&packet_id)
    }

    pub fn get_mut(&mut self, packet_id: u16) -> Option<&mut OutboundInflight> {
        self.map.get_mut(&packet_id)
    }

    pub fn remove(&mut self, packet_id: u16) -> Option<OutboundInflight> {
        self.map.remove(&packet_id)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u16, &mut OutboundInflight)> {
        self.map.iter_mut()
    }

    /// 扫描所有 inflight，对超过 `timeout` 未应答且未达 `max_retries` 的项：
    /// - 更新 last_sent_at 与 retry_count
    /// - 返回 (packet_id, 报文) 列表供上层重发
    ///
    /// 达到 max_retries 的项会被移除并计入 failed 列表
    pub fn retry_expired(
        &mut self,
        timeout: Duration,
        max_retries: u32,
    ) -> (Vec<(u16, Packet)>, Vec<u16>) {
        let now = Instant::now();
        let mut to_resend = Vec::new();
        let mut failed = Vec::new();
        let mut drop_ids: Vec<u16> = Vec::new();

        for (pid, entry) in self.map.iter_mut() {
            if now.duration_since(entry.last_sent_at) < timeout {
                continue;
            }
            if entry.retry_count >= max_retries {
                failed.push(*pid);
                drop_ids.push(*pid);
                continue;
            }
            entry.retry_count += 1;
            entry.last_sent_at = now;
            // 重发 PUBLISH 时 DUP=1；PUBREL 不需要 DUP 标志
            let pkt = match entry.stage {
                AckStage::WaitFirstAck => entry.build_packet(*pid, true),
                AckStage::WaitPubcomp => entry.build_packet(*pid, false),
            };
            to_resend.push((*pid, pkt));
        }

        for pid in drop_ids {
            self.map.remove(&pid);
        }

        (to_resend, failed)
    }
}

/// 入站 QoS2 inflight 跟踪：记录已收到 PUBLISH 但等待对端 PUBREL 的 packet_id
#[derive(Default)]
pub struct InboundQos2Tracker {
    seen: HashSet<u16>,
}

impl InboundQos2Tracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// 收到一条 QoS2 PUBLISH：
    /// - 若 packet_id 已存在 → 返回 true（重复，不路由但仍需回 PUBREC）
    /// - 否则插入并返回 false（首次收到，需路由）
    pub fn on_publish(&mut self, packet_id: u16) -> bool {
        !self.seen.insert(packet_id)
    }

    /// 收到 PUBREL：清理 inflight，返回是否曾存在
    pub fn on_pubrel(&mut self, packet_id: u16) -> bool {
        self.seen.remove(&packet_id)
    }

    pub fn contains(&self, packet_id: u16) -> bool {
        self.seen.contains(&packet_id)
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    pub fn clear(&mut self) {
        self.seen.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbound_qos2_dedup() {
        let mut t = InboundQos2Tracker::new();
        assert!(!t.on_publish(5));
        assert!(t.on_publish(5));
        assert!(t.contains(5));
        assert!(t.on_pubrel(5));
        assert!(!t.contains(5));
    }

    #[test]
    fn outbound_inflight_lifecycle_qos2() {
        let mut table = OutboundInflightTable::new();
        let entry = OutboundInflight::new(
            "a/b".into(),
            vec![1, 2, 3],
            QoS::ExactlyOnce,
            false,
        );
        table.insert(7, entry);
        assert_eq!(table.len(), 1);

        // 收到 PUBREC → 推进到 WaitPubcomp
        let e = table.get_mut(7).unwrap();
        assert_eq!(e.stage, AckStage::WaitFirstAck);
        e.stage = AckStage::WaitPubcomp;
        e.last_sent_at = Instant::now();

        // 收到 PUBCOMP → 移除
        assert!(table.remove(7).is_some());
        assert!(table.is_empty());
    }

    #[test]
    fn retry_expired_resends_and_fails() {
        let mut table = OutboundInflightTable::new();
        let mut entry = OutboundInflight::new("t".into(), vec![], QoS::AtLeastOnce, false);
        // 让 last_sent_at 处于过去，确保超时
        entry.last_sent_at = Instant::now() - Duration::from_secs(10);
        table.insert(1, entry);

        // max_retries=2: 第 1 次 send (count 0→1), 第 2 次 send (count 1→2), 第 3 次 fail
        let (resend, failed) = table.retry_expired(Duration::from_secs(1), 2);
        assert_eq!(resend.len(), 1);
        assert!(failed.is_empty());
        assert_eq!(table.get(1).unwrap().retry_count, 1);

        // 第 2 次：仍可重发
        {
            let e = table.get_mut(1).unwrap();
            e.last_sent_at = Instant::now() - Duration::from_secs(10);
        }
        let (resend, failed) = table.retry_expired(Duration::from_secs(1), 2);
        assert_eq!(resend.len(), 1);
        assert!(failed.is_empty());
        assert_eq!(table.get(1).unwrap().retry_count, 2);

        // 第 3 次：达到 max_retries，应失败并移除
        {
            let e = table.get_mut(1).unwrap();
            e.last_sent_at = Instant::now() - Duration::from_secs(10);
        }
        let (resend, failed) = table.retry_expired(Duration::from_secs(1), 2);
        assert!(resend.is_empty());
        assert_eq!(failed, vec![1]);
        assert!(table.is_empty());
    }
}
