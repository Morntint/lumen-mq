use std::sync::Arc;

use crate::broker::retain::SharedRetainStore;
use crate::broker::session::{delivery_qos, DeliveryOutcome, SharedSessionManager};
use crate::broker::subscription::SharedSubscriptionTree;
use crate::codec::{Packet, Publish, QoS};
use crate::monitor::METRICS;
use crate::storage::SharedStorage;
use crate::utils::BrokerResult;

/// 投递给某个会话的"待发送发布请求"
/// 连接循环负责分配 packet_id 并组装 PUBLISH 报文（QoS1/2 的 inflight 管理在连接循环侧维护）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OutboundPublish {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

/// 消息路由器：将 PUBLISH 分发给匹配订阅者
pub struct Router {
    subscriptions: SharedSubscriptionTree,
    sessions: SharedSessionManager,
    retain: SharedRetainStore,
    storage: Option<SharedStorage>,
}

impl Router {
    pub fn new(
        subscriptions: SharedSubscriptionTree,
        sessions: SharedSessionManager,
        retain: SharedRetainStore,
    ) -> Self {
        Self { subscriptions, sessions, retain, storage: None }
    }

    pub fn with_storage(
        subscriptions: SharedSubscriptionTree,
        sessions: SharedSessionManager,
        retain: SharedRetainStore,
        storage: SharedStorage,
    ) -> Self {
        Self {
            subscriptions,
            sessions,
            retain,
            storage: Some(storage),
        }
    }

    pub fn retain_store(&self) -> &SharedRetainStore {
        &self.retain
    }

    /// 路由一条发布消息到所有匹配订阅者
    /// `exclude_client_id`：发布者自身不接收自己的消息（MQTT 3.1.1 不强制，常见实现如此）
    /// `trace_id`：轻量链路追踪 ID，贯穿路由→投递日志，便于 grep 定位单条消息流向
    pub fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
        exclude_client_id: Option<&str>,
        trace_id: &str,
    ) -> BrokerResult<()> {
        // 1. Retain 处理（无论是否有订阅者）
        if retain {
            self.retain.set(topic, payload.to_vec(), qos);
            tracing::debug!(%trace_id, %topic, ?qos, "retained message stored");
        }

        // 2. 收集普通订阅匹配
        let mut matches = self.subscriptions.matches(topic);
        // 3. 收集共享订阅匹配（每个组轮询选一个成员）
        matches.extend(self.subscriptions.matches_shared(topic));

        tracing::debug!(%trace_id, %topic, ?qos, subscribers = matches.len(), "routing publish");

        // 4. 路由给在线/离线订阅者
        for (client_id, sub_qos) in matches {
            if exclude_client_id == Some(client_id.as_str()) {
                continue;
            }
            self.deliver_one(&client_id, topic, payload, qos, sub_qos, trace_id);
        }

        Ok(())
    }

    /// 向单个订阅者投递一条消息（处理在线/离线/背压）
    fn deliver_one(
        &self,
        client_id: &str,
        topic: &str,
        payload: &[u8],
        pub_qos: QoS,
        sub_qos: QoS,
        trace_id: &str,
    ) {
        let dq = delivery_qos(pub_qos, sub_qos);
        let req = OutboundPublish {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            qos: dq,
            // 投递给订阅者时 retain=0（仅 retained 投递给新订阅者时由订阅逻辑置 1）
            retain: false,
        };

        match self.sessions.deliver_or_enqueue(client_id, req.clone()) {
            DeliveryOutcome::Sent => {
                METRICS.inc_sent();
                tracing::debug!(%trace_id, client = %client_id, %topic, ?dq, "delivered to online subscriber");
            }
            DeliveryOutcome::Enqueued => {
                METRICS.inc_sent();
                // 落盘离线消息（write-through）
                if let Some(s) = &self.storage {
                    if let Err(e) = s.push_offline(client_id, &req) {
                        tracing::warn!(%trace_id, error = %e, client = %client_id, "persist push_offline failed");
                    }
                }
                tracing::debug!(%trace_id, client = %client_id, "outbound enqueued to offline queue");
            }
            DeliveryOutcome::ChannelFull => {
                METRICS.inc_message_dropped();
                tracing::warn!(
                    %trace_id,
                    client = %client_id,
                    "outbound channel full, dropping publish (backpressure handling in phase 5)"
                );
            }
            DeliveryOutcome::Dropped => {
                tracing::debug!(%trace_id, client = %client_id, "outbound dropped (clean session offline)");
            }
            DeliveryOutcome::NoSession => {
                // 订阅树与 session 不一致（理论不应发生）；忽略
            }
        }
    }

    /// 便捷构造：从入站 PUBLISH 报文路由
    pub fn route_inbound_publish(
        &self,
        p: &Publish,
        publisher: Option<&str>,
        trace_id: &str,
    ) -> BrokerResult<()> {
        self.publish(&p.topic, &p.payload, p.qos, p.retain, publisher, trace_id)
    }

    /// 构造一条 PUBLISH 报文（供连接循环在收到 OutboundPublish 时使用）
    pub fn build_publish(req: OutboundPublish, packet_id: Option<u16>) -> Packet {
        Packet::Publish(Publish {
            dup: false,
            qos: req.qos,
            retain: req.retain,
            topic: req.topic,
            packet_id,
            payload: req.payload,
        })
    }
}

pub type SharedRouter = Arc<Router>;
