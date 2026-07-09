use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// 全局运行时指标（阶段五接入 Prometheus 文本格式导出）
#[derive(Default)]
pub struct Metrics {
    // —— 连接指标 ——
    /// 累计连接总数（单调递增）
    pub connections_total: AtomicU64,
    /// 当前在线连接数（可增可减）
    pub connections_current: AtomicI64,
    /// 当前半连接数（已 accept 尚未完成鉴权）；用于 max_connections 准入控制
    pub connections_pending: AtomicI64,
    /// 累计掉线/断开次数
    pub disconnect_count: AtomicU64,

    // —— 消息指标 ——
    /// 累计入站消息总数（含 PUBLISH/SUBSCRIBE 等所有报文）
    pub messages_received: AtomicU64,
    /// 累计出站消息总数（投递给订阅者）
    pub messages_sent: AtomicU64,
    /// 累计入站 PUBLISH 数
    pub publish_received: AtomicU64,
    /// 按 QoS 分类的入站 PUBLISH 数（索引 0/1/2 对应 QoS0/1/2）
    pub publish_by_qos: [AtomicU64; 3],
    /// 累计因背压/通道满被丢弃的出站消息数
    pub messages_dropped: AtomicU64,

    // —— 订阅指标 ——
    /// 累计 SUBSCRIBE 报文数
    pub subscribe_count: AtomicU64,
    /// 累计 UNSUBSCRIBE 报文数
    pub unsubscribe_count: AtomicU64,
    /// 当前普通订阅总数（不含共享订阅）
    pub subscriptions_current: AtomicI64,
    /// 当前共享订阅组成员总数
    pub shared_subscriptions_current: AtomicI64,

    // —— 会话指标 ——
    /// 当前总会话数（在线 + 离线保留）
    pub sessions_total: AtomicI64,
    /// 当前离线保留会话数（clean_session=false 且已断开）
    pub sessions_offline: AtomicI64,
    /// 因 session_expiry 到期被清理的会话数
    pub sessions_expired: AtomicU64,

    // —— 存储指标 ——
    /// 累计离线消息写入次数
    pub storage_writes: AtomicU64,
    /// 累计离线消息读取次数
    pub storage_reads: AtomicU64,
    /// 累计 retained 消息写入次数
    pub retained_stored: AtomicU64,

    // —— 安全/插件指标 ——
    /// 被安全中间件拒绝的连接/消息数
    pub security_rejected: AtomicU64,
    /// 被插件中间件拒绝的发布/订阅数
    pub plugin_rejected: AtomicU64,
    /// HTTP 转发器因队列满丢弃的消息数
    pub forward_dropped: AtomicU64,
}

impl Metrics {
    pub fn inc_connections(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_current.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec_connections(&self) {
        self.connections_current.fetch_sub(1, Ordering::Relaxed);
    }
    /// 半连接计数 +1（accept 后、鉴权前）
    pub fn inc_pending(&self) {
        self.connections_pending.fetch_add(1, Ordering::Relaxed);
    }
    /// 半连接计数 -1（鉴权完成或连接提前断开）
    pub fn dec_pending(&self) {
        self.connections_pending.fetch_sub(1, Ordering::Relaxed);
    }
    /// 当前半连接数（供 accept 循环准入判断）
    pub fn pending_connections(&self) -> i64 {
        self.connections_pending.load(Ordering::Relaxed)
    }
    pub fn inc_publish(&self) {
        self.publish_received.fetch_add(1, Ordering::Relaxed);
        self.messages_received.fetch_add(1, Ordering::Relaxed);
    }
    /// 按入站 PUBLISH 的 QoS 分类计数（qos_level: 0/1/2）
    pub fn inc_publish_qos(&self, qos_level: u8) {
        if let Some(idx) = match qos_level {
            0 => Some(0),
            1 => Some(1),
            2 => Some(2),
            _ => None,
        } {
            self.publish_by_qos[idx].fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn inc_sent(&self) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_message_dropped(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_subscribe(&self) {
        self.subscribe_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_unsubscribe(&self) {
        self.unsubscribe_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_disconnect(&self) {
        self.disconnect_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn set_subscriptions(&self, n: i64) {
        self.subscriptions_current.store(n, Ordering::Relaxed);
    }
    pub fn set_shared_subscriptions(&self, n: i64) {
        self.shared_subscriptions_current.store(n, Ordering::Relaxed);
    }
    pub fn set_sessions_total(&self, n: i64) {
        self.sessions_total.store(n, Ordering::Relaxed);
    }
    pub fn set_sessions_offline(&self, n: i64) {
        self.sessions_offline.store(n, Ordering::Relaxed);
    }
    pub fn inc_sessions_expired(&self) {
        self.sessions_expired.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_storage_write(&self) {
        self.storage_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_storage_read(&self) {
        self.storage_reads.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_retained_stored(&self) {
        self.retained_stored.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_security_rejected(&self) {
        self.security_rejected.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_plugin_rejected(&self) {
        self.plugin_rejected.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_forward_dropped(&self) {
        self.forward_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_current: self.connections_current.load(Ordering::Relaxed),
            connections_pending: self.connections_pending.load(Ordering::Relaxed),
            disconnect_count: self.disconnect_count.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_sent: self.messages_sent.load(Ordering::Relaxed),
            publish_received: self.publish_received.load(Ordering::Relaxed),
            publish_qos0: self.publish_by_qos[0].load(Ordering::Relaxed),
            publish_qos1: self.publish_by_qos[1].load(Ordering::Relaxed),
            publish_qos2: self.publish_by_qos[2].load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            subscribe_count: self.subscribe_count.load(Ordering::Relaxed),
            unsubscribe_count: self.unsubscribe_count.load(Ordering::Relaxed),
            subscriptions_current: self.subscriptions_current.load(Ordering::Relaxed),
            shared_subscriptions_current: self.shared_subscriptions_current
                .load(Ordering::Relaxed),
            sessions_total: self.sessions_total.load(Ordering::Relaxed),
            sessions_offline: self.sessions_offline.load(Ordering::Relaxed),
            sessions_expired: self.sessions_expired.load(Ordering::Relaxed),
            storage_writes: self.storage_writes.load(Ordering::Relaxed),
            storage_reads: self.storage_reads.load(Ordering::Relaxed),
            retained_stored: self.retained_stored.load(Ordering::Relaxed),
            security_rejected: self.security_rejected.load(Ordering::Relaxed),
            plugin_rejected: self.plugin_rejected.load(Ordering::Relaxed),
            forward_dropped: self.forward_dropped.load(Ordering::Relaxed),
        }
    }

    /// 导出为 Prometheus 文本格式（exposition format v0.0.4）
    pub fn prometheus_text(&self) -> String {
        let s = self.snapshot();
        let mut out = String::with_capacity(4096);
        // 连接指标
        push_metric(&mut out, "lumenmq_connections_total", "Cumulative connections since start", s.connections_total, "counter");
        push_metric(&mut out, "lumenmq_connections_current", "Current online connections", s.connections_current.max(0) as u64, "gauge");
        push_metric(&mut out, "lumenmq_connections_pending", "Half-open connections (accepted, not yet authenticated)", s.connections_pending.max(0) as u64, "gauge");
        push_metric(&mut out, "lumenmq_disconnect_total", "Cumulative disconnects", s.disconnect_count, "counter");
        // 消息指标
        push_metric(&mut out, "lumenmq_messages_received_total", "Cumulative inbound messages", s.messages_received, "counter");
        push_metric(&mut out, "lumenmq_messages_sent_total", "Cumulative outbound messages delivered", s.messages_sent, "counter");
        push_metric(&mut out, "lumenmq_publish_received_total", "Cumulative inbound PUBLISH", s.publish_received, "counter");
        push_metric(&mut out, "lumenmq_publish_qos0_total", "Inbound PUBLISH at QoS0", s.publish_qos0, "counter");
        push_metric(&mut out, "lumenmq_publish_qos1_total", "Inbound PUBLISH at QoS1", s.publish_qos1, "counter");
        push_metric(&mut out, "lumenmq_publish_qos2_total", "Inbound PUBLISH at QoS2", s.publish_qos2, "counter");
        push_metric(&mut out, "lumenmq_messages_dropped_total", "Outbound messages dropped (backpressure/channel full)", s.messages_dropped, "counter");
        // 订阅指标
        push_metric(&mut out, "lumenmq_subscribe_total", "Cumulative SUBSCRIBE packets", s.subscribe_count, "counter");
        push_metric(&mut out, "lumenmq_unsubscribe_total", "Cumulative UNSUBSCRIBE packets", s.unsubscribe_count, "counter");
        push_metric(&mut out, "lumenmq_subscriptions_current", "Current non-shared subscriptions", s.subscriptions_current.max(0) as u64, "gauge");
        push_metric(&mut out, "lumenmq_shared_subscriptions_current", "Current shared subscription members", s.shared_subscriptions_current.max(0) as u64, "gauge");
        // 会话指标
        push_metric(&mut out, "lumenmq_sessions_total", "Current total sessions (online + offline retained)", s.sessions_total.max(0) as u64, "gauge");
        push_metric(&mut out, "lumenmq_sessions_offline", "Current offline retained sessions", s.sessions_offline.max(0) as u64, "gauge");
        push_metric(&mut out, "lumenmq_sessions_expired_total", "Sessions cleaned up due to session_expiry", s.sessions_expired, "counter");
        // 存储指标
        push_metric(&mut out, "lumenmq_storage_writes_total", "Offline message writes to storage", s.storage_writes, "counter");
        push_metric(&mut out, "lumenmq_storage_reads_total", "Offline message reads from storage", s.storage_reads, "counter");
        push_metric(&mut out, "lumenmq_retained_stored_total", "Cumulative retained messages stored", s.retained_stored, "counter");
        // 安全/插件指标
        push_metric(&mut out, "lumenmq_security_rejected_total", "Connections/messages rejected by security middleware", s.security_rejected, "counter");
        push_metric(&mut out, "lumenmq_plugin_rejected_total", "Publishes/subscribes rejected by plugin middleware", s.plugin_rejected, "counter");
        push_metric(&mut out, "lumenmq_forward_dropped_total", "HTTP forwarder messages dropped (queue full)", s.forward_dropped, "counter");
        out
    }
}

fn push_metric(out: &mut String, name: &str, help: &str, value: u64, kind: &str) {
    out.push_str(&format!("# HELP {name} {help}\n"));
    out.push_str(&format!("# TYPE {name} {kind}\n"));
    out.push_str(&format!("{name} {value}\n"));
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct MetricsSnapshot {
    pub connections_total: u64,
    pub connections_current: i64,
    pub connections_pending: i64,
    pub disconnect_count: u64,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub publish_received: u64,
    pub publish_qos0: u64,
    pub publish_qos1: u64,
    pub publish_qos2: u64,
    pub messages_dropped: u64,
    pub subscribe_count: u64,
    pub unsubscribe_count: u64,
    pub subscriptions_current: i64,
    pub shared_subscriptions_current: i64,
    pub sessions_total: i64,
    pub sessions_offline: i64,
    pub sessions_expired: u64,
    pub storage_writes: u64,
    pub storage_reads: u64,
    pub retained_stored: u64,
    pub security_rejected: u64,
    pub plugin_rejected: u64,
    pub forward_dropped: u64,
}

/// 全局指标单例
pub static METRICS: Metrics = Metrics {
    connections_total: AtomicU64::new(0),
    connections_current: AtomicI64::new(0),
    connections_pending: AtomicI64::new(0),
    disconnect_count: AtomicU64::new(0),
    messages_received: AtomicU64::new(0),
    messages_sent: AtomicU64::new(0),
    publish_received: AtomicU64::new(0),
    publish_by_qos: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
    messages_dropped: AtomicU64::new(0),
    subscribe_count: AtomicU64::new(0),
    unsubscribe_count: AtomicU64::new(0),
    subscriptions_current: AtomicI64::new(0),
    shared_subscriptions_current: AtomicI64::new(0),
    sessions_total: AtomicI64::new(0),
    sessions_offline: AtomicI64::new(0),
    sessions_expired: AtomicU64::new(0),
    storage_writes: AtomicU64::new(0),
    storage_reads: AtomicU64::new(0),
    retained_stored: AtomicU64::new(0),
    security_rejected: AtomicU64::new(0),
    plugin_rejected: AtomicU64::new(0),
    forward_dropped: AtomicU64::new(0),
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_text_contains_required_lines() {
        let text = METRICS.prometheus_text();
        assert!(text.contains("# HELP lumenmq_connections_total"));
        assert!(text.contains("# TYPE lumenmq_connections_total counter"));
        assert!(text.contains("lumenmq_connections_total "));
        assert!(text.contains("# TYPE lumenmq_subscriptions_current gauge"));
        assert!(text.contains("lumenmq_publish_qos0_total "));
        assert!(text.contains("lumenmq_sessions_expired_total "));
    }

    #[test]
    fn qos_counter_increments_correctly() {
        let m = Metrics::default();
        m.inc_publish_qos(0);
        m.inc_publish_qos(1);
        m.inc_publish_qos(1);
        m.inc_publish_qos(2);
        assert_eq!(m.publish_by_qos[0].load(Ordering::Relaxed), 1);
        assert_eq!(m.publish_by_qos[1].load(Ordering::Relaxed), 2);
        assert_eq!(m.publish_by_qos[2].load(Ordering::Relaxed), 1);
    }
}
