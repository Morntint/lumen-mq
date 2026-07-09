use std::net::SocketAddr;
use std::time::Instant;

/// 单设备连接上下文（连接生命周期内的元数据）
pub struct ConnContext {
    /// 内部连接 ID（不等于 MQTT client_id）
    pub conn_id: String,
    pub peer_addr: SocketAddr,
    pub connected_at: Instant,
    /// MQTT 客户端 ID（CONNECT 后设置）
    pub client_id: Option<String>,
    /// 心跳保活时长（秒），0 表示禁用
    pub keep_alive: u16,
    /// 最近一次报文收发时间
    pub last_activity: Instant,
    /// 是否已完成 CONNECT 握手
    pub authenticated: bool,
    /// 协议级别（3=3.1, 4=3.1.1, 5=5.0）
    pub protocol_level: u8,
}

impl ConnContext {
    pub fn new(peer_addr: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            conn_id: crate::utils::time::short_id(),
            peer_addr,
            connected_at: now,
            client_id: None,
            keep_alive: 0,
            last_activity: now,
            authenticated: false,
            protocol_level: 4,
        }
    }

    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// 返回心跳超时阈值（秒）。MQTT 规范：实际超时 = keep_alive * 1.5
    pub fn keep_alive_secs(&self) -> u16 {
        self.keep_alive
    }

    pub fn is_keep_alive_expired(&self) -> bool {
        if self.keep_alive == 0 {
            return false;
        }
        let elapsed = self.last_activity.elapsed();
        let timeout = std::time::Duration::from_secs((self.keep_alive as u64 * 3) / 2 + 1);
        elapsed > timeout
    }
}
