//! MQTT-SN UDP 网关（最小实现）
//!
//! 提供能力：
//! - `MqttSnServer`：监听 UDP，按 MQTT-SN 1.1 协议处理传感器设备
//! - 编解码：CONNECT/CONNACK、REGISTER/REGACK、PUBLISH/PUBACK、SUBSCRIBE/SUBACK、
//!   UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT
//! - 桥接：SN 客户端复用 `BrokerState` 的 `sessions`/`subscriptions`/`router`，
//!   出站消息由独立 forwarder 任务从会话 tx 通道取出，封装为 SN PUBLISH 经 UDP 回送
//!
//! 范围限制（最小实现）：
//! - QoS0 / QoS1；QoS2 不支持（PUBLISH QoS2 返回 NOT_SUPPORTED）
//! - 主题 ID 动态分配（REGISTER / SUBSCRIBE 流程）；预定义主题 ID 不支持
//! - 不实现 WILL（遗嘱）流程；CONNECT 携带 Will 标志将被忽略
//! - 不实现 SEARCHGW/GWINFO 网关发现（设备直连配置好的网关地址）

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::broker::router::OutboundPublish;
use crate::broker::BrokerState;
use crate::codec::QoS;
use crate::monitor::METRICS;
use crate::net::ShutdownRx;

// ---------- MQTT-SN 报文类型 ----------

#[allow(dead_code)]
const MSG_ADVERTISE: u8 = 0x01;
#[allow(dead_code)]
const MSG_SEARCHGW: u8 = 0x02;
#[allow(dead_code)]
const MSG_GWINFO: u8 = 0x03;
const MSG_CONNECT: u8 = 0x04;
const MSG_CONNACK: u8 = 0x05;
const MSG_REGISTER: u8 = 0x0A;
const MSG_REGACK: u8 = 0x0B;
const MSG_PUBLISH: u8 = 0x0C;
const MSG_PUBACK: u8 = 0x0D;
const MSG_SUBSCRIBE: u8 = 0x11;
const MSG_SUBACK: u8 = 0x12;
const MSG_UNSUBSCRIBE: u8 = 0x13;
const MSG_UNSUBACK: u8 = 0x14;
const MSG_PINGREQ: u8 = 0x15;
const MSG_PINGRESP: u8 = 0x16;
const MSG_DISCONNECT: u8 = 0x17;

// ---------- MQTT-SN 返回码 ----------

const RC_ACCEPTED: u8 = 0x00;
const RC_CONGESTION: u8 = 0x01;
const RC_INVALID_TOPIC_ID: u8 = 0x02;
const RC_NOT_SUPPORTED: u8 = 0x03;
/// 拒绝（规范未定义专用码，0xFF 为业界通用拒绝码）
const RC_REJECTED: u8 = 0xFF;

/// MQTT-SN 协议 ID（1.1）
const PROTOCOL_ID_MQTTSN: u8 = 0x01;

/// 主题 ID 分配起点（0x0000 与 0xFFFF 保留）
const TOPIC_ID_MIN: u16 = 0x0001;
const TOPIC_ID_MAX: u16 = 0xFFFE;

/// Flags 位掩码
#[allow(dead_code)]
const FLAG_DUP: u8 = 0b1000_0000;
const FLAG_QOS_MASK: u8 = 0b0110_0000;
const FLAG_QOS_SHIFT: u8 = 5;
const FLAG_RETAIN: u8 = 0b0001_0000;
const FLAG_WILL: u8 = 0b0000_1000;
const FLAG_CLEAN: u8 = 0b0000_0100;
const FLAG_TOPIC_ID_TYPE_MASK: u8 = 0b0000_0011;
#[allow(dead_code)]
const TOPIC_ID_TYPE_NORMAL: u8 = 0b00;
#[allow(dead_code)]
const TOPIC_ID_TYPE_PREDEFINED: u8 = 0b01;
const TOPIC_ID_TYPE_SHORT: u8 = 0b10;

/// 从 flags 字节解析 QoS
fn flags_to_qos(flags: u8) -> Result<QoS, u8> {
    let q = (flags & FLAG_QOS_MASK) >> FLAG_QOS_SHIFT;
    match q {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        // 3 = -1 (reserved/invalid in SN)
        _ => Err(RC_NOT_SUPPORTED),
    }
}

/// 把 QoS 编码进 flags 字节
fn qos_to_flags(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0b000_00000,
        QoS::AtLeastOnce => 0b001_00000,
        QoS::ExactlyOnce => 0b010_00000,
    }
}

// ---------- 编解码 ----------

/// 解析后的 MQTT-SN 报文（最小集）
#[derive(Debug, Clone)]
enum SnPacket {
    /// (flags, protocol_id, duration, client_id)
    Connect { flags: u8, protocol_id: u8, duration: u16, client_id: String },
    /// (flags, return_code)
    Connack { return_code: u8 },
    /// (flags, topic_id, msg_id, topic_name) — topic_id=0 表示客户端请求分配
    Register { flags: u8, topic_id: u16, msg_id: u16, topic_name: String },
    /// (flags, topic_id, msg_id, return_code)
    Regack { flags: u8, topic_id: u16, msg_id: u16, return_code: u8 },
    /// (flags, topic_id, msg_id, data)
    Publish { flags: u8, topic_id: u16, msg_id: u16, data: Vec<u8> },
    /// (flags, topic_id, msg_id, return_code)
    Puback { flags: u8, topic_id: u16, msg_id: u16, return_code: u8 },
    /// (flags, msg_id, topic) — topic 可为主题名（normal）或短主题名（2字节）
    Subscribe { flags: u8, msg_id: u16, topic: Vec<u8> },
    /// (flags, topic_id, msg_id, return_code)
    Suback { flags: u8, topic_id: u16, msg_id: u16, return_code: u8 },
    /// (flags, msg_id, topic)
    Unsubscribe { #[allow(dead_code)] flags: u8, msg_id: u16, topic: Vec<u8> },
    /// (msg_id)
    Unsuback { msg_id: u16 },
    Pingreq,
    Pingresp,
    /// (duration) — duration=None 表示立即断开；Some(d) 表示睡眠时长
    Disconnect { duration: Option<u16> },
    /// 未知/不支持类型，仅保留 type 与原始载荷供调试
    Unknown { #[allow(dead_code)] msg_type: u8 },
}

/// 解析一条 MQTT-SN 报文
fn parse_packet(buf: &[u8]) -> Result<SnPacket, &'static str> {
    if buf.len() < 2 {
        return Err("packet too short");
    }
    // 长度字段：1 字节（若 != 0xFF）或 0x01 + 2 字节大端
    let (_total_len, hdr) = if buf[0] != 0x01 {
        let len = buf[0] as usize;
        if buf.len() < len {
            return Err("incomplete packet");
        }
        (len, &buf[1..])
    } else {
        if buf.len() < 4 {
            return Err("incomplete long packet");
        }
        let len = u16::from_be_bytes([buf[1], buf[2]]) as usize;
        if buf.len() < len {
            return Err("incomplete long packet");
        }
        (len, &buf[3..])
    };
    if hdr.is_empty() {
        return Err("empty header");
    }
    let msg_type = hdr[0];
    let body = &hdr[1..];

    Ok(match msg_type {
        MSG_CONNECT => {
            if body.len() < 4 {
                return Err("connect body too short");
            }
            let flags = body[0];
            let protocol_id = body[1];
            let duration = u16::from_be_bytes([body[2], body[3]]);
            let client_id = String::from_utf8_lossy(&body[4..]).to_string();
            SnPacket::Connect { flags, protocol_id, duration, client_id }
        }
        MSG_CONNACK => {
            if body.is_empty() {
                return Err("connack body empty");
            }
            SnPacket::Connack { return_code: body[0] }
        }
        MSG_REGISTER => {
            if body.len() < 5 {
                return Err("register body too short");
            }
            let flags = body[0];
            let topic_id = u16::from_be_bytes([body[1], body[2]]);
            let msg_id = u16::from_be_bytes([body[3], body[4]]);
            let topic_name = String::from_utf8_lossy(&body[5..]).to_string();
            SnPacket::Register { flags, topic_id, msg_id, topic_name }
        }
        MSG_REGACK => {
            if body.len() < 6 {
                return Err("regack body too short");
            }
            let flags = body[0];
            let topic_id = u16::from_be_bytes([body[1], body[2]]);
            let msg_id = u16::from_be_bytes([body[3], body[4]]);
            let return_code = body[5];
            SnPacket::Regack { flags, topic_id, msg_id, return_code }
        }
        MSG_PUBLISH => {
            if body.len() < 5 {
                return Err("publish body too short");
            }
            let flags = body[0];
            let topic_id = u16::from_be_bytes([body[1], body[2]]);
            let msg_id = u16::from_be_bytes([body[3], body[4]]);
            let data = body[5..].to_vec();
            SnPacket::Publish { flags, topic_id, msg_id, data }
        }
        MSG_PUBACK => {
            if body.len() < 6 {
                return Err("puback body too short");
            }
            let flags = body[0];
            let topic_id = u16::from_be_bytes([body[1], body[2]]);
            let msg_id = u16::from_be_bytes([body[3], body[4]]);
            let return_code = body[5];
            SnPacket::Puback { flags, topic_id, msg_id, return_code }
        }
        MSG_SUBSCRIBE => {
            if body.len() < 3 {
                return Err("subscribe body too short");
            }
            let flags = body[0];
            let msg_id = u16::from_be_bytes([body[1], body[2]]);
            let topic = body[3..].to_vec();
            SnPacket::Subscribe { flags, msg_id, topic }
        }
        MSG_SUBACK => {
            if body.len() < 6 {
                return Err("suback body too short");
            }
            let flags = body[0];
            let topic_id = u16::from_be_bytes([body[1], body[2]]);
            let msg_id = u16::from_be_bytes([body[3], body[4]]);
            let return_code = body[5];
            SnPacket::Suback { flags, topic_id, msg_id, return_code }
        }
        MSG_UNSUBSCRIBE => {
            if body.len() < 3 {
                return Err("unsubscribe body too short");
            }
            let flags = body[0];
            let msg_id = u16::from_be_bytes([body[1], body[2]]);
            let topic = body[3..].to_vec();
            SnPacket::Unsubscribe { flags, msg_id, topic }
        }
        MSG_UNSUBACK => {
            if body.len() < 2 {
                return Err("unsuback body too short");
            }
            let msg_id = u16::from_be_bytes([body[0], body[1]]);
            SnPacket::Unsuback { msg_id }
        }
        MSG_PINGREQ => SnPacket::Pingreq,
        MSG_PINGRESP => SnPacket::Pingresp,
        MSG_DISCONNECT => {
            // Duration 字段可选：存在表示进入睡眠态，时长 = duration
            let duration = if body.len() >= 2 {
                Some(u16::from_be_bytes([body[0], body[1]]))
            } else {
                None
            };
            SnPacket::Disconnect { duration }
        }
        _ => SnPacket::Unknown { msg_type },
    })
}

/// 编码一条 MQTT-SN 报文为字节
fn encode_packet(p: &SnPacket) -> Vec<u8> {
    let mut out = Vec::new();
    // 占位长度字段（先写 1 字节 0，后续根据总长回填）
    out.push(0u8);
    match p {
        SnPacket::Connack { return_code } => {
            // MQTT-SN 规范：CONNACK = Length | MsgType | ReturnCode（无 flags 字节）
            out.push(MSG_CONNACK);
            out.push(*return_code);
        }
        SnPacket::Regack { flags, topic_id, msg_id, return_code } => {
            out.push(MSG_REGACK);
            out.push(*flags);
            out.extend_from_slice(&topic_id.to_be_bytes());
            out.extend_from_slice(&msg_id.to_be_bytes());
            out.push(*return_code);
        }
        SnPacket::Suback { flags, topic_id, msg_id, return_code } => {
            out.push(MSG_SUBACK);
            out.push(*flags);
            out.extend_from_slice(&topic_id.to_be_bytes());
            out.extend_from_slice(&msg_id.to_be_bytes());
            out.push(*return_code);
        }
        SnPacket::Puback { flags, topic_id, msg_id, return_code } => {
            out.push(MSG_PUBACK);
            out.push(*flags);
            out.extend_from_slice(&topic_id.to_be_bytes());
            out.extend_from_slice(&msg_id.to_be_bytes());
            out.push(*return_code);
        }
        SnPacket::Unsuback { msg_id } => {
            out.push(MSG_UNSUBACK);
            out.extend_from_slice(&msg_id.to_be_bytes());
        }
        SnPacket::Pingresp => {
            out.push(MSG_PINGRESP);
        }
        SnPacket::Disconnect { duration } => {
            out.push(MSG_DISCONNECT);
            if let Some(d) = duration {
                out.extend_from_slice(&d.to_be_bytes());
            }
        }
        SnPacket::Publish { flags, topic_id, msg_id, data } => {
            out.push(MSG_PUBLISH);
            out.push(*flags);
            out.extend_from_slice(&topic_id.to_be_bytes());
            out.extend_from_slice(&msg_id.to_be_bytes());
            out.extend_from_slice(data);
        }
        SnPacket::Register { flags, topic_id, msg_id, topic_name } => {
            // 网关发起的 REGISTER（主动注册）：topic_id 由网关分配
            out.push(MSG_REGISTER);
            out.push(*flags);
            out.extend_from_slice(&topic_id.to_be_bytes());
            out.extend_from_slice(&msg_id.to_be_bytes());
            out.extend_from_slice(topic_name.as_bytes());
        }
        // 以下类型网关通常不主动发送，仅占位
        SnPacket::Connect { .. }
        | SnPacket::Subscribe { .. }
        | SnPacket::Unsubscribe { .. }
        | SnPacket::Pingreq
        | SnPacket::Unknown { .. } => {
            // 不应编码；返回空包
            return Vec::new();
        }
    }

    // 回填长度字段
    let total = out.len();
    if total <= 0xFF {
        out[0] = total as u8;
    } else {
        // 长格式：0x01 + 2 字节大端长度 + 内容
        // 长度字段值 = 整个报文的总字节数（含 0x01 前缀和 2 字节长度字段本身）
        // total = 1(占位) + type + body = 1 + content
        // 报文总长 = 1(0x01) + 2(length) + content = 3 + (total - 1) = total + 2
        let packet_len = (total + 2) as u16;
        let mut long = Vec::with_capacity(total + 2);
        long.push(0x01);
        long.extend_from_slice(&packet_len.to_be_bytes());
        // out[0] 是短格式占位长度，out[1..] 才是真实内容（type + body）
        long.extend_from_slice(&out[1..]);
        return long;
    }
    out
}

// ---------- 客户端状态 ----------

/// 单个 SN 客户端的连接态
struct SnClient {
    client_id: String,
    epoch: u64,
    clean: bool,
    keep_alive_secs: u16,
    last_seen: Instant,
    peer: SocketAddr,
    /// 主题名 → 主题 ID
    topic_to_id: HashMap<String, u16>,
    /// 主题 ID → 主题名
    id_to_topic: HashMap<u16, String>,
    /// 出站 QoS1 msg_id 分配
    next_msg_id: u16,
}

impl SnClient {
    fn new(client_id: String, epoch: u64, clean: bool, keep_alive_secs: u16, peer: SocketAddr) -> Self {
        Self {
            client_id,
            epoch,
            clean,
            keep_alive_secs,
            last_seen: Instant::now(),
            peer,
            topic_to_id: HashMap::new(),
            id_to_topic: HashMap::new(),
            next_msg_id: 1,
        }
    }

    fn touch(&mut self) {
        self.last_seen = Instant::now();
    }

    /// 分配主题 ID（若已存在则返回既有）
    fn assign_topic_id(&mut self, topic: &str, next_topic_id: &AtomicU16) -> u16 {
        if let Some(&id) = self.topic_to_id.get(topic) {
            return id;
        }
        let id = loop {
            let v = next_topic_id.fetch_add(1, Ordering::Relaxed);
            let v = if v > TOPIC_ID_MAX { TOPIC_ID_MIN } else { v };
            if v == 0 {
                continue;
            }
            break v;
        };
        self.topic_to_id.insert(topic.to_string(), id);
        self.id_to_topic.insert(id, topic.to_string());
        id
    }

    /// 按 ID 查找主题名
    fn topic_name_of(&self, topic_id: u16) -> Option<&str> {
        self.id_to_topic.get(&topic_id).map(|s| s.as_str())
    }

    /// 分配一个出站 msg_id（绕过 0）
    fn allocate_msg_id(&mut self) -> u16 {
        let m = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1);
        if self.next_msg_id == 0 {
            self.next_msg_id = 1;
        }
        m
    }
}

// ---------- 服务器 ----------

/// MQTT-SN UDP 网关服务
pub struct MqttSnServer {
    bind: SocketAddr,
    broker: Arc<BrokerState>,
    #[allow(dead_code)]
    max_connections: usize,
    shutdown: ShutdownRx,
    /// 全局主题 ID 分配计数器（跨客户端不冲突也可，简单起见用全局）
    next_topic_id: Arc<AtomicU16>,
}

impl MqttSnServer {
    pub fn new(
        bind: SocketAddr,
        broker: Arc<BrokerState>,
        max_connections: usize,
        shutdown: ShutdownRx,
    ) -> Self {
        Self {
            bind,
            broker,
            max_connections,
            shutdown,
            next_topic_id: Arc::new(AtomicU16::new(TOPIC_ID_MIN)),
        }
    }

    /// 启动并阻塞运行，直到收到关闭信号
    pub async fn run(self) -> std::io::Result<()> {
        let socket = Arc::new(UdpSocket::bind(self.bind).await?);
        info!(addr = %self.bind, "MQTT-SN UDP listener started");

        let clients: Arc<DashMap<SocketAddr, SnClient>> = Arc::new(DashMap::new());
        let next_topic_id = self.next_topic_id.clone();
        let broker = self.broker.clone();

        // 心跳超时清理任务
        let cleanup_clients = clients.clone();
        let cleanup_broker = broker.clone();
        let mut cleanup_shutdown = self.shutdown.clone();
        let cleanup_handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = cleanup_shutdown.changed() => break,
                    _ = tick.tick() => {
                        let now = Instant::now();
                        let expired: Vec<(SocketAddr, String, u64, bool)> = cleanup_clients
                            .iter()
                            .filter_map(|e| {
                                if e.keep_alive_secs == 0 {
                                    return None;
                                }
                                let limit = Duration::from_secs(
                                    ((e.keep_alive_secs as u64).max(1) * 3).div_ceil(2),
                                );
                                if now.duration_since(e.last_seen) > limit {
                                    Some((e.peer, e.client_id.clone(), e.epoch, e.clean))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        for (peer, client_id, epoch, clean) in expired {
                            warn!(%peer, client = %client_id, "MQTT-SN keep-alive expired");
                            cleanup_clients.remove(&peer);
                            // 清理会话（clean=true 删除；clean=false 仅标记离线）
                            cleanup_session(&cleanup_broker, &client_id, epoch, clean);
                        }
                    }
                }
            }
        });

        let mut buf = vec![0u8; 65535];
        let mut shutdown = self.shutdown.clone();
        loop {
            tokio::select! {
                biased;
                res = shutdown.changed() => {
                    if res.is_ok() && *shutdown.borrow() {
                        info!("MQTT-SN server shutting down");
                        break;
                    }
                }
                recv = socket.recv_from(&mut buf) => {
                    let (n, peer) = match recv {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "MQTT-SN recv_from failed");
                            continue;
                        }
                    };
                    let data = &buf[..n];
                    let pkt = match parse_packet(data) {
                        Ok(p) => p,
                        Err(e) => {
                            debug!(%peer, error = e, "MQTT-SN parse error, dropping");
                            continue;
                        }
                    };
                    Self::handle(
                        pkt, peer, &socket, &broker, &clients, &next_topic_id,
                    ).await;
                }
            }
        }

        // 清理所有客户端会话
        for entry in clients.iter() {
            cleanup_session(&broker, &entry.client_id, entry.epoch, entry.clean);
        }
        cleanup_handle.abort();
        Ok(())
    }

    async fn handle(
        pkt: SnPacket,
        peer: SocketAddr,
        socket: &Arc<UdpSocket>,
        broker: &Arc<BrokerState>,
        clients: &Arc<DashMap<SocketAddr, SnClient>>,
        next_topic_id: &Arc<AtomicU16>,
    ) {
        match pkt {
            SnPacket::Connect { flags, protocol_id, duration, client_id } => {
                Self::handle_connect(
                    broker, clients, socket, peer, flags, protocol_id, duration, client_id,
                    next_topic_id.clone(),
                ).await;
            }
            SnPacket::Register { flags: _, topic_id: _, msg_id, topic_name } => {
                // 客户端请求分配主题 ID
                let resp = if let Some(mut c) = clients.get_mut(&peer) {
                    c.touch();
                    let tid = c.assign_topic_id(&topic_name, next_topic_id);
                    SnPacket::Regack {
                        flags: 0,
                        topic_id: tid,
                        msg_id,
                        return_code: RC_ACCEPTED,
                    }
                } else {
                    SnPacket::Regack {
                        flags: 0,
                        topic_id: 0,
                        msg_id,
                        return_code: RC_NOT_SUPPORTED,
                    }
                };
                send(socket, peer, &resp).await;
            }
            SnPacket::Publish { flags, topic_id, msg_id, data } => {
                Self::handle_publish(
                    broker, clients, socket, peer, flags, topic_id, msg_id, data,
                ).await;
            }
            SnPacket::Subscribe { flags, msg_id, topic } => {
                Self::handle_subscribe(
                    broker, clients, socket, peer, flags, msg_id, topic, next_topic_id,
                ).await;
            }
            SnPacket::Unsubscribe { flags: _, msg_id, topic } => {
                let topic_name = String::from_utf8_lossy(&topic).to_string();
                let resp = if let Some(mut c) = clients.get_mut(&peer) {
                    c.touch();
                    let _ = broker.subscriptions().unsubscribe(&c.client_id, &topic_name);
                    // 若有主题 ID 映射，移除
                    if let Some(&tid) = c.topic_to_id.get(&topic_name) {
                        c.topic_to_id.remove(&topic_name);
                        c.id_to_topic.remove(&tid);
                    }
                    SnPacket::Unsuback { msg_id }
                } else {
                    SnPacket::Unsuback { msg_id }
                };
                METRICS.inc_unsubscribe();
                send(socket, peer, &resp).await;
            }
            SnPacket::Pingreq => {
                if let Some(mut c) = clients.get_mut(&peer) {
                    c.touch();
                }
                send(socket, peer, &SnPacket::Pingresp).await;
            }
            SnPacket::Disconnect { duration } => {
                match duration {
                    Some(d) => {
                        // 睡眠态：标记会话离线（消息入离线队列），保留会话订阅以便唤醒后恢复
                        debug!(%peer, sleep_secs = d, "MQTT-SN client entering sleep state");
                        if let Some((_, c)) = clients.remove(&peer) {
                            if broker.sessions().owns(&c.client_id, c.epoch) {
                                broker.sessions().mark_offline(&c.client_id, c.epoch);
                            }
                            METRICS.dec_connections();
                        }
                    }
                    None => {
                        // 真正断开：清理会话（clean=true 删除订阅；clean=false 标记离线）
                        debug!(%peer, "MQTT-SN client disconnected");
                        if let Some((_, c)) = clients.remove(&peer) {
                            cleanup_session(broker, &c.client_id, c.epoch, c.clean);
                        }
                    }
                }
            }
            // 以下为客户端→网关方向不应主动发送，或本实现忽略
            SnPacket::Puback { .. } => {
                // QoS1 出站消息确认：本最小实现不维护出站 inflight，仅 touch
                if let Some(mut c) = clients.get_mut(&peer) {
                    c.touch();
                }
            }
            SnPacket::Connack { .. }
            | SnPacket::Regack { .. }
            | SnPacket::Suback { .. }
            | SnPacket::Unsuback { .. }
            | SnPacket::Pingresp
            | SnPacket::Unknown { .. } => {
                debug!(%peer, "MQTT-SN ignoring unexpected packet");
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_connect(
        broker: &Arc<BrokerState>,
        clients: &Arc<DashMap<SocketAddr, SnClient>>,
        socket: &Arc<UdpSocket>,
        peer: SocketAddr,
        flags: u8,
        protocol_id: u8,
        duration: u16,
        client_id: String,
        next_topic_id: Arc<AtomicU16>,
    ) {
        // 协议版本校验
        if protocol_id != PROTOCOL_ID_MQTTSN {
            send(socket, peer, &SnPacket::Connack { return_code: RC_NOT_SUPPORTED }).await;
            return;
        }

        // 鉴权：MQTT-SN 报文无 username/password，仅支持匿名模式（最小实现）
        if !broker.config().auth.allow_anonymous {
            warn!(%peer, client = %client_id, "MQTT-SN requires anonymous mode");
            send(socket, peer, &SnPacket::Connack { return_code: RC_NOT_SUPPORTED }).await;
            return;
        }
        if client_id.is_empty() {
            send(socket, peer, &SnPacket::Connack { return_code: RC_REJECTED }).await;
            return;
        }

        // 连接数限制
        let current = METRICS.connections_current.load(std::sync::atomic::Ordering::Relaxed);
        if current as usize >= broker.config().broker.max_connections {
            send(socket, peer, &SnPacket::Connack { return_code: RC_CONGESTION }).await;
            return;
        }

        let clean = (flags & FLAG_CLEAN) != 0;
        let _will = (flags & FLAG_WILL) != 0; // 本实现忽略遗嘱

        // 注册会话：复用 broker.sessions，用一个 tx 通道 + forwarder 任务
        let cap = broker.config().broker.max_inflight.max(16);
        let (tx, rx) = mpsc::channel::<OutboundPublish>(cap);
        let dummy_addr: SocketAddr = peer;
        let (epoch, _session_present, _offline) = broker.sessions().register(
            client_id.clone(),
            clean,
            tx,
            None, // SN 最小实现不传遗嘱
            4,    // 复用 MQTT 3.1.1 level
            dummy_addr,
            None, // MQTT-SN 不携带 Session Expiry
        );

        METRICS.inc_connections();
        info!(%peer, client = %client_id, clean, keep_alive = duration, "MQTT-SN client connected");

        let client = SnClient::new(client_id.clone(), epoch, clean, duration, peer);
        clients.insert(peer, client);

        // CONNACK
        send(socket, peer, &SnPacket::Connack { return_code: RC_ACCEPTED }).await;

        // 启动 forwarder：drain rx → 封装 SN PUBLISH → UDP 回送
        let socket_cloned = socket.clone();
        let clients_cloned = clients.clone();
        tokio::spawn(async move {
            forward_outbound(rx, socket_cloned, peer, client_id.clone(), clients_cloned, next_topic_id).await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_publish(
        broker: &Arc<BrokerState>,
        clients: &Arc<DashMap<SocketAddr, SnClient>>,
        socket: &Arc<UdpSocket>,
        peer: SocketAddr,
        flags: u8,
        topic_id: u16,
        msg_id: u16,
        data: Vec<u8>,
    ) {
        let qos = match flags_to_qos(flags) {
            Ok(q) => q,
            Err(rc) => {
                // QoS 不支持：回 PUBACK 错误码（QoS2 等）
                if topic_id != 0 {
                    send(socket, peer, &SnPacket::Puback {
                        flags: 0,
                        topic_id,
                        msg_id,
                        return_code: rc,
                    }).await;
                }
                return;
            }
        };

        let (client_id, topic_name) = match clients.get_mut(&peer) {
            Some(mut c) => {
                c.touch();
                let name = c.topic_name_of(topic_id).map(|s| s.to_string());
                (c.client_id.clone(), name)
            }
            None => {
                debug!(%peer, "PUBLISH from unknown client");
                return;
            }
        };

        let topic_name = match topic_name {
            Some(t) => t,
            None => {
                // 短主题名：topic_id 本身即 2 字节短名
                let id_type = flags & FLAG_TOPIC_ID_TYPE_MASK;
                if id_type == TOPIC_ID_TYPE_SHORT && topic_id != 0 {
                    let bytes = topic_id.to_be_bytes();
                    String::from_utf8_lossy(&bytes).to_string()
                } else {
                    warn!(%peer, topic_id, "PUBLISH with unknown topic id");
                    if qos != QoS::AtMostOnce {
                        send(socket, peer, &SnPacket::Puback {
                            flags: 0,
                            topic_id,
                            msg_id,
                            return_code: RC_INVALID_TOPIC_ID,
                        }).await;
                    }
                    return;
                }
            }
        };

        let retain = (flags & FLAG_RETAIN) != 0;
        // 路由到 broker
        let trace_id = crate::utils::time::trace_id();
        debug!(%peer, %trace_id, %topic_name, ?qos, "MQTT-SN PUBLISH received");
        if let Err(e) = broker.router().publish(&topic_name, &data, qos, retain, Some(&client_id), &trace_id) {
            warn!(%peer, %trace_id, error = %e, "MQTT-SN route publish failed");
        } else {
            METRICS.inc_publish();
        }

        // QoS1 → 回 PUBACK
        if qos == QoS::AtLeastOnce {
            send(socket, peer, &SnPacket::Puback {
                flags: 0,
                topic_id,
                msg_id,
                return_code: RC_ACCEPTED,
            }).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_subscribe(
        broker: &Arc<BrokerState>,
        clients: &Arc<DashMap<SocketAddr, SnClient>>,
        socket: &Arc<UdpSocket>,
        peer: SocketAddr,
        flags: u8,
        msg_id: u16,
        topic: Vec<u8>,
        next_topic_id: &AtomicU16,
    ) {
        let qos = match flags_to_qos(flags) {
            Ok(q) => q,
            Err(rc) => {
                send(socket, peer, &SnPacket::Suback {
                    flags: 0,
                    topic_id: 0,
                    msg_id,
                    return_code: rc,
                }).await;
                return;
            }
        };

        let id_type = flags & FLAG_TOPIC_ID_TYPE_MASK;
        let topic_name = String::from_utf8_lossy(&topic).to_string();

        let resp = if let Some(mut c) = clients.get_mut(&peer) {
            c.touch();
            // 订阅注册到 broker 订阅树
            match broker.subscriptions().subscribe(&c.client_id, &topic_name, qos) {
                Ok(()) => {
                    METRICS.inc_subscribe();
                    // 分配主题 ID（短主题名使用 topic_id 本身）
                    let tid = if id_type == TOPIC_ID_TYPE_SHORT && topic_name.len() == 2 {
                        let bytes = topic_name.as_bytes();
                        u16::from_be_bytes([bytes[0], bytes[1]])
                    } else {
                        c.assign_topic_id(&topic_name, next_topic_id)
                    };
                    SnPacket::Suback {
                        flags: qos_to_flags(qos),
                        topic_id: tid,
                        msg_id,
                        return_code: RC_ACCEPTED,
                    }
                }
                Err(_) => SnPacket::Suback {
                    flags: 0,
                    topic_id: 0,
                    msg_id,
                    return_code: RC_NOT_SUPPORTED,
                },
            }
        } else {
            SnPacket::Suback {
                flags: 0,
                topic_id: 0,
                msg_id,
                return_code: RC_NOT_SUPPORTED,
            }
        };
        send(socket, peer, &resp).await;
    }
}

/// 出站 forwarder：从会话 tx 通道取出 OutboundPublish，封装为 SN PUBLISH 经 UDP 回送
async fn forward_outbound(
    mut rx: mpsc::Receiver<OutboundPublish>,
    socket: Arc<UdpSocket>,
    peer: SocketAddr,
    client_id: String,
    clients: Arc<DashMap<SocketAddr, SnClient>>,
    next_topic_id: Arc<AtomicU16>,
) {
    while let Some(msg) = rx.recv().await {
        let (topic_id, msg_id, qos) = match clients.get_mut(&peer) {
            Some(mut c) => {
                // 复用既有主题 ID；若该主题未被客户端 REGISTER/SUBSCRIBE，则现分配一个
                // 使用全局原子计数器 assign_topic_id，避免线性扫描与跨客户端 ID 冲突
                let tid = c.assign_topic_id(&msg.topic, &next_topic_id);
                let mid = if msg.qos != QoS::AtMostOnce {
                    Some(c.allocate_msg_id())
                } else {
                    None
                };
                (tid, mid, msg.qos)
            }
            None => {
                // 客户端已不在：退出
                break;
            }
        };

        // SN PUBLISH 仅支持 QoS0/1；QoS2 降级为 QoS1（最小实现）
        let sn_qos = if qos == QoS::ExactlyOnce { QoS::AtLeastOnce } else { qos };
        let mut flags = qos_to_flags(sn_qos);
        // 出站 PUBLISH 主题 ID 类型：normal
        flags &= !FLAG_TOPIC_ID_TYPE_MASK;
        let mid = msg_id.unwrap_or(0);
        let pkt = SnPacket::Publish {
            flags,
            topic_id,
            msg_id: mid,
            data: msg.payload.clone(),
        };
        let data = encode_packet(&pkt);
        if !data.is_empty() {
            if let Err(e) = socket.send_to(&data, peer).await {
                warn!(%peer, client = %client_id, error = %e, "MQTT-SN outbound send failed");
                break;
            }
        }
    }
    debug!(%peer, client = %client_id, "MQTT-SN forwarder exited");
}

/// 清理 SN 客户端会话（clean=true 删除订阅与会话；clean=false 标记离线）
fn cleanup_session(broker: &Arc<BrokerState>, client_id: &str, epoch: u64, clean: bool) {
    if !broker.sessions().owns(client_id, epoch) {
        return;
    }
    if clean {
        broker.subscriptions().unsubscribe_all(client_id);
        broker.sessions().remove(client_id);
    } else {
        broker.sessions().mark_offline(client_id, epoch);
    }
    METRICS.dec_connections();
    METRICS.inc_disconnect();
}

/// 编码并发送一条 SN 报文到指定 peer
async fn send(socket: &Arc<UdpSocket>, peer: SocketAddr, pkt: &SnPacket) {
    let data = encode_packet(pkt);
    if data.is_empty() {
        return;
    }
    if let Err(e) = socket.send_to(&data, peer).await {
        warn!(%peer, error = %e, "MQTT-SN send_to failed");
    }
}
