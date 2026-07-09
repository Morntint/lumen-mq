use bitflags::bitflags;
use std::fmt;

use crate::utils::CodecError;

/// MQTT 报文类型（固定头高 4 位）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
}

impl PacketType {
    pub fn from_u8(v: u8) -> Result<Self, CodecError> {
        Ok(match v {
            1 => Self::Connect,
            2 => Self::Connack,
            3 => Self::Publish,
            4 => Self::Puback,
            5 => Self::Pubrec,
            6 => Self::Pubrel,
            7 => Self::Pubcomp,
            8 => Self::Subscribe,
            9 => Self::Suback,
            10 => Self::Unsubscribe,
            11 => Self::Unsuback,
            12 => Self::Pingreq,
            13 => Self::Pingresp,
            14 => Self::Disconnect,
            other => return Err(CodecError::InvalidPacketType(other)),
        })
    }
}

/// QoS 等级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(v: u8) -> Result<Self, CodecError> {
        Ok(match v {
            0 => Self::AtMostOnce,
            1 => Self::AtLeastOnce,
            2 => Self::ExactlyOnce,
            other => return Err(CodecError::MalformedBody(format!("invalid qos {other}"))),
        })
    }
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QoS::AtMostOnce => write!(f, "0"),
            QoS::AtLeastOnce => write!(f, "1"),
            QoS::ExactlyOnce => write!(f, "2"),
        }
    }
}

bitflags! {
    /// CONNECT 报文标志位
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct ConnectFlags: u8 {
        const CLEAN_SESSION = 0b0000_0010;
        const WILL_FLAG     = 0b0000_0100;
        const WILL_QOS_0    = 0b0000_0000;
        const WILL_QOS_1    = 0b0000_1000;
        const WILL_QOS_2    = 0b0001_0000;
        const WILL_QOS_MASK = 0b0001_1000;
        const WILL_RETAIN   = 0b0010_0000;
        const PASSWORD      = 0b0100_0000;
        const USERNAME      = 0b1000_0000;
    }
}

/// MQTT 3.1.1 协议级别
pub const MQTT_3_1_1_LEVEL: u8 = 4;
pub const MQTT_3_1_LEVEL: u8 = 3;
/// MQTT 5.0 协议级别
pub const MQTT_5_LEVEL: u8 = 5;

/// 统一报文枚举
#[derive(Debug, Clone)]
pub enum Packet {
    Connect(Connect),
    Connack(Connack),
    Publish(Publish),
    Puback(u16),
    Pubrec(u16),
    Pubrel(u16),
    Pubcomp(u16),
    Subscribe(Subscribe),
    Suback(Suback),
    Unsubscribe(Unsubscribe),
    Unsuback(u16),
    Pingreq,
    Pingresp,
    Disconnect,
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::Connack(_) => PacketType::Connack,
            Packet::Publish(_) => PacketType::Publish,
            Packet::Puback(_) => PacketType::Puback,
            Packet::Pubrec(_) => PacketType::Pubrec,
            Packet::Pubrel(_) => PacketType::Pubrel,
            Packet::Pubcomp(_) => PacketType::Pubcomp,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::Suback(_) => PacketType::Suback,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::Unsuback(_) => PacketType::Unsuback,
            Packet::Pingreq => PacketType::Pingreq,
            Packet::Pingresp => PacketType::Pingresp,
            Packet::Disconnect => PacketType::Disconnect,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Connect {
    pub protocol_level: u8,
    pub keep_alive: u16,
    pub client_id: String,
    pub clean_session: bool,
    pub will: Option<LastWill>,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    /// MQTT 5.0 CONNECT 属性（仅 protocol_level==5 时存在）
    pub properties: Option<ConnectProperties>,
}

/// MQTT 5.0 CONNECT 属性（轻量解析：仅提取会话过期，其余跳过）
#[derive(Debug, Clone, Default)]
pub struct ConnectProperties {
    /// 会话过期间隔（秒）；0 = 立即过期（等价 3.1.1 clean=true）
    pub session_expiry_interval: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct LastWill {
    pub topic: String,
    pub message: Vec<u8>,
    pub qos: QoS,
    pub retain: bool,
}

#[derive(Debug, Clone)]
pub struct Connack {
    pub session_present: bool,
    /// 0=接受;1=协议版本;2=clientid被拒;3=服务不可用;4=用户名密码错;5=未授权
    pub return_code: u8,
    /// 客户端协议级别（决定 CONNACK 是否编码 5.0 属性段）
    pub protocol_level: u8,
}

impl Connack {
    pub fn accepted(session_present: bool) -> Self {
        Self { session_present, return_code: 0, protocol_level: MQTT_3_1_1_LEVEL }
    }
    pub fn accepted_5(session_present: bool) -> Self {
        Self { session_present, return_code: 0, protocol_level: MQTT_5_LEVEL }
    }
}

#[derive(Debug, Clone)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    pub packet_id: u16,
    pub topics: Vec<SubscribeTopic>,
}

#[derive(Debug, Clone)]
pub struct SubscribeTopic {
    pub topic_filter: String,
    pub qos: QoS,
}

#[derive(Debug, Clone)]
pub struct Suback {
    pub packet_id: u16,
    /// 0/1/2 = 接受对应 QoS; 0x80 = 失败
    pub return_codes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub topic_filters: Vec<String>,
}

// ---------- CONNACK 返回码 ----------
pub mod connack_code {
    pub const ACCEPTED: u8 = 0;
    pub const BAD_PROTOCOL_VERSION: u8 = 1;
    pub const IDENTIFIER_REJECTED: u8 = 2;
    pub const SERVER_UNAVAILABLE: u8 = 3;
    pub const BAD_USERNAME_PASSWORD: u8 = 4;
    pub const NOT_AUTHORIZED: u8 = 5;
}

// ---------- SUBACK 返回码 ----------
pub mod suback_code {
    pub const MAX_QOS_0: u8 = 0;
    pub const MAX_QOS_1: u8 = 1;
    pub const MAX_QOS_2: u8 = 2;
    pub const FAILURE: u8 = 0x80;
}
