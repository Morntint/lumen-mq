use thiserror::Error;

/// LumenMQ 统一错误类型，分层划分协议/网络/业务/存储/配置/鉴权错误
#[derive(Debug, Error)]
pub enum BrokerError {
    // ---------- 协议编解码错误 ----------
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),

    // ---------- 网络传输错误 ----------
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("connection closed: {0}")]
    ConnectionClosed(String),

    #[error("connection reset by peer")]
    ConnectionReset,

    #[error("heartbeat timeout: client {0} keep-alive expired")]
    HeartbeatTimeout(String),

    #[error("packet too large: {0} bytes exceeds limit {1}")]
    PacketTooLarge(usize, usize),

    // ---------- Broker 业务错误 ----------
    #[error("session error: {0}")]
    Session(String),

    #[error("subscription error: {0}")]
    Subscription(String),

    #[error("topic invalid: {0}")]
    InvalidTopic(String),

    #[error("client id empty or too long")]
    InvalidClientId,

    #[error("qos invalid: {0}")]
    InvalidQos(u8),

    // ---------- 鉴权错误 ----------
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),

    // ---------- 配置错误 ----------
    #[error("config error: {0}")]
    Config(String),

    // ---------- 存储错误 ----------
    #[error("storage error: {0}")]
    Storage(String),

    // ---------- 限流/风控错误 ----------
    #[error("rate limited: {0}")]
    RateLimited(String),

    // ---------- 通用错误 ----------
    #[error("{0}")]
    Other(String),
}

/// 协议编解码错误
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("incomplete packet: need more bytes")]
    Incomplete,

    #[error("invalid packet type: {0}")]
    InvalidPacketType(u8),

    #[error("invalid flags: type={0} flags=0x{1:02x}")]
    InvalidFlags(u8, u8),

    #[error("invalid protocol name/level: {0}")]
    InvalidProtocol(String),

    #[error("invalid remaining length: {0}")]
    InvalidRemainingLength(usize),

    #[error("malformed utf8 string")]
    MalformedUtf8,

    #[error("malformed packet body: {0}")]
    MalformedBody(String),

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
}

/// FramedRead/FramedWrite 要求 Error 可由 io::Error 转换而来
impl From<std::io::Error> for CodecError {
    fn from(e: std::io::Error) -> Self {
        CodecError::MalformedBody(format!("io: {e}"))
    }
}

/// 鉴权错误
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("authentication failed: bad credentials")]
    BadCredentials,

    #[error("authentication failed: client id not authorized")]
    UnauthorizedClientId,

    #[error("authentication failed: anonymous not allowed")]
    AnonymousForbidden,

    #[error("authentication failed: too many connections")]
    TooManyConnections,
}

pub type BrokerResult<T> = Result<T, BrokerError>;
