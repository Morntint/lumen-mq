//! MQTT-SN UDP 网关集成测试
//!
//! 通过真实 UDP socket + 原始 MQTT-SN 报文验证：
//! - CONNECT/CONNACK
//! - REGISTER/REGACK（主题 ID 分配）
//! - PUBLISH（QoS0/QoS1）→ 路由到 TCP 订阅者
//! - SUBSCRIBE/SUBACK → TCP 发布者发布后 SN 客户端收到出站 PUBLISH
//! - PINGREQ/PINGRESP
//! - DISCONNECT

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{MqttCodec, Packet, Publish, QoS, Subscribe, SubscribeTopic, MQTT_3_1_1_LEVEL};
use lumenmq::config::{AuthConfig, AuthMode, BrokerConfig, Settings};
use lumenmq::net::{new_shutdown_channel, MqttSnServer, TcpServer};

// ---------- MQTT-SN 常量 ----------

const MSG_CONNECT: u8 = 0x04;
const MSG_CONNACK: u8 = 0x05;
const MSG_REGISTER: u8 = 0x0A;
const MSG_REGACK: u8 = 0x0B;
const MSG_PUBLISH: u8 = 0x0C;
const MSG_PUBACK: u8 = 0x0D;
const MSG_SUBSCRIBE: u8 = 0x11;
const MSG_SUBACK: u8 = 0x12;
const MSG_PINGREQ: u8 = 0x15;
const MSG_PINGRESP: u8 = 0x16;
const MSG_DISCONNECT: u8 = 0x17;

const RC_ACCEPTED: u8 = 0x00;

const PROTOCOL_ID_MQTTSN: u8 = 0x01;

// ---------- 测试辅助 ----------

fn make_broker() -> Arc<BrokerState> {
    let mut settings = Settings::default();
    settings.broker = BrokerConfig {
        max_connections: 100,
        max_packet_size: 64 * 1024,
        default_keep_alive: 60,
        max_subscriptions_per_client: 32,
        max_inflight: 64,
        retry_interval_secs: Some(2),
        max_retries: Some(2),
        ..BrokerConfig::default()
    };
    settings.auth = AuthConfig {
        mode: AuthMode::Anonymous,
        allow_anonymous: true,
        users: vec![],
    };
    let config = Arc::new(settings);
    let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
    BrokerState::new(config, auth)
}

/// 启动 TCP + SN 服务，返回 (broker, tcp_addr, sn_addr, shutdown_tx, join_handles)
async fn spawn_servers() -> (
    Arc<BrokerState>,
    std::net::SocketAddr,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    Vec<tokio::task::JoinHandle<()>>,
) {
    let broker = make_broker();
    let (shutdown_tx, shutdown_rx_tcp) = new_shutdown_channel();
    let shutdown_rx_sn = shutdown_tx.subscribe();

    // TCP 服务
    let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tcp_addr = tcp_listener.local_addr().unwrap();
    drop(tcp_listener);
    let tcp_server = TcpServer::new(tcp_addr, broker.clone(), 100, shutdown_rx_tcp);
    let tcp_handle = tokio::spawn(async move {
        let _ = tcp_server.run().await;
    });

    // SN UDP 服务
    let sn_listener = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sn_addr = sn_listener.local_addr().unwrap();
    drop(sn_listener);
    let sn_server = MqttSnServer::new(sn_addr, broker.clone(), 100, shutdown_rx_sn);
    let sn_handle = tokio::spawn(async move {
        let _ = sn_server.run().await;
    });

    (broker, tcp_addr, sn_addr, shutdown_tx, vec![tcp_handle, sn_handle])
}

/// 编码 SN CONNECT
fn encode_connect(client_id: &str, clean: bool, duration: u16) -> Vec<u8> {
    let id_bytes = client_id.as_bytes();
    // Length(1) + MsgType(1) + Flags(1) + ProtocolId(1) + Duration(2) + ClientId
    let total = 1 + 1 + 1 + 1 + 2 + id_bytes.len();
    let flags = if clean { 0b0000_0100 } else { 0 };
    let mut out = Vec::with_capacity(total);
    out.push(total as u8);
    out.push(MSG_CONNECT);
    out.push(flags);
    out.push(PROTOCOL_ID_MQTTSN);
    out.extend_from_slice(&duration.to_be_bytes());
    out.extend_from_slice(id_bytes);
    out
}

/// 编码 SN REGISTER（topic_id=0 表示请求分配）
fn encode_register(msg_id: u16, topic_name: &str) -> Vec<u8> {
    let t = topic_name.as_bytes();
    let total = 1 + 1 + 1 + 2 + 2 + t.len();
    let mut out = Vec::with_capacity(total);
    out.push(total as u8);
    out.push(MSG_REGISTER);
    out.push(0); // flags
    out.extend_from_slice(&0u16.to_be_bytes()); // topic_id=0
    out.extend_from_slice(&msg_id.to_be_bytes());
    out.extend_from_slice(t);
    out
}

/// 编码 SN PUBLISH
fn encode_publish(flags: u8, topic_id: u16, msg_id: u16, data: &[u8]) -> Vec<u8> {
    let total = 1 + 1 + 1 + 2 + 2 + data.len();
    let mut out = Vec::with_capacity(total);
    out.push(total as u8);
    out.push(MSG_PUBLISH);
    out.push(flags);
    out.extend_from_slice(&topic_id.to_be_bytes());
    out.extend_from_slice(&msg_id.to_be_bytes());
    out.extend_from_slice(data);
    out
}

/// 编码 SN SUBSCRIBE（normal topic name）
fn encode_subscribe(qos: QoS, msg_id: u16, topic: &str) -> Vec<u8> {
    let t = topic.as_bytes();
    let total = 1 + 1 + 1 + 2 + t.len();
    let mut out = Vec::with_capacity(total);
    let qos_bits: u8 = match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    };
    let flags = qos_bits << 5;
    out.push(total as u8);
    out.push(MSG_SUBSCRIBE);
    out.push(flags);
    out.extend_from_slice(&msg_id.to_be_bytes());
    out.extend_from_slice(t);
    out
}

fn encode_pingreq() -> Vec<u8> {
    vec![2, MSG_PINGREQ]
}

fn encode_disconnect() -> Vec<u8> {
    vec![2, MSG_DISCONNECT]
}

/// 从 UDP recv 解析一条报文（返回 (msg_type, body_after_type)）
fn parse_sn(buf: &[u8]) -> Option<(u8, &[u8])> {
    if buf.len() < 2 {
        return None;
    }
    let (total_len, body_start) = if buf[0] != 0x01 {
        (buf[0] as usize, 1)
    } else {
        if buf.len() < 4 {
            return None;
        }
        (u16::from_be_bytes([buf[1], buf[2]]) as usize, 3)
    };
    if buf.len() < total_len {
        return None;
    }
    let msg_type = buf[body_start];
    Some((msg_type, &buf[body_start + 1..]))
}

// ---------- 简化的 TCP MQTT 客户端 ----------

struct TcpMqttClient {
    sink: FramedWrite<tokio::io::WriteHalf<TcpStream>, MqttCodec>,
    stream: FramedRead<tokio::io::ReadHalf<TcpStream>, MqttCodec>,
}

impl TcpMqttClient {
    async fn connect(addr: std::net::SocketAddr, client_id: &str) -> anyhow::Result<Self> {
        use futures::SinkExt;
        let socket = TcpStream::connect(addr).await?;
        let _ = socket.set_nodelay(true);
        let (r, w) = tokio::io::split(socket);
        let codec = MqttCodec::default();
        let mut sink = FramedWrite::new(w, codec.clone());
        let stream = FramedRead::new(r, codec);
        sink.send(Packet::Connect(lumenmq::codec::Connect {
            protocol_level: MQTT_3_1_1_LEVEL,
            keep_alive: 60,
            client_id: client_id.into(),
            clean_session: true,
            will: None,
            username: None,
            password: None,
            properties: None,
        }))
        .await?;
        Ok(Self { sink, stream })
    }

    async fn recv(&mut self) -> anyhow::Result<Packet> {
        use futures::StreamExt;
        match self.stream.next().await {
            Some(Ok(p)) => Ok(p),
            _ => anyhow::bail!("stream closed"),
        }
    }

    async fn send(&mut self, p: Packet) -> anyhow::Result<()> {
        use futures::SinkExt;
        self.sink.send(p).await?;
        Ok(())
    }
}

// ---------- 测试用例 ----------

#[tokio::test]
async fn sn_connect_and_connack() -> anyhow::Result<()> {
    let (_broker, _tcp, sn_addr, tx, _handles) = spawn_servers().await;

    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    sock.connect(sn_addr).await?;

    // CONNECT
    sock.send(&encode_connect("sn-client-1", true, 60)).await?;
    let mut buf = vec![0u8; 1024];
    let (n, _) = sock.recv_from(&mut buf).await?;
    let (msg_type, body) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(msg_type, MSG_CONNACK);
    // CONNACK: ReturnCode（MQTT-SN 规范：Length | MsgType | ReturnCode，无 Flags 字节）
    assert_eq!(body[0], RC_ACCEPTED, "CONNACK return code should be accepted");

    // PINGREQ → PINGRESP
    sock.send(&encode_pingreq()).await?;
    let (n, _) = sock.recv_from(&mut buf).await?;
    let (msg_type, _) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(msg_type, MSG_PINGRESP);

    // DISCONNECT
    sock.send(&encode_disconnect()).await?;

    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn sn_publish_to_tcp_subscriber() -> anyhow::Result<()> {
    let (_broker, tcp_addr, sn_addr, tx, _handles) = spawn_servers().await;

    // 1. TCP 订阅者：订阅 sensor/temp QoS0
    let mut sub = TcpMqttClient::connect(tcp_addr, "tcp-sub-sn").await?;
    let _ = sub.recv().await?; // CONNACK
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "sensor/temp".into(), qos: QoS::AtMostOnce }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    // 2. SN 客户端：CONNECT + REGISTER(sensor/temp)
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    sock.connect(sn_addr).await?;
    sock.send(&encode_connect("sn-pub", true, 60)).await?;
    let mut buf = vec![0u8; 1024];
    let (n, _) = sock.recv_from(&mut buf).await?;
    assert_eq!(parse_sn(&buf[..n]).unwrap().0, MSG_CONNACK);

    // REGISTER sensor/temp → 收到 REGACK，拿到 topic_id
    sock.send(&encode_register(1, "sensor/temp")).await?;
    let (n, _) = sock.recv_from(&mut buf).await?;
    let (msg_type, body) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(msg_type, MSG_REGACK);
    // REGACK: Flags, TopicId(2), MsgId(2), ReturnCode
    let topic_id = u16::from_be_bytes([body[1], body[2]]);
    let msg_id = u16::from_be_bytes([body[3], body[4]]);
    assert_eq!(msg_id, 1);
    assert_eq!(body[5], RC_ACCEPTED);
    assert!(topic_id != 0, "assigned topic_id must be non-zero");

    // 3. SN PUBLISH QoS0 到 sensor/temp
    let flags = 0u8; // QoS0, no retain
    sock.send(&encode_publish(flags, topic_id, 0, b"23.5C")).await?;

    // 4. TCP 订阅者应收到 PUBLISH
    let inbound = tokio::time::timeout(Duration::from_secs(2), sub.recv()).await??;
    match inbound {
        Packet::Publish(Publish { topic, payload, qos, .. }) => {
            assert_eq!(topic, "sensor/temp");
            assert_eq!(payload, b"23.5C");
            assert_eq!(qos, QoS::AtMostOnce);
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }

    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn sn_subscriber_receives_from_tcp_publish() -> anyhow::Result<()> {
    let (_broker, tcp_addr, sn_addr, tx, _handles) = spawn_servers().await;

    // 1. SN 客户端：CONNECT + SUBSCRIBE(cmd/+) QoS0
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    sock.connect(sn_addr).await?;
    sock.send(&encode_connect("sn-sub", true, 60)).await?;
    let mut buf = vec![0u8; 1024];
    let (n, _) = sock.recv_from(&mut buf).await?;
    assert_eq!(parse_sn(&buf[..n]).unwrap().0, MSG_CONNACK);

    sock.send(&encode_subscribe(QoS::AtMostOnce, 1, "cmd/+")).await?;
    let (n, _) = sock.recv_from(&mut buf).await?;
    let (msg_type, body) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(msg_type, MSG_SUBACK);
    // SUBACK: Flags, TopicId(2), MsgId(2), ReturnCode
    let topic_id = u16::from_be_bytes([body[1], body[2]]);
    let msg_id = u16::from_be_bytes([body[3], body[4]]);
    assert_eq!(msg_id, 1);
    assert_eq!(body[5], RC_ACCEPTED);
    assert!(topic_id != 0, "SUBACK must assign topic_id");

    // 2. TCP 发布者发 PUBLISH 到 cmd/reboot
    let mut pub_ = TcpMqttClient::connect(tcp_addr, "tcp-pub-sn").await?;
    let _ = pub_.recv().await?; // CONNACK
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "cmd/reboot".into(),
        packet_id: None,
        payload: b"now".to_vec(),
    }))
    .await?;

    // 3. SN 客户端应收到出站 PUBLISH
    let (n, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
        .await??;
    let (msg_type, body) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(msg_type, MSG_PUBLISH, "SN client should receive outbound PUBLISH");
    // PUBLISH: Flags, TopicId(2), MsgId(2), Data
    let _recv_topic_id = u16::from_be_bytes([body[1], body[2]]);
    let _recv_msg_id = u16::from_be_bytes([body[3], body[4]]);
    let payload = &body[5..];
    assert_eq!(payload, b"now", "outbound payload must match");

    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn sn_publish_qos1_returns_puback() -> anyhow::Result<()> {
    let (_broker, tcp_addr, sn_addr, tx, _handles) = spawn_servers().await;

    // TCP 订阅者
    let mut sub = TcpMqttClient::connect(tcp_addr, "tcp-sub-q1").await?;
    let _ = sub.recv().await?;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "q1/topic".into(), qos: QoS::AtLeastOnce }],
    }))
    .await?;
    let _ = sub.recv().await?;

    // SN 客户端
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    sock.connect(sn_addr).await?;
    sock.send(&encode_connect("sn-pub-q1", true, 60)).await?;
    let mut buf = vec![0u8; 1024];
    let (n, _) = sock.recv_from(&mut buf).await?;
    assert_eq!(parse_sn(&buf[..n]).unwrap().0, MSG_CONNACK);

    // REGISTER q1/topic
    sock.send(&encode_register(1, "q1/topic")).await?;
    let (n, _) = sock.recv_from(&mut buf).await?;
    let (mt, body) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(mt, MSG_REGACK);
    let topic_id = u16::from_be_bytes([body[1], body[2]]);

    // SN PUBLISH QoS1
    let flags = 0b0010_0000u8; // QoS1
    sock.send(&encode_publish(flags, topic_id, 100, b"qos1-data")).await?;

    // 应收到 PUBACK
    let (n, _) = sock.recv_from(&mut buf).await?;
    let (msg_type, body) = parse_sn(&buf[..n]).unwrap();
    assert_eq!(msg_type, MSG_PUBACK, "QoS1 PUBLISH must be acked with PUBACK");
    // PUBACK: Flags, TopicId(2), MsgId(2), ReturnCode
    let ack_topic_id = u16::from_be_bytes([body[1], body[2]]);
    let ack_msg_id = u16::from_be_bytes([body[3], body[4]]);
    let rc = body[5];
    assert_eq!(ack_topic_id, topic_id);
    assert_eq!(ack_msg_id, 100);
    assert_eq!(rc, RC_ACCEPTED);

    let _ = tx.send(true);
    Ok(())
}
