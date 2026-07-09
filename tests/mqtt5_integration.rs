//! MQTT 5.0 轻量支持集成测试
//!
//! 验证：
//! - MQTT 5.0 协议级别识别（CONNECT level=5 被接受，CONNACK 带属性段）
//! - CONNECT 属性段编解码（Session Expiry Interval 透传）
//! - 共享订阅端到端投递（$share/{group}/{filter} 轮询选一个成员）
//! - Session Expiry：expiry=0 时 clean=false 会话立即清理；expiry>0 保留
//! - MQTT 5.0 客户端与 3.1.1 客户端共存

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{
    Connack, Connect, ConnectProperties, MqttCodec, Packet, Publish, QoS, Subscribe,
    SubscribeTopic, Suback, MQTT_3_1_1_LEVEL, MQTT_5_LEVEL,
};
use lumenmq::config::{AuthConfig, AuthMode, BrokerConfig, Settings};
use lumenmq::net::{new_shutdown_channel, TcpServer};

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

async fn spawn_server() -> (
    Arc<BrokerState>,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let broker = make_broker();
    let (shutdown_tx, shutdown_rx) = new_shutdown_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = TcpServer::new(addr, broker.clone(), 100, shutdown_rx);
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, addr, shutdown_tx, handle)
}

struct MqttClient {
    sink: FramedWrite<tokio::io::WriteHalf<TcpStream>, MqttCodec>,
    stream: FramedRead<tokio::io::ReadHalf<TcpStream>, MqttCodec>,
}

impl MqttClient {
    async fn connect_raw(addr: std::net::SocketAddr, connect: Connect) -> anyhow::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        let _ = socket.set_nodelay(true);
        let (r, w) = tokio::io::split(socket);
        let codec = MqttCodec::default();
        let mut sink = FramedWrite::new(w, codec.clone());
        let stream = FramedRead::new(r, codec);
        sink.send(Packet::Connect(connect)).await?;
        Ok(Self { sink, stream })
    }

    async fn connect_v5(
        addr: std::net::SocketAddr,
        client_id: &str,
        clean: bool,
        session_expiry: Option<u32>,
    ) -> anyhow::Result<Self> {
        let connect = Connect {
            protocol_level: MQTT_5_LEVEL,
            keep_alive: 60,
            client_id: client_id.into(),
            clean_session: clean,
            will: None,
            username: None,
            password: None,
            properties: Some(ConnectProperties {
                session_expiry_interval: session_expiry,
            }),
        };
        Self::connect_raw(addr, connect).await
    }

    async fn connect_v311(
        addr: std::net::SocketAddr,
        client_id: &str,
        clean: bool,
    ) -> anyhow::Result<Self> {
        let connect = Connect {
            protocol_level: MQTT_3_1_1_LEVEL,
            keep_alive: 60,
            client_id: client_id.into(),
            clean_session: clean,
            will: None,
            username: None,
            password: None,
            properties: None,
        };
        Self::connect_raw(addr, connect).await
    }

    async fn send(&mut self, p: Packet) -> anyhow::Result<()> {
        self.sink.send(p).await?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<Packet> {
        match self.stream.next().await {
            Some(Ok(p)) => Ok(p),
            _ => anyhow::bail!("stream closed"),
        }
    }

    async fn recv_opt(&mut self, timeout: Duration) -> anyhow::Result<Option<Packet>> {
        match tokio::time::timeout(timeout, self.stream.next()).await {
            Ok(Some(Ok(p))) => Ok(Some(p)),
            Ok(Some(Err(_))) | Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }
}

async fn disconnect(mut c: MqttClient) {
    let _ = c.send(Packet::Disconnect).await;
}

/// MQTT 5.0 CONNECT 应被接受并返回带属性段的 CONNACK
#[tokio::test]
async fn mqtt5_connect_accepted() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    let mut c = MqttClient::connect_v5(addr, "v5-client", true, None).await?;
    let connack = c.recv().await?;
    match connack {
        Packet::Connack(Connack { return_code: 0, session_present: false, protocol_level }) => {
            assert_eq!(protocol_level, MQTT_5_LEVEL, "CONNACK should echo level 5");
        }
        other => panic!("expected CONNACK level=5, got {other:?}"),
    }
    disconnect(c).await;
    Ok(())
}

/// MQTT 5.0 与 3.1.1 客户端可共存
#[tokio::test]
async fn mqtt5_and_v311_coexist() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    let mut v5 = MqttClient::connect_v5(addr, "coexist-v5", true, None).await?;
    let _ = v5.recv().await?;

    let mut v3 = MqttClient::connect_v311(addr, "coexist-v3", true).await?;
    let _ = v3.recv().await?;

    // v5 订阅，v3 发布
    v5.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "coexist/test".into(), qos: QoS::AtLeastOnce }],
    }))
    .await?;
    let _ = v5.recv().await?; // SUBACK

    v3.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "coexist/test".into(),
        packet_id: None,
        payload: bytes::Bytes::from_static(b"cross-version"),
    }))
    .await?;

    let inbound = v5.recv().await?;
    match inbound {
        Packet::Publish(Publish { topic, payload, .. }) => {
            assert_eq!(topic, "coexist/test");
            assert_eq!(&payload[..], b"cross-version");
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }

    disconnect(v5).await;
    disconnect(v3).await;
    Ok(())
}

/// 共享订阅：两个成员订阅 $share/g/topic/+，发布者发消息，仅一个成员收到
#[tokio::test]
async fn shared_subscription_one_of_two_receives() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    // 两个共享订阅成员
    let mut m1 = MqttClient::connect_v311(addr, "share-m1", true).await?;
    let _ = m1.recv().await?;
    m1.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "$share/g1/job/+".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await?;
    let _ = m1.recv().await?; // SUBACK

    let mut m2 = MqttClient::connect_v311(addr, "share-m2", true).await?;
    let _ = m2.recv().await?;
    m2.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "$share/g1/job/+".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await?;
    let _ = m2.recv().await?; // SUBACK

    // 发布者发一条消息
    let mut pub_ = MqttClient::connect_v311(addr, "share-pub", true).await?;
    let _ = pub_.recv().await?;
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "job/x".into(),
        packet_id: None,
        payload: bytes::Bytes::from_static(b"shared-job"),
    }))
    .await?;

    // 仅一个成员应收到，另一个不应收到
    let m1_pkt = m1.recv_opt(Duration::from_millis(500)).await?;
    let m2_pkt = m2.recv_opt(Duration::from_millis(500)).await?;
    let received_count = [m1_pkt.is_some(), m2_pkt.is_some()].iter().filter(|&&x| x).count();
    assert_eq!(received_count, 1, "exactly one shared member should receive the message");

    // 验证收到的内容正确
    let received_payload = match (m1_pkt, m2_pkt) {
        (Some(Packet::Publish(p)), None) => p.payload,
        (None, Some(Packet::Publish(p))) => p.payload,
        _ => panic!("unexpected packet pattern"),
    };
    assert_eq!(&received_payload[..], b"shared-job");

    disconnect(m1).await;
    disconnect(m2).await;
    disconnect(pub_).await;
    Ok(())
}

/// 共享订阅轮询：连续 3 条消息应分发给 2 个成员（轮询计数器递增）
#[tokio::test]
async fn shared_subscription_round_robin_delivery() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    let mut m1 = MqttClient::connect_v311(addr, "rr-m1", true).await?;
    let _ = m1.recv().await?;
    m1.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "$share/gr/rr/+".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = m1.recv().await?;

    let mut m2 = MqttClient::connect_v311(addr, "rr-m2", true).await?;
    let _ = m2.recv().await?;
    m2.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "$share/gr/rr/+".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = m2.recv().await?;

    let mut pub_ = MqttClient::connect_v311(addr, "rr-pub", true).await?;
    let _ = pub_.recv().await?;

    // 发 4 条消息，应至少每个成员各收到 1 条（轮询）
    for i in 0..4u8 {
        pub_.send(Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "rr/x".into(),
            packet_id: None,
            payload: bytes::Bytes::from(vec![i]),
        }))
        .await?;
    }

    // 收集 m1 和 m2 在短时间窗口内收到的所有消息
    let mut m1_payloads: Vec<u8> = Vec::new();
    let mut m2_payloads: Vec<u8> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(800);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let m1_pkt = m1.recv_opt(Duration::from_millis(200)).await?;
        let m2_pkt = m2.recv_opt(Duration::from_millis(200)).await?;
        if let Some(Packet::Publish(p)) = m1_pkt {
            m1_payloads.push(p.payload[0]);
        }
        if let Some(Packet::Publish(p)) = m2_pkt {
            m2_payloads.push(p.payload[0]);
        }
    }

    let total: Vec<u8> = {
        let mut v = m1_payloads.clone();
        v.extend(m2_payloads.clone());
        v.sort();
        v
    };
    // 应收到全部 4 条（无丢失）
    assert_eq!(total, vec![0, 1, 2, 3], "all 4 messages should be delivered across shared members");
    // 两个成员都应至少收到 1 条（轮询保证均衡）
    assert!(!m1_payloads.is_empty(), "m1 should receive at least one message");
    assert!(!m2_payloads.is_empty(), "m2 should receive at least one message");

    disconnect(m1).await;
    disconnect(m2).await;
    disconnect(pub_).await;
    Ok(())
}

/// 共享订阅 + 普通订阅共存：共享组选一个，普通订阅者也收到
#[tokio::test]
async fn shared_and_normal_subscription_both_deliver() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    // 共享组两个成员
    let mut sm1 = MqttClient::connect_v311(addr, "mix-sm1", true).await?;
    let _ = sm1.recv().await?;
    sm1.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "$share/gm/data/+".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sm1.recv().await?;

    let mut sm2 = MqttClient::connect_v311(addr, "mix-sm2", true).await?;
    let _ = sm2.recv().await?;
    sm2.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "$share/gm/data/+".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sm2.recv().await?;

    // 普通订阅者
    let mut normal = MqttClient::connect_v311(addr, "mix-normal", true).await?;
    let _ = normal.recv().await?;
    normal.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "data/+".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = normal.recv().await?;

    // 发布
    let mut pub_ = MqttClient::connect_v311(addr, "mix-pub", true).await?;
    let _ = pub_.recv().await?;
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "data/x".into(),
        packet_id: None,
        payload: bytes::Bytes::from_static(b"mix-payload"),
    }))
    .await?;

    // 普通订阅者必收到
    let normal_inbound = normal.recv().await?;
    match normal_inbound {
        Packet::Publish(Publish { payload, .. }) => {
            assert_eq!(&payload[..], b"mix-payload");
        }
        other => panic!("normal subscriber expected PUBLISH, got {other:?}"),
    }

    // 共享组仅一个收到
    let sm1_pkt = sm1.recv_opt(Duration::from_millis(500)).await?;
    let sm2_pkt = sm2.recv_opt(Duration::from_millis(500)).await?;
    let shared_count = [sm1_pkt.is_some(), sm2_pkt.is_some()].iter().filter(|&&x| x).count();
    assert_eq!(shared_count, 1, "shared group should deliver to exactly one member");

    disconnect(sm1).await;
    disconnect(sm2).await;
    disconnect(normal).await;
    disconnect(pub_).await;
    Ok(())
}

/// Session Expiry = 0：clean=false 会话在断开后立即被清理，重连 session_present=false
#[tokio::test]
async fn session_expiry_zero_cleans_immediately() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    // 首次连接：clean=false + session_expiry=0
    let mut c1 = MqttClient::connect_v5(addr, "expiry-zero", false, Some(0)).await?;
    let connack1 = c1.recv().await?;
    assert!(matches!(
        connack1,
        Packet::Connack(Connack { session_present: false, return_code: 0, .. })
    ));
    // 订阅一条
    c1.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "expiry/zero".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await?;
    let _ = c1.recv().await?; // SUBACK
    disconnect(c1).await;

    // 等待服务端处理断开 + cleanup（expiry=0 应立即清理）
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 重连：session_present 应为 false（会话已被 expiry=0 清理）
    let mut c2 = MqttClient::connect_v5(addr, "expiry-zero", false, Some(0)).await?;
    let connack2 = c2.recv().await?;
    match connack2 {
        Packet::Connack(Connack { session_present, return_code: 0, .. }) => {
            assert!(!session_present, "session should be cleaned up when expiry=0");
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }
    disconnect(c2).await;
    Ok(())
}

/// Session Expiry > 0：clean=false 会话保留，重连 session_present=true
#[tokio::test]
async fn session_expiry_positive_keeps_session() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    // 首次连接：clean=false + session_expiry=3600
    let mut c1 = MqttClient::connect_v5(addr, "expiry-pos", false, Some(3600)).await?;
    let connack1 = c1.recv().await?;
    assert!(matches!(
        connack1,
        Packet::Connack(Connack { session_present: false, return_code: 0, .. })
    ));
    c1.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "expiry/pos".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await?;
    let _ = c1.recv().await?; // SUBACK
    disconnect(c1).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 重连：session_present 应为 true（会话在 expiry 窗口内保留）
    let mut c2 = MqttClient::connect_v5(addr, "expiry-pos", false, Some(3600)).await?;
    let connack2 = c2.recv().await?;
    match connack2 {
        Packet::Connack(Connack { session_present, return_code: 0, .. }) => {
            assert!(session_present, "session should be preserved within expiry window");
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }
    disconnect(c2).await;
    Ok(())
}

/// 验证 SUBACK 在 MQTT 5.0 下的返回码（与 3.1.1 兼容：0/1/2 表示接受）
#[tokio::test]
async fn mqtt5_subscribe_ack() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_server().await;

    let mut c = MqttClient::connect_v5(addr, "v5-sub", true, None).await?;
    let _ = c.recv().await?;

    c.send(Packet::Subscribe(Subscribe {
        packet_id: 42,
        topics: vec![SubscribeTopic {
            topic_filter: "v5/sub/test".into(),
            qos: QoS::ExactlyOnce,
        }],
    }))
    .await?;
    let suback = c.recv().await?;
    match suback {
        Packet::Suback(Suback { packet_id, return_codes }) => {
            assert_eq!(packet_id, 42);
            assert_eq!(return_codes, vec![2], "QoS2 subscription should be acked with code 2");
        }
        other => panic!("expected Suback, got {other:?}"),
    }
    disconnect(c).await;
    Ok(())
}
