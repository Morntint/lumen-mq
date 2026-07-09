//! 阶段二/三端到端集成测试
//!
//! 通过真实 TCP 连接 + MqttCodec 验证：
//! - QoS2 完整四步握手（入站去重 + 出站 inflight 推进）
//! - Retain 保留消息：发布后新订阅者订阅即收到
//! - CleanSession=false 离线消息缓存与重连恢复
//! - 遗嘱消息触发
//! - 阶段三：sled 持久化（broker 重启后恢复会话订阅 + 离线消息 + retained）

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{
    Connack, Connect, ConnectFlags, LastWill, MqttCodec, Packet, Publish, QoS, Subscribe,
    SubscribeTopic, Suback, Unsubscribe, MQTT_3_1_1_LEVEL,
};
use lumenmq::config::{AuthConfig, BrokerConfig, Settings, StorageConfig};
use lumenmq::net::{new_shutdown_channel, TcpServer};

/// 构造测试用 BrokerState：匿名鉴权 + 小容量 inflight
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
        mode: lumenmq::config::AuthMode::Anonymous,
        allow_anonymous: true,
        users: vec![],
    };
    settings.storage.max_offline_messages = 100;
    settings.storage.offline_message_ttl = 3600;
    let config = Arc::new(settings);
    let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
    BrokerState::new(config, auth)
}

/// 构造开启 sled 持久化的 BrokerState，数据写入指定 path
fn make_broker_with_storage(path: std::path::PathBuf) -> Arc<BrokerState> {
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
        mode: lumenmq::config::AuthMode::Anonymous,
        allow_anonymous: true,
        users: vec![],
    };
    settings.storage = StorageConfig {
        enabled: true,
        path,
        max_offline_messages: 100,
        offline_message_ttl: 3600,
    };
    let config = Arc::new(settings);
    let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
    BrokerState::new(config, auth)
}

/// 启动一个绑定随机端口的 TcpServer，返回 (broker, addr, shutdown_tx)
async fn spawn_server() -> (
    Arc<BrokerState>,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
) {
    let broker = make_broker();
    let (b, addr, tx, _handle) = spawn_server_with(broker).await;
    (b, addr, tx)
}

/// 用指定 broker 启动 TcpServer，返回 (broker, addr, shutdown_tx, join_handle)
/// join_handle 用于等待 accept 循环退出，确保 broker 的 Arc 被 drop
async fn spawn_server_with(
    broker: Arc<BrokerState>,
) -> (
    Arc<BrokerState>,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let (shutdown_tx, shutdown_rx) = new_shutdown_channel();
    // 绑定 127.0.0.1:0 让 OS 分配端口
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = TcpServer::new(addr, broker.clone(), 100, shutdown_rx);
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, addr, shutdown_tx, handle)
}

/// 一个简化的 MQTT 客户端：编解码 + 收发
struct MqttClient {
    sink: FramedWrite<tokio::io::WriteHalf<TcpStream>, MqttCodec>,
    stream: FramedRead<tokio::io::ReadHalf<TcpStream>, MqttCodec>,
}

impl MqttClient {
    async fn connect(addr: std::net::SocketAddr, connect: Connect) -> anyhow::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        let _ = socket.set_nodelay(true);
        let (r, w) = tokio::io::split(socket);
        let codec = MqttCodec::default();
        let mut sink = FramedWrite::new(w, codec.clone());
        let stream = FramedRead::new(r, codec);
        sink.send(Packet::Connect(connect)).await?;
        Ok(Self { sink, stream })
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

fn make_connect(client_id: &str, clean: bool, will: Option<LastWill>) -> Connect {
    Connect {
        protocol_level: MQTT_3_1_1_LEVEL,
        keep_alive: 60,
        client_id: client_id.into(),
        clean_session: clean,
        will,
        username: None,
        password: None,
        properties: None,
    }
}

/// 主动 DISCONNECT 并关闭
async fn disconnect(mut c: MqttClient) {
    let _ = c.send(Packet::Disconnect).await;
}

#[tokio::test]
async fn qos2_full_handshake_inbound_and_outbound() -> anyhow::Result<()> {
    let (_broker, addr, _tx) = spawn_server().await;

    // 1. 订阅者：clean=true，订阅 a/b QoS2
    let mut sub = MqttClient::connect(addr, make_connect("sub-qos2", true, None)).await?;
    let connack = sub.recv().await?;
    assert!(matches!(connack, Packet::Connack(Connack { return_code: 0, .. })));
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "a/b".into(), qos: QoS::ExactlyOnce }],
    })).await?;
    let suback = sub.recv().await?;
    match suback {
        Packet::Suback(Suback { packet_id: 1, return_codes }) => {
            assert_eq!(return_codes, vec![2]);
        }
        other => panic!("expected Suback, got {other:?}"),
    }

    // 2. 发布者：发送 QoS2 PUBLISH packet_id=100
    let mut pub_ = MqttClient::connect(addr, make_connect("pub-qos2", true, None)).await?;
    let _ = pub_.recv().await?;
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::ExactlyOnce,
        retain: false,
        topic: "a/b".into(),
        packet_id: Some(100),
        payload: bytes::Bytes::from_static(b"hello-qos2"),
    })).await?;

    // 3. 发布者应收到 PUBREC → 发 PUBREL → 收 PUBCOMP
    let pubrec = pub_.recv().await?;
    assert!(matches!(pubrec, Packet::Pubrec(100)));
    pub_.send(Packet::Pubrel(100)).await?;
    let pubcomp = pub_.recv().await?;
    assert!(matches!(pubcomp, Packet::Pubcomp(100)));

    // 4. 订阅者应收到 QoS2 PUBLISH → 回 PUBREC → 收 PUBREL → 回 PUBCOMP
    let inbound = sub.recv().await?;
    let delivered_pid = match inbound {
        Packet::Publish(Publish { qos, packet_id, payload, topic, .. }) => {
            assert_eq!(qos, QoS::ExactlyOnce);
            assert_eq!(topic, "a/b");
            assert_eq!(&payload[..], b"hello-qos2");
            packet_id.unwrap()
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    };
    sub.send(Packet::Pubrec(delivered_pid)).await?;
    let pubrel = sub.recv().await?;
    assert!(matches!(pubrel, Packet::Pubrel(pid) if pid == delivered_pid));
    sub.send(Packet::Pubcomp(delivered_pid)).await?;

    disconnect(sub).await;
    disconnect(pub_).await;
    Ok(())
}

#[tokio::test]
async fn qos2_inbound_duplicate_dropped() -> anyhow::Result<()> {
    let (_broker, addr, _tx) = spawn_server().await;

    let mut sub = MqttClient::connect(addr, make_connect("sub-dup", true, None)).await?;
    let _ = sub.recv().await?;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "dup/+".into(), qos: QoS::ExactlyOnce }],
    })).await?;
    let _ = sub.recv().await?;

    let mut pub_ = MqttClient::connect(addr, make_connect("pub-dup", true, None)).await?;
    let _ = pub_.recv().await?;

    // 同一 packet_id 发两次（模拟 DUP 重发）
    for _ in 0..2 {
        pub_.send(Packet::Publish(Publish {
            dup: false,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "dup/x".into(),
            packet_id: Some(200),
            payload: bytes::Bytes::from_static(b"dup"),
        })).await?;
        // 每次都应收到 PUBREC
        let pubrec = pub_.recv().await?;
        assert!(matches!(pubrec, Packet::Pubrec(200)));
    }

    // 订阅者应只收到一次 PUBLISH
    let first = sub.recv().await?;
    let pid = match first {
        Packet::Publish(Publish { packet_id, .. }) => packet_id.unwrap(),
        other => panic!("expected PUBLISH, got {other:?}"),
    };
    sub.send(Packet::Pubrec(pid)).await?;
    let _ = sub.recv().await; // PUBREL
    sub.send(Packet::Pubcomp(pid)).await?;

    // 短时间内不应再收到
    let next = sub.recv_opt(Duration::from_millis(300)).await?;
    assert!(next.is_none(), "duplicate QoS2 should not be redelivered to subscriber");

    Ok(())
}

#[tokio::test]
async fn retain_message_delivered_to_new_subscriber() -> anyhow::Result<()> {
    let (_broker, addr, _tx) = spawn_server().await;

    // 1. 发布者发一条 retained 消息（无订阅者）
    let mut pub_ = MqttClient::connect(addr, make_connect("pub-retain", true, None)).await?;
    let _ = pub_.recv().await?;
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: true,
        topic: "sensor/ret".into(),
        packet_id: Some(10),
        payload: bytes::Bytes::from_static(b"retained-value"),
    })).await?;
    let puback = pub_.recv().await?;
    assert!(matches!(puback, Packet::Puback(10)));

    // 2. 新订阅者上线并订阅 → 应立即收到 retained
    let mut sub = MqttClient::connect(addr, make_connect("sub-retain", true, None)).await?;
    let _ = sub.recv().await?;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "sensor/+".into(), qos: QoS::AtLeastOnce }],
    })).await?;
    // 先 SUBACK，再 retained PUBLISH
    let suback = sub.recv().await?;
    assert!(matches!(suback, Packet::Suback(_)));
    let retained = sub.recv().await?;
    match retained {
        Packet::Publish(Publish { topic, payload, retain, qos, .. }) => {
            assert_eq!(topic, "sensor/ret");
            assert_eq!(&payload[..], b"retained-value");
            assert!(retain, "retained delivery must set retain=1");
            assert_eq!(qos, QoS::AtLeastOnce);
        }
        other => panic!("expected retained PUBLISH, got {other:?}"),
    }
    // 回 PUBACK
    sub.send(Packet::Puback(1)).await?; // 注意 packet_id 由 broker 分配，可能是 1
    Ok(())
}

#[tokio::test]
async fn offline_message_replayed_on_reconnect() -> anyhow::Result<()> {
    let (_broker, addr, _tx) = spawn_server().await;

    // 1. 订阅者 clean=false 上线订阅
    let mut sub = MqttClient::connect(addr, make_connect("persistent-sub", false, None)).await?;
    let connack = sub.recv().await?;
    // 首次连接 session_present=false
    assert!(matches!(connack, Packet::Connack(Connack { session_present: false, return_code: 0, .. })));
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "off/+".into(), qos: QoS::AtLeastOnce }],
    })).await?;
    let _ = sub.recv().await?;

    // 2. 主动断开（clean=false，会话保留）
    disconnect(sub).await;
    // 等待服务端处理断开
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. 发布者发消息（订阅者已离线）
    let mut pub_ = MqttClient::connect(addr, make_connect("pub-off", true, None)).await?;
    let _ = pub_.recv().await?;
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "off/msg".into(),
        packet_id: Some(50),
        payload: bytes::Bytes::from_static(b"offline-msg"),
    })).await?;
    let _ = pub_.recv().await; // PUBACK

    // 4. 订阅者重连 clean=false → 应取回离线消息
    let mut sub2 = MqttClient::connect(addr, make_connect("persistent-sub", false, None)).await?;
    let connack = sub2.recv().await?;
    // 重连应 session_present=true
    match connack {
        Packet::Connack(Connack { session_present, return_code, .. }) => {
            assert!(session_present, "session_present must be true on reconnect");
            assert_eq!(return_code, 0);
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }
    // 应收到之前入队的离线 PUBLISH
    let replayed = sub2.recv().await?;
    match replayed {
        Packet::Publish(Publish { topic, payload, qos, .. }) => {
            assert_eq!(topic, "off/msg");
            assert_eq!(&payload[..], b"offline-msg");
            assert_eq!(qos, QoS::AtLeastOnce);
        }
        other => panic!("expected replayed PUBLISH, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn last_will_fired_on_abnormal_disconnect() -> anyhow::Result<()> {
    let (_broker, addr, _tx) = spawn_server().await;

    // 1. 受遗嘱客户端：携带遗嘱消息
    let will = LastWill {
        topic: "will/topic".into(),
        message: b"client-dead".to_vec(),
        qos: QoS::AtMostOnce,
        retain: false,
    };
    let mut willer = MqttClient::connect(addr, make_connect("willer", true, Some(will))).await?;
    let _ = willer.recv().await?;

    // 2. 订阅者监听 will/topic
    let mut sub = MqttClient::connect(addr, make_connect("sub-will", true, None)).await?;
    let _ = sub.recv().await?;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "will/#".into(), qos: QoS::AtMostOnce }],
    })).await?;
    let _ = sub.recv().await?;

    // 3. 模拟异常断开：直接 drop 连接（不发 DISCONNECT）
    // 关闭底层 TCP 即可触发
    drop(willer);

    // 4. 订阅者应在合理时间内收到遗嘱消息
    let will_msg = sub.recv_opt(Duration::from_secs(2)).await?;
    match will_msg {
        Some(Packet::Publish(Publish { topic, payload, .. })) => {
            assert_eq!(topic, "will/topic");
            assert_eq!(&payload[..], b"client-dead");
        }
        other => panic!("expected will PUBLISH, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn disconnect_does_not_fire_will() -> anyhow::Result<()> {
    let (_broker, addr, _tx) = spawn_server().await;

    let will = LastWill {
        topic: "will/skip".into(),
        message: b"should-not-fire".to_vec(),
        qos: QoS::AtMostOnce,
        retain: false,
    };
    let mut willer = MqttClient::connect(addr, make_connect("willer-clean", true, Some(will))).await?;
    let _ = willer.recv().await?;

    let mut sub = MqttClient::connect(addr, make_connect("sub-skip", true, None)).await?;
    let _ = sub.recv().await?;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "will/#".into(), qos: QoS::AtMostOnce }],
    })).await?;
    let _ = sub.recv().await?;

    // 主动 DISCONNECT → 不应触发遗嘱
    disconnect(willer).await;

    let will_msg = sub.recv_opt(Duration::from_millis(500)).await?;
    assert!(will_msg.is_none(), "active DISCONNECT must not fire will");

    // 抑制未使用 warning
    let _ = ConnectFlags::CLEAN_SESSION;
    let _ = Unsubscribe { packet_id: 1, topic_filters: vec!["x".into()] };
    let _ = BytesMut::new();
    // encoder/decoder 必须在作用域内被引用以满足 trait bound
    let _ = MqttCodec::default();
    Ok(())
}

#[tokio::test]
async fn persistence_survives_broker_restart() -> anyhow::Result<()> {
    // 使用 tempdir 隔离 sled 数据
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_path_buf();

    // ===== 第一阶段：开启持久化的 broker =====
    let broker1 = make_broker_with_storage(path.clone());
    let (_b1, addr1, shutdown_tx1, handle1) = spawn_server_with(broker1.clone()).await;

    // 1) 持久订阅者 clean=false 上线订阅
    let mut sub = MqttClient::connect(addr1, make_connect("persist-sub", false, None)).await?;
    let connack = sub.recv().await?;
    assert!(matches!(connack, Packet::Connack(Connack { session_present: false, return_code: 0, .. })));
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "persist/+".into(), qos: QoS::AtLeastOnce }],
    })).await?;
    let _ = sub.recv().await?; // SUBACK

    // 2) 主动 DISCONNECT（clean=false → 会话与订阅持久化到磁盘）
    disconnect(sub).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 3) 发布者发一条 retained 消息到 persist/topic
    //    订阅者已离线 → 进入 offline 队列（内存 + 磁盘）
    //    retained 同时落盘到 retained Tree
    let mut pub_ = MqttClient::connect(addr1, make_connect("persist-pub", true, None)).await?;
    let _ = pub_.recv().await?;
    pub_.send(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: true,
        topic: "persist/topic".into(),
        packet_id: Some(700),
        payload: bytes::Bytes::from_static(b"retained-persist"),
    })).await?;
    let _ = pub_.recv().await?; // PUBACK
    disconnect(pub_).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 4) 关闭第一个 broker（模拟重启）：发 shutdown + 等 accept 循环退出 + drop 所有 Arc
    let _ = shutdown_tx1.send(true);
    // 等 accept 循环退出（它会 drop 内部的 broker Arc）
    let _ = tokio::time::timeout(Duration::from_secs(2), handle1).await;
    drop(_b1);
    drop(broker1);
    // 给操作系统一点时间释放 sled 文件锁
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ===== 第二阶段：用同一个 sled 路径重新创建 broker =====
    let broker2 = make_broker_with_storage(path.clone());
    let (_b2, addr2, _shutdown_tx2, _handle2) = spawn_server_with(broker2.clone()).await;

    // 5) 持久订阅者重连 clean=false → 应 session_present=true，并收到回放的离线 PUBLISH
    let mut sub2 = MqttClient::connect(addr2, make_connect("persist-sub", false, None)).await?;
    let connack = sub2.recv().await?;
    match connack {
        Packet::Connack(Connack { session_present, return_code, .. }) => {
            assert!(session_present, "session must be present after restart (recovered from disk)");
            assert_eq!(return_code, 0);
        }
        other => panic!("expected CONNACK, got {other:?}"),
    }

    // 6) 应收到回放的离线 retained 消息（订阅在重启后已恢复，无需重新订阅）
    let replayed = sub2.recv_opt(Duration::from_secs(2)).await?;
    match replayed {
        Some(Packet::Publish(Publish { topic, payload, .. })) => {
            assert_eq!(topic, "persist/topic");
            assert_eq!(&payload[..], b"retained-persist");
        }
        other => panic!("expected replayed offline PUBLISH, got {other:?}"),
    }

    // 7) 一个全新订阅者订阅 persist/+ → 应立即收到磁盘上恢复的 retained 消息
    let mut new_sub = MqttClient::connect(addr2, make_connect("persist-new", true, None)).await?;
    let _ = new_sub.recv().await?;
    new_sub.send(Packet::Subscribe(Subscribe {
        packet_id: 2,
        topics: vec![SubscribeTopic { topic_filter: "persist/+".into(), qos: QoS::AtLeastOnce }],
    })).await?;
    let _ = new_sub.recv().await?; // SUBACK
    let retained = new_sub.recv_opt(Duration::from_secs(2)).await?;
    match retained {
        Some(Packet::Publish(Publish { topic, payload, retain, .. })) => {
            assert_eq!(topic, "persist/topic");
            assert_eq!(&payload[..], b"retained-persist");
            assert!(retain, "retained delivery must set retain=1");
        }
        other => panic!("expected retained PUBLISH after restart, got {other:?}"),
    }

    Ok(())
}
