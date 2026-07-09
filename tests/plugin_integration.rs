//! 消息插件中间件集成测试
//!
//! 通过真实 TCP socket 验证：
//! - 主题黑名单拒绝 PUBLISH（订阅者收不到）
//! - 主题黑名单拒绝 SUBSCRIBE（回 SUBACK FAILURE）
//! - 载荷黑名单拒绝 PUBLISH（订阅者收不到）
//! - HTTP 转发 hook 将匹配消息 POST 到外部端点
//! - 热更新：运行期修改规则即时生效

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{MqttCodec, Packet, Publish, QoS, MQTT_3_1_1_LEVEL};
use lumenmq::config::{
    AuthConfig, AuthMode, BrokerConfig, ForwardConfig, PayloadFilterConfig, PluginConfig,
    Settings, TopicAclConfig,
};
use lumenmq::net::{new_shutdown_channel, TcpServer};

// ---------- 辅助 ----------

fn make_broker_with_plugin(plugin: PluginConfig) -> Arc<BrokerState> {
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
    settings.plugin = plugin;
    let config = Arc::new(settings);
    let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
    BrokerState::new(config, auth)
}

async fn spawn_tcp_server(
    broker: Arc<BrokerState>,
) -> (std::net::SocketAddr, tokio::sync::watch::Sender<bool>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let (shutdown_tx, shutdown_rx) = new_shutdown_channel();
    let server = TcpServer::new(addr, broker, 100, shutdown_rx);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    (addr, shutdown_tx)
}

struct TcpMqttClient {
    sink: FramedWrite<tokio::io::WriteHalf<TcpStream>, MqttCodec>,
    stream: FramedRead<tokio::io::ReadHalf<TcpStream>, MqttCodec>,
}

impl TcpMqttClient {
    async fn connect_raw(addr: std::net::SocketAddr, client_id: &str) -> anyhow::Result<Self> {
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

    async fn connect_and_wait_connack(
        addr: std::net::SocketAddr,
        client_id: &str,
    ) -> anyhow::Result<Self> {
        let mut c = Self::connect_raw(addr, client_id).await?;
        let _ = c.recv().await?; // CONNACK
        Ok(c)
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

/// 构造一条 QoS0 PUBLISH
fn publish_qos0(topic: &str, payload: &[u8]) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.into(),
        packet_id: None,
        payload: bytes::Bytes::from(payload.to_vec()),
    })
}

// ---------- 测试用例 ----------

#[tokio::test]
async fn plugin_disabled_allows_all() -> anyhow::Result<()> {
    let broker = make_broker_with_plugin(PluginConfig::default());
    let (addr, tx) = spawn_tcp_server(broker).await;

    let mut sub = TcpMqttClient::connect_and_wait_connack(addr, "sub").await?;
    sub.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 1,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "any/#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    let mut pub_ = TcpMqttClient::connect_and_wait_connack(addr, "pub").await?;
    pub_.send(publish_qos0("any/topic", b"hello")).await?;

    // 应收到消息
    let got = tokio::time::timeout(Duration::from_secs(1), sub.recv()).await;
    assert!(got.is_ok(), "should receive message when plugin disabled");
    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn topic_blacklist_blocks_publish() -> anyhow::Result<()> {
    // 禁止发布到 cmd/#
    let mut plugin = PluginConfig::default();
    plugin.enabled = true;
    plugin.topic_acl = TopicAclConfig {
        publish_blacklist: vec!["cmd/#".into()],
        ..TopicAclConfig::default()
    };
    let broker = make_broker_with_plugin(plugin);
    let (addr, tx) = spawn_tcp_server(broker).await;

    let mut sub = TcpMqttClient::connect_and_wait_connack(addr, "sub").await?;
    sub.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 1,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    let mut pub_ = TcpMqttClient::connect_and_wait_connack(addr, "pub").await?;
    // 被黑名单拦截的主题：订阅者不应收到
    pub_.send(publish_qos0("cmd/reboot", b"bad")).await?;
    // 放行的主题：订阅者应收到
    pub_.send(publish_qos0("sensor/temp", b"good")).await?;

    // 仅应收到 sensor/temp
    let got = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await;
    match got {
        Ok(Ok(Packet::Publish(p))) => assert_eq!(p.topic, "sensor/temp"),
        _ => anyhow::bail!("expected sensor/temp message"),
    }
    // 不应再收到消息（cmd/reboot 被拦截）
    let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
    assert!(none.is_err(), "blacklisted topic should not be delivered");
    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn topic_blacklist_blocks_subscribe() -> anyhow::Result<()> {
    // 禁止订阅 internal/#
    let mut plugin = PluginConfig::default();
    plugin.enabled = true;
    plugin.topic_acl = TopicAclConfig {
        subscribe_blacklist: vec!["internal/#".into()],
        ..TopicAclConfig::default()
    };
    let broker = make_broker_with_plugin(plugin);
    let (addr, tx) = spawn_tcp_server(broker).await;

    let mut c = TcpMqttClient::connect_and_wait_connack(addr, "sub").await?;
    // 被禁的订阅：应回 FAILURE
    c.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 1,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "internal/secret".into(),
            qos: QoS::AtLeastOnce,
        }],
    }))
    .await?;
    let suback = c.recv().await?;
    match suback {
        Packet::Suback(s) => {
            assert_eq!(s.return_codes.len(), 1);
            assert_eq!(
                s.return_codes[0],
                lumenmq::codec::packet::suback_code::FAILURE
            );
        }
        _ => anyhow::bail!("expected SUBACK"),
    }
    // 放行的订阅：应回正常 QoS
    c.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 2,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "sensor/#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let suback2 = c.recv().await?;
    match suback2 {
        Packet::Suback(s) => {
            assert_eq!(s.return_codes.len(), 1);
            assert_eq!(s.return_codes[0], QoS::AtMostOnce.as_u8());
        }
        _ => anyhow::bail!("expected SUBACK"),
    }
    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn payload_blacklist_blocks_publish() -> anyhow::Result<()> {
    let mut plugin = PluginConfig::default();
    plugin.enabled = true;
    plugin.payload_filter = PayloadFilterConfig {
        enabled: true,
        blacklist_keywords: vec!["forbidden".into()],
        whitelist_keywords: vec![],
    };
    let broker = make_broker_with_plugin(plugin);
    let (addr, tx) = spawn_tcp_server(broker).await;

    let mut sub = TcpMqttClient::connect_and_wait_connack(addr, "sub").await?;
    sub.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 1,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "test/#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    let mut pub_ = TcpMqttClient::connect_and_wait_connack(addr, "pub").await?;
    // 含黑名单关键字：订阅者不应收到
    pub_.send(publish_qos0("test/a", b"this is forbidden data")).await?;
    // 干净载荷：订阅者应收到
    pub_.send(publish_qos0("test/b", b"clean data")).await?;

    let got = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await;
    match got {
        Ok(Ok(Packet::Publish(p))) => {
            assert_eq!(p.topic, "test/b");
            assert_eq!(&p.payload[..], b"clean data");
        }
        _ => anyhow::bail!("expected clean message"),
    }
    let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
    assert!(none.is_err(), "blacklisted payload should not be delivered");
    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn reload_plugin_updates_rules() -> anyhow::Result<()> {
    // 初始无限制
    let plugin = PluginConfig {
        enabled: true,
        ..PluginConfig::default()
    };
    let broker = make_broker_with_plugin(plugin);
    let (addr, tx) = spawn_tcp_server(broker.clone()).await;

    let mut sub = TcpMqttClient::connect_and_wait_connack(addr, "sub").await?;
    sub.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 1,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    let mut pub_ = TcpMqttClient::connect_and_wait_connack(addr, "pub").await?;
    // 初始可发布
    pub_.send(publish_qos0("cmd/x", b"before")).await?;
    let _ = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await?;

    // 热更新：加入黑名单
    let mut new_plugin = PluginConfig {
        enabled: true,
        ..PluginConfig::default()
    };
    new_plugin.topic_acl = TopicAclConfig {
        publish_blacklist: vec!["cmd/#".into()],
        ..TopicAclConfig::default()
    };
    broker.reload_plugin(&new_plugin)?;

    // 现在被拦截
    pub_.send(publish_qos0("cmd/y", b"after")).await?;
    let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
    assert!(none.is_err(), "blacklisted topic should be blocked after reload");
    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn http_forwarder_posts_matching_messages() -> anyhow::Result<()> {
    // 启动一个简易 HTTP 接收端点
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = listener.local_addr().unwrap();
    let received_count = Arc::new(AtomicU32::new(0));
    let received = received_count.clone();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => break,
            };
            // 读 HTTP 请求（简化处理：读到 EOF 或足够数据后回 200）
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = vec![0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf)).await;
            received.fetch_add(1, Ordering::SeqCst);
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
            let _ = sock.write_all(resp.as_bytes()).await;
        }
    });

    let mut plugin = PluginConfig::default();
    plugin.enabled = true;
    plugin.forward = ForwardConfig {
        enabled: true,
        url: format!("http://{http_addr}/mqtt"),
        topic_filter: "sensor/#".into(),
        timeout_secs: 2,
        max_queue: 64,
        allow_private_network: true,
    };
    let broker = make_broker_with_plugin(plugin);
    let (addr, tx) = spawn_tcp_server(broker).await;

    let mut pub_ = TcpMqttClient::connect_and_wait_connack(addr, "fwd-pub").await?;
    // 匹配 sensor/#：应转发
    pub_.send(publish_qos0("sensor/temp", b"25.5")).await?;
    pub_.send(publish_qos0("sensor/humidity", b"60")).await?;
    // 不匹配：不应转发
    pub_.send(publish_qos0("cmd/reboot", b"now")).await?;

    // 等待 HTTP 端点收到 2 条
    for _ in 0..20 {
        if received_count.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let count = received_count.load(Ordering::SeqCst);
    assert_eq!(count, 2, "should forward exactly 2 matching messages");
    let _ = tx.send(true);
    Ok(())
}

#[tokio::test]
async fn invalid_plugin_config_falls_back_to_disabled() -> anyhow::Result<()> {
    // forward.url 非法 scheme 应导致 forwarder 降级，但 broker 仍可启动
    let mut plugin = PluginConfig::default();
    plugin.enabled = true;
    plugin.forward = ForwardConfig {
        enabled: true,
        url: "ftp://invalid".into(),
        topic_filter: "".into(),
        timeout_secs: 1,
        max_queue: 16,
        allow_private_network: false,
    };
    let broker = make_broker_with_plugin(plugin);
    let (addr, tx) = spawn_tcp_server(broker).await;

    // broker 正常运行（forwarder 降级，主题 ACL 仍可用）
    let mut sub = TcpMqttClient::connect_and_wait_connack(addr, "sub").await?;
    sub.send(Packet::Subscribe(lumenmq::codec::Subscribe {
        packet_id: 1,
        topics: vec![lumenmq::codec::SubscribeTopic {
            topic_filter: "test/#".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    let mut pub_ = TcpMqttClient::connect_and_wait_connack(addr, "pub").await?;
    pub_.send(publish_qos0("test/a", b"data")).await?;
    let got = tokio::time::timeout(Duration::from_millis(500), sub.recv()).await;
    assert!(got.is_ok(), "broker should still route when forwarder disabled");
    let _ = tx.send(true);
    Ok(())
}
