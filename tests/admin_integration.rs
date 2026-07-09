//! Admin HTTP 运维 API 端到端集成测试
//!
//! 验证：
//! - /health、/metrics HTTP 端点真实响应
//! - /api/v1/sessions 查询真实 MQTT 客户端会话
//! - /api/v1/publish 手动发布消息 → MQTT 订阅者实际收到
//! - /api/v1/reload/security 热重载生效
//! - DELETE /api/v1/sessions/:client_id 清理会话后订阅者不再收到消息

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use lumenmq::admin::AdminServer;
use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{Connect, MqttCodec, Packet, Publish, QoS, Subscribe, SubscribeTopic, MQTT_3_1_1_LEVEL};
use lumenmq::config::{AuthConfig, AuthMode, BrokerConfig, Settings, SecurityConfig};
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

/// 启动 MQTT TCP + Admin HTTP 双服务
async fn spawn_all() -> (
    Arc<BrokerState>,
    std::net::SocketAddr, // mqtt addr
    std::net::SocketAddr, // admin addr
) {
    let broker = make_broker();

    // MQTT TCP（将 shutdown tx 移入 task 以避免 sender 提前 drop 导致 watch 通道关闭）
    let (mqtt_tx, mqtt_rx) = new_shutdown_channel();
    let mqtt_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt_addr = mqtt_listener.local_addr().unwrap();
    drop(mqtt_listener);
    let mqtt_server = TcpServer::new(mqtt_addr, broker.clone(), 100, mqtt_rx);
    tokio::spawn(async move {
        let _hold = mqtt_tx; // 保持 sender 存活，防止 watch 通道关闭触发 busy-loop
        let _ = mqtt_server.run().await;
    });

    // Admin HTTP（直接传入已绑定的 listener 避免 rebind 竞态；sender 移入 task 防止 axum graceful_shutdown 提前完成）
    let (admin_tx, admin_rx) = new_shutdown_channel();
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();
    let admin_server = AdminServer::new(admin_addr, broker.clone());
    tokio::spawn(async move {
        let _hold = admin_tx; // 保持 sender 存活，否则 rx.changed() 返回 Err 触发 axum 立即 graceful shutdown
        let _ = admin_server.run_with_listener(admin_listener, admin_rx).await;
    });

    // 给服务一点启动时间
    tokio::time::sleep(Duration::from_millis(100)).await;
    (broker, mqtt_addr, admin_addr)
}

struct MqttClient {
    sink: FramedWrite<tokio::io::WriteHalf<TcpStream>, MqttCodec>,
    stream: FramedRead<tokio::io::ReadHalf<TcpStream>, MqttCodec>,
}

impl MqttClient {
    async fn connect(addr: std::net::SocketAddr, client_id: &str) -> anyhow::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        let _ = socket.set_nodelay(true);
        let (r, w) = tokio::io::split(socket);
        let codec = MqttCodec::default();
        let mut sink = FramedWrite::new(w, codec.clone());
        let stream = FramedRead::new(r, codec);
        sink.send(Packet::Connect(Connect {
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
            _ => Ok(None),
        }
    }
}

async fn http_get(url: &str) -> anyhow::Result<(u16, String)> {
    let resp = reqwest::get(url).await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((status, text))
}

async fn http_post_json(url: &str, body: serde_json::Value) -> anyhow::Result<(u16, String)> {
    let client = reqwest::Client::new();
    let resp = client.post(url).json(&body).send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((status, text))
}

async fn http_delete(url: &str) -> anyhow::Result<(u16, String)> {
    let client = reqwest::Client::new();
    let resp = client.delete(url).send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    Ok((status, text))
}

#[tokio::test]
async fn admin_health_returns_broker_info() -> anyhow::Result<()> {
    let (_broker, _mqtt, admin) = spawn_all().await;
    let (status, body) = http_get(&format!("http://127.0.0.1:{}/health", admin.port())).await?;
    assert_eq!(status, 200);
    assert!(body.contains("\"status\":\"ok\""));
    assert!(body.contains("lumenmq@"));
    Ok(())
}

#[tokio::test]
async fn admin_metrics_returns_prometheus_format() -> anyhow::Result<()> {
    let (_broker, _mqtt, admin) = spawn_all().await;
    let (status, body) = http_get(&format!("http://127.0.0.1:{}/metrics", admin.port())).await?;
    assert_eq!(status, 200);
    assert!(body.contains("# HELP lumenmq_connections_total"));
    assert!(body.contains("# TYPE lumenmq_connections_total counter"));
    assert!(body.contains("lumenmq_connections_total "));
    assert!(body.contains("lumenmq_publish_qos0_total "));
    Ok(())
}

#[tokio::test]
async fn admin_list_sessions_shows_connected_client() -> anyhow::Result<()> {
    let (_broker, mqtt, admin) = spawn_all().await;

    // 连一个 MQTT 客户端
    let mut c = MqttClient::connect(mqtt, "admin-list-test").await?;
    let _ = c.recv().await?; // CONNACK

    let (status, body) =
        http_get(&format!("http://127.0.0.1:{}/api/v1/sessions", admin.port())).await?;
    assert_eq!(status, 200);
    assert!(body.contains("\"online\":1"), "body: {body}");
    assert!(body.contains("admin-list-test"), "body: {body}");

    let _ = c.send(Packet::Disconnect).await;
    Ok(())
}

#[tokio::test]
async fn admin_manual_publish_delivers_to_mqtt_subscriber() -> anyhow::Result<()> {
    let (_broker, mqtt, admin) = spawn_all().await;

    // MQTT 客户端订阅 admin/topic
    let mut sub = MqttClient::connect(mqtt, "admin-pub-sub").await?;
    let _ = sub.recv().await?; // CONNACK
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "admin/topic".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?; // SUBACK

    // 通过 Admin HTTP 发布消息
    let (status, body) = http_post_json(
        &format!("http://127.0.0.1:{}/api/v1/publish", admin.port()),
        serde_json::json!({
            "topic": "admin/topic",
            "payload": "hello-from-admin",
            "qos": 0,
            "retain": false
        }),
    )
    .await?;
    assert_eq!(status, 200);
    assert!(body.contains("\"ok\":true"));

    // MQTT 订阅者应收到
    let inbound = sub.recv_opt(Duration::from_millis(1000)).await?;
    match inbound {
        Some(Packet::Publish(Publish { topic, payload, .. })) => {
            assert_eq!(topic, "admin/topic");
            assert_eq!(&payload[..], b"hello-from-admin");
        }
        other => panic!("expected PUBLISH from admin, got {other:?}"),
    }

    let _ = sub.send(Packet::Disconnect).await;
    Ok(())
}

#[tokio::test]
async fn admin_delete_session_removes_subscriptions() -> anyhow::Result<()> {
    let (_broker, mqtt, admin) = spawn_all().await;

    // MQTT 客户端订阅 del/topic
    let mut sub = MqttClient::connect(mqtt, "del-target").await?;
    let _ = sub.recv().await?;
    sub.send(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic {
            topic_filter: "del/topic".into(),
            qos: QoS::AtMostOnce,
        }],
    }))
    .await?;
    let _ = sub.recv().await?;
    // 断开（clean=true 会话仍在 sessions map 直到 cleanup）
    let _ = sub.send(Packet::Disconnect).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 通过 Admin HTTP 删除该会话
    let (status, body) = http_delete(&format!(
        "http://127.0.0.1:{}/api/v1/sessions/del-target",
        admin.port()
    ))
    .await?;
    assert_eq!(status, 200);
    assert!(body.contains("\"ok\":true"));

    // 再次查询，该会话不应出现
    let (_status2, body2) =
        http_get(&format!("http://127.0.0.1:{}/api/v1/sessions", admin.port())).await?;
    assert!(!body2.contains("del-target"), "session should be removed: {body2}");
    Ok(())
}

#[tokio::test]
async fn admin_reload_security_takes_effect() -> anyhow::Result<()> {
    let (_broker, mqtt, admin) = spawn_all().await;

    // 先连一个客户端（security 默认 disabled，允许连接）
    let mut c = MqttClient::connect(mqtt, "reload-test").await?;
    let _ = c.recv().await?;
    let _ = c.send(Packet::Disconnect).await;

    // 热重载：启用 security，把 127.0.0.1 加入黑名单
    let new_cfg = SecurityConfig {
        enabled: true,
        ip_blacklist: vec!["127.0.0.1/32".into()],
        ip_whitelist: vec![],
        max_connections_per_ip: 0,
        publish_rate_per_second: 0,
        max_payload_bytes: 0,
    };
    let (status, body) = http_post_json(
        &format!("http://127.0.0.1:{}/api/v1/reload/security", admin.port()),
        serde_json::to_value(&new_cfg).unwrap(),
    )
    .await?;
    assert_eq!(status, 200, "reload body: {body}");
    assert!(body.contains("\"ok\":true"));

    // 新连接应被拒绝（TCP 连接成功但 MQTT 握手被 security 拒绝）
    // 注意：TcpServer 当前在 accept 阶段做 IP 过滤；连接可能直接被关闭
    let result = MqttClient::connect(mqtt, "should-be-blocked").await;
    // 连接可能成功（TCP 层）但 CONNACK 失败，或连接直接被关
    // 这里验证：要么连接失败，要么收不到正常 CONNACK
    match result {
        Ok(mut c) => {
            let connack = c.recv_opt(Duration::from_millis(500)).await?;
            // security 启用后 127.0.0.1 在黑名单，应被拒绝（无 CONNACK 或连接关闭）
            assert!(connack.is_none() || !matches!(connack, Some(Packet::Connack(_))),
                "blacklisted IP should not receive accepted CONNACK");
        }
        Err(_) => {
            // 连接直接失败也符合预期
        }
    }
    Ok(())
}
