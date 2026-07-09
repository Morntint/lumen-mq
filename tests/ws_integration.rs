//! WebSocket MQTT 接入集成测试
//!
//! 通过真实 WebSocket 连接（tokio-tungstenite 客户端）+ MqttCodec 验证：
//! - WS 握手 + MQTT CONNECT/CONNACK
//! - WS 上的订阅/发布端到端流转
//! - `mqtt` 子协议协商

use std::sync::Arc;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::codec::{Decoder, Encoder};

use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{
    Connack, Connect, MqttCodec, Packet, Publish, QoS, Subscribe, SubscribeTopic, MQTT_3_1_1_LEVEL,
};
use lumenmq::config::{AuthConfig, AuthMode, BrokerConfig, Settings};
use lumenmq::net::{new_shutdown_channel, WsServer};

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
        mode: AuthMode::Anonymous,
        allow_anonymous: true,
        users: vec![],
    };
    let config = Arc::new(settings);
    let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
    BrokerState::new(config, auth)
}

/// 启动一个绑定随机端口的 WsServer，返回 (broker, addr, shutdown_tx, join_handle)
async fn spawn_ws_server() -> (
    Arc<BrokerState>,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let broker = make_broker();
    let (shutdown_tx, shutdown_rx) = new_shutdown_channel();
    // 绑定 127.0.0.1:0 让 OS 分配端口
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = WsServer::new(addr, broker.clone(), 100, shutdown_rx, "/mqtt".into());
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, addr, shutdown_tx, handle)
}

/// 编码一条 MQTT 报文为字节
fn encode_packet(p: Packet) -> Vec<u8> {
    let mut codec = MqttCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(p, &mut buf).unwrap();
    buf.to_vec()
}

/// 从字节解码一条 MQTT 报文（若数据不足返回 None）
fn decode_packet(data: &[u8]) -> Option<Packet> {
    let mut codec = MqttCodec::default();
    let mut buf = BytesMut::from(data);
    codec.decode(&mut buf).unwrap()
}

fn make_connect(client_id: &str, clean: bool) -> Connect {
    Connect {
        protocol_level: MQTT_3_1_1_LEVEL,
        keep_alive: 60,
        client_id: client_id.into(),
        clean_session: clean,
        will: None,
        username: None,
        password: None,
        properties: None,
    }
}

/// 简化的 WS MQTT 客户端：在 WebSocket 上收发原始 MQTT 报文
type ClientWsStream = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

struct WsMqttClient {
    ws: ClientWsStream,
}

impl WsMqttClient {
    async fn connect(addr: std::net::SocketAddr, connect: Connect) -> anyhow::Result<Self> {
        let url = format!("ws://127.0.0.1:{}/mqtt", addr.port());
        let (ws, _resp) = tokio_tungstenite::connect_async(url).await?;
        let mut client = Self { ws };
        client.send_packet(Packet::Connect(connect)).await?;
        Ok(client)
    }

    async fn send_packet(&mut self, p: Packet) -> anyhow::Result<()> {
        self.ws.send(Message::Binary(encode_packet(p))).await?;
        Ok(())
    }

    async fn recv_packet(&mut self) -> anyhow::Result<Packet> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Binary(data))) => {
                    if let Some(p) = decode_packet(&data) {
                        return Ok(p);
                    }
                    // 数据不足或空帧：继续读
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) => anyhow::bail!("ws closed"),
                Some(Ok(Message::Text(_))) => anyhow::bail!("unexpected text frame"),
                Some(Ok(Message::Frame(_))) => continue,
                Some(Err(e)) => anyhow::bail!("ws error: {e}"),
                None => anyhow::bail!("ws stream ended"),
            }
        }
    }

    async fn close(mut self) {
        let _ = self.ws.close(None).await;
    }
}

#[tokio::test]
async fn websocket_mqtt_connect_and_subpub() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_ws_server().await;

    // 1. 订阅者：WS 连接 + CONNECT
    let mut sub = WsMqttClient::connect(addr, make_connect("ws-sub", true)).await?;
    let connack = sub.recv_packet().await?;
    assert!(matches!(
        connack,
        Packet::Connack(Connack { return_code: 0, session_present: false, .. })
    ));

    // 2. 订阅 a/b QoS1
    sub.send_packet(Packet::Subscribe(Subscribe {
        packet_id: 1,
        topics: vec![SubscribeTopic { topic_filter: "a/b".into(), qos: QoS::AtLeastOnce }],
    }))
    .await?;
    let suback = sub.recv_packet().await?;
    match suback {
        Packet::Suback(s) => {
            assert_eq!(s.packet_id, 1);
            assert_eq!(s.return_codes, vec![1]);
        }
        other => panic!("expected Suback, got {other:?}"),
    }

    // 3. 发布者：另一个 WS 连接，发 QoS1 PUBLISH
    let mut pub_ = WsMqttClient::connect(addr, make_connect("ws-pub", true)).await?;
    let _ = pub_.recv_packet().await?; // CONNACK
    pub_.send_packet(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "a/b".into(),
        packet_id: Some(50),
        payload: b"hello-ws".to_vec(),
    }))
    .await?;
    // 发布者应收到 PUBACK
    let puback = pub_.recv_packet().await?;
    assert!(matches!(puback, Packet::Puback(50)));

    // 4. 订阅者应收到 PUBLISH
    let inbound = sub.recv_packet().await?;
    match inbound {
        Packet::Publish(Publish { topic, payload, qos, .. }) => {
            assert_eq!(topic, "a/b");
            assert_eq!(payload, b"hello-ws");
            assert_eq!(qos, QoS::AtLeastOnce);
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }

    sub.close().await;
    pub_.close().await;
    Ok(())
}

#[tokio::test]
async fn websocket_subprotocol_negotiated() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_ws_server().await;

    // 客户端带上 Sec-WebSocket-Protocol: mqtt
    let url = format!("ws://127.0.0.1:{}/mqtt", addr.port());
    let mut req = url.into_client_request()?;
    req.headers_mut()
        .insert("sec-websocket-protocol", "mqtt".parse().unwrap());

    let (_ws, resp) = tokio_tungstenite::connect_async(req).await?;

    // 服务端应回选 mqtt 子协议
    let negotiated = resp
        .headers()
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok());
    assert_eq!(negotiated, Some("mqtt"), "server must echo back 'mqtt' subprotocol");
    Ok(())
}

#[tokio::test]
async fn websocket_qos1_publish_ack() -> anyhow::Result<()> {
    let (_broker, addr, _tx, _handle) = spawn_ws_server().await;

    // 单客户端发 QoS1 PUBLISH，应收到 PUBACK
    let mut c = WsMqttClient::connect(addr, make_connect("ws-qos1", true)).await?;
    let _ = c.recv_packet().await?; // CONNACK

    c.send_packet(Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "qos1/test".into(),
        packet_id: Some(777),
        payload: b"qos1-payload".to_vec(),
    }))
    .await?;
    let ack = c.recv_packet().await?;
    assert!(matches!(ack, Packet::Puback(777)), "expected PUBACK(777), got {ack:?}");

    c.close().await;
    Ok(())
}
