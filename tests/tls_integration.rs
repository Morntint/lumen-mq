//! 阶段三 TLS 集成测试
//!
//! 使用 rcgen 动态生成自签 CA + 服务器证书，启动 TlsServer，
//! 客户端用 tokio-rustls 完成 TLS 握手后走 MQTT CONNECT/CONNACK 流程。
//! 覆盖：
//! - 单向 TLS（服务端证书，客户端校验）
//! - load_tls_acceptor 错误路径（证书文件缺失/非法）

#![allow(clippy::field_reassign_with_default)]

use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_util::codec::{FramedRead, FramedWrite};

use lumenmq::broker::{Authenticator, BrokerState};
use lumenmq::codec::{Connack, Connect, MqttCodec, Packet, MQTT_3_1_1_LEVEL};
use lumenmq::config::{AuthConfig, BrokerConfig, Settings, StorageConfig, TlsConfig};
use lumenmq::net::{load_tls_acceptor, new_shutdown_channel, TlsServer};

/// 生成测试用 CA + 服务器证书（PEM 文本）
struct TestCerts {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
}

fn generate_test_certs() -> anyhow::Result<TestCerts> {
    // CA
    let mut ca_params = CertificateParams::new(vec![])?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name.push(DnType::CommonName, "LumenMQ Test CA");
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca_params.self_signed(&ca_key)?;

    // Server cert (signed by CA, SAN=localhost)
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()])?;
    server_params.distinguished_name.push(DnType::CommonName, "localhost");
    let server_key = KeyPair::generate()?;
    let server_cert = server_params.signed_by(&server_key, &ca_cert, &ca_key)?;

    Ok(TestCerts {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
    })
}

fn write_pem(dir: &std::path::Path, name: &str, pem: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, pem).unwrap();
    path
}

/// 构造开启 TLS 的 BrokerState（匿名鉴权）
fn make_tls_broker() -> Arc<BrokerState> {
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
        enabled: false,
        ..StorageConfig::default()
    };
    let config = Arc::new(settings);
    let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
    BrokerState::new(config, auth)
}

/// 启动 TlsServer（绑定随机端口），返回 (broker, addr, shutdown_tx, handle)
async fn spawn_tls_server(
    tls_cfg: TlsConfig,
) -> (
    Arc<BrokerState>,
    std::net::SocketAddr,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let broker = make_tls_broker();
    let acceptor = load_tls_acceptor(&tls_cfg).expect("load_tls_acceptor failed");
    let (shutdown_tx, shutdown_rx) = new_shutdown_channel();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = TlsServer::new(addr, broker.clone(), 100, shutdown_rx, acceptor);
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    (broker, addr, shutdown_tx, handle)
}

/// 构造信任 TestCerts.ca 的 TlsConnector
fn make_client_connector(ca_pem: &str) -> anyhow::Result<TlsConnector> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut root_store = rustls::RootCertStore::empty();
    let mut reader = BufReader::new(ca_pem.as_bytes());
    for cert in rustls_pemfile::certs(&mut reader) {
        root_store.add(cert?)?;
    }
    let cfg = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(cfg)))
}

fn make_connect(client_id: &str) -> Connect {
    Connect {
        protocol_level: MQTT_3_1_1_LEVEL,
        keep_alive: 60,
        client_id: client_id.into(),
        clean_session: true,
        will: None,
        username: None,
        password: None,
        properties: None,
    }
}

#[tokio::test]
async fn tls_one_way_mqtt_connect() -> anyhow::Result<()> {
    let certs = generate_test_certs()?;
    let dir = tempfile::tempdir()?;
    let cert_path = write_pem(dir.path(), "server.crt", &certs.server_cert_pem);
    let key_path = write_pem(dir.path(), "server.key", &certs.server_key_pem);

    let tls_cfg = TlsConfig {
        enabled: true,
        bind: "127.0.0.1:0".into(),
        cert: cert_path,
        key: key_path,
        ca: std::path::PathBuf::new(), // 单向 TLS 不需要 CA
        mutual: false,
    };

    let (_broker, addr, shutdown_tx, handle) = spawn_tls_server(tls_cfg).await;

    // 客户端：用信任 CA 的 connector 连接
    let connector = make_client_connector(&certs.ca_pem)?;
    let tcp = TcpStream::connect(addr).await?;
    let _ = tcp.set_nodelay(true);
    let server_name = rustls::pki_types::ServerName::try_from("localhost")?;
    let tls_stream = connector.connect(server_name, tcp).await?;

    // MQTT CONNECT / CONNACK over TLS
    let (r, w) = tokio::io::split(tls_stream);
    let codec = MqttCodec::default();
    let mut sink = FramedWrite::new(w, codec.clone());
    let mut stream = FramedRead::new(r, codec);

    sink.send(Packet::Connect(make_connect("tls-client"))).await?;
    let connack = stream.next().await;
    match connack {
        Some(Ok(Packet::Connack(Connack { return_code: 0, .. }))) => {}
        other => panic!("expected CONNACK(0), got {other:?}"),
    }

    // 清理
    let _ = sink.send(Packet::Disconnect).await;
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    Ok(())
}

#[tokio::test]
async fn tls_acceptor_rejects_missing_cert_file() -> anyhow::Result<()> {
    let tls_cfg = TlsConfig {
        enabled: true,
        bind: "127.0.0.1:0".into(),
        cert: std::path::PathBuf::from("/nonexistent/cert.pem"),
        key: std::path::PathBuf::from("/nonexistent/key.pem"),
        ca: std::path::PathBuf::new(),
        mutual: false,
    };
    let result = load_tls_acceptor(&tls_cfg);
    assert!(result.is_err(), "load_tls_acceptor must fail with missing cert files");
    Ok(())
}

#[tokio::test]
async fn tls_acceptor_rejects_invalid_pem() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let garbage_path = write_pem(dir.path(), "garbage.pem", "not a pem file");
    let tls_cfg = TlsConfig {
        enabled: true,
        bind: "127.0.0.1:0".into(),
        cert: garbage_path.clone(),
        key: garbage_path,
        ca: std::path::PathBuf::new(),
        mutual: false,
    };
    let result = load_tls_acceptor(&tls_cfg);
    assert!(result.is_err(), "load_tls_acceptor must fail with invalid PEM");
    Ok(())
}
