//! TLS 加密层（基于 tokio-rustls / rustls 0.23）
//!
//! 提供能力：
//! - `load_tls_acceptor`：从配置加载证书/私钥/CA，构造 `tokio_rustls::TlsAcceptor`
//! - `TlsServer`：监听 TCP，完成 TLS 握手后把内层流交给 `BrokerState::handle_connection`
//!
//! 支持两种模式：
//! - 单向加密（仅服务端证书）：`mutual = false`
//! - 双向证书认证（客户端必须提供由 CA 签发的证书）：`mutual = true`

use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use crate::broker::BrokerState;
use crate::config::TlsConfig;
use crate::utils::{BrokerError, BrokerResult};

/// 安装 rustls ring 加密后端为进程默认（多次调用安全，仅首次生效）
fn install_default_crypto_provider() {
    // install_default 返回 Err 表示已被安装（正常）或安装失败（异常）
    if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
        // 已安装是预期行为（多模块初始化），仅对真正的安装失败告警
        // rustls 的 CryptoProvider::install_default 已安装时返回 Err 但不携带具体原因
        debug!(error = ?e, "rustls crypto provider already installed or install failed (may be harmless if already installed)");
    }
}

/// 从 PEM 文件读取证书链
fn load_certs(path: &Path) -> BrokerResult<Vec<CertificateDer<'static>>> {
    let mut reader = BufReader::new(
        std::fs::File::open(path)
            .map_err(|e| BrokerError::Storage(format!("open cert file {}: {e}", path.display())))?,
    );
    certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| BrokerError::Storage(format!("read certs from {}: {e}", path.display())))
}

/// 从 PEM 文件读取私钥（自动识别 PKCS8 / PKCS1 / SEC1）
fn load_private_key(path: &Path) -> BrokerResult<PrivateKeyDer<'static>> {
    let mut reader = BufReader::new(
        std::fs::File::open(path)
            .map_err(|e| BrokerError::Storage(format!("open key file {}: {e}", path.display())))?,
    );
    private_key(&mut reader)
        .map_err(|e| BrokerError::Storage(format!("read private key from {}: {e}", path.display())))?
        .ok_or_else(|| BrokerError::Storage(format!("no private key found in {}", path.display())))
}

/// 从 CA PEM 文件加载根证书到 RootCertStore
fn load_ca_store(path: &Path) -> BrokerResult<RootCertStore> {
    let certs = load_certs(path)?;
    let mut store = RootCertStore::empty();
    for c in certs {
        store
            .add(c)
            .map_err(|e| BrokerError::Storage(format!("add CA cert: {e}")))?;
    }
    if store.is_empty() {
        return Err(BrokerError::Storage(format!(
            "no CA certs loaded from {}",
            path.display()
        )));
    }
    Ok(store)
}

/// 从配置构造 TLS Acceptor
///
/// - `mutual = false`：单向 TLS，仅服务端出示证书
/// - `mutual = true`：双向认证，客户端必须提供由 `ca` 签发的证书
pub fn load_tls_acceptor(cfg: &TlsConfig) -> BrokerResult<TlsAcceptor> {
    install_default_crypto_provider();

    let certs = load_certs(&cfg.cert)?;
    let key = load_private_key(&cfg.key)?;

    let builder = ServerConfig::builder();
    let server_config = if cfg.mutual {
        // 双向认证：要求客户端证书
        let ca_store = load_ca_store(&cfg.ca)?;
        let verifier = WebPkiClientVerifier::builder(ca_store.into())
            .build()
            .map_err(|e| BrokerError::Storage(format!("build client verifier: {e}")))?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| BrokerError::Storage(format!("build server config (mutual): {e}")))?
    } else {
        // 单向 TLS
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| BrokerError::Storage(format!("build server config (one-way): {e}")))?
    };

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// MQTT over TLS 服务
pub struct TlsServer {
    bind: SocketAddr,
    broker: Arc<BrokerState>,
    max_connections: usize,
    shutdown: watch::Receiver<bool>,
    acceptor: TlsAcceptor,
}

impl TlsServer {
    pub fn new(
        bind: SocketAddr,
        broker: Arc<BrokerState>,
        max_connections: usize,
        shutdown: watch::Receiver<bool>,
        acceptor: TlsAcceptor,
    ) -> Self {
        Self { bind, broker, max_connections, shutdown, acceptor }
    }

    /// 启动并阻塞运行，直到收到关闭信号
    pub async fn run(mut self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.bind).await?;
        info!(addr = %self.bind, mutual = self.broker.config().tls.mutual, "TLS MQTT listener started");

        loop {
            tokio::select! {
                biased;
                res = self.shutdown.changed() => {
                    if res.is_ok() && *self.shutdown.borrow() {
                        info!("TLS server shutting down");
                        break;
                    }
                }
                accept = listener.accept() => {
                    let (socket, peer) = match accept {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "TLS accept failed");
                            continue;
                        }
                    };
                    let current = self.broker.metrics().connections_current
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if current as usize >= self.max_connections {
                        warn!(peer = %peer, current, max = self.max_connections, "max connections reached, refusing TLS");
                        drop(socket);
                        continue;
                    }

                    let _ = socket.set_nodelay(true);
                    let acceptor = self.acceptor.clone();
                    let broker = self.broker.clone();
                    let shutdown_rx = self.shutdown.clone();

                    tokio::spawn(async move {
                        // 先完成 TLS 握手
                        match acceptor.accept(socket).await {
                            Ok(tls_stream) => {
                                let _ = broker
                                    .handle_connection(tls_stream, peer, shutdown_rx)
                                    .await;
                            }
                            Err(e) => {
                                warn!(error = %e, %peer, "TLS handshake failed");
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }
}
