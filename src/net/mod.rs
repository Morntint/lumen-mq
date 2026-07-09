pub mod conn_context;
pub mod heartbeat;
pub mod mqtt_sn;
pub mod tcp_server;
pub mod tls_layer;
pub mod ws_server;

pub use conn_context::ConnContext;
pub use heartbeat::HeartbeatTimer;
pub use mqtt_sn::MqttSnServer;
pub use tcp_server::TcpServer;
pub use tls_layer::{load_tls_acceptor, TlsServer};
pub use ws_server::{WsServer, WsStream};

/// 关闭信号通道（watch<bool>）
pub type ShutdownRx = tokio::sync::watch::Receiver<bool>;
pub type ShutdownTx = tokio::sync::watch::Sender<bool>;

pub fn new_shutdown_channel() -> (ShutdownTx, ShutdownRx) {
    tokio::sync::watch::channel(false)
}
