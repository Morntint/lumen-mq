use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::broker::BrokerState;

/// MQTT over TCP 服务
pub struct TcpServer {
    bind: SocketAddr,
    broker: Arc<BrokerState>,
    max_connections: usize,
    shutdown: watch::Receiver<bool>,
}

impl TcpServer {
    pub fn new(
        bind: SocketAddr,
        broker: Arc<BrokerState>,
        max_connections: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self { bind, broker, max_connections, shutdown }
    }

    /// 启动并阻塞运行，直到收到关闭信号
    pub async fn run(mut self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.bind).await?;
        info!(addr = %self.bind, "TCP MQTT listener started");

        loop {
            tokio::select! {
                biased;
                // 关闭信号优先
                res = self.shutdown.changed() => {
                    if res.is_ok() && *self.shutdown.borrow() {
                        info!("TCP server shutting down");
                        break;
                    }
                }
                accept = listener.accept() => {
                    let (socket, peer) = match accept {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "accept failed");
                            continue;
                        }
                    };
                    let current = self.broker.metrics().connections_current.load(std::sync::atomic::Ordering::Relaxed);
                    if current as usize >= self.max_connections {
                        warn!(peer = %peer, current, max = self.max_connections, "max connections reached, refusing");
                        drop(socket);
                        continue;
                    }

                    // 关闭订阅：每个连接独立持有 watch receiver
                    let shutdown_rx = self.shutdown.clone();
                    let broker = self.broker.clone();

                    // 关闭 Nagle，降低工业小报文延迟
                    let _ = socket.set_nodelay(true);

                    tokio::spawn(async move {
                        let _ = broker.handle_tcp_connection(socket, peer, shutdown_rx).await;
                    });
                }
            }
        }
        Ok(())
    }
}
