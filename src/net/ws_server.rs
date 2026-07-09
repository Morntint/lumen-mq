//! WebSocket MQTT 服务（MQTT over WebSocket）
//!
//! 提供能力：
//! - `WsServer`：监听 TCP，完成 WebSocket 握手后把内层二进制流交给
//!   `BrokerState::handle_connection`（复用 TCP/TLS 同一套协议处理逻辑）
//! - `WsStream`：把 `WebSocketStream` 适配为 `AsyncRead + AsyncWrite`，承载 MQTT 二进制报文
//!
//! 子协议：握手时若客户端请求 `mqtt`（MQTT 3.1.1 over WebSocket 约定），则在响应中回选
//! 数据帧：仅接受 Binary 帧（MQTT 报文为二进制）；Text 帧视为协议错误

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::sink::Sink;
use futures::stream::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, error, info, warn};

use crate::broker::BrokerState;

/// MQTT over WebSocket 约定的子协议名
const WS_SUBPROTOCOL_MQTT: &str = "mqtt";

/// WebSocket 子协议协商回调：若客户端请求中含 `mqtt`，则在响应中回选
fn negotiate_subprotocol(req: &Request, resp: &mut Response) {
    if let Some(protos) = req.headers().get("sec-websocket-protocol") {
        if let Ok(s) = protos.to_str() {
            let has_mqtt = s.split(',').any(|p| p.trim() == WS_SUBPROTOCOL_MQTT);
            if has_mqtt {
                if let Ok(v) = WS_SUBPROTOCOL_MQTT.parse() {
                    resp.headers_mut().insert("sec-websocket-protocol", v);
                }
            }
        }
    }
}

/// 把 `WebSocketStream` 适配为 `AsyncRead + AsyncWrite`
///
/// - 读：把 Binary 帧的负载按序填入调用方缓冲；Ping/Pong 由 tungstenite 自动处理，忽略
/// - 写：把调用方给出的字节封装为 Binary 帧发送
pub struct WsStream<S> {
    inner: WebSocketStream<S>,
    /// 跨帧未消费的读缓冲（一帧可能比调用方一次 read 的容量大）
    read_buf: BytesMut,
    closed: bool,
}

impl<S> WsStream<S> {
    pub fn new(inner: WebSocketStream<S>) -> Self {
        Self { inner, read_buf: BytesMut::new(), closed: false }
    }
}

impl<S> AsyncRead for WsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // 1. 先消费上一帧遗留的缓冲
        if !this.read_buf.is_empty() {
            let n = std::cmp::min(this.read_buf.len(), buf.remaining());
            buf.put_slice(&this.read_buf.split_to(n));
            return Poll::Ready(Ok(()));
        }

        if this.closed {
            return Poll::Ready(Ok(())); // EOF
        }

        // 2. 从 WebSocketStream 拉取下一帧
        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message::Binary(data) => {
                        let n = std::cmp::min(data.len(), buf.remaining());
                        buf.put_slice(&data[..n]);
                        if n < data.len() {
                            this.read_buf.extend_from_slice(&data[n..]);
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Message::Ping(_) | Message::Pong(_) => {
                        // tungstenite 默认自动回 Pong；继续读下一帧
                        continue;
                    }
                    Message::Close(_) => {
                        this.closed = true;
                        return Poll::Ready(Ok(())); // EOF
                    }
                    Message::Text(_) => {
                        // MQTT over WebSocket 仅使用二进制帧
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "text frame not supported by MQTT over WebSocket",
                        )));
                    }
                    Message::Frame(_) => {
                        // 已被 WebSocketStream 分类，不会到达
                        continue;
                    }
                },
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::other(
                        e.to_string(),
                    )));
                }
                Poll::Ready(None) => {
                    this.closed = true;
                    return Poll::Ready(Ok(())); // EOF
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for WsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "ws closed",
            )));
        }

        match Pin::new(&mut this.inner).poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                match Pin::new(&mut this.inner)
                    .start_send(Message::binary(buf.to_vec()))
                {
                    Ok(()) => Poll::Ready(Ok(buf.len())),
                    Err(e) => Poll::Ready(Err(io::Error::other(
                        e.to_string(),
                    ))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(
                e.to_string(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_flush(cx)
            .map_err(|e| io::Error::other(e.to_string()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_close(cx) {
            Poll::Ready(Ok(())) => {
                this.closed = true;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(io::Error::other(
                e.to_string(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// MQTT over WebSocket 服务
pub struct WsServer {
    bind: SocketAddr,
    broker: Arc<BrokerState>,
    max_connections: usize,
    shutdown: watch::Receiver<bool>,
    /// 配置的 WS 路径（如 `/mqtt`）；仅用于日志，握手时不强制拒绝其他路径以最大化兼容性
    path: String,
}

impl WsServer {
    pub fn new(
        bind: SocketAddr,
        broker: Arc<BrokerState>,
        max_connections: usize,
        shutdown: watch::Receiver<bool>,
        path: String,
    ) -> Self {
        Self { bind, broker, max_connections, shutdown, path }
    }

    /// 启动并阻塞运行，直到收到关闭信号
    #[allow(clippy::result_large_err)]
    pub async fn run(mut self) -> std::io::Result<()> {
        let listener = TcpListener::bind(self.bind).await?;
        info!(addr = %self.bind, path = %self.path, "WebSocket MQTT listener started");

        loop {
            tokio::select! {
                biased;
                res = self.shutdown.changed() => {
                    if res.is_ok() && *self.shutdown.borrow() {
                        info!("WebSocket server shutting down");
                        break;
                    }
                }
                accept = listener.accept() => {
                    let (socket, peer) = match accept {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error = %e, "WS accept failed");
                            continue;
                        }
                    };

                    // 预置安全检查：accept 后立即校验 IP 黑白名单，
                    // 命中则直接关闭，避免 WS 握手消耗资源
                    if let Err(e) = self.broker.security().check_connection(peer) {
                        warn!(peer = %peer, error = %e, "WS accept rejected by security guard");
                        drop(socket);
                        continue;
                    }

                    let current = self.broker.metrics().connections_current
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let pending = self.broker.metrics().pending_connections();
                    if (current + pending) as usize >= self.max_connections {
                        warn!(peer = %peer, current, pending, max = self.max_connections, "max connections reached (incl. pending), refusing WS");
                        drop(socket);
                        continue;
                    }

                    let _ = socket.set_nodelay(true);
                    let broker = self.broker.clone();
                    let shutdown_rx = self.shutdown.clone();
                    let expected_path = self.path.clone();

                    tokio::spawn(async move {
                        // WebSocket 握手（含子协议协商 + 路径校验，10s 超时避免慢速攻击）
                        let handshake = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            tokio_tungstenite::accept_hdr_async(socket, |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> { // clippy::result_large_err: axum 回调签名约束，无法规避
                                // 路径软校验：记录不匹配情况，但不拒绝（兼容浏览器/MQTT.js 任意路径）
                                let req_path = req.uri().path();
                                if req_path != expected_path.as_str() {
                                    debug!(req_path = %req_path, expected = %expected_path, "ws path mismatch, accepting anyway");
                                }
                                negotiate_subprotocol(req, &mut resp);
                                Ok(resp)
                            }),
                        ).await;

                        match handshake {
                            Ok(Ok(ws_stream)) => {
                                let adapter = WsStream::new(ws_stream);
                                let _ = broker
                                    .handle_connection(adapter, peer, shutdown_rx)
                                    .await;
                            }
                            Ok(Err(e)) => {
                                warn!(error = %e, %peer, "WebSocket handshake failed");
                            }
                            Err(_) => {
                                warn!(%peer, "WebSocket handshake timeout (10s), closing");
                            }
                        }
                    });
                }
            }
        }
        Ok(())
    }
}
