//! Admin HTTP 运维管理接口（阶段五）
//!
//! 提供能力：
//! - `GET /dashboard`：内置 Web 仪表盘（实时可视化）
//! - `GET /api/v1/ws`：WebSocket 实时推送指标快照（仪表盘数据源）
//! - `GET /health`：健康检查（返回 broker 状态摘要）
//! - `GET /metrics`：Prometheus 文本格式指标
//! - `GET /api/v1/sessions`：查询在线/离线会话列表
//! - `DELETE /api/v1/sessions/:client_id`：清理指定会话（订阅 + 离线队列 + 持久化快照）
//! - `POST /api/v1/reload/security`：热重载安全中间件配置
//! - `POST /api/v1/reload/plugin`：热重载插件配置
//! - `POST /api/v1/publish`：手动发布一条消息（运维测试用）
//!
//! 设计要点：
//! - 基于 axum 0.7，与 MQTT 监听端口分离（独立 AdminConfig.bind）
//! - 所有写操作返回 JSON 结果，便于运维脚本解析
//! - 不做鉴权（生产部署应通过反向代理 + 网络隔离保护）

mod dashboard;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::broker::BrokerState;
use crate::codec::QoS;
use crate::config::{PluginConfig, SecurityConfig};
use crate::monitor::{MetricsSnapshot, METRICS};
use crate::utils::BrokerResult;

/// Admin HTTP 服务器
pub struct AdminServer {
    bind: SocketAddr,
    broker: Arc<BrokerState>,
}

impl AdminServer {
    pub fn new(bind: SocketAddr, broker: Arc<BrokerState>) -> Self {
        Self { bind, broker }
    }

    /// 启动 HTTP 服务（阻塞至 shutdown_rx 触发）
    pub async fn run(
        self,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> BrokerResult<()> {
        let app = build_router(self.broker.clone());

        let listener = tokio::net::TcpListener::bind(self.bind)
            .await
            .map_err(|e| crate::utils::BrokerError::Other(format!("admin bind {}: {e}", self.bind)))?;
        info!(bind = %self.bind, "Admin HTTP server listening");

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let mut rx = shutdown_rx;
                if rx.changed().await.is_ok() {
                    info!("admin HTTP server shutting down");
                }
            })
            .await
            .map_err(|e| crate::utils::BrokerError::Other(format!("admin serve: {e}")))?;
        Ok(())
    }

    /// 使用已绑定的 listener 启动 HTTP 服务（便于测试获取实际端口）
    pub async fn run_with_listener(
        self,
        listener: tokio::net::TcpListener,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) -> BrokerResult<()> {
        let app = build_router(self.broker.clone());
        let addr = listener.local_addr().ok();
        info!(bind = ?addr, "Admin HTTP server listening (with listener)");

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let mut rx = shutdown_rx;
                if rx.changed().await.is_ok() {
                    info!("admin HTTP server shutting down");
                }
            })
            .await
            .map_err(|e| crate::utils::BrokerError::Other(format!("admin serve: {e}")))?;
        Ok(())
    }
}

/// 构建 axum 路由（独立暴露，便于测试）
pub fn build_router(broker: Arc<BrokerState>) -> axum::Router {
    axum::Router::new()
        .route("/dashboard", get(dashboard_page))
        .route("/api/v1/ws", get(ws_dashboard))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/v1/sessions", get(list_sessions))
        .route("/api/v1/sessions/:client_id", delete(delete_session))
        .route("/api/v1/reload/security", post(reload_security))
        .route("/api/v1/reload/plugin", post(reload_plugin))
        .route("/api/v1/publish", post(manual_publish))
        .with_state(broker)
}

// —— 响应结构 ——

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    node_id: String,
    version: &'static str,
    online_connections: i64,
    total_sessions: usize,
    subscriptions: usize,
    uptime_hint: &'static str,
}

#[derive(Debug, Serialize)]
struct SessionInfo {
    client_id: String,
    connected: bool,
    peer_addr: String,
    connected_at_unix: Option<u64>,
}

#[derive(Debug, Serialize)]
struct SessionsListResponse {
    total: usize,
    online: usize,
    offline: usize,
    sessions: Vec<SessionInfo>,
}

#[derive(Debug, Serialize)]
struct ActionResponse {
    ok: bool,
    message: String,
}

#[derive(Debug, Deserialize)]
struct ManualPublishRequest {
    topic: String,
    payload: String,
    #[serde(default)]
    qos: u8,
    #[serde(default)]
    retain: bool,
}

// —— 处理函数 ——

async fn health(State(broker): State<Arc<BrokerState>>) -> Json<HealthResponse> {
    let snap = broker.metrics().snapshot();
    Json(HealthResponse {
        status: "ok",
        node_id: broker.config().broker.node_id.clone(),
        version: env!("CARGO_PKG_VERSION"),
        online_connections: snap.connections_current,
        total_sessions: broker.sessions().total_count(),
        subscriptions: broker.subscriptions().subscriber_count(),
        uptime_hint: "see process start time",
    })
}

async fn metrics() -> Response {
    let text = METRICS.prometheus_text();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        text,
    )
        .into_response()
}

async fn list_sessions(State(broker): State<Arc<BrokerState>>) -> Json<SessionsListResponse> {
    let snap = broker.sessions().iter_snapshot();
    let online = snap.iter().filter(|(_, c, _, _)| *c).count();
    let offline = snap.len().saturating_sub(online);
    let sessions = snap
        .into_iter()
        .map(|(client_id, connected, peer_addr, _connected_at)| SessionInfo {
            client_id,
            connected,
            peer_addr: peer_addr.to_string(),
            // Instant 无法转 unix 时间戳；这里返回 None，前端可忽略
            connected_at_unix: None,
        })
        .collect();
    Json(SessionsListResponse {
        total: online + offline,
        online,
        offline,
        sessions,
    })
}

async fn delete_session(
    State(broker): State<Arc<BrokerState>>,
    Path(client_id): Path<String>,
) -> Response {
    info!(client = %client_id, "admin: delete session requested");
    // 移除会话 + 订阅 + 离线队列 + 持久化快照
    broker.subscriptions().unsubscribe_all(&client_id);
    broker.sessions().remove(&client_id);
    // 清理磁盘快照（若启用持久化）
    if let Some(storage) = broker.storage() {
        let _ = storage.delete_session(&client_id);
        let _ = storage.drain_offline(&client_id);
    }
    Json(ActionResponse {
        ok: true,
        message: format!("session '{client_id}' cleaned up"),
    })
    .into_response()
}

async fn reload_security(
    State(broker): State<Arc<BrokerState>>,
    Json(cfg): Json<SecurityConfig>,
) -> Response {
    match broker.reload_security(&cfg) {
        Ok(()) => Json(ActionResponse {
            ok: true,
            message: "security config reloaded".into(),
        })
        .into_response(),
        Err(e) => {
            warn!(error = %e, "admin: reload security failed");
            (
                StatusCode::BAD_REQUEST,
                Json(ActionResponse {
                    ok: false,
                    message: format!("reload failed: {e}"),
                }),
            )
                .into_response()
        }
    }
}

async fn reload_plugin(
    State(broker): State<Arc<BrokerState>>,
    Json(cfg): Json<PluginConfig>,
) -> Response {
    match broker.reload_plugin(&cfg) {
        Ok(()) => Json(ActionResponse {
            ok: true,
            message: "plugin config reloaded".into(),
        })
        .into_response(),
        Err(e) => {
            warn!(error = %e, "admin: reload plugin failed");
            (
                StatusCode::BAD_REQUEST,
                Json(ActionResponse {
                    ok: false,
                    message: format!("reload failed: {e}"),
                }),
            )
                .into_response()
        }
    }
}

async fn manual_publish(
    State(broker): State<Arc<BrokerState>>,
    Json(req): Json<ManualPublishRequest>,
) -> Response {
    let qos = match req.qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ActionResponse {
                    ok: false,
                    message: "qos must be 0, 1, or 2".into(),
                }),
            )
                .into_response();
        }
    };
    let publish = crate::codec::Publish {
        dup: false,
        qos,
        retain: req.retain,
        topic: req.topic.clone(),
        packet_id: None,
        payload: req.payload.into_bytes(),
    };
    let trace_id = crate::utils::time::trace_id();
    match broker.router().route_inbound_publish(&publish, Some("admin-api"), &trace_id) {
        Ok(()) => {
            // 计入指标（手动发布也计入 publish_received）
            METRICS.inc_publish();
            METRICS.inc_publish_qos(req.qos);
            Json(ActionResponse {
                ok: true,
                message: format!("published to '{}'", req.topic),
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ActionResponse {
                ok: false,
                message: format!("publish failed: {e}"),
            }),
        )
            .into_response(),
    }
}

// —— 仪表盘 & WebSocket 实时推送 ——

/// 仪表盘 HTML 页面
async fn dashboard_page() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        dashboard::DASHBOARD_HTML,
    )
        .into_response()
}

/// WebSocket 仪表盘数据快照（推送给前端）
#[derive(Debug, Serialize)]
struct DashboardSnapshot {
    timestamp: u64,
    node_id: String,
    version: &'static str,
    metrics: MetricsSnapshot,
    sessions: Vec<SessionInfo>,
}

/// WebSocket 升级处理：每 2 秒推送一次指标 + 会话快照
async fn ws_dashboard(
    ws: WebSocketUpgrade,
    State(broker): State<Arc<BrokerState>>,
) -> Response {
    ws.on_upgrade(|socket| handle_ws_dashboard(socket, broker))
}

/// WebSocket 连接处理循环：周期性推送仪表盘快照
async fn handle_ws_dashboard(mut socket: WebSocket, broker: Arc<BrokerState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.tick().await; // 首次立即推送

    loop {
        interval.tick().await;
        let snap = build_dashboard_snapshot(&broker);
        let json = match serde_json::to_string(&snap) {
            Ok(j) => j,
            Err(e) => {
                warn!(error = %e, "dashboard snapshot serialize failed");
                continue;
            }
        };
        if socket.send(Message::Text(json)).await.is_err() {
            break; // 客户端断开
        }
    }
    let _ = socket.close().await;
}

/// 构建一次仪表盘快照
fn build_dashboard_snapshot(broker: &BrokerState) -> DashboardSnapshot {
    let metrics = METRICS.snapshot();
    let snap = broker.sessions().iter_snapshot();
    let sessions = snap
        .into_iter()
        .map(|(client_id, connected, peer_addr, _)| SessionInfo {
            client_id,
            connected,
            peer_addr: peer_addr.to_string(),
            connected_at_unix: None,
        })
        .collect();
    DashboardSnapshot {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        node_id: broker.config().broker.node_id.clone(),
        version: env!("CARGO_PKG_VERSION"),
        metrics,
        sessions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{Authenticator, BrokerState};
    use crate::config::{AuthConfig, AuthMode, Settings};
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn make_broker() -> Arc<BrokerState> {
        let mut settings = Settings::default();
        settings.auth = AuthConfig {
            mode: AuthMode::Anonymous,
            allow_anonymous: true,
            users: vec![],
        };
        let config = Arc::new(settings);
        let auth = Arc::new(Authenticator::new(Arc::new(config.auth.clone())));
        BrokerState::new(config, auth)
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let broker = make_broker();
        let app = build_router(broker);
        let resp = app
            .oneshot(Request::builder().uri("/health").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 4096).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("\"status\":\"ok\""));
        assert!(text.contains("\"online_connections\":0"));
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_text() {
        let broker = make_broker();
        let app = build_router(broker);
        let resp = app
            .oneshot(Request::builder().uri("/metrics").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
        assert!(ct.contains("text/plain"), "content-type: {ct}");
        let body = to_bytes(resp.into_body(), 8192).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("# HELP lumenmq_connections_total"));
        assert!(text.contains("# TYPE lumenmq_connections_total counter"));
        assert!(text.contains("lumenmq_connections_total "));
    }

    #[tokio::test]
    async fn list_sessions_empty_broker() {
        let broker = make_broker();
        let app = build_router(broker);
        let resp = app
            .oneshot(Request::builder().uri("/api/v1/sessions").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 4096).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("\"total\":0"));
        assert!(text.contains("\"online\":0"));
    }

    #[tokio::test]
    async fn manual_publish_with_invalid_qos_returns_400() {
        let broker = make_broker();
        let app = build_router(broker);
        let body = serde_json::json!({
            "topic": "test/topic",
            "payload": "hello",
            "qos": 5,
            "retain": false
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/publish")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn manual_publish_qos0_succeeds() {
        let broker = make_broker();
        let app = build_router(broker);
        let body = serde_json::json!({
            "topic": "admin/test",
            "payload": "hello-admin",
            "qos": 0,
            "retain": false
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/publish")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 4096).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("\"ok\":true"));
        // 指标应计数
        let snap = METRICS.snapshot();
        assert!(snap.publish_received >= 1);
    }

    #[tokio::test]
    async fn dashboard_returns_html() {
        let broker = make_broker();
        let app = build_router(broker);
        let resp = app
            .oneshot(Request::builder().uri("/dashboard").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_string();
        assert!(ct.contains("text/html"), "content-type: {ct}");
        let body = to_bytes(resp.into_body(), 65536).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("<title>LumenMQ Dashboard</title>"));
        assert!(text.contains("/api/v1/ws")); // WebSocket 端点引用
    }

    #[test]
    fn dashboard_snapshot_serializes_correctly() {
        let broker = make_broker();
        let snap = build_dashboard_snapshot(&broker);
        let json = serde_json::to_string(&snap).unwrap();
        // 验证 JSON 包含关键指标字段
        assert!(json.contains("\"connections_current\""));
        assert!(json.contains("\"publish_received\""));
        assert!(json.contains("\"messages_sent\""));
        assert!(json.contains("\"sessions_total\""));
        assert!(json.contains("\"node_id\""));
        assert!(json.contains("\"version\""));
        assert!(json.contains("\"timestamp\""));
        assert!(json.contains("\"sessions\""));
    }
}
