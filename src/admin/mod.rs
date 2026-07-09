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
    // 若配置了 admin.token，则对写操作（DELETE/POST）应用 Token 鉴权中间件。
    // 读端点（/health、/metrics、/dashboard、/api/v1/ws）保持开放（运维观测常用）。
    let admin_token = broker.config().admin.token.clone();
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
        .layer(axum::middleware::from_fn_with_state(
            admin_token,
            admin_token_auth,
        ))
}

/// Admin API Token 鉴权中间件
///
/// - 仅对写操作（DELETE /api/v1/* 与 POST /api/v1/*）生效；读端点放行
/// - 未配置 token 时全部放行（启动时 validate.rs 已强制非环回必须配 token）
/// - 客户端通过 `Authorization: Bearer <token>` 携带凭证
async fn admin_token_auth(
    State(token): State<String>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    // 未配置 token → 放行（启动期 validate 已保证非环回必须配 token）
    if token.is_empty() {
        return next.run(req).await;
    }
    let path = req.uri().path();
    let method = req.method().clone();
    // 仅保护 /api/v1/* 下的写操作
    let is_protected = path.starts_with("/api/v1/")
        && (method == axum::http::Method::POST || method == axum::http::Method::DELETE);
    if !is_protected {
        return next.run(req).await;
    }
    // 校验 Authorization: Bearer <token>
    let auth_ok = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|provided| constant_time_eq(provided.as_bytes(), token.as_bytes()))
        .unwrap_or(false);
    if !auth_ok {
        return (
            StatusCode::UNAUTHORIZED,
            Json(ActionResponse {
                ok: false,
                message: "missing or invalid admin token".into(),
            }),
        )
            .into_response();
    }
    next.run(req).await
}

/// 常数时间字符串比较，避免计时侧信道泄露 token
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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

async fn metrics(State(broker): State<Arc<BrokerState>>) -> Response {
    // 同步 gauge 指标（订阅数 / 会话数），确保 Prometheus 导出值准确
    let (normal_subs, shared_subs) = broker.subscriptions().subscription_counts();
    METRICS.set_subscriptions(normal_subs as i64);
    METRICS.set_shared_subscriptions(shared_subs as i64);
    let total_sessions = broker.sessions().total_count();
    let online_sessions = broker.sessions().online_count();
    METRICS.set_sessions_total(total_sessions as i64);
    METRICS.set_sessions_offline((total_sessions.saturating_sub(online_sessions)) as i64);

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
    let total = snap.len();
    let online = snap.iter().filter(|(_, c, _, _)| *c).count();
    let offline = total - online;
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
        total,
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

    // 校验会话是否在线：在线会话拒绝删除，避免误伤重连后的新会话。
    // 运维应先断开客户端再调用此接口清理残留离线会话。
    if broker.sessions().is_online(&client_id) {
        return (
            StatusCode::CONFLICT,
            Json(ActionResponse {
                ok: false,
                message: format!(
                    "session '{client_id}' is currently online; disconnect the client first"
                ),
            }),
        )
            .into_response();
    }

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
        payload: bytes::Bytes::from(req.payload.into_bytes()),
    };
    // 走与正常 PUBLISH 相同的 security/plugin 检查，避免 admin 接口绕过限流/内容过滤
    if let Err(e) = broker.security().check_publish(
        "admin-api",
        publish.payload.len(),
        broker.config().broker.max_packet_size,
    ) {
        METRICS.inc_security_rejected();
        return (
            StatusCode::FORBIDDEN,
            Json(ActionResponse {
                ok: false,
                message: format!("rejected by security guard: {e}"),
            }),
        )
            .into_response();
    }
    if let Err(e) = broker.plugin().check_publish(&publish) {
        METRICS.inc_plugin_rejected();
        return (
            StatusCode::FORBIDDEN,
            Json(ActionResponse {
                ok: false,
                message: format!("rejected by plugin guard: {e}"),
            }),
        )
            .into_response();
    }
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

/// WebSocket 连接处理循环：周期性推送仪表盘快照，同时处理客户端帧（Close/Ping）
async fn handle_ws_dashboard(mut socket: WebSocket, broker: Arc<BrokerState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.tick().await; // 首次立即推送

    loop {
        tokio::select! {
            // 周期推送仪表盘快照
            _ = interval.tick() => {
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
            // 同时接收客户端帧：处理 Close（及时退出）、Ping；忽略其他
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(Message::Ping(p))) => {
                        let _ = socket.send(Message::Pong(p)).await;
                    }
                    Some(Ok(_)) => {} // 忽略 Text/Binary/Pong
                }
            }
        }
    }
    let _ = socket.close().await;
}

/// 构建一次仪表盘快照
fn build_dashboard_snapshot(broker: &BrokerState) -> DashboardSnapshot {
    // 从实际数据结构同步 gauge 指标（订阅数 / 会话数），避免指标永远为 0
    let (normal_subs, shared_subs) = broker.subscriptions().subscription_counts();
    METRICS.set_subscriptions(normal_subs as i64);
    METRICS.set_shared_subscriptions(shared_subs as i64);
    let total_sessions = broker.sessions().total_count();
    let online_sessions = broker.sessions().online_count();
    METRICS.set_sessions_total(total_sessions as i64);
    METRICS.set_sessions_offline((total_sessions.saturating_sub(online_sessions)) as i64);

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
        let settings = Settings {
            auth: AuthConfig {
                mode: AuthMode::Anonymous,
                allow_anonymous: true,
                users: vec![],
            },
            ..Default::default()
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
