pub mod auth;
pub mod qos;
pub mod retain;
pub mod router;
pub mod session;
pub mod store_msg;
pub mod subscription;

pub use auth::{AuthIdentity, Authenticator, WillMessage};
pub use qos::{AckStage, InboundQos2Tracker, OutboundInflight, OutboundInflightTable};
pub use retain::{RetainStore, RetainedMessage, SharedRetainStore};
pub use router::{OutboundPublish, Router, SharedRouter};
pub use session::{DeliveryOutcome, SessionEntry, SessionManager, SharedSessionManager};
pub use store_msg::OfflineQueue;
pub use subscription::{SharedSubscriptionTree, SubscriptionTree};

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Interval;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, info, warn};

use crate::codec::{
    connack_code, packet::suback_code, Connack, MqttCodec, Packet, Publish, QoS, MQTT_3_1_1_LEVEL,
    MQTT_5_LEVEL,
};
use crate::config::Settings;
use crate::monitor::{Metrics, METRICS};
use crate::net::{ConnContext, HeartbeatTimer, ShutdownRx};
use crate::storage::{SharedStorage, Storage, SessionSnapshot};
use crate::utils::{BrokerError, BrokerResult};

/// 连接循环控制流
enum Control {
    Continue,
    Disconnect,
}

/// Broker 全局状态，跨连接共享
pub struct BrokerState {
    sessions: SharedSessionManager,
    subscriptions: SharedSubscriptionTree,
    router: SharedRouter,
    retain: SharedRetainStore,
    storage: Option<SharedStorage>,
    auth: Arc<Authenticator>,
    config: Arc<Settings>,
    security: Arc<crate::security::SecurityGuard>,
    plugin: Arc<crate::plugin::PluginGuard>,
}

impl BrokerState {
    pub fn new(config: Arc<Settings>, auth: Arc<Authenticator>) -> Arc<Self> {
        let subscriptions: SharedSubscriptionTree = Arc::new(SubscriptionTree::new());
        let sessions: SharedSessionManager = Arc::new(SessionManager::with_limits(
            config.storage.max_offline_messages,
            Duration::from_secs(config.storage.offline_message_ttl),
        ));

        // 可选 sled 持久化
        let storage: Option<SharedStorage> = if config.storage.enabled {
            match Storage::open(&config.storage.path) {
                Ok(s) => {
                    info!(path = ?config.storage.path, "sled storage opened");
                    Some(Arc::new(s))
                }
                Err(e) => {
                    warn!(error = %e, "open sled storage failed, falling back to memory-only");
                    None
                }
            }
        } else {
            None
        };

        // RetainStore：若开启存储则附着
        let retain: SharedRetainStore = match &storage {
            Some(s) => {
                let r = Arc::new(RetainStore::with_storage(s.clone()));
                if let Err(e) = r.load_from_storage(s) {
                    warn!(error = %e, "load retained from storage failed");
                }
                r
            }
            None => Arc::new(RetainStore::new()),
        };

        // Router：若开启存储则附着
        let router = match &storage {
            Some(s) => Arc::new(Router::with_storage(
                subscriptions.clone(),
                sessions.clone(),
                retain.clone(),
                s.clone(),
            )),
            None => Arc::new(Router::new(
                subscriptions.clone(),
                sessions.clone(),
                retain.clone(),
            )),
        };

        // 安全中间件：从配置构建；解析失败则降级为禁用
        let security = match crate::security::SecurityGuard::new(&config.security) {
            Ok(g) => g,
            Err(e) => {
                warn!(error = %e, "security config invalid, falling back to disabled");
                crate::security::SecurityGuard::disabled()
            }
        };

        // 消息插件中间件：从配置构建；解析失败则降级为禁用
        let plugin = match crate::plugin::PluginGuard::new(&config.plugin) {
            Ok(g) => g,
            Err(e) => {
                warn!(error = %e, "plugin config invalid, falling back to disabled");
                crate::plugin::PluginGuard::disabled()
            }
        };

        let broker = Arc::new(Self {
            sessions,
            subscriptions,
            router,
            retain,
            storage,
            auth,
            config,
            security,
            plugin,
        });

        // 启动加载历史会话（clean_session=false 持久化的）
        if let Err(e) = broker.load_persistent_sessions() {
            warn!(error = %e, "load persistent sessions from storage failed");
        }

        broker
    }

    /// 启动时加载持久化会话：恢复订阅树 + 创建离线会话 + 回填离线消息队列
    fn load_persistent_sessions(&self) -> BrokerResult<()> {
        let Some(storage) = &self.storage else {
            return Ok(());
        };
        let snapshots = storage.load_all_sessions()?;
        if snapshots.is_empty() {
            return Ok(());
        }
        info!(count = snapshots.len(), "loading persistent sessions from storage");
        for snap in snapshots {
            // 恢复订阅树
            for (filter, qos) in &snap.subscriptions {
                if let Err(e) = self.subscriptions.subscribe(&snap.client_id, filter, *qos) {
                    warn!(error = %e, client = %snap.client_id, filter = %filter, "restore subscription failed");
                }
            }
            // 创建离线会话（用 dummy tx 占位；real connection 接管时会替换）
            let (dummy_tx, _dummy_rx) = mpsc::channel::<OutboundPublish>(1);
            let dummy_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let (epoch, _, _) = self.sessions.register(
                snap.client_id.clone(),
                false,
                dummy_tx,
                None,
                MQTT_3_1_1_LEVEL,
                dummy_addr,
                None, // 恢复的会话无 expiry 信息（broker 重启后），永久保留
            );
            self.sessions.mark_offline(&snap.client_id, epoch);

            // 把磁盘上残留的离线消息回填到内存队列
            let offline_msgs = match storage.drain_offline(&snap.client_id) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, client = %snap.client_id, "drain offline from storage failed");
                    continue;
                }
            };
            for m in offline_msgs {
                let _ = self.sessions.deliver_or_enqueue(&snap.client_id, m);
            }
            debug!(client = %snap.client_id, subs = snap.subscriptions.len(), "restored persistent session");
        }
        Ok(())
    }

    /// 持久化某客户端的会话快照（订阅列表）
    fn persist_session_snapshot(&self, client_id: &str) {
        let Some(storage) = &self.storage else { return };
        let subs = self.subscriptions.subscriptions_of(client_id);
        let snap = SessionSnapshot {
            client_id: client_id.to_string(),
            subscriptions: subs,
        };
        if let Err(e) = storage.save_session(client_id, &snap) {
            warn!(error = %e, client = %client_id, "persist session snapshot failed");
        }
    }

    /// 删除某客户端的会话快照（clean=true 清理时）
    fn delete_session_snapshot(&self, client_id: &str) {
        let Some(storage) = &self.storage else { return };
        if let Err(e) = storage.delete_session(client_id) {
            warn!(error = %e, client = %client_id, "delete session snapshot failed");
        }
        if let Err(e) = storage.drain_offline(client_id) {
            warn!(error = %e, client = %client_id, "drain offline on cleanup failed");
        }
    }

    /// 重连时清空磁盘上的离线消息（内存中的已由 SessionManager.register 取出投递）
    fn clear_disk_offline(&self, client_id: &str) {
        let Some(storage) = &self.storage else { return };
        if let Err(e) = storage.drain_offline(client_id) {
            warn!(error = %e, client = %client_id, "clear disk offline failed");
        }
    }

    pub fn metrics(&self) -> &'static Metrics {
        &METRICS
    }

    pub fn sessions(&self) -> &SharedSessionManager {
        &self.sessions
    }

    pub fn subscriptions(&self) -> &SharedSubscriptionTree {
        &self.subscriptions
    }

    pub fn retain(&self) -> &SharedRetainStore {
        &self.retain
    }

    pub fn router(&self) -> &SharedRouter {
        &self.router
    }

    pub fn config(&self) -> &Settings {
        &self.config
    }

    /// 持久化存储句柄（admin API 用于清理会话快照与离线消息）
    pub fn storage(&self) -> Option<&SharedStorage> {
        self.storage.as_ref()
    }

    /// 安全中间件句柄（用于 TCP/TLS/WS server 的 accept 阶段准入检查）
    pub fn security(&self) -> &Arc<crate::security::SecurityGuard> {
        &self.security
    }

    /// 热更新安全策略（运维 API 调用）
    pub fn reload_security(&self, cfg: &crate::config::SecurityConfig) -> BrokerResult<()> {
        self.security.reload(cfg)
    }

    /// 消息插件句柄（用于运维 API 观测）
    pub fn plugin(&self) -> &Arc<crate::plugin::PluginGuard> {
        &self.plugin
    }

    /// 热更新插件策略（运维 API 调用）
    pub fn reload_plugin(&self, cfg: &crate::config::PluginConfig) -> BrokerResult<()> {
        self.plugin.reload(cfg)
    }

    /// 处理一条 TCP MQTT 连接的完整生命周期（薄包装，委托给通用 handle_connection）
    pub async fn handle_tcp_connection(
        &self,
        socket: TcpStream,
        peer: SocketAddr,
        shutdown: ShutdownRx,
    ) -> BrokerResult<()> {
        self.handle_connection(socket, peer, shutdown).await
    }

    /// 处理任意 AsyncRead+AsyncWrite 流上的 MQTT 连接生命周期（TCP / TLS / WebSocket 通用）
    pub async fn handle_connection<S>(
        &self,
        socket: S,
        peer: SocketAddr,
        mut shutdown: ShutdownRx,
    ) -> BrokerResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // 安全中间件：IP 黑白名单 + 单 IP 连接数检查（在读取 CONNECT 之前）
        if let Err(e) = self.security.check_connection(peer) {
            debug!(peer = %peer, error = %e, "connection rejected by security guard");
            return Ok(());
        }
        // 通过准入检查，登记 IP 计数（断开时由 on_disconnect 扣减）
        self.security.on_connect(peer);

        // 运行连接生命周期；无论以何种方式退出，都需通知安全中间件扣减 IP 计数
        let result = self
            .handle_connection_inner(socket, peer, &mut shutdown)
            .await;
        self.security.on_disconnect(peer, None);
        result
    }

    async fn handle_connection_inner<S>(
        &self,
        socket: S,
        peer: SocketAddr,
        shutdown: &mut ShutdownRx,
    ) -> BrokerResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let codec = MqttCodec::new(self.config.broker.max_packet_size);
        let (read_half, write_half) = tokio::io::split(socket);
        let mut stream = FramedRead::new(read_half, codec.clone());
        let mut sink = FramedWrite::new(write_half, codec);

        let mut ctx = ConnContext::new(peer);

        // 1. 读取 CONNECT（首包必须在合理时间内到达）
        let connect = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
            Ok(Some(Ok(Packet::Connect(c)))) => c,
            Ok(Some(Ok(_))) => {
                debug!(peer = %peer, "first packet is not CONNECT, closing");
                return Ok(());
            }
            Ok(Some(Err(e))) => {
                debug!(peer = %peer, error = %e, "connect decode error");
                return Ok(());
            }
            Ok(None) => {
                debug!(peer = %peer, "connection closed before CONNECT");
                return Ok(());
            }
            Err(_) => {
                debug!(peer = %peer, "connect timeout");
                return Ok(());
            }
        };

        // 2. 协议版本校验（接受 3.1.1 与 5.0）
        if connect.protocol_level != MQTT_3_1_1_LEVEL && connect.protocol_level != MQTT_5_LEVEL {
            let _ = sink.send(Packet::connack_error(connack_code::BAD_PROTOCOL_VERSION)).await;
            return Ok(());
        }

        // 3. 鉴权
        let identity = match self.auth.authenticate(
            &connect.client_id,
            connect.username.as_deref(),
            connect.password.as_deref(),
        ) {
            Ok(id) => id,
            Err(e) => {
                let code = match e {
                    crate::utils::AuthError::BadCredentials => connack_code::BAD_USERNAME_PASSWORD,
                    crate::utils::AuthError::UnauthorizedClientId => connack_code::IDENTIFIER_REJECTED,
                    crate::utils::AuthError::AnonymousForbidden => connack_code::NOT_AUTHORIZED,
                    crate::utils::AuthError::TooManyConnections => connack_code::SERVER_UNAVAILABLE,
                };
                warn!(peer = %peer, code, "auth failed");
                let _ = sink.send(Packet::connack_error(code)).await;
                METRICS.inc_disconnect();
                return Ok(());
            }
        };

        // 4. 设置上下文
        let client_id = connect.client_id.clone();
        let clean = connect.clean_session;
        let keep_alive = connect.keep_alive;
        ctx.client_id = Some(client_id.clone());
        ctx.keep_alive = keep_alive;
        ctx.protocol_level = connect.protocol_level;
        ctx.authenticated = true;
        ctx.username = identity.username;
        ctx.touch();

        // 5. 注册会话（含接管旧连接 + 取回离线消息）
        // MQTT 5.0：提取 Session Expiry Interval（仅 clean=false 生效）
        let session_expiry = connect
            .properties
            .as_ref()
            .and_then(|p| p.session_expiry_interval);
        let cap = self.config.broker.max_inflight.max(16);
        let (tx, mut rx) = mpsc::channel::<OutboundPublish>(cap);
        let will = connect.will.as_ref().map(WillMessage::from);
        let (epoch, session_present, offline_messages) = self.sessions.register(
            client_id.clone(),
            clean,
            tx,
            will,
            connect.protocol_level,
            peer,
            session_expiry,
        );
        let session_present = session_present && !clean;

        METRICS.inc_connections();
        info!(
            client = %client_id,
            peer = %peer,
            user = ?ctx.username,
            clean,
            keep_alive,
            session_present,
            pending_offline = offline_messages.len(),
            "client connected"
        );

        // 6. CONNACK（按协议级别编码：5.0 带属性段，3.1.1 不带）
        let connack = Packet::Connack(Connack {
            session_present,
            return_code: connack_code::ACCEPTED,
            protocol_level: connect.protocol_level,
        });
        if sink.send(connack).await.is_err() {
            self.cleanup(&client_id, epoch, clean);
            return Ok(());
        }

        // 7. 主循环
        let mut heartbeat = HeartbeatTimer::new(keep_alive);
        let mut my_filters: HashSet<String> = HashSet::new();
        let mut next_pid: u16 = 1;
        let max_subs = self.config.broker.max_subscriptions_per_client;

        // QoS1/QoS2 出站 inflight 表（每连接独立）
        let mut outbound_inflight = OutboundInflightTable::new();
        // 入站 QoS2 去重表（每连接独立）
        let mut inbound_qos2 = InboundQos2Tracker::new();

        // inflight 重传周期：默认 10s 检查一次
        let retry_timeout = Duration::from_secs(
            self.config.broker.retry_interval_secs.unwrap_or(10),
        );
        let max_retries = self.config.broker.max_retries.unwrap_or(3);
        let mut retry_tick: Interval = tokio::time::interval(retry_timeout);
        // 重传定时器与心跳一样采用 Delay 行为，避免堆积
        retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // 8. 投递离线消息（clean_session=false 会话恢复时）
        // 这些消息需要走 outbound 路径（分配 packet_id + 进 inflight）
        for msg in offline_messages {
            if sink.send(self.build_outbound_packet(
                &msg,
                &mut next_pid,
                &mut outbound_inflight,
            )).await.is_err() {
                self.on_abnormal_disconnect(&client_id, epoch, clean);
                return Ok(());
            }
        }
        // 离线消息已由内存队列取出并投递，清空磁盘上的同名离线记录
        // （磁盘 offline 仅用于 broker 重启后恢复；运行时内存队列是 source of truth）
        self.clear_disk_offline(&client_id);

        loop {
            tokio::select! {
                biased;
                // 关闭信号优先
                res = shutdown.changed() => {
                    if res.is_ok() && *shutdown.borrow() {
                        debug!(client = %client_id, "shutdown signal, closing");
                        let _ = sink.send(Packet::Disconnect).await;
                        self.cleanup(&client_id, epoch, clean);
                        break;
                    }
                }
                // 心跳检测
                _ = heartbeat.tick() => {
                    if ctx.is_keep_alive_expired() {
                        warn!(client = %client_id, "keep-alive timeout, closing");
                        self.on_abnormal_disconnect(&client_id, epoch, clean);
                        break;
                    }
                }
                // 出站 inflight 重传检查
                _ = retry_tick.tick() => {
                    let (to_resend, failed) = outbound_inflight.retry_expired(retry_timeout, max_retries);
                    for (_pid, pkt) in to_resend {
                        if sink.send(pkt).await.is_err() {
                            self.on_abnormal_disconnect(&client_id, epoch, clean);
                            // 跳出 select 后需要外部 break
                            return Ok(());
                        }
                    }
                    if !failed.is_empty() {
                        warn!(client = %client_id, count = failed.len(), "inflight messages exceeded max retries, dropped");
                    }
                }
                // 入站报文
                item = stream.next() => {
                    match item {
                        Some(Ok(packet)) => {
                            ctx.touch();
                            match self.handle_packet(
                                packet,
                                &client_id,
                                &mut my_filters,
                                max_subs,
                                &mut sink,
                                &mut next_pid,
                                &mut outbound_inflight,
                                &mut inbound_qos2,
                            ).await {
                                Ok(Control::Continue) => {}
                                Ok(Control::Disconnect) => {
                                    debug!(client = %client_id, "client sent DISCONNECT");
                                    // 主动断开：清除遗嘱（不触发）
                                    self.sessions.clear_will(&client_id, epoch);
                                    self.cleanup(&client_id, epoch, clean);
                                    break;
                                }
                                Err(e) => {
                                    warn!(client = %client_id, error = %e, "handle packet error, closing");
                                    self.on_abnormal_disconnect(&client_id, epoch, clean);
                                    break;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!(client = %client_id, error = %e, "codec error, closing");
                            self.on_abnormal_disconnect(&client_id, epoch, clean);
                            break;
                        }
                        None => {
                            debug!(client = %client_id, "stream ended");
                            self.on_abnormal_disconnect(&client_id, epoch, clean);
                            break;
                        }
                    }
                }
                // 出站投递（来自路由器）
                req = rx.recv() => {
                    let Some(req) = req else {
                        // 发送端已 drop（理论上不会，会话持有 tx）；防御性退出
                        break;
                    };
                    let pkt = self.build_outbound_packet(&req, &mut next_pid, &mut outbound_inflight);
                    if sink.send(pkt).await.is_err() {
                        self.on_abnormal_disconnect(&client_id, epoch, clean);
                        break;
                    }
                }
            }
        }

        METRICS.dec_connections();
        METRICS.inc_disconnect();
        info!(client = %client_id, "connection closed");
        Ok(())
    }

    /// 构造一条出站 PUBLISH 报文，必要时分配 packet_id 并登记 inflight
    fn build_outbound_packet(
        &self,
        req: &OutboundPublish,
        next_pid: &mut u16,
        inflight: &mut OutboundInflightTable,
    ) -> Packet {
        if req.qos == QoS::AtMostOnce {
            // QoS0：无需 inflight
            return Router::build_publish(req.clone(), None);
        }
        let pid = allocate_pid(next_pid);
        let entry = OutboundInflight::new(
            req.topic.clone(),
            req.payload.clone(),
            req.qos,
            req.retain,
        );
        inflight.insert(pid, entry);
        Router::build_publish(req.clone(), Some(pid))
    }

    /// 处理一条入站报文
    #[allow(clippy::too_many_arguments)]
    async fn handle_packet<S>(
        &self,
        packet: Packet,
        client_id: &str,
        my_filters: &mut HashSet<String>,
        max_subs: usize,
        sink: &mut FramedWrite<tokio::io::WriteHalf<S>, MqttCodec>,
        next_pid: &mut u16,
        outbound_inflight: &mut OutboundInflightTable,
        inbound_qos2: &mut InboundQos2Tracker,
    ) -> BrokerResult<Control>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match packet {
            Packet::Publish(p) => {
                self.handle_publish(p, client_id, sink, inbound_qos2).await?;
            }
            Packet::Subscribe(s) => {
                self.handle_subscribe(s, client_id, my_filters, max_subs, sink, next_pid, outbound_inflight).await?;
            }
            Packet::Unsubscribe(u) => {
                let packet_id = u.packet_id;
                for f in &u.topic_filters {
                    let _ = self.subscriptions.unsubscribe(client_id, f);
                    my_filters.remove(f);
                    debug!(client = %client_id, filter = %f, "unsubscribed");
                }
                sink.send(Packet::Unsuback(packet_id)).await?;
                // 订阅列表变更：同步落盘会话快照
                self.persist_session_snapshot(client_id);
            }
            Packet::Pingreq => {
                sink.send(Packet::Pingresp).await?;
            }
            Packet::Disconnect => {
                return Ok(Control::Disconnect);
            }
            Packet::Puback(id) => {
                // QoS1 出站消息确认：移除 inflight
                if outbound_inflight.remove(id).is_none() {
                    debug!(client = %client_id, pid = id, "PUBACK for unknown inflight");
                }
            }
            Packet::Pubrec(id) => {
                // QoS2 出站消息第一步确认：推进到 WaitPubcomp，发送 PUBREL
                match outbound_inflight.get_mut(id) {
                    Some(e) => {
                        e.stage = AckStage::WaitPubcomp;
                        e.last_sent_at = std::time::Instant::now();
                    }
                    None => {
                        debug!(client = %client_id, pid = id, "PUBREC for unknown inflight");
                    }
                }
                // 无论是否找到 inflight，都按协议回 PUBREL（避免客户端死等）
                sink.send(Packet::Pubrel(id)).await?;
            }
            Packet::Pubrel(id) => {
                // 入站 QoS2 第四步：清理入站 inflight，回 PUBCOMP
                inbound_qos2.on_pubrel(id);
                sink.send(Packet::Pubcomp(id)).await?;
            }
            Packet::Pubcomp(id) => {
                // QoS2 出站流结束：清理 inflight
                if outbound_inflight.remove(id).is_none() {
                    debug!(client = %client_id, pid = id, "PUBCOMP for unknown inflight");
                }
            }
            Packet::Connect(_) | Packet::Connack(_) | Packet::Suback(_) | Packet::Unsuback(_)
            | Packet::Pingresp => {
                // 协议违规：客户端不应发送这些报文
                return Err(BrokerError::Codec(crate::utils::CodecError::MalformedBody(
                    "unexpected packet from client".into(),
                )));
            }
        }
        Ok(Control::Continue)
    }

    async fn handle_publish<S>(
        &self,
        p: Publish,
        client_id: &str,
        sink: &mut FramedWrite<tokio::io::WriteHalf<S>, MqttCodec>,
        inbound_qos2: &mut InboundQos2Tracker,
    ) -> BrokerResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // 安全中间件：载荷长度 + PUBLISH 速率限流
        // 拒绝时仍按协议回 ACK（QoS1/2），避免客户端死等；但不路由消息
        let security_rejected = match self.security.check_publish(
            client_id,
            p.payload.len(),
            self.config.broker.max_packet_size,
        ) {
            Ok(()) => false,
            Err(e) => {
                METRICS.inc_security_rejected();
                warn!(client = %client_id, error = %e, "PUBLISH rejected by security guard");
                true
            }
        };

        // 消息插件：主题 ACL + 载荷内容过滤
        // 拒绝时同样回 ACK 但不路由，避免客户端死等
        let plugin_rejected = if security_rejected {
            false
        } else {
            match self.plugin.check_publish(&p) {
                Ok(()) => false,
                Err(e) => {
                    METRICS.inc_plugin_rejected();
                    warn!(client = %client_id, error = %e, "PUBLISH rejected by plugin guard");
                    true
                }
            }
        };

        METRICS.inc_publish();
        METRICS.inc_publish_qos(match p.qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        });

        // 生成轻量 TraceID，贯穿路由→投递链路（便于 grep 定位单条消息流向）
        let trace_id = crate::utils::time::trace_id();
        debug!(%trace_id, client = %client_id, topic = %p.topic, ?p.qos, payload_len = p.payload.len(), "PUBLISH received");

        // QoS2 入站去重：若已存在同 packet_id 的 inflight，则不重复路由
        let should_route = if security_rejected || plugin_rejected {
            false
        } else if p.qos == QoS::ExactlyOnce {
            if let Some(id) = p.packet_id {
                !inbound_qos2.on_publish(id)
            } else {
                // QoS2 必须有 packet_id；非法报文
                return Err(BrokerError::Codec(crate::utils::CodecError::MalformedBody(
                    "QoS2 PUBLISH missing packet_id".into(),
                )));
            }
        } else {
            true
        };

        if should_route {
            self.router.route_inbound_publish(&p, Some(client_id), &trace_id)?;
            // 消息插件：HTTP 转发 hook（非阻塞，仅 try_send 到后台队列）
            // 仅在通过所有检查并已路由后才转发，避免转发被拒绝的消息
            self.plugin.try_forward(&p, Some(client_id));
        }

        // QoS 应答
        match p.qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => {
                if let Some(id) = p.packet_id {
                    sink.send(Packet::Puback(id)).await?;
                }
            }
            QoS::ExactlyOnce => {
                if let Some(id) = p.packet_id {
                    sink.send(Packet::Pubrec(id)).await?;
                }
            }
        }
        Ok(())
    }

    /// 处理 SUBSCRIBE 报文：注册订阅、回 SUBACK、投递匹配的 retained 消息
    #[allow(clippy::too_many_arguments)]
    async fn handle_subscribe<S>(
        &self,
        s: crate::codec::Subscribe,
        client_id: &str,
        my_filters: &mut HashSet<String>,
        max_subs: usize,
        sink: &mut FramedWrite<tokio::io::WriteHalf<S>, MqttCodec>,
        next_pid: &mut u16,
        outbound_inflight: &mut OutboundInflightTable,
    ) -> BrokerResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let packet_id = s.packet_id;
        let mut return_codes = Vec::with_capacity(s.topics.len());
        // 收集 (filter, qos) 用于 retained 投递
        let mut accepted: Vec<(String, QoS)> = Vec::new();
        for t in s.topics {
            if !my_filters.contains(&t.topic_filter) && my_filters.len() >= max_subs {
                return_codes.push(suback_code::FAILURE);
                continue;
            }
            // 消息插件：订阅主题 ACL 检查（拒绝时回 FAILURE，不注册订阅）
            if let Err(e) = self.plugin.check_subscribe(&t.topic_filter) {
                warn!(client = %client_id, filter = %t.topic_filter, error = %e, "SUBSCRIBE rejected by plugin guard");
                return_codes.push(suback_code::FAILURE);
                continue;
            }
            match self.subscriptions.subscribe(client_id, &t.topic_filter, t.qos) {
                Ok(()) => {
                    my_filters.insert(t.topic_filter.clone());
                    return_codes.push(t.qos.as_u8());
                    METRICS.inc_subscribe();
                    debug!(client = %client_id, filter = %t.topic_filter, qos = %t.qos, "subscribed");
                    accepted.push((t.topic_filter, t.qos));
                }
                Err(_) => return_codes.push(suback_code::FAILURE),
            }
        }
        sink.send(Packet::Suback(crate::codec::Suback { packet_id, return_codes }))
            .await?;

        // 投递匹配的 retained 消息
        for (filter, sub_qos) in accepted {
            let retained = self.retain.matches(&filter);
            for msg in retained {
                let out = RetainStore::build_outbound(&msg, sub_qos);
                let pkt = self.build_outbound_packet(&out, next_pid, outbound_inflight);
                if sink.send(pkt).await.is_err() {
                    // sink 故障：交由外层主循环处理；这里直接返回错误
                    return Err(BrokerError::ConnectionClosed("sink closed during retained delivery".into()));
                }
            }
        }

        // 订阅列表变更：同步落盘会话快照（仅 clean_session=false 才有意义，但统一调用无副作用）
        self.persist_session_snapshot(client_id);
        Ok(())
    }

    /// 异常断开：触发遗嘱消息（若仍持有会话）
    fn on_abnormal_disconnect(&self, client_id: &str, epoch: u64, clean: bool) {
        if self.sessions.owns(client_id, epoch) {
            if let Some(will) = self.sessions.take_will(client_id, epoch) {
                let trace_id = crate::utils::time::trace_id();
                debug!(%trace_id, client = %client_id, topic = %will.topic, "firing last will");
                let _ = self.router.publish(
                    &will.topic,
                    &will.message,
                    will.qos,
                    will.retain,
                    Some(client_id),
                    &trace_id,
                );
            }
        }
        self.cleanup(client_id, epoch, clean);
    }

    /// 会话清理（处理接管：仅当仍持有会话时清理）
    /// MQTT 5.0：clean=false 且 session_expiry=0 时，会话立即过期（等同 clean=true）
    fn cleanup(&self, client_id: &str, epoch: u64, clean: bool) {
        if !self.sessions.owns(client_id, epoch) {
            // 已被新连接接管，不动新会话
            return;
        }
        if clean {
            self.subscriptions.unsubscribe_all(client_id);
            self.sessions.remove(client_id);
            // clean 会话：删除磁盘上的会话快照与残留离线消息
            self.delete_session_snapshot(client_id);
        } else {
            // 先 mark_offline 记录离线时间戳，用于后续 session_expiry 判断
            self.sessions.mark_offline(client_id, epoch);
            // MQTT 5.0：session_expiry=0 表示立即过期，应清理而非保留
            if self.sessions.is_session_expired(client_id) {
                debug!(client = %client_id, "session expired (expiry=0), cleaning up");
                self.subscriptions.unsubscribe_all(client_id);
                self.sessions.remove(client_id);
                self.delete_session_snapshot(client_id);
                return;
            }
            // 持久会话：落盘当前订阅快照，便于 broker 重启后恢复
            self.persist_session_snapshot(client_id);
        }
    }

    /// 后台周期性扫描并清理已过期的离线会话（session_expiry 到期）
    /// 由 main.rs 启动时 spawn；每 `interval` 扫描一次
    pub fn spawn_session_expiry_sweeper(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let broker = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let expired = broker.sessions.cleanup_expired();
                for client_id in &expired {
                    broker.subscriptions.unsubscribe_all(client_id);
                    broker.delete_session_snapshot(client_id);
                    METRICS.inc_sessions_expired();
                    debug!(client = %client_id, "session expired and cleaned up by sweeper");
                }
                if !expired.is_empty() {
                    info!(count = expired.len(), "sweeper removed expired sessions");
                }
            }
        })
    }
}

/// 分配一个未使用的 packet_id（绕过 0）
fn allocate_pid(next: &mut u16) -> u16 {
    let p = *next;
    *next = next.wrapping_add(1);
    if *next == 0 {
        *next = 1;
    }
    p
}
