use std::sync::Arc;

use lumenmq::{
    admin::AdminServer,
    broker::{Authenticator, BrokerState},
    config::ConfigLoader,
    monitor::init_logging,
    net::{load_tls_acceptor, new_shutdown_channel, MqttSnServer, TcpServer, TlsServer, WsServer},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    // 1. 加载配置
    let settings = match ConfigLoader::from_env().load() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("FATAL: load config failed: {e}");
            std::process::exit(1);
        }
    };

    // 2. 初始化日志（guard 需保活）
    let _log_guard = init_logging(&settings.log);

    info!(
        node = %settings.broker.node_id,
        version = env!("CARGO_PKG_VERSION"),
        "LumenMQ starting"
    );

    // 3. 组装 Broker 状态
    let auth = Arc::new(Authenticator::new(Arc::new(settings.auth.clone())));
    let settings = Arc::new(settings);
    let broker = BrokerState::new(settings.clone(), auth);

    // 4. 关闭信号通道
    let (shutdown_tx, shutdown_rx) = new_shutdown_channel();

    // 5. 启动各监听服务
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    if settings.tcp.enabled {
        let bind = settings.tcp.bind.clone();
        let server = TcpServer::new(
            parse_bind(&bind),
            broker.clone(),
            settings.broker.max_connections,
            shutdown_rx.clone(),
        );
        let handle = tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!(error = %e, "TCP server exited with error");
            }
        });
        handles.push(handle);
        info!(bind = %bind, "TCP listener task spawned");
    } else {
        info!("TCP listener disabled by config");
    }

    // TLS 加密监听（单向 / 双向证书认证）
    if settings.tls.enabled {
        let bind = settings.tls.bind.clone();
        let mutual = settings.tls.mutual;
        match load_tls_acceptor(&settings.tls) {
            Ok(acceptor) => {
                let server = TlsServer::new(
                    parse_bind(&bind),
                    broker.clone(),
                    settings.broker.max_connections,
                    shutdown_rx.clone(),
                    acceptor,
                );
                let handle = tokio::spawn(async move {
                    if let Err(e) = server.run().await {
                        error!(error = %e, "TLS server exited with error");
                    }
                });
                handles.push(handle);
                info!(bind = %bind, mutual, "TLS listener task spawned");
            }
            Err(e) => {
                error!(error = %e, "load TLS acceptor failed, TLS listener disabled");
            }
        }
    } else {
        info!("TLS listener disabled by config");
    }

    // WebSocket MQTT 监听（兼容网页前端设备）
    if settings.websocket.enabled {
        let bind = settings.websocket.bind.clone();
        let path = if settings.websocket.path.is_empty() {
            "/mqtt".to_string()
        } else {
            settings.websocket.path.clone()
        };
        let server = WsServer::new(
            parse_bind(&bind),
            broker.clone(),
            settings.broker.max_connections,
            shutdown_rx.clone(),
            path.clone(),
        );
        let handle = tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!(error = %e, "WebSocket server exited with error");
            }
        });
        handles.push(handle);
        info!(bind = %bind, path = %path, "WebSocket listener task spawned");
    } else {
        info!("WebSocket listener disabled by config");
    }

    // MQTT-SN UDP 监听（低功耗传感器设备）
    if settings.mqtt_sn.enabled {
        let bind = settings.mqtt_sn.bind.clone();
        let server = MqttSnServer::new(
            parse_bind(&bind),
            broker.clone(),
            settings.broker.max_connections,
            shutdown_rx.clone(),
        );
        let handle = tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!(error = %e, "MQTT-SN server exited with error");
            }
        });
        handles.push(handle);
        info!(bind = %bind, "MQTT-SN UDP listener task spawned");
    } else {
        info!("MQTT-SN listener disabled by config");
    }

    // 阶段五：Admin HTTP 运维 API（/health、/metrics、/api/v1/*）
    if settings.admin.enabled {
        let bind = parse_bind(&settings.admin.bind);
        let admin_server = AdminServer::new(bind, broker.clone());
        let admin_rx = shutdown_rx.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = admin_server.run(admin_rx).await {
                error!(error = %e, "Admin HTTP server exited with error");
            }
        });
        handles.push(handle);
        info!(bind = %settings.admin.bind, "Admin HTTP listener task spawned");
    } else {
        info!("Admin HTTP listener disabled by config");
    }

    // MQTT 5.0 会话过期后台扫描（每 30s 清理一次 session_expiry 到期的离线会话）
    let sweeper_handle = broker.clone().spawn_session_expiry_sweeper(
        std::time::Duration::from_secs(30),
        shutdown_rx.clone(),
    );
    info!("session expiry sweeper task spawned (interval=30s)");

    // 6. 信号监听：Ctrl+C / SIGTERM 触发优雅关闭
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        wait_for_signal().await;
        info!("shutdown signal received, draining...");
        let _ = shutdown_tx_clone.send(true);
    });

    // 7. 等待所有服务退出
    for h in handles {
        let _ = h.await;
    }
    // 等待 sweeper 退出（最多 2 秒，避免卡死关停）
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), sweeper_handle).await;

    // 8. 优雅关停持久化存储：flush 未落盘数据到磁盘
    // sled::Db::flush() 会同步等待磁盘持久化（阻塞 IO），不能直接在 async 上下文调用，
    // 否则会卡住 tokio 运行时；用 spawn_blocking 把它转移到阻塞线程池执行。
    if let Some(storage) = broker.storage() {
        let storage = storage.clone();
        match tokio::task::spawn_blocking(move || storage.flush()).await {
            Ok(Ok(())) => info!("storage flushed on shutdown"),
            Ok(Err(e)) => error!(error = %e, "flush storage on shutdown failed"),
            Err(e) => error!(error = %e, "flush storage task join failed"),
        }
    }
    info!("LumenMQ stopped, bye");
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("received SIGTERM"),
            _ = sigint.recv() => info!("received SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
        info!("received Ctrl+C");
    }
}

fn parse_bind(bind: &str) -> std::net::SocketAddr {
    bind.parse().unwrap_or_else(|e| {
        eprintln!("FATAL: invalid bind address '{bind}': {e}");
        std::process::exit(1);
    })
}
