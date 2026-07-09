use crate::utils::{BrokerError, BrokerResult};
use super::settings::Settings;

/// 配置合法性校验（端口、证书、存储参数等）
pub fn validate(s: &Settings) -> BrokerResult<()> {
    // 端口/bind 合法性
    validate_bind(&s.tcp.bind, "tcp")?;
    if s.tls.enabled {
        validate_bind(&s.tls.bind, "tls")?;
        if s.tls.cert.as_os_str().is_empty() || s.tls.key.as_os_str().is_empty() {
            return Err(BrokerError::Config("tls.cert / tls.key must be set when tls enabled".into()));
        }
    }
    if s.websocket.enabled {
        validate_bind(&s.websocket.bind, "websocket")?;
    }
    if s.mqtt_sn.enabled {
        validate_bind(&s.mqtt_sn.bind, "mqtt_sn")?;
    }
    if s.admin.enabled {
        validate_bind(&s.admin.bind, "admin")?;
        // 安全策略：admin API 暴露写操作（清理会话/重载配置/发布消息），
        // 若绑定非环回地址则必须配置 token，否则任何网络可达者都能操纵 broker。
        if s.admin.token.is_empty() && !is_loopback_bind(&s.admin.bind) {
            return Err(BrokerError::Config(
                "admin.token must be set when admin.bind is not loopback (non-127.0.0.1/::1)".into(),
            ));
        }
    }

    // broker 参数
    if s.broker.max_connections == 0 {
        return Err(BrokerError::Config("broker.max_connections must be > 0".into()));
    }
    if s.broker.max_packet_size < 16 {
        return Err(BrokerError::Config("broker.max_packet_size too small (<16)".into()));
    }
    if s.broker.max_inflight == 0 {
        return Err(BrokerError::Config("broker.max_inflight must be > 0".into()));
    }

    // 鉴权一致性
    if !s.auth.allow_anonymous && s.auth.mode == crate::config::settings::AuthMode::Anonymous {
        return Err(BrokerError::Config(
            "auth.mode=anonymous but allow_anonymous=false".into(),
        ));
    }
    if !s.auth.allow_anonymous && s.auth.users.is_empty() {
        return Err(BrokerError::Config("no auth.users configured and anonymous disabled".into()));
    }

    Ok(())
}

fn validate_bind(bind: &str, name: &str) -> BrokerResult<()> {
    // 用 SocketAddr::from_str 解析，正确处理 IPv6 形如 "[::1]:1883"
    let addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|_| BrokerError::Config(format!("{name}.bind invalid: '{bind}'")))?;
    if addr.port() == 0 {
        return Err(BrokerError::Config(format!("{name}.bind port must not be 0")));
    }
    Ok(())
}

/// 判断 bind 字符串是否绑定到环回地址（127.0.0.1 / ::1）
fn is_loopback_bind(bind: &str) -> bool {
    match bind.parse::<std::net::SocketAddr>() {
        Ok(addr) => addr.ip().is_loopback(),
        Err(_) => false,
    }
}
