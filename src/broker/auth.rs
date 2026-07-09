use std::sync::Arc;

use crate::config::{AuthConfig, AuthMode};
use crate::codec::LastWill;
use crate::utils::AuthError;

/// 鉴权结果
pub struct AuthIdentity {
    pub username: Option<String>,
}

/// 鉴权器：基于配置的内置账号（阶段四扩展为 token / 证书 / 外部认证）
pub struct Authenticator {
    cfg: Arc<AuthConfig>,
}

impl Authenticator {
    pub fn new(cfg: Arc<AuthConfig>) -> Self {
        Self { cfg }
    }

    /// 校验 CONNECT 携带的凭证
    pub fn authenticate(
        &self,
        client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<AuthIdentity, AuthError> {
        // client_id 合法性
        // 空 client_id 由 broker 在调用 auth 前处理（clean=true 时分配 UUID，
        // clean=false 时拒绝）；此处仅校验长度上限
        if client_id.len() > 65535 {
            return Err(AuthError::UnauthorizedClientId);
        }

        match self.cfg.mode {
            AuthMode::Anonymous => {
                if !self.cfg.allow_anonymous {
                    return Err(AuthError::AnonymousForbidden);
                }
                Ok(AuthIdentity { username: None })
            }
            AuthMode::UsernamePassword | AuthMode::Token => {
                // 允许匿名直通
                if self.cfg.allow_anonymous && username.is_none() {
                    return Ok(AuthIdentity { username: None });
                }
                let username = username.ok_or(AuthError::BadCredentials)?;
                let password = password.unwrap_or(&[]);

                // 匹配内置账号
                let matched = self.cfg.users.iter().find(|u| u.username == username);
                let user = match matched {
                    Some(u) => u,
                    None => return Err(AuthError::BadCredentials),
                };

                // 拒绝空密码配置（防止 config 中 password="" 导致无密码登录）
                if user.password.is_empty() {
                    tracing::warn!(username = %username, "configured user has empty password, rejecting");
                    return Err(AuthError::BadCredentials);
                }

                // 常数时间比对，避免计时侧信道泄露密码
                if !constant_time_eq(user.password.as_bytes(), password) {
                    return Err(AuthError::BadCredentials);
                }
                Ok(AuthIdentity {
                    username: Some(username.to_string()),
                })
            }
        }
    }

    /// 是否允许该 client_id 接入（占位：阶段四接入 ACL）
    pub fn authorize_client_id(&self, _client_id: &str) -> bool {
        true
    }

    pub fn config(&self) -> &AuthConfig {
        &self.cfg
    }
}

/// 遗嘱消息（从编解码层 LastWill 拷贝，便于跨任务传递）
#[derive(Debug, Clone)]
pub struct WillMessage {
    pub topic: String,
    pub message: Vec<u8>,
    pub qos: crate::codec::QoS,
    pub retain: bool,
}

impl From<&LastWill> for WillMessage {
    fn from(w: &LastWill) -> Self {
        Self {
            topic: w.topic.clone(),
            message: w.message.clone(),
            qos: w.qos,
            retain: w.retain,
        }
    }
}

/// 常数时间字节序列比较，避免计时侧信道泄露密码 / token
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
