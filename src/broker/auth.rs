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
        if client_id.is_empty() {
            // MQTT 允许空 client_id 仅当 clean_session=true；此处简化为拒绝
            return Err(AuthError::UnauthorizedClientId);
        }
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

                // 简单明文比对（阶段四接入加盐哈希 / token 验签）
                if user.password.as_bytes() != password {
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
