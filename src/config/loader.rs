use crate::utils::{BrokerError, BrokerResult};
use std::path::{Path, PathBuf};

use super::settings::Settings;

/// 配置加载器：合并 default.toml -> profile.toml -> 环境变量覆盖
pub struct ConfigLoader {
    config_dir: PathBuf,
    profile: Option<String>,
}

impl ConfigLoader {
    pub fn new(config_dir: impl Into<PathBuf>) -> Self {
        Self { config_dir: config_dir.into(), profile: None }
    }

    /// 设置运行 profile（dev/prod），将加载 `config/<profile>.toml` 覆盖默认配置
    pub fn with_profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }

    /// 从环境变量自动构造
    ///   LUMENMQ_CONFIG_DIR  配置目录（默认 ./config）
    ///   LUMENMQ_PROFILE     运行环境（dev/prod）
    pub fn from_env() -> Self {
        let dir = std::env::var("LUMENMQ_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("config"));
        let mut loader = Self::new(dir);
        if let Ok(profile) = std::env::var("LUMENMQ_PROFILE") {
            loader = loader.with_profile(profile);
        }
        loader
    }

    /// 加载并合并配置
    pub fn load(&self) -> BrokerResult<Settings> {
        // 1. 读取 default.toml
        let default_path = self.config_dir.join("default.toml");
        let mut value: toml::Value = read_toml(&default_path)?;

        // 2. 读取 profile 文件并深度合并
        if let Some(profile) = &self.profile {
            let profile_path = self.config_dir.join(format!("{profile}.toml"));
            if profile_path.exists() {
                let overlay = read_toml(&profile_path)?;
                deep_merge(&mut value, overlay);
            } else {
                tracing::warn!(profile = %profile, path = %profile_path.display(), "profile config not found, skipped");
            }
        }

        // 3. 环境变量覆盖（仅对部分关键字段）
        apply_env_overrides(&mut value);

        // 4. 反序列化为 Settings
        let settings: Settings = value
            .try_into()
            .map_err(|e| BrokerError::Config(format!("deserialize failed: {e}")))?;

        // 5. 校验
        super::validate::validate(&settings)?;
        Ok(settings)
    }
}

fn read_toml(path: &Path) -> BrokerResult<toml::Value> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| BrokerError::Config(format!("read {} failed: {e}", path.display())))?;
    toml::from_str(&text)
        .map_err(|e| BrokerError::Config(format!("parse {} failed: {e}", path.display())))
}

/// 深度合并 `overlay` 到 `base`（table 递归，其它类型直接覆盖）
fn deep_merge(base: &mut toml::Value, overlay: toml::Value) {
    use toml::Value::*;
    match (base, overlay) {
        (Table(base_tbl), Table(overlay_tbl)) => {
            for (k, v) in overlay_tbl {
                if let Some(existing) = base_tbl.get_mut(&k) {
                    deep_merge(existing, v);
                } else {
                    base_tbl.insert(k, v);
                }
            }
        }
        (base_val, overlay_val) => {
            *base_val = overlay_val;
        }
    }
}

/// 关键运行参数的环境变量覆盖（工业部署常用）
fn apply_env_overrides(value: &mut toml::Value) {
    use toml::Value::*;
    let Some(tbl) = value.as_table_mut() else { return };

    // 辅助：在 tbl 下取/建一个子 table，若已存在但非 table 类型则 warn 并跳过，
    // 避免 panic（default.toml 中误把 section 写成标量时不应崩溃）
    macro_rules! get_sub_table {
        ($key:expr) => {{
            let entry = tbl
                .entry($key)
                .or_insert_with(|| Table(Default::default()));
            match entry.as_table_mut() {
                Some(t) => Some(t),
                None => {
                    tracing::warn!(
                        key = $key,
                        "config section is not a table, skipping env override"
                    );
                    None
                }
            }
        }};
    }

    if let Ok(bind) = std::env::var("LUMENMQ_TCP_BIND") {
        if let Some(t) = get_sub_table!("tcp") {
            t.insert("bind".into(), String(bind));
        }
    }
    if let Ok(level) = std::env::var("LUMENMQ_LOG_LEVEL") {
        if let Some(t) = get_sub_table!("log") {
            t.insert("level".into(), String(level));
        }
    }
    if let Ok(node) = std::env::var("LUMENMQ_NODE_ID") {
        if let Some(t) = get_sub_table!("broker") {
            t.insert("node_id".into(), String(node));
        }
    }
}
