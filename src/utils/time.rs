use chrono::{DateTime, Utc};

/// 当前 UTC 时间戳（秒）
#[inline]
pub fn now_secs() -> i64 {
    Utc::now().timestamp()
}

/// 当前 UTC 毫秒时间戳
#[inline]
pub fn now_millis() -> i64 {
    Utc::now().timestamp_millis()
}

/// 当前 UTC 时间
#[inline]
pub fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

/// 生成一个短随机 ID（用于内部连接 ID）
pub fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..12].to_string()
}

/// 生成一个轻量 TraceID（8 字符十六进制，用于 PUBLISH 消息链路追踪）
/// 格式：低冲突、可读、便于 grep；约 4 亿种组合，对单节点消息追踪足够
pub fn trace_id() -> String {
    let rand_part = uuid::Uuid::new_v4().as_u128() as u32;
    format!("{:08x}", rand_part)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_is_8_hex_chars() {
        let id = trace_id();
        assert_eq!(id.len(), 8);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn trace_ids_are_unique_enough() {
        let mut set = std::collections::HashSet::new();
        for _ in 0..1000 {
            set.insert(trace_id());
        }
        // 1000 个 ID 中允许极少量冲突（概率极低），但应 > 990 个不同
        assert!(set.len() > 990, "too many collisions: {}", 1000 - set.len());
    }
}
