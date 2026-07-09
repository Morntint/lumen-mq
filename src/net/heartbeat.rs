use std::time::Duration;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

/// 心跳检测定时器：按 keep_alive 的 1.5 倍间隔唤醒
pub struct HeartbeatTimer {
    inner: tokio::time::Interval,
}

impl HeartbeatTimer {
    /// 创建心跳检测定时器。`keep_alive_secs=0` 表示禁用（间隔设为极大值）
    pub fn new(keep_alive_secs: u16) -> Self {
        let secs = if keep_alive_secs == 0 {
            3600
        } else {
            (keep_alive_secs as u64 * 3) / 2 + 1
        };
        let start = Instant::now() + Duration::from_secs(secs);
        let mut inner = interval_at(start, Duration::from_secs(secs));
        inner.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self { inner }
    }

    pub async fn tick(&mut self) {
        self.inner.tick().await;
    }
}
