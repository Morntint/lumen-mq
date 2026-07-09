use std::io::{self, Write};

use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{fmt::MakeWriter, prelude::*, EnvFilter};

use crate::config::{LogConfig, LogFormat, LogRotation};

/// 初始化全局日志：stdout + 可选滚动文件（tee 输出，单 fmt 层）
/// 返回 WorkerGuard，调用方需保活以保证文件日志刷新
pub fn init_logging(cfg: &LogConfig) -> Option<WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.level));

    // 文件 writer（可选）
    let (file_writer, guard) = match build_file_writer(cfg) {
        Some((w, g)) => (Some(w), Some(g)),
        None => (None, None),
    };

    let maker = TeeMaker { file: file_writer };
    // 启用文件日志时关闭 ANSI 着色，避免转义码污染文件
    let ansi = guard.is_none();

    let sub = tracing_subscriber::registry().with(env_filter);
    match cfg.format {
        LogFormat::Json => {
            let mut l = tracing_subscriber::fmt::layer().json().with_writer(maker);
            if !ansi {
                l = l.with_ansi(false);
            }
            sub.with(l).init();
        }
        LogFormat::Compact => {
            let mut l = tracing_subscriber::fmt::layer().compact().with_writer(maker);
            if !ansi {
                l = l.with_ansi(false);
            }
            sub.with(l).init();
        }
        LogFormat::Full => {
            let mut l = tracing_subscriber::fmt::layer().with_writer(maker);
            if !ansi {
                l = l.with_ansi(false);
            }
            sub.with(l).init();
        }
    }
    guard
}

fn build_file_writer(cfg: &LogConfig) -> Option<(NonBlocking, WorkerGuard)> {
    if cfg.dir.is_empty() {
        return None;
    }
    let rotation = match cfg.rotation {
        LogRotation::Hourly => tracing_appender::rolling::Rotation::HOURLY,
        LogRotation::Daily => tracing_appender::rolling::Rotation::DAILY,
        LogRotation::Never => tracing_appender::rolling::Rotation::NEVER,
    };
    let appender = match tracing_appender::rolling::Builder::new()
        .rotation(rotation)
        .filename_prefix("lumenmq")
        .filename_suffix("log")
        .build(&cfg.dir)
    {
        Ok(a) => a,
        Err(e) => {
            eprintln!("init file appender failed: {e}, fallback to stdout only");
            return None;
        }
    };
    let (nb, guard) = tracing_appender::non_blocking(appender);
    Some((nb, guard))
}

/// 同时写 stdout 与可选文件的非阻塞 writer
struct Tee {
    file: Option<NonBlocking>,
}

impl Write for Tee {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let _ = io::stdout().write_all(buf);
        if let Some(f) = self.file.as_mut() {
            let _ = f.write_all(buf);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        let _ = io::stdout().flush();
        if let Some(f) = self.file.as_mut() {
            let _ = f.flush();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct TeeMaker {
    file: Option<NonBlocking>,
}

impl<'a> MakeWriter<'a> for TeeMaker {
    type Writer = Tee;
    fn make_writer(&'a self) -> Self::Writer {
        Tee {
            file: self.file.clone(),
        }
    }
}
