# syntax=docker/dockerfile:1
# LumenMQ — 多阶段构建：Rust 编译 → 精简运行时镜像
#
# 构建命令：
#   docker build -t lumenmq:latest .
# 运行命令：
#   docker run -p 1883:1883 -p 9090:9090 lumenmq:latest
#
# 镜像特点：
# - 运行时基于 debian:bookworm-slim，约 120MB（含 broker 二进制）
# - 无编译工具链、无源码，减小攻击面
# - 非 root 用户运行

# ===== Stage 1: Builder =====
FROM rust:1.82-bookworm AS builder

WORKDIR /build

# 先复制依赖清单，利用 Docker 层缓存加速重复构建
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs

# 预编译依赖（此层在依赖不变时会被缓存复用）
RUN cargo build --release --bin lumenmq 2>/dev/null || true

# 复制实际源码与配置
COPY src/ src/
COPY config/ config/

# 清理 dummy 产物并编译真实二进制
RUN touch src/main.rs src/lib.rs && cargo build --release --bin lumenmq

# ===== Stage 2: Runtime =====
FROM debian:bookworm-slim AS runtime

# 安装最小运行时依赖（ca-certificates 供 reqwest HTTPS 转发；libgcc 供 sled/glibc；curl 供 healthcheck）
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libgcc-s1 curl && \
    rm -rf /var/lib/apt/lists/*

# 创建非 root 运行用户
RUN groupadd -r lumenmq && useradd -r -g lumenmq -s /sbin/nologin lumenmq

# 创建数据/日志/证书目录
RUN mkdir -p /app/config /app/data/lumenmq /app/logs /app/certs && \
    chown -R lumenmq:lumenmq /app

WORKDIR /app

# 从 builder 复制编译产物
COPY --from=builder /build/target/release/lumenmq /app/lumenmq
# 从 builder 复制默认配置
COPY --from=builder /build/config/ /app/config/

USER lumenmq

# 暴露端口：1883=MQTT TCP, 8883=MQTT TLS, 8083=WebSocket, 1884=MQTT-SN UDP, 9090=Admin HTTP
EXPOSE 1883 8883 8083 1884/udp 9090

# 数据卷：持久化存储、日志、证书
VOLUME ["/app/data", "/app/logs", "/app/certs", "/app/config"]

# 环境变量默认值
ENV LUMENMQ_CONFIG_DIR=/app/config
ENV LUMENMQ_PROFILE=prod
ENV RUST_LOG=info
ENV RUST_BACKTRACE=1

ENTRYPOINT ["/app/lumenmq"]
