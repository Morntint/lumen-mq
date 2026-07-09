# LumenMQ

> **声明：本项目仅用于学习与开发目的，不推荐用于生产环境。**
> **Disclaimer: This project is for learning and development purposes only. NOT recommended for production use.**

**轻量化工业级 MQTT Broker** — 使用 Rust 编写，面向工业物联网（IIoT）场景。

LumenMQ 实现了 MQTT 3.1.1 / 5.0 / MQTT-SN 多协议接入，内置安全中间件、消息插件、持久化存储与运维 HTTP API，单节点可承载十万级并发连接，适合在资源受限的工业网关与边缘节点上部署。

## 核心特性

| 能力 | 说明 |
|------|------|
| **多协议接入** | MQTT over TCP（1883）、TLS（8883）、WebSocket（8083）、MQTT-SN UDP（1884） |
| **MQTT 5.0** | 共享订阅（`$share/group/topic`）、Session Expiry、CONNACK 属性段、与 3.1.1 客户端共存 |
| **QoS 全栈** | QoS 0/1/2 完整握手、入站去重、出站 inflight 重传、离线消息队列 |
| **Retain 保留** | 精确主题 retain 存储与新订阅者匹配投递，可选 sled 持久化 |
| **持久化存储** | 基于 sled 的会话快照 / 离线消息 / retained 消息落盘，重启自动恢复 |
| **安全中间件** | IP 黑白名单（CIDR）、单 IP 连接数限制、PUBLISH 速率限流、载荷大小校验，支持热重载 |
| **消息插件** | 主题 ACL（发布/订阅黑白名单）、载荷关键字过滤、HTTP 转发 hook，支持热重载 |
| **运维 HTTP API** | `/health`、`/metrics`（Prometheus）、会话查询/清理、配置热重载、手动发布 |
| **可观测性** | 23 项运行时指标 Prometheus 导出、轻量 TraceID 贯穿 PUBLISH 链路、结构化日志（compact/json） |
| **Docker 部署** | 多阶段构建精简镜像、docker-compose 编排、健康检查、资源限制 |

## 快速开始

### 本地运行

```bash
# 编译并运行（默认加载 config/default.toml）
cargo run --release

# 指定 profile（加载 config/dev.toml 或 config/prod.toml）
LUMENMQ_PROFILE=dev cargo run --release
```

启动后默认监听：
- `1883` — MQTT TCP
- `8883` — MQTT TLS（需提供证书）
- `8083` — WebSocket MQTT
- `1884/udp` — MQTT-SN
- `9090` — Admin HTTP / Prometheus metrics

### 验证服务

```bash
# 健康检查
curl http://localhost:9090/health
# {"status":"ok","node_id":"lumenmq@127.0.0.1","version":"0.1.0",...}

# Prometheus 指标
curl http://localhost:9090/metrics

# 通过 Admin API 手动发布消息
curl -X POST http://localhost:9090/api/v1/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"demo/topic","payload":"hello","qos":0,"retain":false}'
```

### Docker 部署

```bash
# 构建并启动
docker compose up -d

# 查看日志
docker compose logs -f lumenmq

# 健康检查
curl http://localhost:9090/health
```

## 配置

配置文件位于 `config/` 目录，按优先级合并：`default.toml` → `<profile>.toml` → 环境变量覆盖。

| 环境变量 | 说明 | 默认值 |
|----------|------|--------|
| `LUMENMQ_CONFIG_DIR` | 配置目录路径 | `config` |
| `LUMENMQ_PROFILE` | 运行 profile（`dev`/`prod`） | 无 |
| `LUMENMQ_TCP_BIND` | TCP 监听地址覆盖 | 配置文件值 |
| `LUMENMQ_LOG_LEVEL` | 日志级别覆盖 | 配置文件值 |
| `LUMENMQ_NODE_ID` | 节点 ID 覆盖 | 配置文件值 |

主要配置段：

```toml
[broker]
node_id = "lumenmq@127.0.0.1"
max_connections = 100000
max_packet_size = 1048576        # 1 MiB
max_inflight = 1024

[tcp]
enabled = true
bind = "0.0.0.0:1883"

[auth]
mode = "username_password"       # anonymous | username_password | token
allow_anonymous = false

[storage]
enabled = false                  # 开启后启用 sled 持久化
path = "./data/lumenmq"

[admin]
enabled = false
bind = "0.0.0.0:9090"

[security]
enabled = false
ip_blacklist = []
max_connections_per_ip = 0       # 0 = 不限制
publish_rate_per_second = 0

[plugin]
enabled = false
[plugin.topic_acl]
publish_blacklist = []
[plugin.forward]
enabled = false
url = ""
```

## Admin HTTP API

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 健康检查，返回节点状态摘要 |
| GET | `/metrics` | Prometheus 文本格式指标 |
| GET | `/api/v1/sessions` | 查询在线/离线会话列表 |
| DELETE | `/api/v1/sessions/:client_id` | 清理指定会话（订阅+离线队列+持久化快照） |
| POST | `/api/v1/reload/security` | 热重载安全中间件配置 |
| POST | `/api/v1/reload/plugin` | 热重载消息插件配置 |
| POST | `/api/v1/publish` | 手动发布消息（运维测试用） |

## Prometheus 指标

共 23 项指标，关键项包括：

```
lumenmq_connections_total          # 累计连接数（counter）
lumenmq_connections_current        # 当前在线连接（gauge）
lumenmq_publish_received_total     # 累计入站 PUBLISH（counter）
lumenmq_publish_qos0_total         # QoS0 入站计数
lumenmq_publish_qos1_total         # QoS1 入站计数
lumenmq_publish_qos2_total         # QoS2 入站计数
lumenmq_messages_sent_total        # 累计投递消息（counter）
lumenmq_messages_dropped_total     # 背压丢弃消息（counter）
lumenmq_sessions_total             # 累计会话数
lumenmq_sessions_current           # 当前会话数（含离线）
lumenmq_sessions_expired_total     # 过期清理会话数
lumenmq_security_rejected_total    # 安全中间件拒绝数
lumenmq_plugin_rejected_total      # 插件拒绝数
lumenmq_storage_writes_total       # 存储写入数
lumenmq_retained_stored_total      # retained 存储数
```

## 架构

```
┌─────────────────────────────────────────────────────┐
│  接入层 (net/)                                        │
│  TCP / TLS / WebSocket / MQTT-SN                     │
├─────────────────────────────────────────────────────┤
│  安全层 (security/)    插件层 (plugin/)                │
│  IP过滤/限流/载荷校验   主题ACL/载荷过滤/HTTP转发       │
├─────────────────────────────────────────────────────┤
│  协议层 (codec/)         MQTT 3.1.1 / 5.0 编解码      │
├─────────────────────────────────────────────────────┤
│  Broker 核心 (broker/)                                │
│  路由 / 会话 / QoS / Retain / 离线队列                 │
├─────────────────────────────────────────────────────┤
│  基础层 (utils/ config/ monitor/ storage/)            │
│  错误/配置/日志/指标/sled持久化                         │
├─────────────────────────────────────────────────────┤
│  运维层 (admin/)    HTTP API / Prometheus / 热重载    │
└─────────────────────────────────────────────────────┘
```

### 模块说明

| 模块 | 职责 |
|------|------|
| [src/broker/](src/broker/) | 路由器、会话管理、QoS 握手、Retain 存储、离线队列、订阅树（含共享订阅） |
| [src/codec/](src/codec/) | MQTT 3.1.1/5.0 报文编解码（基于 tokio-util Codec） |
| [src/net/](src/net/) | TCP/TLS/WS/MQTT-SN 接入层、连接上下文、心跳保活 |
| [src/security/](src/security/) | IP 黑白名单（CIDR）、令牌桶限流、单 IP 连接计数 |
| [src/plugin/](src/plugin/) | 主题 ACL、载荷黑白名单过滤、HTTP 转发 hook |
| [src/storage/](src/storage/) | sled 嵌入式 KV 存储（会话/离线/retained 持久化） |
| [src/admin/](src/admin/) | axum HTTP 运维 API（健康/指标/会话/热重载/发布） |
| [src/monitor/](src/monitor/) | 23 项运行时指标、Prometheus 导出、结构化日志 |
| [src/config/](src/config/) | TOML 配置加载、profile 合并、环境变量覆盖、校验 |

## TraceID 链路追踪

每条入站 PUBLISH 自动生成 8 字符十六进制 TraceID，贯穿「接收 → 路由 → 投递」全链路日志，便于在工业现场通过 `grep <trace_id>` 快速定位单条消息流向：

```
DEBUG PUBLISH received    trace_id=3a7f1c92 client=sensor-01 topic=plant/temp qos=AtLeastOnce
DEBUG routing publish     trace_id=3a7f1c92 topic=plant/temp subscribers=3
DEBUG delivered to online trace_id=3a7f1c92 client=dashboard topic=plant/temp
```

启用 debug 级别日志（`LUMENMQ_LOG_LEVEL=debug` 或 dev profile）即可查看。

## 测试

```bash
# 全量测试（单元 + 集成）
cargo test

# 仅单元测试
cargo test --lib

# 单个集成测试文件
cargo test --test mqtt5_integration
```

当前测试覆盖 **119 项**（71 单元 + 48 集成），0 失败，涵盖：

- 编解码（QoS0/1/2 roundtrip、空载荷、残包）
- Broker 核心（retain 投递、QoS2 全握手、遗嘱消息、离线重放、持久化重启）
- MQTT 5.0（共享订阅轮询、Session Expiry、与 3.1.1 共存）
- MQTT-SN（UDP 连接、订阅、发布、PUBACK）
- TLS（单向认证、证书校验）
- WebSocket（子协议协商、QoS1 ACK）
- 安全中间件（黑白名单、限流、载荷限制、热重载）
- 插件（主题 ACL、载荷过滤、HTTP 转发、热重载）
- Admin API（健康、指标、会话管理、手动发布、热重载）

## 构建

```bash
# Release 构建（启用 LTO + strip）
cargo build --release

# 产物位置
target/release/lumenmq
```

Release profile 已优化：`opt-level=3`、`lto="thin"`、`codegen-units=1`、`strip=true`、`panic=abort`。

## 项目状态

- 协议：MQTT 3.1.1 / 5.0 / MQTT-SN 全功能实现
- 持久化：sled 嵌入式存储（会话/离线/retained）
- 安全：IP 过滤 + 限流 + 载荷校验 + 主题 ACL
- 运维：HTTP API + Prometheus + 热重载 + TraceID
- 部署：Docker 多阶段构建 + docker-compose

## License

MIT
