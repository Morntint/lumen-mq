//! 内置 Web 仪表盘 HTML 页面
//!
//! 单文件 SPA，无外部依赖，通过 WebSocket /api/v1/ws 实时接收指标快照。
//! 包含：指标卡片、Sparkline 折线图、会话列表表、连接状态指示器。

/// 仪表盘 HTML 页面内容
pub const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LumenMQ Dashboard</title>
<style>
:root {
  --bg: #0f1117;
  --card-bg: #1a1d27;
  --card-border: #2a2d3a;
  --text: #e0e0e0;
  --text-dim: #8a8d99;
  --accent: #00d4aa;
  --accent-dim: #007a63;
  --warn: #ffa940;
  --danger: #ff4d4f;
  --blue: #40a9ff;
  --purple: #b37feb;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  background: var(--bg);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  padding: 20px;
}
.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 24px;
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: 12px;
  margin-bottom: 20px;
}
.header-left { display: flex; align-items: center; gap: 16px; }
.logo {
  font-size: 24px;
  font-weight: 700;
  background: linear-gradient(135deg, var(--accent), var(--blue));
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}
.node-info { font-size: 13px; color: var(--text-dim); }
.status-badge {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 14px;
  border-radius: 20px;
  font-size: 13px;
  font-weight: 600;
}
.status-dot {
  width: 8px; height: 8px;
  border-radius: 50%;
  animation: pulse 2s infinite;
}
.status-connected { background: rgba(0,212,170,0.15); color: var(--accent); }
.status-connected .status-dot { background: var(--accent); }
.status-disconnected { background: rgba(255,77,79,0.15); color: var(--danger); }
.status-disconnected .status-dot { background: var(--danger); animation: none; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.4; } }

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 16px;
  margin-bottom: 20px;
}
.card {
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: 12px;
  padding: 20px;
  transition: border-color 0.3s;
}
.card:hover { border-color: var(--accent-dim); }
.card-label { font-size: 12px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }
.card-value { font-size: 32px; font-weight: 700; }
.card-value.green { color: var(--accent); }
.card-value.blue { color: var(--blue); }
.card-value.purple { color: var(--purple); }
.card-value.warn { color: var(--warn); }
.card-sub { font-size: 12px; color: var(--text-dim); margin-top: 4px; }

.charts {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 20px;
}
@media (max-width: 768px) { .charts { grid-template-columns: 1fr; } }
.chart-card {
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: 12px;
  padding: 20px;
}
.chart-title { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
canvas { width: 100%; height: 120px; display: block; }

.table-card {
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: 12px;
  padding: 20px;
  overflow: hidden;
}
.table-title { font-size: 14px; font-weight: 600; margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
table { width: 100%; border-collapse: collapse; }
th { text-align: left; font-size: 12px; color: var(--text-dim); text-transform: uppercase; letter-spacing: 1px; padding: 8px 12px; border-bottom: 1px solid var(--card-border); }
td { padding: 10px 12px; font-size: 13px; border-bottom: 1px solid var(--card-border); }
.tag { padding: 2px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.tag-online { background: rgba(0,212,170,0.15); color: var(--accent); }
.tag-offline { background: rgba(138,141,153,0.15); color: var(--text-dim); }
.empty-state { text-align: center; padding: 40px; color: var(--text-dim); font-size: 14px; }
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="logo">LumenMQ</div>
    <div class="node-info">
      <span id="node-id">—</span> · v<span id="version">—</span>
    </div>
  </div>
  <div id="status" class="status-badge status-disconnected">
    <div class="status-dot"></div>
    <span id="status-text">连接中...</span>
  </div>
</div>

<div class="cards">
  <div class="card">
    <div class="card-label">在线连接</div>
    <div class="card-value green" id="m-connections">0</div>
    <div class="card-sub">累计 <span id="m-connections-total">0</span></div>
  </div>
  <div class="card">
    <div class="card-label">PUBLISH 速率</div>
    <div class="card-value blue" id="m-pub-rate">0</div>
    <div class="card-sub">msg/s · 累计 <span id="m-publish-total">0</span></div>
  </div>
  <div class="card">
    <div class="card-label">投递速率</div>
    <div class="card-value purple" id="m-send-rate">0</div>
    <div class="card-sub">msg/s · 累计 <span id="m-sent-total">0</span></div>
  </div>
  <div class="card">
    <div class="card-label">总会话</div>
    <div class="card-value" id="m-sessions">0</div>
    <div class="card-sub">离线 <span id="m-sessions-offline">0</span></div>
  </div>
  <div class="card">
    <div class="card-label">订阅数</div>
    <div class="card-value" id="m-subscriptions">0</div>
    <div class="card-sub">共享 <span id="m-shared-subs">0</span></div>
  </div>
  <div class="card">
    <div class="card-label">丢弃消息</div>
    <div class="card-value warn" id="m-dropped">0</div>
    <div class="card-sub">安全拒绝 <span id="m-security-rejected">0</span></div>
  </div>
</div>

<div class="charts">
  <div class="chart-card">
    <div class="chart-title">PUBLISH 速率 (msg/s)</div>
    <canvas id="chart-pub"></canvas>
  </div>
  <div class="chart-card">
    <div class="chart-title">投递速率 (msg/s)</div>
    <canvas id="chart-send"></canvas>
  </div>
</div>

<div class="table-card">
  <div class="table-title">
    <span>会话列表</span>
    <span style="font-weight:400;color:var(--text-dim);" id="session-count">0 个会话</span>
  </div>
  <table>
    <thead>
      <tr><th>Client ID</th><th>状态</th><th>Peer Address</th></tr>
    </thead>
    <tbody id="sessions-body">
      <tr><td colspan="3" class="empty-state">等待数据...</td></tr>
    </tbody>
  </table>
</div>

<script>
(function() {
  const MAX_POINTS = 60;
  let pubHistory = [];
  let sendHistory = [];
  let lastPubTotal = null;
  let lastSendTotal = null;
  let lastTime = null;

  function $(id) { return document.getElementById(id); }

  function setStatus(connected) {
    const el = $('status');
    const txt = $('status-text');
    if (connected) {
      el.className = 'status-badge status-connected';
      txt.textContent = '实时连接';
    } else {
      el.className = 'status-badge status-disconnected';
      txt.textContent = '已断开';
    }
  }

  function fmt(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  function drawSparkline(canvasId, data, color) {
    const canvas = $(canvasId);
    const ctx = canvas.getContext('2d');
    const w = canvas.width = canvas.offsetWidth;
    const h = canvas.height = canvas.offsetHeight;
    ctx.clearRect(0, 0, w, h);
    if (data.length < 2) return;
    const max = Math.max(...data, 1);
    const step = w / (MAX_POINTS - 1);

    // gradient fill
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, color + '40');
    grad.addColorStop(1, color + '00');
    ctx.beginPath();
    ctx.moveTo(0, h);
    data.forEach((v, i) => {
      const x = i * step;
      const y = h - (v / max) * (h - 4) - 2;
      ctx.lineTo(x, y);
    });
    ctx.lineTo((data.length - 1) * step, h);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();

    // line
    ctx.beginPath();
    data.forEach((v, i) => {
      const x = i * step;
      const y = h - (v / max) * (h - 4) - 2;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.stroke();
  }

  function update(data) {
    const m = data.metrics;
    const now = Date.now();

    // 计算速率
    let pubRate = 0, sendRate = 0;
    if (lastTime !== null && lastPubTotal !== null) {
      const dt = (now - lastTime) / 1000;
      if (dt > 0) {
        pubRate = Math.max(0, Math.round((m.publish_received - lastPubTotal) / dt));
        sendRate = Math.max(0, Math.round((m.messages_sent - lastSendTotal) / dt));
      }
    }
    lastTime = now;
    lastPubTotal = m.publish_received;
    lastSendTotal = m.messages_sent;

    pubHistory.push(pubRate);
    sendHistory.push(sendRate);
    if (pubHistory.length > MAX_POINTS) pubHistory.shift();
    if (sendHistory.length > MAX_POINTS) sendHistory.shift();

    // 更新卡片
    $('node-id').textContent = data.node_id;
    $('version').textContent = data.version;
    $('m-connections').textContent = m.connections_current;
    $('m-connections-total').textContent = fmt(m.connections_total);
    $('m-pub-rate').textContent = fmt(pubRate);
    $('m-publish-total').textContent = fmt(m.publish_received);
    $('m-send-rate').textContent = fmt(sendRate);
    $('m-sent-total').textContent = fmt(m.messages_sent);
    $('m-sessions').textContent = m.sessions_total;
    $('m-sessions-offline').textContent = m.sessions_offline;
    $('m-subscriptions').textContent = m.subscriptions_current;
    $('m-shared-subs').textContent = m.shared_subscriptions_current;
    $('m-dropped').textContent = m.messages_dropped;
    $('m-security-rejected').textContent = m.security_rejected;

    // 绘制图表
    drawSparkline('chart-pub', pubHistory, '#40a9ff');
    drawSparkline('chart-send', sendHistory, '#b37feb');

    // 会话表
    const body = $('sessions-body');
    const sessions = data.sessions || [];
    $('session-count').textContent = sessions.length + ' 个会话';
    if (sessions.length === 0) {
      body.innerHTML = '<tr><td colspan="3" class="empty-state">暂无会话</td></tr>';
    } else {
      body.innerHTML = sessions.map(s =>
        '<tr><td>' + escapeHtml(s.client_id) + '</td>' +
        '<td><span class="tag ' + (s.connected ? 'tag-online' : 'tag-offline') + '">' +
        (s.connected ? '在线' : '离线') + '</span></td>' +
        '<td>' + escapeHtml(s.peer_addr) + '</td></tr>'
      ).join('');
    }
  }

  function escapeHtml(str) {
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
  }

  function connect() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(proto + '//' + location.host + '/api/v1/ws');

    ws.onopen = function() { setStatus(true); };
    ws.onmessage = function(ev) {
      try { update(JSON.parse(ev.data)); } catch(e) { console.error(e); }
    };
    ws.onclose = function() {
      setStatus(false);
      setTimeout(connect, 3000); // 自动重连
    };
    ws.onerror = function() { ws.close(); };
  }

  connect();
})();
</script>
</body>
</html>"#;
