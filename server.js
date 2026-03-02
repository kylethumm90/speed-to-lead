const express = require("express");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── In-memory lead store (persists as long as server is running) ────────────
let leads = [];
let clients = []; // SSE subscribers

// ─── SSE: broadcast to all connected dashboard clients ──────────────────────
function broadcast(eventName, data) {
  const payload = `event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`;
  clients = clients.filter((res) => {
    try {
      res.write(payload);
      return true;
    } catch {
      return false;
    }
  });
}

// ─── WEBHOOK: New Lead (GHL fires this when a contact is created) ────────────
// GHL Workflow Action: "Send Webhook" → POST https://your-domain.railway.app/webhook/lead-in
app.post("/webhook/lead-in", (req, res) => {
  const body = req.body;

  // Support both GHL native format and custom mapped fields
  const lead = {
    id: body.contact_id || body.id || `lead_${Date.now()}`,
    firstName: body.first_name || body.firstName || body.contact?.firstName || "Unknown",
    lastName: body.last_name || body.lastName || body.contact?.lastName || "",
    phone: body.phone || body.phone_raw || body.contact?.phone || "--",
    source: body.source || body.lead_source || body.attributionSource?.medium || "Other",
    arrivedAt: Date.now(),
    called: false,
    callTime: null,
  };

  // Prevent duplicate contact IDs
  const exists = leads.find((l) => l.id === lead.id);
  if (!exists) {
    leads.unshift(lead);
    // Keep last 100 leads in memory
    if (leads.length > 100) leads = leads.slice(0, 100);
    broadcast("lead_in", lead);
    console.log(`[LEAD IN] ${lead.firstName} ${lead.lastName} — ${lead.source}`);
  }

  res.json({ ok: true, lead_id: lead.id });
});

// ─── WEBHOOK: Call Made (GHL fires this when an outbound call is placed) ────
// GHL Workflow Trigger: "Call Status" → POST https://your-domain.railway.app/webhook/call-made
app.post("/webhook/call-made", (req, res) => {
  const body = req.body;
  const contactId = body.contact_id || body.id || body.contactId;

  const lead = leads.find((l) => l.id === contactId);
  if (lead && !lead.called) {
    lead.called = true;
    lead.callTime = Math.round((Date.now() - lead.arrivedAt) / 1000);
    lead.callStatus = body.call_status || body.status || "connected";
    broadcast("call_made", { id: lead.id, callTime: lead.callTime, callStatus: lead.callStatus });
    console.log(`[CALL MADE] ${lead.firstName} ${lead.lastName} — ${lead.callTime}s`);
  }

  res.json({ ok: true });
});

// ─── SSE: Dashboard subscribes here for real-time updates ───────────────────
app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.flushHeaders();

  // Send current state immediately on connect
  res.write(`event: init\ndata: ${JSON.stringify({ leads })}\n\n`);

  clients.push(res);

  // Heartbeat every 25s to keep connection alive
  const heartbeat = setInterval(() => {
    try { res.write(": heartbeat\n\n"); } catch { clearInterval(heartbeat); }
  }, 25000);

  req.on("close", () => {
    clearInterval(heartbeat);
    clients = clients.filter((c) => c !== res);
  });
});

// ─── API: Get all leads (for page load fallback) ─────────────────────────────
app.get("/api/leads", (req, res) => {
  res.json({ leads });
});

// ─── API: Manual call mark (fallback if GHL auto-detect isn't wired yet) ────
app.post("/api/mark-called/:id", (req, res) => {
  const lead = leads.find((l) => l.id === req.params.id);
  if (lead && !lead.called) {
    lead.called = true;
    lead.callTime = Math.round((Date.now() - lead.arrivedAt) / 1000);
    broadcast("call_made", { id: lead.id, callTime: lead.callTime });
  }
  res.json({ ok: true });
});

// ─── API: Clear all leads (admin) ────────────────────────────────────────────
app.post("/api/clear", (req, res) => {
  leads = [];
  broadcast("clear", {});
  res.json({ ok: true });
});

// ─── Serve the dashboard UI ──────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.send(getDashboardHTML());
});

// ─── Start ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Speed to Lead running on port ${PORT}`);
  console.log(`Webhook endpoints:`);
  console.log(`  POST /webhook/lead-in`);
  console.log(`  POST /webhook/call-made`);
});

// ─── Dashboard HTML ──────────────────────────────────────────────────────────
function getDashboardHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Speed to Lead — RTP</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0a0c0f; --surface: #111418; --border: #1e2329;
    --border-bright: #2a3040; --text: #e8eaf0; --muted: #5a6070;
    --green: #00e676; --yellow: #ffd740; --orange: #ff6d00;
    --red: #ff1744; --blue: #40c4ff;
  }
  body { background: var(--bg); color: var(--text); font-family: 'DM Sans', sans-serif; min-height: 100vh; }
  body::before {
    content: ''; position: fixed; inset: 0; pointer-events: none; z-index: 0; opacity: 0.3;
    background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.05'/%3E%3C/svg%3E");
  }
  .wrap { position: relative; z-index: 1; max-width: 1100px; margin: 0 auto; padding: 32px 24px; }
  header { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 40px; padding-bottom: 24px; border-bottom: 1px solid var(--border); }
  .logo-eyebrow { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.2em; color: var(--muted); text-transform: uppercase; }
  .logo-title { font-family: 'Bebas Neue', sans-serif; font-size: 42px; letter-spacing: 0.05em; line-height: 1; }
  .logo-title span { color: var(--green); }
  .live-badge { display: flex; align-items: center; gap: 6px; font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.15em; color: var(--green); text-transform: uppercase; }
  .live-dot { width: 7px; height: 7px; background: var(--green); border-radius: 50%; animation: pulse 1.8s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1;transform:scale(1);box-shadow:0 0 0 0 rgba(0,230,118,.4)}50%{opacity:.7;transform:scale(1.1);box-shadow:0 0 0 5px rgba(0,230,118,0)} }
  .conn-dot-disconnected { background: var(--red) !important; animation: none !important; }

  .stats-bar { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 32px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px 20px; }
  .stat-label { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.18em; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }
  .stat-value { font-family: 'Bebas Neue', sans-serif; font-size: 32px; letter-spacing: 0.04em; line-height: 1; }
  .green { color: var(--green); } .yellow { color: var(--yellow); } .red { color: var(--red); } .blue { color: var(--blue); }

  .section-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
  .section-title { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.2em; color: var(--muted); text-transform: uppercase; }
  .btn-clear { background: transparent; border: 1px solid var(--border); color: var(--muted); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.1em; text-transform: uppercase; padding: 5px 12px; border-radius: 5px; cursor: pointer; transition: all .2s; }
  .btn-clear:hover { border-color: var(--red); color: var(--red); }

  .leads-list { display: flex; flex-direction: column; gap: 10px; }
  .lead-card { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 18px 22px; display: grid; grid-template-columns: 1fr auto auto; align-items: center; gap: 24px; animation: slideIn .35s ease; transition: border-color .2s; }
  @keyframes slideIn { from{opacity:0;transform:translateY(-12px)} to{opacity:1;transform:translateY(0)} }
  .lead-card.status-new { border-left: 3px solid var(--green); }
  .lead-card.status-warning { border-left: 3px solid var(--yellow); }
  .lead-card.status-urgent { border-left: 3px solid var(--orange); }
  .lead-card.status-critical { border-left: 3px solid var(--red); animation: cardFlash 1.5s ease-in-out infinite; }
  .lead-card.status-called { border-left: 3px solid #2a3040; opacity: 0.6; }
  @keyframes cardFlash { 0%,100%{border-color:var(--red)}50%{border-color:rgba(255,23,68,.3)} }

  .lead-name { font-weight: 500; font-size: 15px; margin-bottom: 5px; }
  .lead-meta { display: flex; align-items: center; gap: 12px; }
  .lead-phone { font-family: 'DM Mono', monospace; font-size: 12px; color: var(--muted); }
  .source-pill { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; padding: 2px 8px; border-radius: 20px; border: 1px solid; }
  .source-facebook { color:#4fc3f7; border-color:#1a3a4a; background:rgba(79,195,247,.08); }
  .source-google { color:#a5d6a7; border-color:#1a3a22; background:rgba(165,214,167,.08); }
  .source-website { color:#ce93d8; border-color:#2a1a3a; background:rgba(206,147,216,.08); }
  .source-referral { color:#ffcc80; border-color:#3a2a10; background:rgba(255,204,128,.08); }
  .source-other { color:var(--muted); border-color:var(--border); }

  .timer-display { font-family: 'Bebas Neue', sans-serif; font-size: 36px; letter-spacing: .05em; line-height: 1; min-width: 100px; text-align: center; }
  .timer-label { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .15em; color: var(--muted); text-transform: uppercase; text-align: center; margin-top: 3px; }
  .t-new{color:var(--green)} .t-warning{color:var(--yellow)} .t-urgent{color:var(--orange)}
  .t-critical{color:var(--red);animation:timerPulse .8s ease-in-out infinite}
  .t-called{color:var(--muted);font-size:26px}
  @keyframes timerPulse{0%,100%{opacity:1}50%{opacity:.4}}

  .btn-call { background: transparent; border: 1px solid var(--green); color: var(--green); font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: .15em; text-transform: uppercase; padding: 8px 16px; border-radius: 6px; cursor: pointer; transition: all .2s; white-space: nowrap; }
  .btn-call:hover { background: var(--green); color: #000; }
  .btn-called { background: transparent; border: 1px solid var(--border); color: var(--muted); font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: .15em; text-transform: uppercase; padding: 8px 16px; border-radius: 6px; cursor: default; white-space: nowrap; }
  .call-time-result { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); text-align: center; margin-top: 4px; }

  .empty-state { text-align: center; padding: 80px 24px; color: var(--muted); }
  .empty-icon { font-size: 48px; margin-bottom: 16px; opacity: .3; }
  .empty-text { font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: .15em; text-transform: uppercase; }

  .info-box { margin-top: 40px; background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 24px; }
  .info-title { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: .2em; color: var(--muted); text-transform: uppercase; margin-bottom: 14px; }
  .endpoint-row { display: flex; align-items: center; gap: 10px; background: var(--bg); border: 1px solid var(--border-bright); border-radius: 6px; padding: 10px 14px; margin-bottom: 8px; }
  .ep-method { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; color: var(--orange); background: rgba(255,109,0,.1); border: 1px solid rgba(255,109,0,.2); padding: 2px 7px; border-radius: 4px; flex-shrink: 0; }
  .ep-path { font-family: 'DM Mono', monospace; font-size: 12px; color: var(--blue); flex: 1; }
  .ep-desc { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); }
  .copy-btn { background: transparent; border: 1px solid var(--border-bright); color: var(--muted); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; padding: 4px 10px; border-radius: 4px; cursor: pointer; transition: all .2s; flex-shrink: 0; }
  .copy-btn:hover { border-color: var(--blue); color: var(--blue); }

  .sim-bar { margin-top: 16px; padding-top: 16px; border-top: 1px solid var(--border); display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .sim-label { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .15em; color: var(--muted); text-transform: uppercase; }
  .btn-sim { background: transparent; border: 1px solid var(--border-bright); color: var(--text); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; padding: 6px 13px; border-radius: 5px; cursor: pointer; transition: all .2s; }
  .btn-sim:hover { border-color: var(--green); color: var(--green); }
  .btn-sim.fb { border-color:#1a3a4a; color:#4fc3f7; } .btn-sim.fb:hover { background:rgba(79,195,247,.08); }
  .btn-sim.gg { border-color:#1a3a22; color:#a5d6a7; } .btn-sim.gg:hover { background:rgba(165,214,167,.08); }

  @media(max-width:700px){
    .stats-bar{grid-template-columns:repeat(2,1fr)}
    .lead-card{grid-template-columns:1fr;gap:12px}
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <div class="logo-eyebrow">Rooftop Power Co</div>
      <div class="logo-title">Speed to <span>Lead</span></div>
    </div>
    <div class="live-badge">
      <div class="live-dot" id="conn-dot"></div>
      <span id="conn-label">Connecting...</span>
    </div>
  </header>

  <div class="stats-bar">
    <div class="stat-card"><div class="stat-label">Leads Today</div><div class="stat-value blue" id="s-total">0</div></div>
    <div class="stat-card"><div class="stat-label">Avg Speed</div><div class="stat-value green" id="s-avg">--</div></div>
    <div class="stat-card"><div class="stat-label">Called</div><div class="stat-value green" id="s-called">0</div></div>
    <div class="stat-card"><div class="stat-label">Waiting</div><div class="stat-value red" id="s-waiting">0</div></div>
  </div>

  <div class="section-head">
    <span class="section-title">Active Leads</span>
    <button class="btn-clear" onclick="clearAll()">Clear All</button>
  </div>
  <div class="leads-list" id="leads-list"></div>

  <div class="info-box">
    <div class="info-title">GHL Webhook Endpoints</div>
    <div class="endpoint-row">
      <span class="ep-method">POST</span>
      <span class="ep-path" id="ep-lead">${"DOMAIN"}/webhook/lead-in</span>
      <span class="ep-desc">New lead created</span>
      <button class="copy-btn" onclick="copyEp('ep-lead')">Copy</button>
    </div>
    <div class="endpoint-row">
      <span class="ep-method">POST</span>
      <span class="ep-path" id="ep-call">${"DOMAIN"}/webhook/call-made</span>
      <span class="ep-desc">Outbound call placed</span>
      <button class="copy-btn" onclick="copyEp('ep-call')">Copy</button>
    </div>
    <div class="sim-bar">
      <span class="sim-label">Simulate:</span>
      <button class="btn-sim fb" onclick="sim('Facebook')">+ Facebook</button>
      <button class="btn-sim gg" onclick="sim('Google')">+ Google</button>
      <button class="btn-sim" onclick="sim('Website')">+ Website</button>
      <button class="btn-sim" onclick="sim('Referral')">+ Referral</button>
      <button class="btn-sim" style="margin-left:auto;border-color:#1a3a22;color:#a5d6a7" onclick="simCall()">📞 Sim Call</button>
    </div>
  </div>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────────
let leads = [];
let es;

// Set domain in endpoint display
const domain = window.location.origin;
document.getElementById('ep-lead').textContent = domain + '/webhook/lead-in';
document.getElementById('ep-call').textContent = domain + '/webhook/call-made';

// ── SSE Connection ─────────────────────────────────────────────────────────
function connect() {
  es = new EventSource('/events');

  es.addEventListener('init', e => {
    const data = JSON.parse(e.data);
    leads = data.leads || [];
    render();
    setConn(true);
  });

  es.addEventListener('lead_in', e => {
    const lead = JSON.parse(e.data);
    if (!leads.find(l => l.id === lead.id)) {
      leads.unshift(lead);
      render();
    }
  });

  es.addEventListener('call_made', e => {
    const { id, callTime, callStatus } = JSON.parse(e.data);
    const lead = leads.find(l => l.id === id);
    if (lead) { lead.called = true; lead.callTime = callTime; render(); }
  });

  es.addEventListener('clear', () => { leads = []; render(); });

  es.onerror = () => { setConn(false); setTimeout(connect, 3000); };
}

function setConn(ok) {
  document.getElementById('conn-dot').className = 'live-dot' + (ok ? '' : ' conn-dot-disconnected');
  document.getElementById('conn-label').textContent = ok ? 'Live' : 'Reconnecting...';
}

// ── Helpers ────────────────────────────────────────────────────────────────
function fmt(s) {
  if (s < 60) return '0:' + String(s).padStart(2,'0');
  const m = Math.floor(s/60), sec = s%60;
  if (m < 60) return m + ':' + String(sec).padStart(2,'0');
  return Math.floor(m/60) + 'h ' + (m%60) + 'm';
}
function tClass(s, called) {
  if (called) return 't-called';
  if (s < 60) return 't-new';
  if (s < 180) return 't-warning';
  if (s < 300) return 't-urgent';
  return 't-critical';
}
function cStatus(s, called) {
  if (called) return 'status-called';
  if (s < 60) return 'status-new';
  if (s < 180) return 'status-warning';
  if (s < 300) return 'status-urgent';
  return 'status-critical';
}
function tLabel(s, called) {
  if (called) return 'Called';
  if (s < 60) return 'On fire 🔥';
  if (s < 180) return 'Good';
  if (s < 300) return 'Urgent';
  return 'CRITICAL';
}
function srcClass(src) {
  const m = {Facebook:'source-facebook',Google:'source-google',Website:'source-website',Referral:'source-referral'};
  return m[src] || 'source-other';
}

// ── Render ─────────────────────────────────────────────────────────────────
function render() {
  const list = document.getElementById('leads-list');
  if (!leads.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">📡</div><div class="empty-text">Waiting for incoming leads</div></div>';
    updateStats(); return;
  }
  list.innerHTML = leads.map(lead => {
    const el = lead.called ? lead.callTime : Math.round((Date.now() - lead.arrivedAt) / 1000);
    const btn = lead.called
      ? '<div class="btn-called">✓ Called</div><div class="call-time-result">' + fmt(lead.callTime) + '</div>'
      : '<button class="btn-call" onclick="manualCall(\\'' + lead.id + '\\')">Mark Called</button>';
    return '<div class="lead-card ' + cStatus(el, lead.called) + '" id="c-' + lead.id + '">' +
      '<div><div class="lead-name">' + lead.firstName + ' ' + lead.lastName + '</div>' +
      '<div class="lead-meta"><span class="lead-phone">' + lead.phone + '</span>' +
      '<span class="source-pill ' + srcClass(lead.source) + '">' + lead.source + '</span></div></div>' +
      '<div><div class="timer-display ' + tClass(el, lead.called) + '" id="t-' + lead.id + '">' + fmt(el) + '</div>' +
      '<div class="timer-label" id="tl-' + lead.id + '">' + tLabel(el, lead.called) + '</div></div>' +
      '<div>' + btn + '</div></div>';
  }).join('');
  updateStats();
}

function updateTimers() {
  leads.forEach(lead => {
    if (lead.called) return;
    const el = Math.round((Date.now() - lead.arrivedAt) / 1000);
    const te = document.getElementById('t-' + lead.id);
    const tle = document.getElementById('tl-' + lead.id);
    const ce = document.getElementById('c-' + lead.id);
    if (!te) return;
    te.textContent = fmt(el);
    te.className = 'timer-display ' + tClass(el, false);
    tle.textContent = tLabel(el, false);
    ce.className = 'lead-card ' + cStatus(el, false);
  });
  updateStats();
}

function updateStats() {
  const called = leads.filter(l => l.called);
  const waiting = leads.filter(l => !l.called);
  document.getElementById('s-total').textContent = leads.length;
  document.getElementById('s-called').textContent = called.length;
  document.getElementById('s-waiting').textContent = waiting.length;
  if (called.length) {
    const avg = Math.round(called.reduce((s,l) => s + l.callTime, 0) / called.length);
    document.getElementById('s-avg').textContent = fmt(avg);
  } else {
    document.getElementById('s-avg').textContent = '--';
  }
}

// ── Actions ────────────────────────────────────────────────────────────────
function manualCall(id) {
  fetch('/api/mark-called/' + id, { method: 'POST' });
}

function clearAll() {
  if (confirm('Clear all leads?')) fetch('/api/clear', { method: 'POST' });
}

function copyEp(id) {
  const el = document.getElementById(id);
  navigator.clipboard.writeText(el.textContent).then(() => {
    const btn = el.nextElementSibling.nextElementSibling;
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 1500);
  });
}

// ── Simulate (dev only) ────────────────────────────────────────────────────
const NAMES = [['James','Wilson'],['Maria','Garcia'],['David','Chen'],['Sarah','Johnson'],['Mike','Torres'],['Lisa','Patel']];
const PHONES = ['+14015550101','+14015550202','+15085550303','+18605550404'];
let si = 0;
function sim(source) {
  const [fn, ln] = NAMES[si++ % NAMES.length];
  fetch('/webhook/lead-in', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ contact_id: 'sim_'+Date.now(), first_name:fn, last_name:ln, phone:PHONES[Math.floor(Math.random()*PHONES.length)], source })
  });
}
function simCall() {
  const pending = leads.filter(l => !l.called);
  if (!pending.length) { alert('No pending leads.'); return; }
  fetch('/webhook/call-made', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ contact_id: pending[pending.length-1].id, call_status:'connected' })
  });
}

// ── Boot ───────────────────────────────────────────────────────────────────
connect();
setInterval(updateTimers, 1000);
</script>
</body>
</html>`;
}
