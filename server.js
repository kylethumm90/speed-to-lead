const express = require("express");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ─── Configuration ──────────────────────────────────────────────────────────
// Update these with your actual closer names
const CLOSERS = [
  "Closer 1", "Closer 2", "Closer 3", "Closer 4", "Closer 5",
  "Closer 6", "Closer 7", "Closer 8", "Closer 9",
];

// ─── PostgreSQL ─────────────────────────────────────────────────────────────
const pool = process.env.DATABASE_URL
  ? new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false },
    })
  : null;

async function initDB() {
  if (!pool) return;
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS leads (
        id VARCHAR PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        phone VARCHAR,
        source VARCHAR,
        arrived_at BIGINT,
        called BOOLEAN DEFAULT FALSE,
        call_time INTEGER,
        call_status VARCHAR,
        setter_name VARCHAR,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS appointments (
        id SERIAL PRIMARY KEY,
        contact_id VARCHAR,
        first_name VARCHAR,
        last_name VARCHAR,
        appointment_time VARCHAR,
        calendar_name VARCHAR,
        status VARCHAR,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS calls (
        id SERIAL PRIMARY KEY,
        contact_id VARCHAR,
        call_status VARCHAR,
        call_time INTEGER,
        setter_name VARCHAR,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);
    console.log("[DB] Tables initialized");
  } catch (err) {
    console.error("[DB] Failed to initialize tables:", err.message);
  }
}

function todayStart() {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d;
}

function dbRowToLead(row) {
  return {
    id: row.id,
    firstName: row.first_name || "New Lead",
    lastName: row.last_name || "",
    phone: row.phone || "--",
    source: row.source || "Other",
    arrivedAt: Number(row.arrived_at),
    called: row.called || false,
    callTime: row.call_time || null,
    callStatus: row.call_status || null,
    setter: row.setter_name || null,
    calledAt: row.called && row.call_time
      ? Number(row.arrived_at) + row.call_time * 1000
      : null,
  };
}

function dbRowToAppointment(row) {
  return {
    id: row.id,
    contactId: row.contact_id,
    firstName: row.first_name || "",
    lastName: row.last_name || "",
    appointmentTime: row.appointment_time,
    calendarName: row.calendar_name,
    status: row.status,
    createdAt: row.created_at,
  };
}

// ─── JSON File Persistence (fallback when no DATABASE_URL) ──────────────────
const DATA_DIR = path.join(__dirname, "data");
const LEADS_FILE = path.join(DATA_DIR, "leads.json");

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function loadLeadsFromFile() {
  ensureDataDir();
  try {
    if (fs.existsSync(LEADS_FILE)) {
      return JSON.parse(fs.readFileSync(LEADS_FILE, "utf8"));
    }
  } catch (err) {
    console.error("[DATA] Failed to load leads:", err.message);
  }
  return [];
}

function saveLeadsToFile() {
  ensureDataDir();
  try {
    fs.writeFileSync(LEADS_FILE, JSON.stringify(leads, null, 2));
  } catch (err) {
    console.error("[DATA] Failed to save leads:", err.message);
  }
}

function getDailySummaryPath() {
  const today = new Date().toISOString().split("T")[0];
  return path.join(DATA_DIR, "summary-" + today + ".json");
}

function loadDailySummaryFromFile() {
  const file = getDailySummaryPath();
  try {
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, "utf8"));
    }
  } catch (err) {
    console.error("[DATA] Failed to load summary:", err.message);
  }
  return [];
}

function appendToSummaryFile(entry) {
  ensureDataDir();
  const entries = loadDailySummaryFromFile();
  entries.push(entry);
  try {
    fs.writeFileSync(getDailySummaryPath(), JSON.stringify(entries, null, 2));
  } catch (err) {
    console.error("[DATA] Failed to save summary:", err.message);
  }
}

function getOutboundCountPath() {
  const today = new Date().toISOString().split("T")[0];
  return path.join(DATA_DIR, "outbound-" + today + ".json");
}

function loadOutboundCountFromFile() {
  const file = getOutboundCountPath();
  try {
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, "utf8")).count || 0;
    }
  } catch (err) {}
  return 0;
}

function incrementOutboundCountFile() {
  ensureDataDir();
  const count = loadOutboundCountFromFile() + 1;
  try {
    fs.writeFileSync(getOutboundCountPath(), JSON.stringify({ count }));
  } catch (err) {}
  return count;
}

// ─── In-memory state (loaded from DB or file on startup) ────────────────────
let leads = [];
let appointments = [];
let clients = []; // SSE subscribers

async function loadTodaysData() {
  if (pool) {
    try {
      const start = todayStart();
      const leadsRes = await pool.query(
        "SELECT * FROM leads WHERE arrived_at >= $1 ORDER BY arrived_at DESC LIMIT 100",
        [start.getTime()]
      );
      leads = leadsRes.rows.map(dbRowToLead);
      const aptsRes = await pool.query(
        "SELECT * FROM appointments WHERE created_at >= $1 ORDER BY created_at DESC",
        [start]
      );
      appointments = aptsRes.rows.map(dbRowToAppointment);
      console.log("[DB] Loaded " + leads.length + " leads and " + appointments.length + " appointments for today");
    } catch (err) {
      console.error("[DB] Failed to load today's data, falling back to file:", err.message);
      leads = loadLeadsFromFile();
    }
  } else {
    leads = loadLeadsFromFile();
  }
}

async function getOutboundCount() {
  if (pool) {
    try {
      const res = await pool.query(
        "SELECT COUNT(*) FROM calls WHERE created_at >= $1",
        [todayStart()]
      );
      return parseInt(res.rows[0].count);
    } catch (err) {
      return loadOutboundCountFromFile();
    }
  }
  return loadOutboundCountFromFile();
}

// ─── SSE: broadcast to all connected dashboard clients ──────────────────────
function broadcast(eventName, data) {
  const payload = "event: " + eventName + "\ndata: " + JSON.stringify(data) + "\n\n";
  clients = clients.filter((res) => {
    try {
      res.write(payload);
      return true;
    } catch {
      return false;
    }
  });
}

// ─── WEBHOOK: New Lead (GHL fires this when a contact is created) ───────────
app.post("/webhook/lead-in", async (req, res) => {
  const body = req.body;

  const lead = {
    id: body.contact_id || body.id || "lead_" + Date.now(),
    firstName: body.first_name || body.firstName || body.contact?.firstName || "New Lead",
    lastName: body.last_name || body.lastName || body.contact?.lastName || "",
    phone: body.phone || body.phone_raw || body.contact?.phone || "--",
    source: body.source || body.lead_source || body.attributionSource?.medium || "Other",
    arrivedAt: Date.now(),
    called: false,
    callTime: null,
    setter: null,
  };

  const exists = leads.find((l) => l.id === lead.id);
  if (!exists) {
    if (pool) {
      try {
        await pool.query(
          "INSERT INTO leads (id, first_name, last_name, phone, source, arrived_at) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (id) DO NOTHING",
          [lead.id, lead.firstName, lead.lastName, lead.phone, lead.source, lead.arrivedAt]
        );
      } catch (err) {
        console.error("[DB] Failed to insert lead:", err.message);
      }
    }
    leads.unshift(lead);
    if (leads.length > 100) leads = leads.slice(0, 100);
    saveLeadsToFile();
    broadcast("lead_in", lead);
    console.log("[LEAD IN] " + lead.firstName + " " + lead.lastName + " — " + lead.source);
  }

  res.json({ ok: true, lead_id: lead.id });
});

// ─── WEBHOOK: Call Made (GHL fires this when an outbound call is placed) ────
app.post("/webhook/call-made", async (req, res) => {
  const body = req.body;
  const contactId = body.contact_id || body.id || body.contactId;
  const callStatus = body.call_status || body.status || "connected";

  const lead = leads.find((l) => l.id === contactId);
  const callTime = lead && !lead.called ? Math.round((Date.now() - lead.arrivedAt) / 1000) : null;
  const setterName = lead ? (lead.setter || null) : null;

  // Always insert into calls table (every outbound call)
  if (pool) {
    try {
      await pool.query(
        "INSERT INTO calls (contact_id, call_status, call_time, setter_name) VALUES ($1, $2, $3, $4)",
        [contactId, callStatus, callTime, setterName]
      );
    } catch (err) {
      console.error("[DB] Failed to insert call:", err.message);
    }
  }

  // Get updated outbound count
  const outboundCount = pool ? await getOutboundCount() : incrementOutboundCountFile();
  broadcast("outbound_update", { count: outboundCount });

  // Update lead if found and not already called
  if (lead && !lead.called) {
    lead.called = true;
    lead.callTime = callTime;
    lead.callStatus = callStatus;
    lead.calledAt = Date.now();
    if (pool) {
      try {
        await pool.query(
          "UPDATE leads SET called = true, call_time = $1, call_status = $2 WHERE id = $3",
          [callTime, callStatus, contactId]
        );
      } catch (err) {
        console.error("[DB] Failed to update lead:", err.message);
      }
    }
    saveLeadsToFile();
    appendToSummaryFile({
      leadId: lead.id,
      name: lead.firstName + " " + lead.lastName,
      setter: lead.setter || "Unassigned",
      timeToCall: lead.callTime,
      calledAt: new Date().toISOString(),
      arrivedAt: new Date(lead.arrivedAt).toISOString(),
      source: lead.source,
    });
    broadcast("call_made", { id: lead.id, callTime: lead.callTime, callStatus: lead.callStatus, setter: lead.setter });
    console.log("[CALL MADE] " + lead.firstName + " " + lead.lastName + " — " + lead.callTime + "s — " + (lead.setter || "Unassigned"));
  }

  res.json({ ok: true });
});

// ─── WEBHOOK: Appointment Booked ────────────────────────────────────────────
app.post("/webhook/appointment-booked", async (req, res) => {
  const body = req.body;
  const apt = {
    contactId: body.contact_id || body.id || body.contactId || null,
    firstName: body.first_name || body.firstName || body.contact?.firstName || "",
    lastName: body.last_name || body.lastName || body.contact?.lastName || "",
    appointmentTime: body.appointment_time || body.start_time || body.selectedTimeslot || "",
    calendarName: body.calendar_name || body.calendarName || body.calendar?.name || "",
    status: body.status || body.appointment_status || "scheduled",
  };

  if (pool) {
    try {
      const result = await pool.query(
        "INSERT INTO appointments (contact_id, first_name, last_name, appointment_time, calendar_name, status) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id, created_at",
        [apt.contactId, apt.firstName, apt.lastName, apt.appointmentTime, apt.calendarName, apt.status]
      );
      apt.id = result.rows[0].id;
      apt.createdAt = result.rows[0].created_at;
    } catch (err) {
      console.error("[DB] Failed to insert appointment:", err.message);
    }
  }

  appointments.unshift(apt);
  broadcast("appointment_booked", apt);
  console.log("[APPOINTMENT] " + apt.firstName + " " + apt.lastName + " — " + apt.calendarName);

  res.json({ ok: true });
});

// ─── SSE: Dashboard subscribes here for real-time updates ───────────────────
app.get("/events", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.flushHeaders();

  const outboundCount = await getOutboundCount();
  res.write("event: init\ndata: " + JSON.stringify({ leads, closers: CLOSERS, outboundCount }) + "\n\n");

  clients.push(res);

  const heartbeat = setInterval(() => {
    try { res.write(": heartbeat\n\n"); } catch { clearInterval(heartbeat); }
  }, 25000);

  req.on("close", () => {
    clearInterval(heartbeat);
    clients = clients.filter((c) => c !== res);
  });
});

// ─── API: Get all leads ────────────────────────────────────────────────────
app.get("/api/leads", async (req, res) => {
  const outboundCount = await getOutboundCount();
  res.json({ leads, closers: CLOSERS, outboundCount });
});

// ─── API: Assign setter/closer to lead ──────────────────────────────────────
app.post("/api/assign-setter/:id", async (req, res) => {
  const lead = leads.find((l) => l.id === req.params.id);
  if (lead) {
    lead.setter = req.body.setter || null;
    if (pool) {
      try {
        await pool.query("UPDATE leads SET setter_name = $1 WHERE id = $2", [lead.setter, lead.id]);
      } catch (err) {
        console.error("[DB] Failed to update setter:", err.message);
      }
    }
    saveLeadsToFile();
    broadcast("setter_assigned", { id: lead.id, setter: lead.setter });
    console.log("[ASSIGN] " + lead.firstName + " " + lead.lastName + " → " + (lead.setter || "Unassigned"));
  }
  res.json({ ok: true });
});

// ─── API: Manual call mark ─────────────────────────────────────────────────
app.post("/api/mark-called/:id", async (req, res) => {
  const lead = leads.find((l) => l.id === req.params.id);
  if (lead && !lead.called) {
    lead.called = true;
    lead.callTime = Math.round((Date.now() - lead.arrivedAt) / 1000);
    lead.calledAt = Date.now();
    if (pool) {
      try {
        await pool.query(
          "INSERT INTO calls (contact_id, call_status, call_time, setter_name) VALUES ($1, $2, $3, $4)",
          [lead.id, "manual", lead.callTime, lead.setter || null]
        );
        await pool.query(
          "UPDATE leads SET called = true, call_time = $1, call_status = 'manual' WHERE id = $2",
          [lead.callTime, lead.id]
        );
      } catch (err) {
        console.error("[DB] Failed to mark called:", err.message);
      }
    }
    const outboundCount = pool ? await getOutboundCount() : incrementOutboundCountFile();
    broadcast("outbound_update", { count: outboundCount });
    saveLeadsToFile();
    appendToSummaryFile({
      leadId: lead.id,
      name: lead.firstName + " " + lead.lastName,
      setter: lead.setter || "Unassigned",
      timeToCall: lead.callTime,
      calledAt: new Date().toISOString(),
      arrivedAt: new Date(lead.arrivedAt).toISOString(),
      source: lead.source,
    });
    broadcast("call_made", { id: lead.id, callTime: lead.callTime, setter: lead.setter });
    console.log("[CALL MADE] " + lead.firstName + " " + lead.lastName + " — " + lead.callTime + "s — " + (lead.setter || "Unassigned"));
  }
  res.json({ ok: true });
});

// ─── API: Daily summary ────────────────────────────────────────────────────
app.get("/api/daily-summary", async (req, res) => {
  if (pool) {
    try {
      const result = await pool.query(
        "SELECT c.contact_id, c.call_time, c.setter_name, c.created_at, l.first_name, l.last_name, l.source, l.arrived_at FROM calls c LEFT JOIN leads l ON c.contact_id = l.id WHERE c.created_at >= $1 AND c.call_time IS NOT NULL ORDER BY c.created_at ASC",
        [todayStart()]
      );
      const entries = result.rows.map((row) => ({
        leadId: row.contact_id,
        name: ((row.first_name || "") + " " + (row.last_name || "")).trim() || "Unknown",
        setter: row.setter_name || "Unassigned",
        timeToCall: row.call_time,
        calledAt: row.created_at.toISOString(),
        arrivedAt: row.arrived_at ? new Date(Number(row.arrived_at)).toISOString() : null,
        source: row.source || "",
      }));
      return res.json({ entries });
    } catch (err) {
      console.error("[DB] Failed to load daily summary:", err.message);
    }
  }
  res.json({ entries: loadDailySummaryFromFile() });
});

// ─── API: Clear all leads (dashboard only — does NOT delete from database) ─
app.post("/api/clear", (req, res) => {
  leads = [];
  saveLeadsToFile();
  broadcast("clear", {});
  res.json({ ok: true });
});

// ─── API: History — Leads ──────────────────────────────────────────────────
app.get("/api/history/leads", async (req, res) => {
  const days = parseInt(req.query.days) || 7;
  if (!pool) return res.json({ leads: [], message: "Database not configured" });
  try {
    const since = new Date();
    since.setDate(since.getDate() - days);
    const result = await pool.query(
      "SELECT * FROM leads WHERE arrived_at >= $1 ORDER BY arrived_at DESC",
      [since.getTime()]
    );
    res.json({ leads: result.rows.map(dbRowToLead) });
  } catch (err) {
    console.error("[DB] History leads error:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
});

// ─── API: History — Appointments ────────────────────────────────────────────
app.get("/api/history/appointments", async (req, res) => {
  const days = parseInt(req.query.days) || 7;
  if (!pool) return res.json({ appointments: [], message: "Database not configured" });
  try {
    const since = new Date();
    since.setDate(since.getDate() - days);
    const result = await pool.query(
      "SELECT * FROM appointments WHERE created_at >= $1 ORDER BY created_at DESC",
      [since]
    );
    res.json({ appointments: result.rows.map(dbRowToAppointment) });
  } catch (err) {
    console.error("[DB] History appointments error:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
});

// ─── API: All-time Stats ────────────────────────────────────────────────────
app.get("/api/stats/all-time", async (req, res) => {
  if (!pool) {
    return res.json({
      totalLeads: 0, totalAppointments: 0,
      avgSpeedToLead: null, bestSpeedToLead: null,
      message: "Database not configured",
    });
  }
  try {
    const [leadsRes, aptsRes, speedRes] = await Promise.all([
      pool.query("SELECT COUNT(*) FROM leads"),
      pool.query("SELECT COUNT(*) FROM appointments"),
      pool.query("SELECT AVG(call_time) as avg_speed, MIN(call_time) as best_speed FROM leads WHERE called = true AND call_time IS NOT NULL"),
    ]);
    res.json({
      totalLeads: parseInt(leadsRes.rows[0].count),
      totalAppointments: parseInt(aptsRes.rows[0].count),
      avgSpeedToLead: speedRes.rows[0].avg_speed ? Math.round(parseFloat(speedRes.rows[0].avg_speed)) : null,
      bestSpeedToLead: speedRes.rows[0].best_speed ? parseInt(speedRes.rows[0].best_speed) : null,
    });
  } catch (err) {
    console.error("[DB] All-time stats error:", err.message);
    res.status(500).json({ error: "Database query failed" });
  }
});

// ─── Serve the dashboard UI ────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.send(getDashboardHTML());
});

// ─── Start ─────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

async function boot() {
  await initDB();
  await loadTodaysData();
  app.listen(PORT, () => {
    console.log("Speed to Lead running on port " + PORT);
    console.log("Webhook endpoints:");
    console.log("  POST /webhook/lead-in");
    console.log("  POST /webhook/call-made");
    console.log("  POST /webhook/appointment-booked");
    console.log("Database: " + (pool ? "PostgreSQL connected" : "JSON file fallback"));
    console.log("Data directory: " + DATA_DIR);
  });
}

boot().catch((err) => {
  console.error("[BOOT] Fatal error:", err);
  process.exit(1);
});

// ─── Dashboard HTML ────────────────────────────────────────────────────────
function getDashboardHTML() {
  const closersJSON = JSON.stringify(CLOSERS);
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
  .wrap { position: relative; z-index: 1; max-width: 1200px; margin: 0 auto; padding: 28px 24px; }

  /* Header — 3-column: logo | clock | controls */
  header { display: grid; grid-template-columns: auto 1fr auto; align-items: center; gap: 24px; margin-bottom: 32px; padding-bottom: 20px; border-bottom: 1px solid var(--border); }
  .logo-eyebrow { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.2em; color: var(--muted); text-transform: uppercase; }
  .logo-title { font-family: 'Bebas Neue', sans-serif; font-size: 42px; letter-spacing: 0.05em; line-height: 1; }
  .logo-title span { color: var(--green); }
  .header-center { text-align: center; }
  .live-clock { font-family: 'Bebas Neue', sans-serif; font-size: 32px; letter-spacing: 0.04em; line-height: 1; color: var(--text); }
  .live-date { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.1em; color: var(--muted); margin-top: 4px; }
  .header-right { display: flex; align-items: center; gap: 12px; }
  .live-badge { display: flex; align-items: center; gap: 6px; font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.15em; color: var(--green); text-transform: uppercase; }
  .live-dot { width: 7px; height: 7px; background: var(--green); border-radius: 50%; animation: pulse 1.8s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1;transform:scale(1);box-shadow:0 0 0 0 rgba(0,230,118,.4)}50%{opacity:.7;transform:scale(1.1);box-shadow:0 0 0 5px rgba(0,230,118,0)} }
  .conn-dot-disconnected { background: var(--red) !important; animation: none !important; }

  /* Control buttons (gear, fullscreen) */
  .ctrl-btn { width: 34px; height: 34px; background: var(--surface); border: 1px solid var(--border); color: var(--muted); border-radius: 7px; cursor: pointer; font-size: 16px; display: flex; align-items: center; justify-content: center; transition: all 0.2s; }
  .ctrl-btn:hover { border-color: var(--border-bright); color: var(--text); }
  .ctrl-btn svg { display: block; }

  /* Stats bar — 6 columns */
  .stats-bar { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 28px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 14px 18px; }
  .stat-label { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.18em; color: var(--muted); text-transform: uppercase; margin-bottom: 4px; }
  .stat-value { font-family: 'Bebas Neue', sans-serif; font-size: 32px; letter-spacing: 0.04em; line-height: 1; }
  .stat-sub { font-family: 'DM Mono', monospace; font-size: 9px; color: var(--muted); margin-top: 2px; }
  .green { color: var(--green); } .yellow { color: var(--yellow); } .red { color: var(--red); } .blue { color: var(--blue); } .orange { color: var(--orange); }

  /* Tabs */
  .tabs-bar { display: flex; gap: 0; margin-bottom: 20px; border-bottom: 1px solid var(--border); }
  .tab-btn { background: transparent; border: none; border-bottom: 2px solid transparent; color: var(--muted); font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.15em; text-transform: uppercase; padding: 10px 20px; cursor: pointer; transition: all 0.2s; }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--green); border-bottom-color: var(--green); }

  .section-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
  .section-title { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: 0.2em; color: var(--muted); text-transform: uppercase; }

  /* Lead cards — wallboard optimized, 2-column: info | timer */
  .leads-list { display: flex; flex-direction: column; }
  .lead-card {
    background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
    padding: 18px 28px; display: grid; grid-template-columns: 1fr 180px;
    align-items: center; gap: 24px; margin-bottom: 10px;
    max-height: 150px; overflow: hidden;
    transition: opacity 1s ease-out, max-height 1s ease-out, padding 1s ease-out, margin 1s ease-out, border-width 1s ease-out;
  }
  .lead-card.slide-in { animation: slideIn .35s ease; }
  @keyframes slideIn { from{opacity:0;transform:translateY(-12px)} to{opacity:1;transform:translateY(0)} }
  .lead-card.status-new { border-left: 3px solid var(--green); }
  .lead-card.status-warning { border-left: 3px solid var(--yellow); }
  .lead-card.status-urgent { border-left: 3px solid var(--orange); }
  .lead-card.status-critical {
    border-left: 3px solid var(--red);
    animation: criticalGlow 1.5s ease-in-out infinite;
  }
  @keyframes criticalGlow {
    0%, 100% { box-shadow: 0 0 15px rgba(255,23,68,0.15), inset 0 0 15px rgba(255,23,68,0.03); border-left-color: var(--red); }
    50% { box-shadow: 0 0 30px rgba(255,23,68,0.4), 0 0 60px rgba(255,23,68,0.12), inset 0 0 20px rgba(255,23,68,0.06); border-left-color: #ff5252; }
  }
  .lead-card.status-called { border-left: 3px solid #2a3040; opacity: 0.5; }
  /* Auto-fade called leads after 60s */
  .lead-card.lead-fading {
    opacity: 0;
    max-height: 0;
    padding-top: 0;
    padding-bottom: 0;
    margin-bottom: 0;
    border-width: 0;
  }

  .lead-name { font-weight: 500; font-size: 16px; margin-bottom: 5px; }
  .lead-meta { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .lead-phone { font-family: 'DM Mono', monospace; font-size: 12px; color: var(--muted); }
  .lead-timestamp { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); margin-top: 4px; }
  .source-pill { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; padding: 2px 8px; border-radius: 20px; border: 1px solid; }
  .source-facebook { color:#4fc3f7; border-color:#1a3a4a; background:rgba(79,195,247,.08); }
  .source-google { color:#a5d6a7; border-color:#1a3a22; background:rgba(165,214,167,.08); }
  .source-website { color:#ce93d8; border-color:#2a1a3a; background:rgba(206,147,216,.08); }
  .source-referral { color:#ffcc80; border-color:#3a2a10; background:rgba(255,204,128,.08); }
  .source-other { color:var(--muted); border-color:var(--border); }

  /* Timer — larger, more dominant */
  .timer-display { font-family: 'Bebas Neue', sans-serif; font-size: 52px; letter-spacing: .05em; line-height: 1; text-align: center; }
  .timer-label { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .15em; color: var(--muted); text-transform: uppercase; text-align: center; margin-top: 4px; }
  .t-new{color:var(--green)} .t-warning{color:var(--yellow)} .t-urgent{color:var(--orange)}
  .t-critical{color:var(--red);animation:timerPulse .8s ease-in-out infinite}
  .t-called{color:var(--muted);font-size:34px}
  @keyframes timerPulse{0%,100%{opacity:1}50%{opacity:.4}}

  .empty-state { text-align: center; padding: 80px 24px; color: var(--muted); }
  .empty-icon { font-size: 48px; margin-bottom: 16px; opacity: .3; }
  .empty-text { font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: .15em; text-transform: uppercase; }

  /* Critical flash notification */
  .critical-flash { position: fixed; top: 0; left: 0; right: 0; padding: 16px 24px; background: linear-gradient(90deg, #ff1744, #d50000); color: #fff; font-family: 'DM Mono', monospace; font-size: 13px; font-weight: 500; letter-spacing: 0.05em; text-align: center; z-index: 1000; transform: translateY(-100%); transition: transform 0.4s ease; box-shadow: 0 4px 24px rgba(255,23,68,0.4); }
  .critical-flash.active { transform: translateY(0); }

  /* Leaderboard */
  .lb-wrap { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  .lb-table { width: 100%; border-collapse: collapse; }
  .lb-table th { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.15em; color: var(--muted); text-transform: uppercase; text-align: left; padding: 12px 16px; border-bottom: 1px solid var(--border); background: var(--bg); }
  .lb-table td { font-size: 13px; padding: 14px 16px; border-bottom: 1px solid var(--border); }
  .lb-table tr:last-child td { border-bottom: none; }
  .lb-table tr:hover td { background: rgba(255,255,255,0.015); }
  .lb-rank { font-family: 'Bebas Neue', sans-serif; font-size: 22px; width: 50px; }
  .lb-rank-1 { color: #ffd740; }
  .lb-rank-2 { color: #b0bec5; }
  .lb-rank-3 { color: #ff8a65; }
  .lb-name { font-weight: 500; }
  .lb-avg { font-family: 'Bebas Neue', sans-serif; font-size: 24px; }
  .lb-empty { text-align: center; padding: 40px; color: var(--muted); font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: 0.1em; }

  /* Call log */
  .log-wrap { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  .log-entry { padding: 12px 16px; border-bottom: 1px solid var(--border); display: grid; grid-template-columns: 1fr auto auto auto; gap: 16px; align-items: center; }
  .log-entry:last-child { border-bottom: none; }
  .log-name { font-size: 13px; font-weight: 500; }
  .log-name-sub { font-family: 'DM Mono', monospace; font-size: 10px; color: var(--muted); }
  .log-detail { font-family: 'DM Mono', monospace; font-size: 11px; color: var(--muted); }
  .log-time { font-family: 'Bebas Neue', sans-serif; font-size: 22px; }

  /* Settings panel (slide-out from right) */
  .settings-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.5); z-index: 98; display: none; }
  .settings-overlay.active { display: block; }
  .settings-panel {
    position: fixed; top: 0; right: 0; bottom: 0; width: 420px; max-width: 90vw;
    background: var(--bg); border-left: 1px solid var(--border); z-index: 99;
    padding: 24px; transform: translateX(100%); transition: transform 0.3s ease;
    overflow-y: auto;
  }
  .settings-panel.active { transform: translateX(0); }
  .settings-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; }
  .settings-title { font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: 0.2em; color: var(--muted); text-transform: uppercase; }
  .settings-section { margin-bottom: 24px; }
  .settings-section .info-title { font-family: 'DM Mono', monospace; font-size: 10px; letter-spacing: .2em; color: var(--muted); text-transform: uppercase; margin-bottom: 14px; }
  .endpoint-row { display: flex; align-items: center; gap: 10px; background: var(--surface); border: 1px solid var(--border-bright); border-radius: 6px; padding: 10px 14px; margin-bottom: 8px; }
  .ep-method { font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; color: var(--orange); background: rgba(255,109,0,.1); border: 1px solid rgba(255,109,0,.2); padding: 2px 7px; border-radius: 4px; flex-shrink: 0; }
  .ep-path { font-family: 'DM Mono', monospace; font-size: 11px; color: var(--blue); flex: 1; word-break: break-all; }
  .copy-btn { background: transparent; border: 1px solid var(--border-bright); color: var(--muted); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; padding: 4px 10px; border-radius: 4px; cursor: pointer; transition: all .2s; flex-shrink: 0; }
  .copy-btn:hover { border-color: var(--blue); color: var(--blue); }
  .sim-bar { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
  .btn-sim { background: transparent; border: 1px solid var(--border-bright); color: var(--text); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: .1em; text-transform: uppercase; padding: 6px 13px; border-radius: 5px; cursor: pointer; transition: all .2s; }
  .btn-sim:hover { border-color: var(--green); color: var(--green); }
  .btn-sim.fb { border-color:#1a3a4a; color:#4fc3f7; } .btn-sim.fb:hover { background:rgba(79,195,247,.08); }
  .btn-sim.gg { border-color:#1a3a22; color:#a5d6a7; } .btn-sim.gg:hover { background:rgba(165,214,167,.08); }
  .btn-clear { background: transparent; border: 1px solid var(--border); color: var(--muted); font-family: 'DM Mono', monospace; font-size: 9px; letter-spacing: 0.1em; text-transform: uppercase; padding: 6px 14px; border-radius: 5px; cursor: pointer; transition: all .2s; }
  .btn-clear:hover { border-color: var(--red); color: var(--red); }

  @media(max-width:900px){
    .stats-bar{grid-template-columns:repeat(3,1fr)}
  }
  @media(max-width:600px){
    .stats-bar{grid-template-columns:repeat(2,1fr)}
    .lead-card{grid-template-columns:1fr;gap:12px}
    .log-entry{grid-template-columns:1fr;gap:8px}
    header{grid-template-columns:1fr;gap:12px;text-align:center}
    .header-right{justify-content:center}
  }
</style>
</head>
<body>

<!-- Critical threshold flash notification -->
<div id="critical-flash" class="critical-flash">
  &#9888; CRITICAL: <span id="flash-name"></span> has been waiting over 5 minutes!
</div>

<!-- Settings slide-out panel -->
<div id="settings-overlay" class="settings-overlay" onclick="toggleSettings()"></div>
<div id="settings-panel" class="settings-panel">
  <div class="settings-header">
    <span class="settings-title">Settings</span>
    <button class="ctrl-btn" onclick="toggleSettings()" title="Close">&#10005;</button>
  </div>
  <div class="settings-section">
    <div class="info-title">GHL Webhook Endpoints</div>
    <div class="endpoint-row">
      <span class="ep-method">POST</span>
      <span class="ep-path" id="ep-lead">${"DOMAIN"}/webhook/lead-in</span>
      <button class="copy-btn" onclick="copyEp('ep-lead')">Copy</button>
    </div>
    <div class="endpoint-row">
      <span class="ep-method">POST</span>
      <span class="ep-path" id="ep-call">${"DOMAIN"}/webhook/call-made</span>
      <button class="copy-btn" onclick="copyEp('ep-call')">Copy</button>
    </div>
    <div class="endpoint-row">
      <span class="ep-method">POST</span>
      <span class="ep-path" id="ep-appt">${"DOMAIN"}/webhook/appointment-booked</span>
      <button class="copy-btn" onclick="copyEp('ep-appt')">Copy</button>
    </div>
  </div>
  <div class="settings-section">
    <div class="info-title">Simulate</div>
    <div class="sim-bar">
      <button class="btn-sim fb" onclick="sim('Facebook')">+ Facebook</button>
      <button class="btn-sim gg" onclick="sim('Google')">+ Google</button>
      <button class="btn-sim" onclick="sim('Website')">+ Website</button>
      <button class="btn-sim" onclick="sim('Referral')">+ Referral</button>
    </div>
    <div style="margin-top:12px">
      <button class="btn-sim" style="border-color:#1a3a22;color:#a5d6a7" onclick="simCall()">&#128222; Sim Call</button>
    </div>
  </div>
  <div class="settings-section">
    <div class="info-title">Data</div>
    <button class="btn-clear" onclick="clearAll()">Clear All Leads</button>
  </div>
</div>

<div class="wrap">
  <header>
    <div>
      <div class="logo-eyebrow">Rooftop Power Co</div>
      <div class="logo-title">Speed to <span>Lead</span></div>
    </div>
    <div class="header-center">
      <div class="live-clock" id="live-clock"></div>
      <div class="live-date" id="live-date"></div>
    </div>
    <div class="header-right">
      <div class="live-badge">
        <div class="live-dot" id="conn-dot"></div>
        <span id="conn-label">Connecting...</span>
      </div>
      <button class="ctrl-btn" onclick="toggleSettings()" title="Settings">&#9881;</button>
      <button class="ctrl-btn" id="fs-btn" onclick="toggleFullscreen()" title="Fullscreen">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 6V2h4M14 6V2h-4M2 10v4h4M14 10v4h-4"/></svg>
      </button>
    </div>
  </header>

  <div class="stats-bar">
    <div class="stat-card"><div class="stat-label">Leads Today</div><div class="stat-value blue" id="s-total">0</div></div>
    <div class="stat-card"><div class="stat-label">Avg Speed</div><div class="stat-value green" id="s-avg">--</div></div>
    <div class="stat-card"><div class="stat-label">New Leads Called</div><div class="stat-value green" id="s-called">0</div></div>
    <div class="stat-card"><div class="stat-label">Outbound Calls</div><div class="stat-value blue" id="s-outbound">0</div></div>
    <div class="stat-card"><div class="stat-label">Waiting</div><div class="stat-value red" id="s-waiting">0</div></div>
    <div class="stat-card"><div class="stat-label">Top Closer</div><div class="stat-value yellow" id="s-top">--</div><div class="stat-sub" id="s-top-name">&nbsp;</div></div>
  </div>

  <!-- Tab navigation -->
  <div class="tabs-bar">
    <button class="tab-btn active" data-tab="leads" onclick="switchTab('leads')">Active Leads</button>
    <button class="tab-btn" data-tab="leaderboard" onclick="switchTab('leaderboard')">Leaderboard</button>
  </div>

  <!-- Leads tab -->
  <div id="tab-leads">
    <div class="section-head">
      <span class="section-title">Active Leads</span>
    </div>
    <div class="leads-list" id="leads-list"></div>
  </div>

  <!-- Leaderboard tab -->
  <div id="tab-leaderboard" style="display:none">
    <div class="section-head">
      <span class="section-title">Closer Performance</span>
    </div>
    <div id="leaderboard"></div>

    <div class="section-head" style="margin-top:28px">
      <span class="section-title">Today's Call Log</span>
    </div>
    <div id="call-log"></div>
  </div>
</div>

<script>
// ── State ──────────────────────────────────────────────────────────────────
var leads = [];
var CLOSERS = ${closersJSON};
var es;
var audioCtx;
var criticalAlerted = {};
var currentTab = 'leads';
var outboundCount = 0;

// Set domain in endpoint display
var domain = window.location.origin;
document.getElementById('ep-lead').textContent = domain + '/webhook/lead-in';
document.getElementById('ep-call').textContent = domain + '/webhook/call-made';

// ── Live Clock ─────────────────────────────────────────────────────────────
var DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
var MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
function updateClock() {
  var now = new Date();
  var h = now.getHours();
  var m = String(now.getMinutes()).padStart(2, '0');
  var s = String(now.getSeconds()).padStart(2, '0');
  var ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  document.getElementById('live-clock').textContent = h + ':' + m + ':' + s + ' ' + ampm;
  document.getElementById('live-date').textContent = DAYS[now.getDay()] + ', ' + MONTHS[now.getMonth()] + ' ' + now.getDate() + ', ' + now.getFullYear();
}
updateClock();

// ── Audio ─────────────────────────────────────────────────────────────────
function getAudioCtx() {
  if (!audioCtx) {
    try { audioCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch(e) {}
  }
  return audioCtx;
}

function playNewLeadSound() {
  var ctx = getAudioCtx();
  if (!ctx) return;
  try {
    var osc1 = ctx.createOscillator();
    var gain1 = ctx.createGain();
    osc1.connect(gain1);
    gain1.connect(ctx.destination);
    osc1.frequency.value = 880;
    osc1.type = 'sine';
    gain1.gain.setValueAtTime(0.25, ctx.currentTime);
    gain1.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.3);
    osc1.start(ctx.currentTime);
    osc1.stop(ctx.currentTime + 0.3);
    var osc2 = ctx.createOscillator();
    var gain2 = ctx.createGain();
    osc2.connect(gain2);
    gain2.connect(ctx.destination);
    osc2.frequency.value = 1320;
    osc2.type = 'sine';
    gain2.gain.setValueAtTime(0.25, ctx.currentTime + 0.15);
    gain2.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.5);
    osc2.start(ctx.currentTime + 0.15);
    osc2.stop(ctx.currentTime + 0.5);
  } catch(e) {}
}

function playUrgentSound() {
  var ctx = getAudioCtx();
  if (!ctx) return;
  try {
    for (var i = 0; i < 3; i++) {
      var osc = ctx.createOscillator();
      var gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.frequency.value = 440;
      osc.type = 'square';
      var t = ctx.currentTime + i * 0.2;
      gain.gain.setValueAtTime(0.15, t);
      gain.gain.exponentialRampToValueAtTime(0.01, t + 0.12);
      osc.start(t);
      osc.stop(t + 0.12);
    }
  } catch(e) {}
}

// ── Critical Flash ─────────────────────────────────────────────────────────
function showCriticalFlash(lead) {
  var name = leadDisplayName(lead);
  var flash = document.getElementById('critical-flash');
  document.getElementById('flash-name').textContent = name;
  flash.classList.add('active');
  playUrgentSound();
  setTimeout(function() { flash.classList.remove('active'); }, 4000);
}

// ── Settings Panel ─────────────────────────────────────────────────────────
function toggleSettings() {
  document.getElementById('settings-panel').classList.toggle('active');
  document.getElementById('settings-overlay').classList.toggle('active');
}

// ── Fullscreen ─────────────────────────────────────────────────────────────
function toggleFullscreen() {
  if (!document.fullscreenElement) {
    document.documentElement.requestFullscreen().catch(function(){});
  } else {
    document.exitFullscreen();
  }
}
document.addEventListener('fullscreenchange', function() {
  var btn = document.getElementById('fs-btn');
  if (document.fullscreenElement) {
    btn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M5 2v3H2M11 2v3h3M5 14v-3H2M11 14v-3h3"/></svg>';
  } else {
    btn.innerHTML = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 6V2h4M14 6V2h-4M2 10v4h4M14 10v4h-4"/></svg>';
  }
});

// ── Tab Management ─────────────────────────────────────────────────────────
function switchTab(tab) {
  currentTab = tab;
  var btns = document.querySelectorAll('.tab-btn');
  for (var i = 0; i < btns.length; i++) {
    btns[i].classList.toggle('active', btns[i].getAttribute('data-tab') === tab);
  }
  document.getElementById('tab-leads').style.display = tab === 'leads' ? '' : 'none';
  document.getElementById('tab-leaderboard').style.display = tab === 'leaderboard' ? '' : 'none';
  if (tab === 'leaderboard') {
    renderLeaderboard();
    fetchCallLog();
  }
}

// ── SSE Connection ─────────────────────────────────────────────────────────
function connect() {
  es = new EventSource('/events');

  es.addEventListener('init', function(e) {
    var data = JSON.parse(e.data);
    leads = data.leads || [];
    if (data.closers) CLOSERS = data.closers;
    outboundCount = data.outboundCount || 0;
    leads.forEach(function(l) {
      if (!l.called && (Date.now() - l.arrivedAt) >= 300000) {
        criticalAlerted[l.id] = true;
      }
    });
    render();
    setConn(true);
  });

  es.addEventListener('lead_in', function(e) {
    var lead = JSON.parse(e.data);
    if (!leads.find(function(l) { return l.id === lead.id; })) {
      leads.unshift(lead);
      render();
      playNewLeadSound();
    }
  });

  es.addEventListener('call_made', function(e) {
    var d = JSON.parse(e.data);
    var lead = leads.find(function(l) { return l.id === d.id; });
    if (lead) {
      lead.called = true;
      lead.callTime = d.callTime;
      lead.calledAt = Date.now();
      lead.setter = d.setter || lead.setter;
      render();
    }
  });

  es.addEventListener('setter_assigned', function(e) {
    var d = JSON.parse(e.data);
    var lead = leads.find(function(l) { return l.id === d.id; });
    if (lead) {
      lead.setter = d.setter;
    }
  });

  es.addEventListener('outbound_update', function(e) {
    var d = JSON.parse(e.data);
    outboundCount = d.count;
    document.getElementById('s-outbound').textContent = outboundCount;
  });

  es.addEventListener('clear', function() {
    leads = [];
    criticalAlerted = {};
    render();
  });

  es.onerror = function() { setConn(false); setTimeout(connect, 3000); };
}

function setConn(ok) {
  document.getElementById('conn-dot').className = 'live-dot' + (ok ? '' : ' conn-dot-disconnected');
  document.getElementById('conn-label').textContent = ok ? 'Live' : 'Reconnecting...';
}

// ── Helpers ────────────────────────────────────────────────────────────────
function fmt(s) {
  if (s < 60) return '0:' + String(s).padStart(2,'0');
  var m = Math.floor(s/60), sec = s%60;
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
  if (s < 60) return 'On fire';
  if (s < 180) return 'Good';
  if (s < 300) return 'Urgent';
  return 'CRITICAL';
}
function srcClass(src) {
  var m = {Facebook:'source-facebook',Google:'source-google',Website:'source-website',Referral:'source-referral'};
  return m[src] || 'source-other';
}
function fmtTime(ts) {
  var d = new Date(ts);
  var h = d.getHours();
  var min = String(d.getMinutes()).padStart(2, '0');
  var ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return h + ':' + min + ' ' + ampm;
}
function fmtTimeFull(iso) {
  var d = new Date(iso);
  var h = d.getHours();
  var min = String(d.getMinutes()).padStart(2, '0');
  var sec = String(d.getSeconds()).padStart(2, '0');
  var ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return h + ':' + min + ':' + sec + ' ' + ampm;
}
function leadDisplayName(lead) {
  var name = (lead.firstName + ' ' + lead.lastName).trim();
  if (!name || name === 'Unknown') return 'New Lead';
  return name;
}

// ── Tab Title Badge ────────────────────────────────────────────────────────
function updateTabTitle() {
  var waiting = 0;
  for (var i = 0; i < leads.length; i++) { if (!leads[i].called) waiting++; }
  document.title = waiting > 0 ? '(' + waiting + ') Speed to Lead — RTP' : 'Speed to Lead — RTP';
}

// ── Render ─────────────────────────────────────────────────────────────────
function render() {
  var list = document.getElementById('leads-list');
  if (!leads.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">&#128225;</div><div class="empty-text">Waiting for incoming leads</div></div>';
    updateStats(); updateTabTitle(); return;
  }
  // Filter out called leads older than 90s (already faded out)
  var visible = leads.filter(function(lead) {
    if (lead.called && lead.calledAt) {
      var sinceCall = Math.round((Date.now() - lead.calledAt) / 1000);
      if (sinceCall > 90) return false;
    }
    return true;
  });
  if (!visible.length) {
    list.innerHTML = '<div class="empty-state"><div class="empty-icon">&#128225;</div><div class="empty-text">All leads have been called</div></div>';
    updateStats(); updateTabTitle(); return;
  }
  list.innerHTML = visible.map(function(lead) {
    var el = lead.called ? lead.callTime : Math.round((Date.now() - lead.arrivedAt) / 1000);
    var name = leadDisplayName(lead);
    var fadingClass = '';
    if (lead.called && lead.calledAt) {
      var sinceCall = Math.round((Date.now() - lead.calledAt) / 1000);
      if (sinceCall >= 60) fadingClass = ' lead-fading';
    }
    var timerContent = '';
    if (lead.called) {
      timerContent = '<div class="timer-display t-called" id="t-' + lead.id + '">' + fmt(lead.callTime) + '</div>' +
        '<div class="timer-label" id="tl-' + lead.id + '">Called</div>';
    } else {
      timerContent = '<div class="timer-display ' + tClass(el, false) + '" id="t-' + lead.id + '">' + fmt(el) + '</div>' +
        '<div class="timer-label" id="tl-' + lead.id + '">' + tLabel(el, false) + '</div>';
    }
    return '<div class="lead-card slide-in ' + cStatus(el, lead.called) + fadingClass + '" id="c-' + lead.id + '">' +
      '<div><div class="lead-name">' + name + '</div>' +
      '<div class="lead-meta"><span class="lead-phone">' + lead.phone + '</span>' +
      '<span class="source-pill ' + srcClass(lead.source) + '">' + lead.source + '</span></div>' +
      '<div class="lead-timestamp">Arrived ' + fmtTime(lead.arrivedAt) + '</div></div>' +
      '<div>' + timerContent + '</div></div>';
  }).join('');
  updateStats();
  updateTabTitle();
}

function updateTimers() {
  updateClock();
  var needsRerender = false;
  leads.forEach(function(lead) {
    // Auto-fade called leads after 60s
    if (lead.called && lead.calledAt) {
      var sinceCall = Math.round((Date.now() - lead.calledAt) / 1000);
      if (sinceCall >= 60 && sinceCall < 92) {
        var card = document.getElementById('c-' + lead.id);
        if (card && !card.classList.contains('lead-fading')) {
          card.classList.add('lead-fading');
        }
      }
      if (sinceCall >= 92) {
        var card = document.getElementById('c-' + lead.id);
        if (card) needsRerender = true;
      }
      return;
    }
    var el = Math.round((Date.now() - lead.arrivedAt) / 1000);
    // Check critical threshold (5 minutes)
    if (el >= 300 && !criticalAlerted[lead.id]) {
      criticalAlerted[lead.id] = true;
      showCriticalFlash(lead);
    }
    var te = document.getElementById('t-' + lead.id);
    var tle = document.getElementById('tl-' + lead.id);
    var ce = document.getElementById('c-' + lead.id);
    if (!te) return;
    te.textContent = fmt(el);
    te.className = 'timer-display ' + tClass(el, false);
    tle.textContent = tLabel(el, false);
    // Preserve slide-in class during status updates
    ce.className = 'lead-card slide-in ' + cStatus(el, false);
  });
  if (needsRerender) render();
  updateStats();
  updateTabTitle();
}

function updateStats() {
  var called = [];
  var waiting = [];
  for (var i = 0; i < leads.length; i++) {
    if (leads[i].called) called.push(leads[i]);
    else waiting.push(leads[i]);
  }
  document.getElementById('s-total').textContent = leads.length;
  document.getElementById('s-called').textContent = called.length;
  document.getElementById('s-waiting').textContent = waiting.length;
  document.getElementById('s-outbound').textContent = outboundCount;
  if (called.length) {
    var avg = Math.round(called.reduce(function(s,l) { return s + l.callTime; }, 0) / called.length);
    document.getElementById('s-avg').textContent = fmt(avg);
  } else {
    document.getElementById('s-avg').textContent = '--';
  }

  // Top closer
  var closerStats = {};
  for (var i = 0; i < leads.length; i++) {
    var l = leads[i];
    if (l.called && l.setter) {
      if (!closerStats[l.setter]) closerStats[l.setter] = { total: 0, count: 0 };
      closerStats[l.setter].total += l.callTime;
      closerStats[l.setter].count++;
    }
  }
  var bestName = null, bestAvg = Infinity;
  for (var name in closerStats) {
    var a = closerStats[name].total / closerStats[name].count;
    if (a < bestAvg) { bestAvg = a; bestName = name; }
  }
  if (bestName) {
    document.getElementById('s-top').textContent = fmt(Math.round(bestAvg));
    document.getElementById('s-top-name').textContent = bestName;
  } else {
    document.getElementById('s-top').textContent = '--';
    document.getElementById('s-top-name').innerHTML = '&nbsp;';
  }
}

// ── Leaderboard ────────────────────────────────────────────────────────────
function renderLeaderboard() {
  var closerStats = {};
  for (var i = 0; i < CLOSERS.length; i++) {
    closerStats[CLOSERS[i]] = { calls: 0, totalTime: 0, best: Infinity, assigned: 0 };
  }
  for (var i = 0; i < leads.length; i++) {
    var l = leads[i];
    if (l.setter && closerStats[l.setter]) {
      closerStats[l.setter].assigned++;
      if (l.called) {
        closerStats[l.setter].calls++;
        closerStats[l.setter].totalTime += l.callTime;
        if (l.callTime < closerStats[l.setter].best) closerStats[l.setter].best = l.callTime;
      }
    }
  }

  var ranked = [];
  for (var i = 0; i < CLOSERS.length; i++) {
    var s = closerStats[CLOSERS[i]];
    if (s.calls > 0) {
      ranked.push({ name: CLOSERS[i], calls: s.calls, avg: Math.round(s.totalTime / s.calls), best: s.best, assigned: s.assigned });
    }
  }
  ranked.sort(function(a, b) { return a.avg - b.avg; });

  var inactive = [];
  for (var i = 0; i < CLOSERS.length; i++) {
    if (closerStats[CLOSERS[i]].calls === 0) {
      inactive.push({ name: CLOSERS[i], assigned: closerStats[CLOSERS[i]].assigned });
    }
  }

  var el = document.getElementById('leaderboard');
  if (!ranked.length && !inactive.length) {
    el.innerHTML = '<div class="lb-wrap"><div class="lb-empty">No closer data yet.</div></div>';
    return;
  }

  var html = '<div class="lb-wrap"><table class="lb-table"><thead><tr>' +
    '<th>Rank</th><th>Closer</th><th>Calls</th><th>Avg Speed</th><th>Best</th>' +
    '</tr></thead><tbody>';

  for (var i = 0; i < ranked.length; i++) {
    var r = ranked[i];
    var rankClass = i < 3 ? ' lb-rank-' + (i+1) : '';
    var avgColor = r.avg < 60 ? 'green' : r.avg < 180 ? 'yellow' : r.avg < 300 ? 'orange' : 'red';
    html += '<tr><td class="lb-rank' + rankClass + '">#' + (i+1) + '</td>' +
      '<td class="lb-name">' + r.name + '</td>' +
      '<td>' + r.calls + '</td>' +
      '<td class="lb-avg ' + avgColor + '">' + fmt(r.avg) + '</td>' +
      '<td>' + fmt(r.best) + '</td></tr>';
  }

  for (var i = 0; i < inactive.length; i++) {
    html += '<tr style="opacity:0.4"><td class="lb-rank">&#8212;</td>' +
      '<td class="lb-name">' + inactive[i].name + '</td>' +
      '<td>' + inactive[i].assigned + ' assigned</td>' +
      '<td class="lb-avg">--</td><td>--</td></tr>';
  }

  html += '</tbody></table></div>';
  el.innerHTML = html;
}

// ── Call Log ───────────────────────────────────────────────────────────────
function fetchCallLog() {
  fetch('/api/daily-summary').then(function(r) { return r.json(); }).then(function(data) {
    var entries = data.entries || [];
    var el = document.getElementById('call-log');
    if (!entries.length) {
      el.innerHTML = '<div class="lb-wrap"><div class="lb-empty">No calls logged today.</div></div>';
      return;
    }
    var html = '<div class="log-wrap">';
    for (var i = entries.length - 1; i >= 0; i--) {
      var e = entries[i];
      var timeColor = e.timeToCall < 60 ? 'green' : e.timeToCall < 180 ? 'yellow' : e.timeToCall < 300 ? 'orange' : 'red';
      html += '<div class="log-entry">' +
        '<div><div class="log-name">' + e.name + '</div><div class="log-name-sub">' + (e.source || '') + '</div></div>' +
        '<div class="log-detail">' + (e.setter || 'Unassigned') + '</div>' +
        '<div class="log-detail">' + fmtTimeFull(e.calledAt) + '</div>' +
        '<div class="log-time ' + timeColor + '">' + fmt(e.timeToCall) + '</div></div>';
    }
    html += '</div>';
    el.innerHTML = html;
  }).catch(function() {});
}

// ── Actions ────────────────────────────────────────────────────────────────
function clearAll() {
  if (confirm('Clear all leads?')) fetch('/api/clear', { method: 'POST' });
}

function copyEp(id) {
  var el = document.getElementById(id);
  navigator.clipboard.writeText(el.textContent).then(function() {
    var btn = el.nextElementSibling;
    btn.textContent = 'Copied!';
    setTimeout(function() { btn.textContent = 'Copy'; }, 1500);
  });
}

// ── Simulate (dev only) ────────────────────────────────────────────────────
var NAMES = [['James','Wilson'],['Maria','Garcia'],['David','Chen'],['Sarah','Johnson'],['Mike','Torres'],['Lisa','Patel']];
var PHONES = ['+14015550101','+14015550202','+15085550303','+18605550404'];
var si = 0;
function sim(source) {
  var pair = NAMES[si++ % NAMES.length];
  fetch('/webhook/lead-in', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ contact_id: 'sim_'+Date.now(), first_name:pair[0], last_name:pair[1], phone:PHONES[Math.floor(Math.random()*PHONES.length)], source: source })
  });
}
function simCall() {
  var pending = leads.filter(function(l) { return !l.called; });
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
