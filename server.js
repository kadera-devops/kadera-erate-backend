require("dotenv").config();
const express  = require("express");
const cors     = require("cors");
const { createClient } = require("@supabase/supabase-js");
const cron     = require("node-cron");
const fetch    = require("node-fetch");

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Supabase ─────────────────────────────────────────────────────────────────
const supabase = createClient(
  process.env.SUPABASE_URL || "https://myeucmajiebssqmphzjm.supabase.co",
  process.env.SUPABASE_ANON_KEY
);

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// ── USAC Open Data API config ─────────────────────────────────────────────────
const USAC_BASE = "https://opendata.usac.org/resource";
const CURRENT_FY = "2025";

const USAC_APP_TOKEN = process.env.USAC_APP_TOKEN || "";

// ── Helper: fetch all pages from USAC Socrata API ─────────────────────────────
async function usacFetch(endpoint, params = {}) {
  const limit  = 1000;
  let   offset = 0;
  let   all    = [];
  while (true) {
    const query = new URLSearchParams({ ...params, "$limit": limit, "$offset": offset });
    const url   = `${USAC_BASE}/${endpoint}?${query}`;
    console.log(`USAC fetch: ${url}`);
    const res  = await fetch(url, { headers: { "X-App-Token": USAC_APP_TOKEN } });
    const data = await res.json();
    if (!Array.isArray(data) || data.length === 0) break;
    all = all.concat(data);
    if (data.length < limit) break;
    offset += limit;
  }
  return all;
}

// ── Sync: Form 470 ───────────────────────────────────────────────────────────
async function sync470s() {
  console.log("Syncing Form 470s (TX only)...");
  try {
    // Filter to TX only to keep memory usage manageable
    // Use $where to filter server-side before data is transferred
    const data = await usacFetch("jt8s-3q52.json", {
      funding_year: CURRENT_FY,
      "$where": "billed_entity_state='TX'"
    });
    if (!data.length) { console.log("No 470 data returned"); return; }
    console.log("Sample 470 keys:", Object.keys(data[0]).join(", "));
    // Deduplicate by application_number before upsert
    const seen = new Set();
    const rows = [];
    for (const d of data) {
      const key = d.application_number || null;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      rows.push({
        application_number:   d.application_number   || null,
        funding_year:         d.funding_year          || CURRENT_FY,
        billed_entity_name:   d.billed_entity_name    || null,
        billed_entity_number: d.billed_entity_number  || null,
        state:                d.billed_entity_state   || null,
        service_category:     d.category_two_description || d.service_category || null,
        application_status:   d.fcc_form_470_status   || d.application_status || null,
        date_posted:          d.certified_date_time   || d.last_modified_date_time || null,
        bid_due_date:         d.allowable_contract_date || null,
        tech_contact_name:    d.technical_contact_name  || d.contact_name || null,
        tech_contact_email:   d.technical_contact_email || d.contact_email || null,
        tech_contact_phone:   d.technical_contact_phone || d.contact_phone || null,
        consultant_name:      null,
        consultant_email:     null,
        narrative:            d.form_nickname         || null,
        raw:                  d,
      });
    }
    console.log(`Deduplicated: ${data.length} → ${rows.length} unique records`);
    for (let i = 0; i < rows.length; i += 200) {
      const batch = rows.slice(i, i + 200);
      const { error } = await supabase.from("form_470s").upsert(batch, { onConflict: "application_number" });
      if (error) console.error("470 upsert error:", error.message);
      else console.log(`  Upserted batch ${Math.floor(i/200)+1} (${batch.length} records)`);
    }
    console.log(`Synced ${rows.length} Form 470 records`);
  } catch (err) {
    console.error("sync470s error:", err.message);
  }
}

// ── Sync: Form 471 ───────────────────────────────────────────────────────────
async function sync471s() {
  console.log("Syncing Form 471s...");
  try {
    const data = await usacFetch("hbj5-3xbd.json", { funding_year: CURRENT_FY, "$where": "applicant_state='TX'" });
    if (!data.length) { console.log("No 471 data returned"); return; }
    const rows = data.map(d => ({
      application_number:   d.application_number  || null,
      frn:                  d.frn                 || null,
      funding_year:         d.funding_year        || CURRENT_FY,
      billed_entity_name:   d.billed_entity_name  || null,
      billed_entity_number: d.billed_entity_number|| null,
      state:                d.state               || null,
      service_type:         d.service_type        || null,
      amount_requested:     parseFloat(d.amount_requested)  || null,
      amount_committed:     parseFloat(d.amount_committed)  || null,
      application_status:   d.application_status  || null,
      priority:             d.priority            || null,
      date_filed:           d.date_filed          || null,
      raw:                  d,
    }));
    const { error } = await supabase.from("form_471s").upsert(rows, { onConflict: "frn" });
    if (error) console.error("471 upsert error:", error.message);
    else console.log(`Synced ${rows.length} Form 471 records`);
  } catch (err) {
    console.error("sync471s error:", err.message);
  }
}

// ── Sync: Commitments ─────────────────────────────────────────────────────────
async function syncCommitments() {
  console.log("Syncing Commitments...");
  try {
    const data = await usacFetch("avi8-svp9.json", { funding_year: CURRENT_FY, "$where": "applicant_state='TX'" });
    if (!data.length) { console.log("No commitments data returned"); return; }
    const rows = data.map(d => ({
      frn:                  d.frn                  || null,
      funding_year:         d.funding_year         || CURRENT_FY,
      billed_entity_name:   d.billed_entity_name   || null,
      billed_entity_number: d.billed_entity_number || null,
      state:                d.state                || null,
      service_type:         d.service_type         || null,
      amount_committed:     parseFloat(d.amount_committed) || null,
      commitment_date:      d.commitment_date      || null,
      disbursed_amount:     parseFloat(d.disbursed_amount) || null,
      status:               d.status               || null,
      raw:                  d,
    }));
    const { error } = await supabase.from("commitments").upsert(rows, { onConflict: "frn" });
    if (error) console.error("Commitments upsert error:", error.message);
    else console.log(`Synced ${rows.length} Commitment records`);
  } catch (err) {
    console.error("syncCommitments error:", err.message);
  }
}

async function syncAll() {
  console.log("=== Starting full USAC sync ===");
  await sync470s();
  await sync471s();
  await syncCommitments();
  console.log("=== USAC sync complete ===");
}

// ── Daily cron: 2am CT ────────────────────────────────────────────────────────
cron.schedule("0 8 * * *", syncAll); // 8am UTC = 2am CT

// ── AUTH middleware ───────────────────────────────────────────────────────────
async function requireAuth(req, res, next) {
  const token = req.headers.authorization?.replace("Bearer ", "");
  if (!token) return res.status(401).json({ status: "error", message: "Unauthorized" });
  const { data: { user }, error } = await supabase.auth.getUser(token);
  if (error || !user) return res.status(401).json({ status: "error", message: "Unauthorized" });
  req.user = user;
  next();
}

// ── ROUTES ────────────────────────────────────────────────────────────────────

// Health check
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", time: new Date().toISOString() });
});

// Temporary sync trigger — no auth required for initial data load
app.get("/api/sync-now", async (req, res) => {
  res.json({ status: "started", message: "Sync running — check logs in 2-3 minutes" });
  syncAll();
});

// Manual sync trigger (requires auth)
app.post("/api/sync", requireAuth, async (req, res) => {
  res.json({ status: "started", message: "Sync running in background" });
  syncAll();
});

// ── GET /api/470s ─────────────────────────────────────────────────────────────
app.get("/api/470s", requireAuth, async (req, res) => {
  try {
    const { state, status, service_category, search, limit = 100, offset = 0 } = req.query;
    let query = supabase.from("form_470s").select("*").order("date_posted", { ascending: false }).range(Number(offset), Number(offset) + Number(limit) - 1);
    if (state)            query = query.eq("state", state.toUpperCase());
    if (status)           query = query.ilike("application_status", `%${status}%`);
    if (service_category) query = query.ilike("service_category", `%${service_category}%`);
    if (search)           query = query.or(`billed_entity_name.ilike.%${search}%,application_number.ilike.%${search}%,billed_entity_number.ilike.%${search}%`);
    const { data, error, count } = await query;
    if (error) throw error;
    res.json({ status: "success", data: data || [], count });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── GET /api/471s ─────────────────────────────────────────────────────────────
app.get("/api/471s", requireAuth, async (req, res) => {
  try {
    const { state, status, service_type, search, limit = 100, offset = 0 } = req.query;
    let query = supabase.from("form_471s").select("*").order("date_filed", { ascending: false }).range(Number(offset), Number(offset) + Number(limit) - 1);
    if (state)        query = query.eq("state", state.toUpperCase());
    if (status)       query = query.ilike("application_status", `%${status}%`);
    if (service_type) query = query.ilike("service_type", `%${service_type}%`);
    if (search)       query = query.or(`billed_entity_name.ilike.%${search}%,frn.ilike.%${search}%,billed_entity_number.ilike.%${search}%`);
    const { data, error } = await query;
    if (error) throw error;
    res.json({ status: "success", data: data || [] });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── GET /api/commitments ──────────────────────────────────────────────────────
app.get("/api/commitments", requireAuth, async (req, res) => {
  try {
    const { state, status, service_type, search, limit = 100, offset = 0 } = req.query;
    let query = supabase.from("commitments").select("*").order("commitment_date", { ascending: false }).range(Number(offset), Number(offset) + Number(limit) - 1);
    if (state)        query = query.eq("state", state.toUpperCase());
    if (status)       query = query.ilike("status", `%${status}%`);
    if (service_type) query = query.ilike("service_type", `%${service_type}%`);
    if (search)       query = query.or(`billed_entity_name.ilike.%${search}%,frn.ilike.%${search}%,billed_entity_number.ilike.%${search}%`);
    const { data, error } = await query;
    if (error) throw error;
    res.json({ status: "success", data: data || [] });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── GET /api/search/contacts ──────────────────────────────────────────────────
app.get("/api/search/contacts", requireAuth, async (req, res) => {
  try {
    const { name, email, state, search } = req.query;
    let query = supabase.from("form_470s")
      .select("application_number, funding_year, billed_entity_name, billed_entity_number, state, tech_contact_name, tech_contact_email, tech_contact_phone, service_category, application_status, date_posted")
      .not("tech_contact_name", "is", null)
      .order("date_posted", { ascending: false })
      .limit(200);
    if (state)  query = query.eq("state", state.toUpperCase());
    if (name)   query = query.ilike("tech_contact_name", `%${name}%`);
    if (email)  query = query.ilike("tech_contact_email", `%${email}%`);
    if (search) query = query.or(`tech_contact_name.ilike.%${search}%,tech_contact_email.ilike.%${search}%,billed_entity_name.ilike.%${search}%`);
    const { data, error } = await query;
    if (error) throw error;
    res.json({ status: "success", data: data || [] });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── GET /api/stats ────────────────────────────────────────────────────────────
app.get("/api/stats", requireAuth, async (req, res) => {
  try {
    const [r470, r471, rCom] = await Promise.all([
      supabase.from("form_470s").select("*", { count: "exact", head: true }),
      supabase.from("form_471s").select("*", { count: "exact", head: true }),
      supabase.from("commitments").select("*", { count: "exact", head: true }),
    ]);
    const open470 = await supabase.from("form_470s").select("*", { count: "exact", head: true }).ilike("application_status", "%open%");
    res.json({
      status: "success",
      data: {
        total_470s:  r470.count  || 0,
        total_471s:  r471.count  || 0,
        total_commitments: rCom.count || 0,
        open_470s:   open470.count || 0,
        funding_year: CURRENT_FY,
      }
    });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log("============================================");
  console.log("  KADERA E-RATE — API Server");
  console.log("============================================");
  console.log(`  Status  : Running`);
  console.log(`  Port    : ${PORT}`);
  console.log(`  Mode    : ${process.env.NODE_ENV || "development"}`);
  console.log(`  Time    : ${new Date().toLocaleTimeString()}`);
  console.log("============================================");
});
