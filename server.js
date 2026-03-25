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
const CURRENT_FY = "2026";

const USAC_APP_TOKEN = process.env.USAC_APP_TOKEN || "";

// ── Helper: fetch all pages from USAC Socrata API ─────────────────────────────
async function usacFetch(endpoint, params = {}, maxRecords = 50000) {
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
    if (all.length >= maxRecords) { console.log(`Max records (${maxRecords}) reached`); break; }
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
// Dataset: 9s6i-myen — E-Rate FCC Form 471 Download Tool
async function sync471s() {
  console.log("Syncing Form 471s (TX only)...");
  try {
    const data = await usacFetch("9s6i-myen.json", { funding_year: CURRENT_FY, "$where": "org_state='TX'" });
    if (!data.length) { console.log("No 471 data returned"); return; }
    console.log(`Fetched ${data.length} Form 471 records`);
    const seen471 = new Set();
    const rows = [];
    for (const d of data) {
      const key = `${d.application_number}-${d.funding_year}`;
      if (!key || seen471.has(key)) continue;
      seen471.add(key);
      rows.push({
        application_number:          d.application_number          || null,
        funding_year:                d.funding_year                || CURRENT_FY,
        organization_name:           d.organization_name           || null,
        epc_organization_id:         d.epc_organization_id         || null,
        org_state:                   d.org_state                   || null,
        chosen_category_of_service:  d.chosen_category_of_service  || null,
        form_471_status_name:        d.form_471_status_name        || null,
        funding_request_amount:      parseFloat(d.funding_request_amount) || null,
        pre_discount_eligible_amount: parseFloat(d.pre_discount_eligible_amount) || null,
        c1_discount:                 d.c1_discount                 || null,
        c2_discount:                 d.c2_discount                 || null,
        cnct_first_name:             d.cnct_first_name             || null,
        cnct_last_name:              d.cnct_last_name              || null,
        cnct_email:                  d.cnct_email                  || null,
        cnct_phone:                  d.cnct_phone                  || null,
        certified_datetime:          d.certified_datetime          || null,
        raw:                         d,
      });
    }
    for (let i = 0; i < rows.length; i += 200) {
      const batch = rows.slice(i, i + 200);
      const { error } = await supabase.from("form_471s").upsert(batch, { onConflict: "application_number,funding_year" });
      if (error) console.error("471 upsert error:", error.message);
      else console.log(`  Upserted 471 batch ${Math.floor(i/200)+1} (${batch.length} records)`);
    }
    console.log(`Synced ${rows.length} Form 471 records`);
  } catch (err) {
    console.error("sync471s error:", err.message);
  }
}

// ── Sync: Commitments ─────────────────────────────────────────────────────────
// Dataset: srbr-2d59 — E-Rate Commitments
async function syncCommitments() {
  console.log("Syncing Commitments (TX only)...");
  try {
    const data = await usacFetch("srbr-2d59.json", { funding_year: CURRENT_FY, "$where": "state='TX'" });
    if (!data.length) { console.log("No commitments data returned"); return; }
    console.log(`Fetched ${data.length} Commitment records`);
    const rows = data.map(d => ({
      funding_request_number:        d.funding_request_number       || null,
      application_number:            d.application_number           || null,
      funding_year:                  d.funding_year                 || CURRENT_FY,
      organization_name:             d.organization_name            || null,
      ben:                           d.ben                          || null,
      state:                         d.state                        || null,
      form_471_service_type_name:    d.form_471_service_type_name   || null,
      form_471_frn_status_name:      d.form_471_frn_status_name     || null,
      funding_commitment_request:    parseFloat(d.funding_commitment_request) || null,
      total_pre_discount_costs:      parseFloat(d.total_pre_discount_costs) || null,
      dis_pct:                       d.dis_pct                      || null,
      fcdl_letter_date:              d.fcdl_letter_date             || null,
      cnct_name:                     d.cnct_name                    || null,
      cnct_email:                    d.cnct_email                   || null,
      spin_name:                     d.spin_name                    || null,
      raw:                           d,
    }));
    for (let i = 0; i < rows.length; i += 200) {
      const batch = rows.slice(i, i + 200);
      const { error } = await supabase.from("commitments").upsert(batch, { onConflict: "funding_request_number" });
      if (error) console.error("Commitments upsert error:", error.message);
      else console.log(`  Upserted commitments batch ${Math.floor(i/200)+1} (${batch.length} records)`);
    }
    console.log(`Synced ${rows.length} Commitment records`);
  } catch (err) {
    console.error("syncCommitments error:", err.message);
  }
}

// ── Sync: FRN Line Items ─────────────────────────────────────────────────────
async function syncLineItems() {
  const LINE_ITEMS_FY = "2025"; // FY2025 — used for competitive intel only, FY2026 data not yet published
  console.log(`Syncing FRN Line Items FY${LINE_ITEMS_FY} (TX only)...`);
  try {
    const data = await usacFetch("hbj5-2bpj.json", {
      funding_year: LINE_ITEMS_FY,
      "$where": "upper(state)='TX'"
    }, 100000);
    if (!data.length) { console.log("No line item data returned"); return; }
    console.log(`Fetched ${data.length} FRN line item records`);
    const seen = new Set();
    const rows = [];
    for (const d of data) {
      const key = `${d.funding_request_number}-${d.form_471_line_item_number}`;
      if (!key || seen.has(key)) continue;
      seen.add(key);
      rows.push({
        application_number:                          d.application_number                          || null,
        funding_year:                                d.funding_year                                || CURRENT_FY,
        funding_request_number:                      d.funding_request_number                      || null,
        form_471_line_item_number:                   d.form_471_line_item_number                   || null,
        ben:                                         d.ben                                         || null,
        organization_name:                           d.organization_name                           || null,
        state:                                       d.state                                       || null,
        form_471_manufacturer_name:                  d.form_471_manufacturer_name                  || null,
        other_manufacturer_desc:                     d.other_manufacturer_desc                     || null,
        model_of_equipment:                          d.model_of_equipment                          || null,
        form_471_product_name:                       d.form_471_product_name                       || null,
        form_471_function_name:                      d.form_471_function_name                      || null,
        form_471_purpose_name:                       d.form_471_purpose_name                       || null,
        price:                                       parseFloat(d.price)                           || null,
        pre_discount_extended_eligible_line_item_costs: parseFloat(d.pre_discount_extended_eligible_line_item_costs) || null,
      });
    }
    for (let i = 0; i < rows.length; i += 200) {
      const batch = rows.slice(i, i + 200);
      const { error } = await supabase.from("frn_line_items").upsert(batch, { onConflict: "funding_request_number,form_471_line_item_number" });
      if (error) console.error("Line items upsert error:", error.message);
      else console.log(`  Upserted line items batch ${Math.floor(i/200)+1} (${batch.length} records)`);
    }
    console.log(`Synced ${rows.length} FRN line item records`);
  } catch (err) {
    console.error("syncLineItems error:", err.message);
  }
}

async function syncAll() {
  console.log("=== Starting full USAC sync ===");
  await sync470s();
  await sync471s();
  await syncCommitments();
  await syncLineItems();
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
// ── TEMP: Line items diagnostic ───────────────────────────────────────────────
app.get("/api/diag-line-items", async (req, res) => {
  try {
    // Check Supabase table
    const [countRes, sampleRes] = await Promise.all([
      supabase.from("frn_line_items").select("*", { count:"exact", head:true }),
      supabase.from("frn_line_items").select("form_471_manufacturer_name, model_of_equipment, form_471_product_name, state").limit(3),
    ]);

    // Also sample the raw USAC API with no state filter to see real field values
    const rawUrl  = `${USAC_BASE}/hbj5-2bpj.json?funding_year=2026&$limit=2`;
    const rawRes  = await fetch(rawUrl, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const rawData = await rawRes.json();

    res.json({
      supabase_count: countRes.count,
      supabase_sample: sampleRes.data,
      usac_sample: Array.isArray(rawData) ? rawData.map(r => ({ state: r.state, manufacturer: r.form_471_manufacturer_name, product: r.form_471_product_name, funding_year: r.funding_year })) : rawData,
    });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

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
    if (search)       query = query.or(`organization_name.ilike.%${search}%,application_number.ilike.%${search}%`);
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
    const open470 = await supabase.from("form_470s").select("*", { count: "exact", head: true }).gte("bid_due_date", new Date().toISOString());
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

// ── GET /api/tags — get all tagged 470s for current user ─────────────────────
app.get("/api/tags", requireAuth, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from("tagged_470s")
      .select("*")
      .eq("user_id", req.user.id)
      .order("tagged_at", { ascending: false });
    if (error) throw error;
    res.json({ status: "success", data: data || [] });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── POST /api/tags — tag a 470 ────────────────────────────────────────────────
app.post("/api/tags", requireAuth, async (req, res) => {
  try {
    const { application_number, billed_entity_name, state, service_category, bid_due_date, funding_year } = req.body;
    if (!application_number) return res.status(400).json({ status: "error", message: "application_number required" });
    const { error } = await supabase.from("tagged_470s").upsert({
      user_id: req.user.id,
      application_number,
      billed_entity_name: billed_entity_name || null,
      state:              state              || null,
      service_category:   service_category   || null,
      bid_due_date:       bid_due_date       || null,
      funding_year:       funding_year       || null,
    }, { onConflict: "user_id,application_number" });
    if (error) throw error;
    res.json({ status: "success" });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── DELETE /api/tags/:appNumber — untag a 470 ─────────────────────────────────
app.delete("/api/tags/:appNumber", requireAuth, async (req, res) => {
  try {
    const { error } = await supabase.from("tagged_470s")
      .delete()
      .eq("user_id", req.user.id)
      .eq("application_number", req.params.appNumber);
    if (error) throw error;
    res.json({ status: "success" });
  } catch (err) {
    res.status(500).json({ status: "error", message: err.message });
  }
});

// ── GET /api/frn-status — FRN status lookup from commitments ─────────────────
app.get("/api/frn-status", requireAuth, async (req, res) => {
  try {
    const { search, search_by = "frn", limit = 50 } = req.query;
    if (!search) return res.status(400).json({ status:"error", message:"search param required" });
    let query = supabase.from("commitments").select("funding_request_number,application_number,organization_name,ben,state,form_471_service_type_name,form_471_frn_status_name,funding_commitment_request,dis_pct,fcdl_letter_date,spin_name").limit(Number(limit));
    if (search_by === "frn")          query = query.ilike("funding_request_number", `%${search}%`);
    else if (search_by === "application") query = query.ilike("application_number", `%${search}%`);
    else if (search_by === "organization") query = query.ilike("organization_name", `%${search}%`);
    else if (search_by === "ben")     query = query.ilike("ben", `%${search}%`);
    else                              query = query.ilike("organization_name", `%${search}%`);
    query = query.order("fcdl_letter_date", { ascending: false });
    const { data, error } = await query;
    if (error) throw error;
    res.json({ status:"success", data: data || [] });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/competitive-intel ────────────────────────────────────────────────
app.get("/api/competitive-intel", requireAuth, async (req, res) => {
  try {
    const MANUFACTURERS = [
      "Juniper","Aruba","HPE","Cisco","Meraki","Ubiquiti","Extreme",
      "Fortinet","Palo Alto","Sophos","Dell","Ruckus","Netgear","Cambium","Zyxel"
    ];

    // Fetch commitments and line items in parallel
    const [comRes, lineRes] = await Promise.all([
      supabase.from("commitments").select("spin_name, form_471_service_type_name, funding_commitment_request").not("spin_name","is",null),
      supabase.from("frn_line_items").select("form_471_manufacturer_name, other_manufacturer_desc, form_471_product_name, pre_discount_extended_eligible_line_item_costs").not("form_471_manufacturer_name","is",null),
    ]);
    if (comRes.error) throw comRes.error;

    const commitments = comRes.data || [];
    const lineItems   = lineRes.data || [];

    // Top 25 providers by commitment count
    const providerMap = {};
    for (const r of commitments) {
      const name = (r.spin_name || "").trim();
      if (!name) continue;
      if (!providerMap[name]) providerMap[name] = { count:0, amount:0 };
      providerMap[name].count++;
      providerMap[name].amount += parseFloat(r.funding_commitment_request) || 0;
    }
    const topProviders = Object.entries(providerMap)
      .map(([name, v]) => ({ name, count: v.count, amount: Math.round(v.amount) }))
      .sort((a,b) => b.count - a.count)
      .slice(0, 25);

    // Manufacturer breakdown — from real form_471_manufacturer_name field
    const mfrMap = {};
    for (const mfr of MANUFACTURERS) mfrMap[mfr] = { count:0, amount:0 };

    for (const r of lineItems) {
      const rawName = (r.form_471_manufacturer_name || r.other_manufacturer_desc || "").trim();
      const amount  = parseFloat(r.pre_discount_extended_eligible_line_item_costs) || 0;
      for (const mfr of MANUFACTURERS) {
        if (rawName.toLowerCase().includes(mfr.toLowerCase())) {
          mfrMap[mfr].count++;
          mfrMap[mfr].amount += amount;
          break; // match first brand found
        }
      }
    }
    const manufacturers = MANUFACTURERS.map(name => ({
      name, count: mfrMap[name].count, amount: Math.round(mfrMap[name].amount),
    })).sort((a,b) => b.count - a.count);

    // Service type breakdown from commitments
    const serviceMap = {};
    for (const r of commitments) {
      const svc = (r.form_471_service_type_name || "Unknown").trim();
      if (!serviceMap[svc]) serviceMap[svc] = 0;
      serviceMap[svc]++;
    }
    const serviceTypes = Object.entries(serviceMap)
      .map(([name, count]) => ({ name, count }))
      .sort((a,b) => b.count - a.count)
      .slice(0, 8);

    // Product breakdown from line items
    const productMap = {};
    for (const r of lineItems) {
      const prod = (r.form_471_product_name || "Unknown").trim();
      if (!productMap[prod]) productMap[prod] = 0;
      productMap[prod]++;
    }
    const topProducts = Object.entries(productMap)
      .map(([name, count]) => ({ name, count }))
      .sort((a,b) => b.count - a.count)
      .slice(0, 10);

    res.json({ status:"success", data:{ topProviders, manufacturers, serviceTypes, topProducts, total: commitments.length, lineItemTotal: lineItems.length, lineItemsFY: "2025" } });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/provider-applicants — applicants for a given SPIN ───────────────
app.get("/api/provider-applicants", requireAuth, async (req, res) => {
  try {
    const { spin_name } = req.query;
    if (!spin_name) return res.status(400).json({ status:"error", message:"spin_name required" });
    const { data, error } = await supabase
      .from("commitments")
      .select("organization_name, ben, funding_commitment_request, form_471_service_type_name, form_471_frn_status_name, application_number")
      .ilike("spin_name", `%${spin_name}%`)
      .order("funding_commitment_request", { ascending: false })
      .limit(100);
    if (error) throw error;
    // Deduplicate by organization_name, summing commitment amounts
    const orgMap = {};
    for (const r of data || []) {
      const key = r.organization_name || "Unknown";
      if (!orgMap[key]) orgMap[key] = { name: key, ben: r.ben, total: 0, count: 0, service: r.form_471_service_type_name };
      orgMap[key].total += parseFloat(r.funding_commitment_request) || 0;
      orgMap[key].count++;
    }
    const applicants = Object.values(orgMap)
      .map(o => ({ ...o, total: Math.round(o.total) }))
      .sort((a,b) => b.total - a.total);
    res.json({ status:"success", data: applicants });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/part-lookup — search model_of_equipment across frn_line_items ────
app.get("/api/part-lookup", requireAuth, async (req, res) => {
  try {
    const { q, limit = 100 } = req.query;
    if (!q || q.trim().length < 2) return res.status(400).json({ status:"error", message:"query must be at least 2 characters" });

    // Search line items by model, product name, or manufacturer
    const { data: lineItems, error } = await supabase
      .from("frn_line_items")
      .select("application_number, organization_name, model_of_equipment, form_471_manufacturer_name, form_471_product_name, form_471_function_name, price, pre_discount_extended_eligible_line_item_costs, funding_request_number")
      .or(`model_of_equipment.ilike.%${q.trim()}%,form_471_product_name.ilike.%${q.trim()}%,form_471_manufacturer_name.ilike.%${q.trim()}%`)
      .order("pre_discount_extended_eligible_line_item_costs", { ascending: false, nullsFirst: false })
      .limit(Number(limit));

    if (error) throw error;
    if (!lineItems || lineItems.length === 0) return res.json({ status:"success", data:[] });

    // Fetch spin_names for all unique application numbers
    const appNums = [...new Set(lineItems.map(r => r.application_number).filter(Boolean))];
    const { data: commits } = await supabase
      .from("commitments")
      .select("application_number, spin_name")
      .in("application_number", appNums);

    const spinMap = {};
    for (const c of commits || []) {
      if (!spinMap[c.application_number]) spinMap[c.application_number] = c.spin_name;
    }

    const results = lineItems.map(r => ({
      application_number:   r.application_number,
      organization_name:    r.organization_name,
      model_of_equipment:   r.model_of_equipment,
      manufacturer:         r.form_471_manufacturer_name,
      product_name:         r.form_471_product_name,
      function_name:        r.form_471_function_name,
      unit_price:           parseFloat(r.price) || null,
      total_cost:           parseFloat(r.pre_discount_extended_eligible_line_item_costs) || null,
      spin_name:            spinMap[r.application_number] || null,
    }));

    res.json({ status:"success", data: results, count: results.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/bid-stages — auto-detect stage for tagged 470s ──────────────────
app.get("/api/bid-stages", requireAuth, async (req, res) => {
  try {
    const { app_numbers } = req.query;
    if (!app_numbers) return res.json({ status:"success", data:{} });
    const nums = app_numbers.split(",").map(s => s.trim()).filter(Boolean);
    if (!nums.length) return res.json({ status:"success", data:{} });

    function detectStage(frn_status, f471_status) {
      const s = (frn_status || "").toLowerCase();
      const t = (f471_status || "").toLowerCase();
      if (s.includes("appeal"))                          return "On Appeal";
      if (s.includes("funded") || s.includes("commit"))  return "Funded";
      if (s.includes("wave"))                            return "Wave Ready";
      if (s.includes("final"))                           return "Final Review";
      if (s.includes("review") || s.includes("pend"))    return "Under Review";
      if (s.includes("deny")   || s.includes("reject"))  return "Denied";
      if (t.includes("certif") || t.includes("submit") || t.length > 0) return "Bid Submitted";
      return null;
    }

    // Fetch commitments and 471s for all app numbers in parallel
    const [comRes, f471Res] = await Promise.all([
      supabase.from("commitments").select("application_number,form_471_frn_status_name").in("application_number", nums),
      supabase.from("form_471s").select("application_number,form_471_status_name").in("application_number", nums),
    ]);

    // Build lookup maps
    const comMap  = {};
    for (const r of (comRes.data || [])) {
      if (!comMap[r.application_number]) comMap[r.application_number] = r.form_471_frn_status_name;
    }
    const f471Map = {};
    for (const r of (f471Res.data || [])) {
      if (!f471Map[r.application_number]) f471Map[r.application_number] = r.form_471_status_name;
    }

    const stages = {};
    for (const num of nums) {
      stages[num] = detectStage(comMap[num], f471Map[num]);
    }

    res.json({ status:"success", data: stages });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── PATCH /api/tags/:appNumber — update bid status / financials ───────────────
app.patch("/api/tags/:appNumber", requireAuth, async (req, res) => {
  try {
    const allowed = ["responded", "bid_status", "bid_amount", "cogs"];
    const fields  = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
    if (!Object.keys(fields).length) return res.status(400).json({ status: "error", message: "No valid fields provided" });
    const { error } = await supabase
      .from("tagged_470s")
      .update(fields)
      .eq("user_id", req.user.id)
      .eq("application_number", req.params.appNumber);
    if (error) throw error;
    res.json({ status: "success" });
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
