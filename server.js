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
        one_time_quantity:                           parseFloat(d.one_time_quantity)               || null,
        monthly_quantity:                            parseFloat(d.monthly_quantity)                || null,
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
    let query = supabase.from("form_471s").select("*").order("certified_datetime", { ascending: false }).range(Number(offset), Number(offset) + Number(limit) - 1);
    if (state)        query = query.eq("org_state", state.toUpperCase());
    if (status)       query = query.ilike("form_471_status_name", `%${status}%`);
    if (service_type) query = query.ilike("chosen_category_of_service", `%${service_type}%`);
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
    let query = supabase.from("commitments").select("*").order("fcdl_letter_date", { ascending: false }).range(Number(offset), Number(offset) + Number(limit) - 1);
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

// ── GET /api/471-detail — fetch 471 details for a BEN + funding year ────────
app.get("/api/471-detail", requireAuth, async (req, res) => {
  try {
    const { ben, funding_year, application_number } = req.query;
    if (!ben && !application_number) return res.status(400).json({ status:"error", message:"Provide ben or application_number" });

    let form471 = null;

    // Try local DB first (FY2026)
    if (application_number) {
      const { data } = await supabase.from("form_471s").select("*")
        .eq("application_number", application_number).limit(1);
      if (data && data.length > 0) form471 = data[0];
    }

    // If not found locally, query USAC API live
    if (!form471) {
      const conditions = [];
      if (ben)              conditions.push(`ben='${ben.trim()}'`);
      if (funding_year)     conditions.push(`funding_year='${funding_year}'`);
      if (application_number) conditions.push(`application_number='${application_number}'`);
      const where = conditions.join(" AND ");
      const url   = `${USAC_BASE}/9s6i-myen.json?$where=${encodeURIComponent(where)}&$limit=5&$order=certified_datetime DESC`;
      console.log("471 detail fetch:", url);
      const r     = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
      const data  = await r.json();
      if (Array.isArray(data) && data.length > 0) {
        const d = data[0];
        form471 = {
          application_number:         d.application_number          || null,
          funding_year:               d.funding_year                || null,
          organization_name:          d.organization_name           || null,
          org_state:                  d.org_state                   || null,
          chosen_category_of_service: d.chosen_category_of_service  || null,
          form_471_status_name:       d.form_471_status_name        || null,
          funding_request_amount:     parseFloat(d.funding_request_amount) || null,
          pre_discount_eligible_amount: parseFloat(d.pre_discount_eligible_amount) || null,
          c1_discount:                d.c1_discount                 || null,
          c2_discount:                d.c2_discount                 || null,
          cnct_first_name:            d.cnct_first_name             || null,
          cnct_last_name:             d.cnct_last_name              || null,
          cnct_email:                 d.cnct_email                  || null,
          cnct_phone:                 d.cnct_phone                  || null,
          certified_datetime:         d.certified_datetime          || null,
          source:                     "usac_live",
        };
      }
    } else {
      form471.source = "local_db";
    }

    if (!form471) return res.json({ status:"success", data: null });
    res.json({ status:"success", data: form471 });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/entity-history — full E-Rate commitment history for a BEN ────────
app.get("/api/entity-history", requireAuth, async (req, res) => {
  try {
    const { ben } = req.query;
    if (!ben) return res.status(400).json({ status:"error", message:"ben required" });

    // Query USAC commitments API live across all funding years
    const where = `ben='${ben.trim()}'`;
    const url   = `${USAC_BASE}/srbr-2d59.json?$where=${encodeURIComponent(where)}&$limit=500&$order=funding_year DESC`;
    console.log("Entity history fetch:", url);
    const r     = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const data  = await r.json();

    if (!Array.isArray(data)) return res.json({ status:"error", message: data?.message || "Unexpected response", raw: data });
    if (data.length === 0)    return res.json({ status:"success", data:[], summary:[] });

    const results = data.map(d => ({
      funding_year:       d.funding_year                  || null,
      application_number: d.application_number            || null,
      frn:                d.funding_request_number        || null,
      service_type:       d.form_471_service_type_name    || null,
      frn_status:         d.form_471_frn_status_name      || null,
      commitment:         parseFloat(d.funding_commitment_request) || null,
      discount_pct:       d.dis_pct ? Math.round(parseFloat(d.dis_pct) * 100) : null,
      spin_name:          d.spin_name                     || null,
      fcdl_date:          d.fcdl_letter_date              || null,
    }));

    // Summary by year
    const yearMap = {};
    for (const r of results) {
      const y = r.funding_year || "Unknown";
      if (!yearMap[y]) yearMap[y] = { year: y, total: 0, count: 0 };
      yearMap[y].total += r.commitment || 0;
      yearMap[y].count++;
    }
    const summary = Object.values(yearMap).sort((a,b) => b.year - a.year).map(y => ({ ...y, total: Math.round(y.total) }));

    res.json({ status:"success", data: results, summary, count: results.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/entity-search — live USAC entity search ─────────────────────────
app.get("/api/entity-search", requireAuth, async (req, res) => {
  try {
    const { search, ben, state, entity_type, limit = 50 } = req.query;
    if (!search && !ben) return res.status(400).json({ status:"error", message:"Provide search or ben" });

    const conditions = [];
    if (ben)                      conditions.push(`entity_number='${ben.trim()}'`);
    if (search)                   conditions.push(`upper(entity_name) like upper('%${search.trim()}%')`);
    if (state && state !== "ALL") conditions.push(`upper(physical_state)='${state.toUpperCase()}'`);
    if (entity_type)              conditions.push(`upper(entity_type) like upper('%${entity_type}%')`);

    const where = conditions.join(" AND ");
    const url   = `${USAC_BASE}/7i5i-83qf.json?$where=${encodeURIComponent(where)}&$limit=${Number(limit)}&$order=entity_name ASC`;
    console.log("Entity search fetch:", url);
    const r     = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const data  = await r.json();

    if (!Array.isArray(data)) return res.json({ status:"error", message: data?.message || "Unexpected USAC response", raw: data });
    if (data.length === 0)    return res.json({ status:"success", data:[] });

    const results = data.map(d => ({
      entity_name:      d.entity_name      || null,
      entity_number:    d.entity_number    || null,
      entity_type:      d.entity_type      || null,
      status:           d.status           || null,
      address:          d.physical_address || null,
      city:             d.physical_city    || null,
      county:           d.physical_county  || null,
      state:            d.physical_state   || null,
      zip:              d.physical_zipcode || null,
      phone:            d.phone_number     || null,
      latitude:         d.latitude         || null,
      longitude:        d.longitude        || null,
      last_updated:     d.last_updated_date || null,
      raw:              d,
    }));

    res.json({ status:"success", data: results, count: results.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/470-detail — full 470 detail for a single application ───────────
app.get("/api/470-detail", requireAuth, async (req, res) => {
  try {
    const { app_num } = req.query;
    if (!app_num) return res.status(400).json({ status:"error", message:"app_num required" });

    // Try local DB first (has raw field with all USAC data)
    const { data: local } = await supabase
      .from("form_470s")
      .select("*")
      .eq("application_number", app_num)
      .limit(1);

    if (local && local.length > 0) {
      const row = local[0];
      const raw = row.raw || {};
      return res.json({
        status: "success",
        source: "local",
        data: {
          application_number:   row.application_number,
          funding_year:         row.funding_year,
          billed_entity_name:   row.billed_entity_name,
          billed_entity_number: row.billed_entity_number,
          state:                row.state,
          service_category:     row.service_category,
          application_status:   row.application_status,
          date_posted:          row.date_posted,
          bid_due_date:         row.bid_due_date,
          tech_contact_name:    row.tech_contact_name,
          tech_contact_email:   row.tech_contact_email,
          tech_contact_phone:   row.tech_contact_phone,
          narrative:            row.narrative,
          // Extra fields from raw
          form_nickname:        raw.form_nickname        || null,
          category_of_service:  raw.category_one_description || raw.category_two_description || raw.service_category || null,
          allowable_contract_date: raw.allowable_contract_date || null,
          signal_type:          raw.signal_type          || null,
          function_type:        raw.function_type        || null,
          pricing_type:         raw.pricing_type         || null,
          wan:                  raw.wan                  || null,
          fiber_type:           raw.fiber_type           || null,
          narrative_description: raw.narrative_description || raw.service_narrative || null,
          special_construction: raw.special_construction || null,
          consultant_name:      raw.consultant_name      || null,
          consultant_phone:     raw.consultant_phone     || null,
          rfp_document_url:     raw.rfp_document_url     || raw.rfp_url || raw.document_url || null,
          // Build USAC direct links
          usac_url:             `https://forms.universalservice.org/portal/form470/view/formSummary?fn=${app_num}&fy=${row.funding_year || 2026}`,
          epc_url:              `https://portal.usac.org/suite/#/470/${app_num}`,
          ffl_url:              `https://legacy.fundsforlearning.com/470/${app_num}`,
          // All raw for reference
          raw,
        }
      });
    }

    // Fallback: query USAC live
    const where = `application_number='${app_num}'`;
    const url   = `${USAC_BASE}/jt8s-3q52.json?$where=${encodeURIComponent(where)}&$limit=1`;
    const r     = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const usac  = await r.json();

    if (!Array.isArray(usac) || !usac.length) {
      return res.json({ status:"success", data: null });
    }

    const d = usac[0];
    res.json({
      status: "success",
      source: "usac_live",
      data: {
        application_number:   d.application_number,
        funding_year:         d.funding_year,
        billed_entity_name:   d.billed_entity_name,
        billed_entity_number: d.billed_entity_number,
        state:                d.billed_entity_state,
        service_category:     d.category_two_description || d.service_category,
        application_status:   d.fcc_form_470_status || d.application_status,
        date_posted:          d.certified_date_time,
        bid_due_date:         d.allowable_contract_date,
        tech_contact_name:    d.technical_contact_name || d.contact_name,
        tech_contact_email:   d.technical_contact_email || d.contact_email,
        tech_contact_phone:   d.technical_contact_phone || d.contact_phone,
        narrative:            d.form_nickname,
        narrative_description: d.narrative_description || d.service_narrative,
        rfp_document_url:     d.rfp_document_url || d.rfp_url || d.document_url,
        usac_url:             `https://forms.universalservice.org/portal/form470/view/formSummary?fn=${d.application_number}&fy=${d.funding_year || 2026}`,
        epc_url:              `https://portal.usac.org/suite/#/470/${d.application_number}`,
        ffl_url:              `https://legacy.fundsforlearning.com/470/${d.application_number}`,
        raw: d,
      }
    });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── TEMP: C2 budget raw diagnostic ───────────────────────────────────────────
app.get("/api/diag-c2", async (req, res) => {
  try {
    // Fetch 2 raw records with no filters to see actual field names
    const url  = `${USAC_BASE}/6brt-5pbv.json?$limit=2`;
    const r    = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const data = await r.json();
    res.json({ fields: Array.isArray(data) ? Object.keys(data[0] || {}) : [], sample: data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/c2-prospects — TX schools/districts with C2 budget but no FY2026 470
app.get("/api/c2-prospects", requireAuth, async (req, res) => {
  try {
    const { min_budget = 0, limit = 200 } = req.query;

    // Step 1: Fetch TX schools + districts with available C2 budget from USAC live
    const typeFilter = `(upper(applicant_type) like upper('%School%') OR upper(applicant_type) like upper('%District%'))`;
    const where = `state='TX' AND available_c2_budget_amount > ${Number(min_budget)} AND c2_budget_cycle='FY2026-2030' AND ${typeFilter}`;
    const url   = `${USAC_BASE}/6brt-5pbv.json?$where=${encodeURIComponent(where)}&$limit=5000&$order=available_c2_budget_amount DESC`;
    console.log("C2 prospects fetch:", url);
    const r     = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const c2data = await r.json();

    if (!Array.isArray(c2data) || c2data.length === 0) return res.json({ status:"success", data:[], count:0 });
    console.log(`Fetched ${c2data.length} TX C2 records`);

    // Step 2: Get all BENs from FY2026 470s in local DB
    const { data: filedBens } = await supabase
      .from("form_470s")
      .select("billed_entity_number")
      .eq("funding_year", CURRENT_FY);

    const filedSet = new Set((filedBens || []).map(r => String(r.billed_entity_number).trim()).filter(Boolean));
    console.log(`Found ${filedSet.size} BENs with FY${CURRENT_FY} 470s`);

    // Step 3: Filter to prospects only (no FY2026 470)
    const prospectData = c2data.filter(d => d.ben && !filedSet.has(String(d.ben).trim()));

    // Step 4: Get last 470 date for each prospect from USAC API (most recent prior filing)
    // Batch BENs into a single query — use OR of up to 100 BENs to avoid URL length limits
    const prospectBens = prospectData.slice(0, Number(limit)).map(d => d.ben);
    const lastFiledMap = {};

    if (prospectBens.length > 0) {
      const batchSize = 50;
      for (let i = 0; i < prospectBens.length; i += batchSize) {
        const batch    = prospectBens.slice(i, i + batchSize);
        const benList  = batch.map(b => `'${b}'`).join(",");
        const h470url  = `${USAC_BASE}/jt8s-3q52.json?$where=${encodeURIComponent(`billed_entity_number IN(${benList}) AND billed_entity_state='TX'`)}&$select=billed_entity_number,certified_date_time,funding_year&$order=certified_date_time DESC&$limit=${batchSize * 10}`;
        try {
          const h470r   = await fetch(h470url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
          const h470data = await h470r.json();
          if (Array.isArray(h470data)) {
            for (const row of h470data) {
              const ben = String(row.billed_entity_number).trim();
              if (!lastFiledMap[ben] && row.certified_date_time) {
                lastFiledMap[ben] = { date: row.certified_date_time, year: row.funding_year };
              }
            }
          }
        } catch (e) { console.error("History batch error:", e.message); }
      }
      console.log(`Got last-filed data for ${Object.keys(lastFiledMap).length} prospects`);
    }

    const now = new Date();
    const prospects = prospectData
      .slice(0, Number(limit))
      .map(d => {
        const last = lastFiledMap[String(d.ben).trim()];
        const daysSince = last?.date ? Math.floor((now - new Date(last.date)) / (1000*60*60*24)) : null;
        return {
          ben:              d.ben,
          entity_name:      d.billed_entity_name         || null,
          city:             d.city                       || null,
          applicant_type:   d.applicant_type             || null,
          budget_cycle:     d.c2_budget_cycle            || null,
          total_budget:     parseFloat(d.c2_budget)      || null,
          funded:           parseFloat(d.funded_c2_budget_amount)    || null,
          available:        parseFloat(d.available_c2_budget_amount) || null,
          students:         d.full_time_students         || null,
          consulting_firm:  d.consulting_firm_name_crn   || null,
          last_470_date:    last?.date                   || null,
          last_470_year:    last?.year                   || null,
          days_since_470:   daysSince,
        };
      });

    res.json({ status:"success", data: prospects, count: prospects.length, total_c2_checked: c2data.length, already_filed: filedSet.size });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/c2-budget — live query USAC C2 budget dataset ──────────────────
// Fields confirmed: ben, billed_entity_name, city, state, applicant_type,
//   c2_budget_cycle, c2_budget, c2_budget_version, funded_c2_budget_amount,
//   pending_c2_budget_amount, available_c2_budget_amount, full_time_students,
//   school_student_multiplier, library_square_footage, library_multiplier
app.get("/api/c2-budget", requireAuth, async (req, res) => {
  try {
    const { search, ben, state, limit = 50 } = req.query;
    if (!search && !ben) return res.status(400).json({ status:"error", message:"Provide search or ben" });

    // Build $where clause — use direct string concat to avoid URLSearchParams double-encoding
    const { cycle = "FY2026-2030" } = req.query;
    const conditions = [];
    if (ben)                          conditions.push(`ben='${ben.trim()}'`);
    if (search)                       conditions.push(`upper(billed_entity_name) like upper('%${search.trim()}%')`);
    if (state && state !== "ALL")     conditions.push(`upper(state)='${state.toUpperCase()}'`);
    if (cycle)                        conditions.push(`c2_budget_cycle='${cycle}'`);

    const where  = conditions.join(" AND ");
    const url    = `${USAC_BASE}/6brt-5pbv.json?$where=${encodeURIComponent(where)}&$limit=${Number(limit)}&$order=billed_entity_name ASC`;
    console.log("C2 Budget fetch:", url);
    const r      = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
    const data   = await r.json();

    if (!Array.isArray(data)) return res.json({ status:"error", message: data?.message || "Unexpected USAC response", raw: data });
    if (data.length === 0)    return res.json({ status:"success", data:[], fields:[] });

    const fields  = Object.keys(data[0]);
    const results = data.map(d => ({
      ben:               d.ben                        || null,
      entity_name:       d.billed_entity_name         || null,
      city:              d.city                       || null,
      state:             d.state                      || null,
      applicant_type:    d.applicant_type             || null,
      budget_cycle:      d.c2_budget_cycle            || null,
      budget_version:    d.c2_budget_version          || null,
      total_budget:      parseFloat(d.c2_budget)      || null,
      funded:            parseFloat(d.funded_c2_budget_amount)    || null,
      pending:           parseFloat(d.pending_c2_budget_amount)   || null,
      available:         parseFloat(d.available_c2_budget_amount) || null,
      students:          d.full_time_students         || null,
      multiplier:        d.school_student_multiplier  || d.library_multiplier || null,
      sq_footage:        d.library_square_footage     || null,
      consulting_firm:   d.consulting_firm_name_crn   || null,
      raw:               d,
    }));

    res.json({ status:"success", data: results, fields, count: results.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
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
    const { funding_year } = req.query;
    const fy = funding_year || "2026";

    const MANUFACTURERS = [
      "Juniper","Aruba","HPE","Cisco","Meraki","Ubiquiti","Extreme",
      "Fortinet","Palo Alto","Sophos","Dell","Ruckus","Netgear","Cambium","Zyxel"
    ];

    // Fetch commitments and line items in parallel, filtered by year
    const [comRes, lineRes] = await Promise.all([
      supabase.from("commitments").select("spin_name, organization_name, form_471_service_type_name, funding_commitment_request").not("spin_name","is",null).eq("funding_year", fy),
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
      if (!providerMap[name]) providerMap[name] = { count:0, total:0, orgs: new Set() };
      providerMap[name].count++;
      providerMap[name].total += parseFloat(r.funding_commitment_request) || 0;
      if (r.organization_name) providerMap[name].orgs.add(r.organization_name);
    }
    const top_providers = Object.entries(providerMap)
      .map(([name, v]) => ({ name, count: v.count, total: Math.round(v.total), orgs: v.orgs.size }))
      .sort((a,b) => b.total - a.total)
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
      name, count: mfrMap[name].count, total: Math.round(mfrMap[name].amount),
    })).sort((a,b) => b.count - a.count);

    // Service type breakdown from commitments
    const serviceMap = {};
    for (const r of commitments) {
      const svc = (r.form_471_service_type_name || "Unknown").trim();
      if (!serviceMap[svc]) serviceMap[svc] = 0;
      serviceMap[svc]++;
    }
    const service_types = Object.entries(serviceMap)
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
    const top_products = Object.entries(productMap)
      .map(([name, count]) => ({ name, count }))
      .sort((a,b) => b.count - a.count)
      .slice(0, 10);

    res.json({ status:"success", data:{ top_providers, manufacturers, service_types, top_products, total: commitments.length, lineItemTotal: lineItems.length } });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/service-area-search — providers doing C2/cabling in an area ───────
app.get("/api/service-area-search", requireAuth, async (req, res) => {
  try {
    const { area, service_type, service_keyword, limit = 200 } = req.query;
    if (!area || area.trim().length < 2) return res.status(400).json({ status:"error", message:"area query required" });

    // Service type filter
    const SERVICE_FILTERS = {
      "internal":  "Internal Connections",
      "c2":        ["Internal Connections", "Basic Maintenance"],
      "all":       null,
    };
    const svcKey    = service_type || "c2";
    const svcFilter = SERVICE_FILTERS[svcKey] || null;

    // Build combined filter — area matches organization_name OR spin_name
    const areaClean = area.trim();
    const areaFilter = `organization_name.ilike.%${areaClean}%,spin_name.ilike.%${areaClean}%`;

    let query = supabase
      .from("commitments")
      .select("spin_name, organization_name, ben, application_number, funding_year, form_471_service_type_name, form_471_frn_status_name, funding_commitment_request, dis_pct, fcdl_letter_date")
      .or(areaFilter)
      .order("funding_commitment_request", { ascending: false })
      .limit(Number(limit));

    // Apply service type filter after area filter
    if (Array.isArray(svcFilter)) {
      query = query.or(svcFilter.map(s => `form_471_service_type_name.ilike.%${s}%`).join(","));
    } else if (svcFilter) {
      query = query.ilike("form_471_service_type_name", `%${svcFilter}%`);
    }

    console.log(`Service area search: area="${areaClean}" svc="${svcKey}"`);
    const { data, error } = await query;
    console.log(`Service area results: ${(data||[]).length} rows, error: ${error?.message}`);
    if (error) throw error;

    const rows = (data || []).map(r => ({
      spin_name:          r.spin_name,
      organization:       r.organization_name,
      ben:                r.ben,
      application_number: r.application_number,
      funding_year:       r.funding_year,
      service_type:       r.form_471_service_type_name,
      frn_status:         r.form_471_frn_status_name,
      commitment:         parseFloat(r.funding_commitment_request) || null,
      discount_pct:       r.dis_pct ? Math.round(parseFloat(r.dis_pct) * 100) : null,
      fcdl_date:          r.fcdl_letter_date,
    }));

    // Provider summary — who has won the most work in this area
    const providerMap = {};
    for (const r of rows) {
      const key = r.spin_name || "Unknown";
      if (!providerMap[key]) providerMap[key] = { spin_name: key, count: 0, total: 0, orgs: new Set() };
      providerMap[key].count++;
      providerMap[key].total += r.commitment || 0;
      if (r.organization) providerMap[key].orgs.add(r.organization);
    }
    const providerSummary = Object.values(providerMap)
      .map(p => ({ spin_name: p.spin_name, count: p.count, total: Math.round(p.total), orgs: p.orgs.size }))
      .sort((a,b) => b.total - a.total);

    const totalCommitted = rows.reduce((s,r) => s + (r.commitment||0), 0);

    res.json({ status:"success", data: rows, providerSummary, count: rows.length, total_committed: Math.round(totalCommitted) });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/provider-search — full commitment detail for a service provider ──
app.get("/api/provider-search", requireAuth, async (req, res) => {
  try {
    const { q, limit = 200 } = req.query;
    if (!q || q.trim().length < 2) return res.status(400).json({ status:"error", message:"query must be at least 2 characters" });

    const { data, error } = await supabase
      .from("commitments")
      .select("spin_name, organization_name, ben, application_number, funding_year, form_471_service_type_name, form_471_frn_status_name, funding_commitment_request, dis_pct, fcdl_letter_date")
      .ilike("spin_name", `%${q.trim()}%`)
      .order("funding_commitment_request", { ascending: false })
      .limit(Number(limit));

    if (error) throw error;

    const rows = (data || []).map(r => ({
      spin_name:       r.spin_name,
      organization:    r.organization_name,
      ben:             r.ben,
      application_number: r.application_number,
      funding_year:    r.funding_year,
      service_type:    r.form_471_service_type_name,
      frn_status:      r.form_471_frn_status_name,
      commitment:      parseFloat(r.funding_commitment_request) || null,
      discount_pct:    r.dis_pct ? Math.round(parseFloat(r.dis_pct) * 100) : null,
      fcdl_date:       r.fcdl_letter_date,
    }));

    // Summary stats
    const total     = rows.reduce((s,r) => s + (r.commitment||0), 0);
    const providers = [...new Set(rows.map(r => r.spin_name).filter(Boolean))];
    const orgs      = [...new Set(rows.map(r => r.organization).filter(Boolean))];

    res.json({ status:"success", data: rows, count: rows.length, total_committed: Math.round(total), unique_providers: providers.length, unique_orgs: orgs.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/provider-applicants — FRNs won by a provider, with year filter ───
app.get("/api/provider-applicants", requireAuth, async (req, res) => {
  try {
    const { spin_name, funding_year } = req.query;
    if (!spin_name) return res.status(400).json({ status:"error", message:"spin_name required" });

    let query = supabase
      .from("commitments")
      .select("funding_request_number, organization_name, ben, application_number, funding_year, form_471_service_type_name, form_471_frn_status_name, funding_commitment_request, dis_pct, fcdl_letter_date, spin_name")
      .ilike("spin_name", `%${spin_name}%`)
      .order("funding_commitment_request", { ascending: false })
      .limit(500);

    if (funding_year && funding_year !== "ALL") {
      query = query.eq("funding_year", funding_year);
    }

    const { data, error } = await query;
    if (error) throw error;

    const rows = (data || []).map(r => ({
      frn:              r.funding_request_number,
      organization:     r.organization_name,
      ben:              r.ben,
      application_number: r.application_number,
      funding_year:     r.funding_year,
      service_type:     r.form_471_service_type_name,
      frn_status:       r.form_471_frn_status_name,
      commitment:       parseFloat(r.funding_commitment_request) || null,
      discount_pct:     r.dis_pct ? Math.round(parseFloat(r.dis_pct) * 100) : null,
      fcdl_date:        r.fcdl_letter_date,
      spin_name:        r.spin_name,
    }));

    const total = rows.reduce((s, r) => s + (r.commitment || 0), 0);
    const orgs  = [...new Set(rows.map(r => r.organization).filter(Boolean))];

    res.json({ status:"success", data: rows, count: rows.length, total_committed: Math.round(total), unique_orgs: orgs.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/part-lookup — search model_of_equipment across frn_line_items ────
app.get("/api/part-lookup", requireAuth, async (req, res) => {
  try {
    const { q, limit = 100 } = req.query;
    if (!q || q.trim().length < 2) return res.status(400).json({ status:"error", message:"query must be at least 2 characters" });

    // Search the full string first — exact phrase match across model and product name
    const qClean = q.trim();
    let { data: lineItems, error } = await supabase
      .from("frn_line_items")
      .select("application_number, organization_name, model_of_equipment, form_471_manufacturer_name, form_471_product_name, form_471_function_name, price, one_time_quantity, monthly_quantity, pre_discount_extended_eligible_line_item_costs, funding_request_number")
      .or(`model_of_equipment.ilike.%${qClean}%,form_471_product_name.ilike.%${qClean}%`)
      .order("pre_discount_extended_eligible_line_item_costs", { ascending: false })
      .limit(500);

    if (error) throw error;

    // If no results, try normalizing dashes/spaces (AP-635 → AP 635 and vice versa)
    if (!lineItems || lineItems.length === 0) {
      const qAlt = qClean.includes("-") ? qClean.replace(/-/g, " ") : qClean.replace(/\s+/g, "-");
      const alt  = await supabase
        .from("frn_line_items")
        .select("application_number, organization_name, model_of_equipment, form_471_manufacturer_name, form_471_product_name, form_471_function_name, price, one_time_quantity, monthly_quantity, pre_discount_extended_eligible_line_item_costs, funding_request_number")
        .or(`model_of_equipment.ilike.%${qAlt}%,form_471_product_name.ilike.%${qAlt}%`)
        .order("pre_discount_extended_eligible_line_item_costs", { ascending: false })
        .limit(500);
      if (!alt.error) lineItems = alt.data || [];
    }

    if (!lineItems || lineItems.length === 0) return res.json({ status:"success", data:[] });

    // Deduplicate — keep highest cost record per org+model combination
    const seen = new Map();
    for (const r of lineItems) {
      const key = `${r.organization_name}|${r.model_of_equipment}`;
      const cur = seen.get(key);
      const cost = parseFloat(r.pre_discount_extended_eligible_line_item_costs) || 0;
      if (!cur || cost > (parseFloat(cur.pre_discount_extended_eligible_line_item_costs) || 0)) {
        seen.set(key, r);
      }
    }
    const deduped = Array.from(seen.values())
      .sort((a,b) => (parseFloat(b.pre_discount_extended_eligible_line_item_costs)||0) - (parseFloat(a.pre_discount_extended_eligible_line_item_costs)||0))
      .slice(0, Number(limit));

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
      unit_price: (() => {
        const p    = parseFloat(r.price);
        if (p) return p;
        const qty  = parseFloat(r.one_time_quantity) || parseFloat(r.monthly_quantity);
        const tot  = parseFloat(r.pre_discount_extended_eligible_line_item_costs);
        return (qty && tot) ? tot / qty : null;
      })(),
      quantity:             parseFloat(r.one_time_quantity) || parseFloat(r.monthly_quantity) || null,
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
// ── POST /api/claude-chat — Claude AI with direct DB tool access ──────────────
app.post("/api/claude-chat", requireAuth, async (req, res) => {
  try {
    const { messages } = req.body;
    if (!messages?.length) return res.status(400).json({ status:"error", message:"messages required" });

    const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY;
    if (!ANTHROPIC_KEY) return res.status(500).json({ status:"error", message:"ANTHROPIC_API_KEY not configured" });

    // ── Tools Claude can use to query your DB ─────────────────────────────────
    const tools = [
      {
        name: "query_470s",
        description: "Search open Form 470 bidding opportunities from the form_470s table. Use this to find open bids, upcoming deadlines, districts seeking bids, service categories, etc.",
        input_schema: {
          type: "object",
          properties: {
            state:            { type: "string", description: "Filter by state code e.g. TX" },
            service_category: { type: "string", description: "Filter by service category e.g. 'Internal Connections'" },
            days_until_due:   { type: "number", description: "Filter to bids due within this many days" },
            entity_name:      { type: "string", description: "Filter by district/entity name (partial match)" },
            funding_year:     { type: "string", description: "Filter by funding year e.g. 2026" },
            limit:            { type: "number", description: "Max records to return (default 20)" },
          }
        }
      },
      {
        name: "query_commitments",
        description: "Search E-Rate commitment decisions from the commitments table. Use this for competitive intelligence — which providers are winning, how much revenue, which districts are funded, service type breakdowns.",
        input_schema: {
          type: "object",
          properties: {
            spin_name:        { type: "string", description: "Filter by service provider name (partial match)" },
            organization_name:{ type: "string", description: "Filter by applicant/district name (partial match)" },
            service_type:     { type: "string", description: "Filter by service type e.g. 'Internal Connections'" },
            funding_year:     { type: "string", description: "Filter by funding year e.g. 2026" },
            frn_status:       { type: "string", description: "Filter by FRN status e.g. 'Funded'" },
            state:            { type: "string", description: "Filter by state code" },
            limit:            { type: "number", description: "Max records to return (default 25)" },
          }
        }
      },
      {
        name: "query_tagged",
        description: "Query the user's tagged 470s pipeline — opportunities they are tracking, bid statuses (won/lost), amounts, margins. Use for pipeline and win rate analysis.",
        input_schema: {
          type: "object",
          properties: {
            bid_status: { type: "string", description: "Filter by status: won, lost, or leave empty for all" },
            responded:  { type: "boolean", description: "Filter by whether a bid was submitted" },
            limit:      { type: "number", description: "Max records to return (default 50)" },
          }
        }
      },
      {
        name: "query_prospects",
        description: "Find C2 budget prospects — TX school districts that have Category 2 budget available but haven't filed a Form 470 yet this year. Great for finding new leads.",
        input_schema: {
          type: "object",
          properties: {
            min_budget: { type: "number", description: "Minimum C2 budget remaining (default 10000)" },
            limit:      { type: "number", description: "Max records to return (default 20)" },
          }
        }
      },
    ];

    // ── Tool execution ─────────────────────────────────────────────────────────
    async function executeTool(name, input) {
      try {
        if (name === "query_470s") {
          let q = supabase.from("form_470s").select("application_number, billed_entity_name, billed_entity_number, state, service_category, application_status, date_posted, bid_due_date, funding_year").order("bid_due_date", { ascending: true }).limit(input.limit || 20);
          if (input.state)            q = q.eq("state", input.state);
          if (input.funding_year)     q = q.eq("funding_year", input.funding_year);
          if (input.service_category) q = q.ilike("service_category", `%${input.service_category}%`);
          if (input.entity_name)      q = q.ilike("billed_entity_name", `%${input.entity_name}%`);
          if (input.days_until_due) {
            const cutoff = new Date();
            cutoff.setDate(cutoff.getDate() + input.days_until_due);
            q = q.lte("bid_due_date", cutoff.toISOString()).gte("bid_due_date", new Date().toISOString());
          }
          const { data, error } = await q;
          if (error) return `Error: ${error.message}`;
          return JSON.stringify(data || []);
        }

        if (name === "query_commitments") {
          let q = supabase.from("commitments").select("organization_name, ben, spin_name, form_471_service_type_name, form_471_frn_status_name, funding_commitment_request, dis_pct, fcdl_letter_date, funding_year, application_number").order("funding_commitment_request", { ascending: false }).limit(input.limit || 25);
          if (input.spin_name)         q = q.ilike("spin_name", `%${input.spin_name}%`);
          if (input.organization_name) q = q.ilike("organization_name", `%${input.organization_name}%`);
          if (input.service_type)      q = q.ilike("form_471_service_type_name", `%${input.service_type}%`);
          if (input.funding_year)      q = q.eq("funding_year", input.funding_year);
          if (input.frn_status)        q = q.ilike("form_471_frn_status_name", `%${input.frn_status}%`);
          if (input.state)             q = q.eq("state", input.state);
          const { data, error } = await q;
          if (error) return `Error: ${error.message}`;
          return JSON.stringify(data || []);
        }

        if (name === "query_tagged") {
          let q = supabase.from("tagged_470s").select("*").order("created_at", { ascending: false }).limit(input.limit || 50);
          if (input.bid_status !== undefined && input.bid_status !== "") q = q.eq("bid_status", input.bid_status);
          if (input.responded !== undefined) q = q.eq("responded", input.responded);
          const { data, error } = await q;
          if (error) return `Error: ${error.message}`;
          return JSON.stringify(data || []);
        }

        if (name === "query_prospects") {
          const min = input.min_budget || 10000;
          const limit = input.limit || 20;
          const url = `${process.env.USAC_BASE || "https://opendata.usac.org/resource"}/6brt-5pbv.json?$where=${encodeURIComponent(`state_code='TX' AND remaining_c2_budget > ${min}`)}&$order=remaining_c2_budget DESC&$limit=${limit}`;
          const r = await fetch(url, { headers:{ "X-App-Token": process.env.USAC_APP_TOKEN || "" } });
          const data = await r.json();
          return JSON.stringify(data || []);
        }

        return "Unknown tool";
      } catch (err) {
        return `Tool error: ${err.message}`;
      }
    }

    // ── Agentic loop — Claude calls tools until it has an answer ──────────────
    const systemPrompt = `You are Kadera AI, an expert E-Rate intelligence assistant embedded in the Kadera E-Rate Dashboard. You have direct access to the user's E-Rate database through the provided tools.

Your database contains:
- form_470s: Open TX FY2026 Form 470 bidding opportunities (competitive bids from school districts)
- commitments: FY2026 TX E-Rate commitment decisions (which providers won, how much, which districts)
- tagged_470s: The user's tracked opportunities with bid status, amounts, and margins
- C2 budget prospects via USAC API

You help the user with:
- Finding bidding opportunities (open 470s, upcoming deadlines)
- Competitive intelligence (which providers are winning, revenue by provider, market share)
- Pipeline analysis (their tagged bids, win rates, revenue)
- Prospect identification (districts with budget but no active 470)
- Market research (service type trends, discount rates, geographic analysis)

Always query the database to give real, specific answers. Format dollar amounts with $ and commas. Be concise and actionable. Today's date is ${new Date().toLocaleDateString("en-US", { month:"long", day:"numeric", year:"numeric" })}.`;

    let claudeMessages = messages.map(m => ({ role: m.role, content: m.content }));
    let finalResponse = "";
    const toolsUsed = [];

    for (let round = 0; round < 5; round++) {
      const response = await fetch("https://api.anthropic.com/v1/messages", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": ANTHROPIC_KEY,
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify({
          model: "claude-sonnet-4-6",
          max_tokens: 4096,
          system: systemPrompt,
          tools,
          messages: claudeMessages,
        }),
      });

      const result = await response.json();
      if (!response.ok) throw new Error(result.error?.message || "Anthropic API error");

      // Add assistant response to message history
      claudeMessages.push({ role: "assistant", content: result.content });

      if (result.stop_reason === "end_turn") {
        finalResponse = result.content.filter(b => b.type === "text").map(b => b.text).join("\n");
        break;
      }

      if (result.stop_reason === "tool_use") {
        const toolResults = [];
        for (const block of result.content) {
          if (block.type === "tool_use") {
            toolsUsed.push({ name: block.name, input: block.input });
            const toolResult = await executeTool(block.name, block.input);
            toolResults.push({
              type: "tool_result",
              tool_use_id: block.id,
              content: toolResult,
            });
          }
        }
        claudeMessages.push({ role: "user", content: toolResults });
      }
    }

    res.json({ status:"success", response: finalResponse, tools_used: toolsUsed });
  } catch (err) {
    console.error("Claude chat error:", err);
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/contact-search — search 470s by entity type + optional product keyword ──
app.get("/api/contact-search", requireAuth, async (req, res) => {
  try {
    const { keywords, product, service_category, funding_year, limit = 500 } = req.query;
    const kws  = (keywords || "").trim();
    const prod = (product  || "").trim();
    if (!kws && !prod) {
      return res.status(400).json({ status:"error", message:"keywords or product required" });
    }

    const fy = funding_year || "2026";
    let results = [];

    if (prod) {
      // ── Product-first strategy ────────────────────────────────────────────────
      // 1. Search USAC services dataset for ALL records matching the product keyword
      //    Use $limit=50000 and paginate via $offset to get everything
      const matchedAppNums = new Set();
      const serviceDetails = {};
      let offset = 0;
      const PAGE = 5000;

      // Use $where with like — correct SODA syntax for this dataset
      while (true) {
        const url = `${USAC_BASE}/39tn-hjzv.json?$where=${encodeURIComponent("manufacturer like '%" + prod + "%'")}&$limit=${PAGE}&$offset=${offset}&$select=application_number,service_type,function,manufacturer,number_of_entities,rfp_documents`;
        try {
          const r    = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
          const rows = await r.json();
          console.log(`USAC services page offset=${offset}: ${Array.isArray(rows) ? rows.length : "error"} rows`, Array.isArray(rows) ? "" : JSON.stringify(rows).slice(0,200));
          if (!Array.isArray(rows) || !rows.length) break;
          for (const row of rows) {
            if (!row.application_number) continue;
            matchedAppNums.add(row.application_number);
            if (!serviceDetails[row.application_number]) serviceDetails[row.application_number] = [];
            const already = serviceDetails[row.application_number].some(s => s.manufacturer === row.manufacturer && s.service_type === row.service_type);
            if (!already) serviceDetails[row.application_number].push({
              service_type: row.service_type,
              function:     row.function,
              manufacturer: row.manufacturer,
              entities:     row.number_of_entities,
              rfp:          row.rfp_documents?.url || null,
            });
          }
          if (rows.length < PAGE) break;
          offset += PAGE;
        } catch (e) { console.error("USAC services fetch error:", e.message); break; }
      }
      console.log(`Product search "${prod}": found ${matchedAppNums.size} matching app numbers`);

      if (!matchedAppNums.size) {
        return res.json({ status:"success", data:[], count:0 });
      }

      // 2. Look up those app numbers in form_470s, filtering by keywords + fy
      const appArr = [...matchedAppNums];
      const BATCH  = 100;
      const allRows = [];

      for (let i = 0; i < appArr.length; i += BATCH) {
        const batch = appArr.slice(i, i + BATCH);
        let q = supabase
          .from("form_470s")
          .select("application_number, billed_entity_name, billed_entity_number, state, service_category, application_status, bid_due_date, date_posted, tech_contact_name, tech_contact_email, tech_contact_phone, funding_year")
          .eq("funding_year", fy)
          .in("application_number", batch);

        if (kws) {
          const terms    = kws.split(",").map(k => k.trim()).filter(Boolean);
          const orFilter = terms.map(t => `billed_entity_name.ilike.%${t}%`).join(",");
          q = q.or(orFilter);
        }
        if (service_category && service_category !== "ALL") {
          q = q.ilike("service_category", `%${service_category}%`);
        }

        const { data: batch_data } = await q;
        allRows.push(...(batch_data || []));
      }

      // Deduplicate by entity
      const entityMap = {};
      for (const r of allRows) {
        const key = r.billed_entity_number || r.billed_entity_name;
        if (!entityMap[key] || new Date(r.date_posted) > new Date(entityMap[key].date_posted)) {
          entityMap[key] = { ...r, matched_services: serviceDetails[r.application_number] || [] };
        }
      }

      results = Object.values(entityMap).sort((a, b) =>
        (a.billed_entity_name || "").localeCompare(b.billed_entity_name || "")
      );

    } else {
      // ── Keywords-only strategy ────────────────────────────────────────────────
      const terms    = kws.split(",").map(k => k.trim()).filter(Boolean);
      const orFilter = terms.map(t => `billed_entity_name.ilike.%${t}%`).join(",");

      let q = supabase
        .from("form_470s")
        .select("application_number, billed_entity_name, billed_entity_number, state, service_category, application_status, bid_due_date, date_posted, tech_contact_name, tech_contact_email, tech_contact_phone, funding_year")
        .eq("funding_year", fy)
        .or(orFilter)
        .order("billed_entity_name", { ascending: true })
        .limit(Number(limit));

      if (service_category && service_category !== "ALL") {
        q = q.ilike("service_category", `%${service_category}%`);
      }

      const { data, error } = await q;
      if (error) throw error;

      const entityMap = {};
      for (const r of data || []) {
        const key = r.billed_entity_number || r.billed_entity_name;
        if (!entityMap[key] || new Date(r.date_posted) > new Date(entityMap[key].date_posted)) {
          entityMap[key] = r;
        }
      }
      results = Object.values(entityMap).sort((a, b) =>
        (a.billed_entity_name || "").localeCompare(b.billed_entity_name || "")
      );
    }

    res.json({ status:"success", data: results, count: results.length });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/vendor-contacts — find tech contacts who bought a specific product/brand ──
app.get("/api/vendor-contacts", requireAuth, async (req, res) => {
  try {
    const { keyword, funding_year, limit = 300 } = req.query;
    if (!keyword || keyword.trim().length < 2) {
      return res.status(400).json({ status:"error", message:"keyword required" });
    }

    const kw = keyword.trim();
    const fy = funding_year || "2025"; // line items are FY2025

    // Step 1: Find all line items matching the keyword (manufacturer, model, or product name)
    const { data: lineItems, error: liErr } = await supabase
      .from("frn_line_items")
      .select("application_number, organization_name, form_471_manufacturer_name, model_of_equipment, form_471_product_name, pre_discount_extended_eligible_line_item_costs")
      .or(`form_471_manufacturer_name.ilike.%${kw}%,model_of_equipment.ilike.%${kw}%,form_471_product_name.ilike.%${kw}%`)
      .order("pre_discount_extended_eligible_line_item_costs", { ascending: false })
      .limit(1000);

    if (liErr) throw liErr;
    if (!lineItems?.length) return res.json({ status:"success", data:[], count:0 });

    // Step 2: Get unique org names and app numbers to look up contacts
    const orgNames = [...new Set(lineItems.map(r => r.organization_name).filter(Boolean))];
    const appNums  = [...new Set(lineItems.map(r => r.application_number).filter(Boolean))];

    // Step 3: Look up tech contacts from form_470s matching those org names (current FY)
    // Build OR filter for org names (up to 50 to avoid query limits)
    const orgSample = orgNames.slice(0, 80);
    const orFilter  = orgSample.map(n => `billed_entity_name.ilike.%${n.split(" ").slice(0,3).join(" ")}%`).join(",");

    const { data: contacts470, error: cErr } = await supabase
      .from("form_470s")
      .select("billed_entity_name, billed_entity_number, application_number, service_category, application_status, bid_due_date, tech_contact_name, tech_contact_email, tech_contact_phone, funding_year")
      .eq("funding_year", "2026")
      .not("tech_contact_email", "is", null)
      .or(orFilter)
      .limit(500);

    if (cErr) throw cErr;

    // Step 4: Cross-reference — build a map of org → line item details
    const orgToLineItem = {};
    for (const li of lineItems) {
      const key = (li.organization_name || "").toLowerCase().trim();
      if (!orgToLineItem[key]) {
        orgToLineItem[key] = {
          manufacturer: li.form_471_manufacturer_name,
          model:        li.model_of_equipment,
          product:      li.form_471_product_name,
          total_spend:  0,
          app_number_471: li.application_number,
        };
      }
      orgToLineItem[key].total_spend += parseFloat(li.pre_discount_extended_eligible_line_item_costs) || 0;
    }

    // Step 5: Match contacts to line item data, deduplicate by entity
    const seen = new Set();
    const results = [];
    for (const c of contacts470 || []) {
      const key = (c.billed_entity_name || "").toLowerCase().trim();
      if (seen.has(key)) continue;
      const liData = orgToLineItem[key] || {};
      // Fuzzy match — check if any word overlap
      const entityWords = key.split(/\s+/).filter(w => w.length > 3);
      const liKey = Object.keys(orgToLineItem).find(k =>
        entityWords.some(w => k.includes(w))
      );
      const matchedLi = liData.manufacturer ? liData : (liKey ? orgToLineItem[liKey] : {});
      seen.add(key);
      results.push({
        entity_name:      c.billed_entity_name,
        ben:              c.billed_entity_number,
        service_category: c.service_category,
        application_status: c.application_status,
        bid_due_date:     c.bid_due_date,
        tech_contact_name:  c.tech_contact_name,
        tech_contact_email: c.tech_contact_email,
        tech_contact_phone: c.tech_contact_phone,
        manufacturer:     matchedLi.manufacturer || kw,
        model:            matchedLi.model,
        product:          matchedLi.product,
        total_spend:      matchedLi.total_spend ? Math.round(matchedLi.total_spend) : null,
        app_number_470:   c.application_number,
        app_number_471:   matchedLi.app_number_471,
      });
    }

    // Sort by total spend descending
    results.sort((a, b) => (b.total_spend || 0) - (a.total_spend || 0));

    res.json({
      status: "success",
      data: results.slice(0, Number(limit)),
      count: results.length,
      raw_line_items: lineItems.length,
      keyword: kw,
    });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});

// ── GET /api/tags/keyword-search — filter tagged 470s by keyword in services ──
app.get("/api/tags/keyword-search", requireAuth, async (req, res) => {
  try {
    const { keyword } = req.query;
    if (!keyword || keyword.trim().length < 2) {
      return res.status(400).json({ status:"error", message:"keyword required" });
    }

    // Step 1: Get all tagged app numbers for this user
    const { data: tagged, error: tagErr } = await supabase
      .from("tagged_470s")
      .select("application_number, billed_entity_name, service_category, bid_due_date, responded, bid_status, bid_amount")
      .eq("user_id", req.user.id);

    if (tagErr) throw tagErr;
    if (!tagged?.length) return res.json({ status:"success", data:[], count:0 });

    const appNums = tagged.map(t => t.application_number).filter(Boolean);

    // Step 2: Query USAC 470 services dataset for those app numbers with keyword match
    // Batch into groups of 20 (SODA IN query limit)
    const kw = keyword.trim();
    const matches = new Set();
    const serviceDetails = {};

    const BATCH = 20;
    for (let i = 0; i < appNums.length; i += BATCH) {
      const batch = appNums.slice(i, i + BATCH);
      const inClause = batch.map(n => `'${n}'`).join(",");
      const where = `application_number IN(${inClause})`;
      const url = `${USAC_BASE}/39tn-hjzv.json?$where=${encodeURIComponent(where)}&$limit=500`;
      try {
        const r = await fetch(url, { headers:{ "X-App-Token": USAC_APP_TOKEN } });
        const rows = await r.json();
        for (const row of rows || []) {
          const text = [
            row.service_type, row.function, row.manufacturer,
            row.manufacturer_other_description
          ].filter(Boolean).join(" ").toLowerCase();
          if (text.includes(kw.toLowerCase())) {
            matches.add(row.application_number);
            if (!serviceDetails[row.application_number]) serviceDetails[row.application_number] = [];
            serviceDetails[row.application_number].push({
              service_type: row.service_type,
              function:     row.function,
              manufacturer: row.manufacturer,
              entities:     row.number_of_entities,
              rfp:          row.rfp_documents?.url || null,
            });
          }
        }
      } catch {}
    }

    // Step 3: Filter tagged to only matching ones, attach tech contacts from form_470s
    const matchingAppNums = [...matches];
    if (!matchingAppNums.length) return res.json({ status:"success", data:[], count:0, keyword: kw });

    const { data: contacts } = await supabase
      .from("form_470s")
      .select("application_number, tech_contact_name, tech_contact_email, tech_contact_phone, billed_entity_number")
      .in("application_number", matchingAppNums);

    const contactMap = {};
    for (const c of contacts || []) contactMap[c.application_number] = c;

    const results = tagged
      .filter(t => matches.has(t.application_number))
      .map(t => ({
        ...t,
        tech_contact_name:  contactMap[t.application_number]?.tech_contact_name  || null,
        tech_contact_email: contactMap[t.application_number]?.tech_contact_email || null,
        tech_contact_phone: contactMap[t.application_number]?.tech_contact_phone || null,
        ben:                contactMap[t.application_number]?.billed_entity_number || null,
        services:           serviceDetails[t.application_number] || [],
      }));

    res.json({ status:"success", data: results, count: results.length, keyword: kw });
  } catch (err) {
    res.status(500).json({ status:"error", message: err.message });
  }
});


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
