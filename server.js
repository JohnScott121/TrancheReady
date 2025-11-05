// server.js — TrancheReady (Pro, fixed & improved)
const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const multer = require("multer");
const { parse } = require("csv-parse/sync"); // robust CSV parser
const dayjs = require("dayjs");
const archiver = require("archiver");
const { v4: uuidv4 } = require("uuid");
const { OpenAI } = require("openai");
const helmet = require("helmet");
const compression = require("compression");
const rateLimit = require("express-rate-limit");
const Papa = require("papaparse"); // kept for export CSV

// --- App setup ----------------------------------------------------------------
const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));
app.use(compression());

app.use(
  helmet({
    contentSecurityPolicy: {
      useDefaults: true,
      directives: {
        "script-src": ["'self'", "'unsafe-inline'"],
        "img-src": ["'self'", "data:"],
        "style-src": ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
        "font-src": ["'self'", "https://fonts.gstatic.com", "data:"],
      },
    },
  })
);

// Basic rate limiting on heavy endpoints
const ingestLimiter = rateLimit({ windowMs: 60 * 1000, max: 20 });
app.use("/ingest", ingestLimiter);

// Static assets
app.use(express.static("public", { maxAge: "1h", etag: true }));

// Use memory storage (faster, no temp files on Render)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB each file
});

const PORT = process.env.PORT || 3000;

// Ensure runtime dirs
function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}
ensureDir(path.join(__dirname, "runs"));

// OpenAI (optional)
function getOpenAI() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return null;
  try {
    return new OpenAI({ apiKey: key });
  } catch {
    return null;
  }
}

// --- Risk engine (robust & explainable) ---------------------------------------
// Countries / thresholds
const HIGH_RISK_COUNTRIES = new Set(["RU", "IR"]); // add as needed
const MED_RISK_COUNTRIES = new Set(["CN", "HK", "AE", "IN"]);
const HIGH_RISK_CORRIDORS = new Set(["RU", "IR", "CN", "HK", "AE", "IN"]);
const CASH_STRUCTURING_MIN = 9600; // near-threshold window
const CASH_STRUCTURING_MAX = 10000;
const LARGE_DOMESTIC = 100000; // AUD
const LOOKBACK_MONTHS = 18;
const MS_PER_DAY = 86400000;

// Case-insensitive header normalizers + synonyms
const CLIENT_KEYMAP = new Map(
  Object.entries({
    clientid: "ClientID",
    client_id: "ClientID",
    id: "ClientID",
    name: "Name",
    entitytype: "EntityType",
    type: "EntityType",
    country: "Country",
    state: "State",
    suburb: "Suburb",
    postcode: "Postcode",
    residencystatus: "ResidencyStatus",
    pep: "PEP",
    kycstatus: "KYCStatus",
    onboarddate: "OnboardDate",
    lastkycreview: "LastKYCReview",
    deliverychannel: "DeliveryChannel",
    channel: "DeliveryChannel",
    servicesused: "ServicesUsed",
    services: "ServicesUsed",
    industry: "Industry",
    annualturnoveraud: "AnnualTurnoverAUD",
    sourceoffunds: "SourceOfFunds",
    sanctionsmatch: "SanctionsMatch",
    riskcountryexposure: "RiskCountryExposure",
  })
);

const TX_KEYMAP = new Map(
  Object.entries({
    txnid: "TxnID",
    id: "TxnID",
    clientid: "ClientID",
    client_id: "ClientID",
    client: "ClientName",
    client_name: "ClientName",
    name: "ClientName",
    date: "Date",
    txn_date: "Date",
    timestamp: "Date",
    amount: "Amount",
    aud_amount: "Amount",
    value: "Amount",
    currency: "Currency",
    type: "Type",
    channel: "Channel",
    method: "Channel",
    location: "Location",
    counterpartyname: "CounterpartyName",
    counterparty_country: "CounterpartyCountry",
    country: "CounterpartyCountry",
    notes: "Notes",
  })
);

const toNum = (x) => {
  if (x == null) return NaN;
  if (typeof x === "number") return x;
  const n = parseFloat(String(x).replace(/[, ]/g, ""));
  return isNaN(n) ? NaN : n;
};
const toUpper = (x) => (x ?? "").toString().trim().toUpperCase();
const toBoolYN = (x) => ["Y", "YES", "TRUE"].includes(toUpper(x));

function parseDate(x) {
  const s = (x ?? "").toString().trim().replace(/\//g, "-");
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d;
}
function monthsAgo(a, b) {
  if (!(a instanceof Date)) a = parseDate(a);
  if (!(b instanceof Date)) b = parseDate(b);
  if (!a || !b) return Infinity;
  return (b.getFullYear() - a.getFullYear()) * 12 + (b.getMonth() - a.getMonth());
}

function normalizeRow(row, keymap) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    const canon = keymap.get(k.toLowerCase().trim());
    if (canon) out[canon] = typeof v === "string" ? v.trim() : v;
  }
  return out;
}
function groupByClient(transactions) {
  const by = new Map();
  for (const t of transactions) {
    const id = t.ClientID || t.ClientName; // support name-based
    if (!id) continue;
    if (!by.has(id)) by.set(id, []);
    by.get(id).push(t);
  }
  return by;
}

function detectStructuring(txList, now) {
  const cash = txList
    .filter((t) => (t.Type || "").toLowerCase().includes("cash deposit"))
    .map((t) => ({ d: parseDate(t.Date), amt: toNum(t.Amount) }))
    .filter(
      (t) =>
        t.d &&
        monthsAgo(t.d, now) <= LOOKBACK_MONTHS &&
        t.amt >= CASH_STRUCTURING_MIN &&
        t.amt < CASH_STRUCTURING_MAX
    )
    .sort((a, b) => a.d - b.d);
  if (cash.length < 4) return { hit: false };

  let run = [cash[0]];
  const runs = [];
  for (let i = 1; i < cash.length; i++) {
    const prev = cash[i - 1],
      cur = cash[i];
    if (cur.d - prev.d <= 7 * MS_PER_DAY) run.push(cur);
    else {
      runs.push(run);
      run = [cur];
    }
  }
  runs.push(run);
  const maxRun = Math.max(...runs.map((r) => r.length));
  return { hit: maxRun >= 4, maxRun, count: cash.length };
}

function detectCorridors(txList, now) {
  const intl = txList
    .filter((t) => (t.Type || "").toLowerCase().includes("international"))
    .map((t) => ({
      country: toUpper(t.CounterpartyCountry),
      amt: toNum(t.Amount),
      d: parseDate(t.Date),
      ccy: toUpper(t.Currency),
    }))
    .filter((t) => t.d && monthsAgo(t.d, now) <= LOOKBACK_MONTHS);
  const risky = intl.filter((t) => HIGH_RISK_CORRIDORS.has(t.country));
  const total = risky.length;
  const big = risky.filter((t) => t.amt >= 20000).length;
  return { hit: total >= 2 && big >= 1, total, big };
}

function detectLargeDomestic(txList, now) {
  const dom = txList
    .filter((t) => (t.Type || "").toLowerCase().includes("domestic"))
    .map((t) => ({ amt: toNum(t.Amount), d: parseDate(t.Date) }))
    .filter((t) => t.d && monthsAgo(t.d, now) <= LOOKBACK_MONTHS && t.amt >= LARGE_DOMESTIC);
  return { hit: dom.length > 0, count: dom.length };
}

function scoreClient(client, txList, now = new Date()) {
  let score = 0;
  const reasons = [];

  // Profile risks
  if (toBoolYN(client.PEP)) {
    score += 30;
    reasons.push("PEP flagged (+30)");
  }
  if (toBoolYN(client.SanctionsMatch)) {
    score += 40;
    reasons.push("Sanctions match (+40)");
  }

  const last = parseDate(client.LastKYCReview || client.OnboardDate);
  if (last && monthsAgo(last, now) > 24) {
    score += 6;
    reasons.push("KYC review stale (>24mo) (+6)");
  }

  if (toUpper(client.ResidencyStatus) === "NON-RESIDENT") {
    score += 5;
    reasons.push("Non-resident (+5)");
  }

  const services = (client.ServicesUsed || "").toLowerCase();
  if (services.includes("remittance")) {
    score += 10;
    reasons.push("Uses remittance (+10)");
  }
  if (services.includes("property")) {
    score += 5;
    reasons.push("Property settlements (+5)");
  }

  const ch = (client.DeliveryChannel || "").toLowerCase();
  if (ch.includes("mixed") || ch.includes("broker") || ch.includes("in-branch")) {
    score += 4;
    reasons.push("Higher-risk delivery channel (+4)");
  }

  // Country exposures
  const exposure = (client.RiskCountryExposure || "") + "," + (client.Country || "");
  let highExp = 0,
    medExp = 0;
  for (const tag of exposure.split(",").map((s) => s.trim()).filter(Boolean)) {
    const c2 = tag.replace(/^HighRisk:|^MedRisk:/i, "").toUpperCase();
    if (HIGH_RISK_COUNTRIES.has(c2)) highExp++;
    if (MED_RISK_COUNTRIES.has(c2)) medExp++;
  }
  if (highExp > 0) {
    score += 12;
    reasons.push(`Exposure to high-risk countries (${highExp}) (+12)`);
  }
  if (medExp > 0) {
    score += 6;
    reasons.push(`Exposure to medium-risk countries (${medExp}) (+6)`);
  }

  // Transactional (last 18 months)
  const struct = detectStructuring(txList, now);
  if (struct.hit) {
    score += 15;
    reasons.push(`Structuring pattern: ${struct.maxRun}+ near-threshold cash deposits (+15)`);
  }
  const corr = detectCorridors(txList, now);
  if (corr.hit) {
    score += 12;
    reasons.push(`High-risk corridors: ${corr.total} intl to RU/CN/HK/AE/IN/IR (+12)`);
  }
  const large = detectLargeDomestic(txList, now);
  if (large.hit) {
    score += 8;
    reasons.push(`Large domestic transfer(s) ≥ ${LARGE_DOMESTIC.toLocaleString()} (+8)`);
  }

  // EDD bump
  if (toUpper(client.KYCStatus || "").includes("ENHANCED")) {
    score += 5;
    reasons.push("EDD in place (+5)");
  }

  const band = score >= 30 ? "High" : score >= 15 ? "Medium" : "Low";
  return { score, band, reasons };
}

function bandColor(band) {
  return band === "High" ? "#b71c1c" : band === "Medium" ? "#f57c00" : "#2e7d32";
}

// --- Evidence helpers ---------------------------------------------------------
function sha256OfFile(filePath) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("hex");
}

function programDocHtml(meta, sector) {
  const today = dayjs().format("YYYY-MM-DD");
  const sectorTitle = (sector || "generic").replace(/_/g, " ");
  return `<!doctype html><html><head><meta charset="utf-8"><style>
  body{font-family:Segoe UI,Arial,sans-serif;max-width:860px;margin:24px auto;padding:0 12px;color:#222}
  h1{margin:0 0 8px} h2{margin-top:24px} code{background:#f2f2f2;padding:2px 4px}
  .box{border:1px solid #e0e0e0;padding:12px;margin:12px 0;border-radius:6px}
  </style></head><body>
  <h1>AML/CTF Program — ${meta.org || "Your Organisation"}</h1>
  <div class="box">Version: 1.0 • Sector: ${sectorTitle} • Date: ${today}</div>
  <h2>1. Governance</h2><p>Compliance Officer: ${meta.compliance || "(assign)"} • Reports to ${
    meta.board || "(board/owner)"
  }.</p>
  <h2>2. ML/TF Risk Assessment</h2><p>Risk factors: customer type, geography, products/services, channels, delivery methods.</p>
  <h2>3. CDD</h2><p>Standard CDD for Low; EDD for High risk/PEPs. Verify identity before service delivery.</p>
  <h2>4. Ongoing Monitoring</h2><p>Rules: large cash ≥ $10,000; structuring; high-risk corridors; unusual patterns.</p>
  <h2>5. Reporting</h2><p>SMRs lodged promptly; internal escalation to the Compliance Officer.</p>
  <h2>6. Record Keeping</h2><p>Retain CDD/transaction records ≥ 7 years. Maintain evidence packs with SHA-256 manifests.</p>
  <h2>7. Training & Review</h2><p>Annual AML training; independent review at least every two years.</p></body></html>`;
}

// --- Routes -------------------------------------------------------------------
app.get("/", (req, res) => res.render("index", { hasKey: !!process.env.OPENAI_API_KEY }));
app.get("/healthz", (_, res) => res.status(200).json({ ok: true, time: Date.now() }));
app.get("/legal", (req, res) => res.render("legal"));
app.get("/manifest.webmanifest", (_, res) =>
  res.sendFile(path.join(__dirname, "public", "manifest.webmanifest"))
);
app.get("/sw.js", (_, res) => res.sendFile(path.join(__dirname, "public", "sw.js")));

// EXPECTS two CSV files: field names must be "clients" and "transactions"
app.post(
  "/ingest",
  upload.fields([
    { name: "clients", maxCount: 1 },
    { name: "transactions", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      const clientsBuf = req.files?.clients?.[0]?.buffer;
      const txBuf = req.files?.transactions?.[0]?.buffer;
      if (!clientsBuf || !txBuf) {
        return res
          .status(400)
          .send("Upload both files. Expect fields named: clients (CSV), transactions (CSV).");
      }

      // Parse CSVs (case-insensitive headers)
      const clientsRaw = parse(clientsBuf.toString("utf8"), {
        columns: true,
        skip_empty_lines: true,
      });
      const txRaw = parse(txBuf.toString("utf8"), {
        columns: true,
        skip_empty_lines: true,
      });

      // Normalize headers to canonical keys
      const clients = clientsRaw.map((r, i) => {
        const n = normalizeRow(r, CLIENT_KEYMAP);
        // fallbacks so we always have essential fields
        n.ClientID = n.ClientID || `C${String(i + 1).padStart(3, "0")}`;
        n.Name = n.Name || n.ClientID;
        n.Country = n.Country || "AU";
        return n;
      });
      const tx = txRaw.map((r) => {
        const n = normalizeRow(r, TX_KEYMAP);
        n.Amount = toNum(n.Amount);
        return n;
      });

      // Debug headers once (check Render logs if something looks off)
      console.log("Sample client keys:", Object.keys(clients[0] || {}));
      console.log("Sample txn keys:", Object.keys(tx[0] || {}));

      // Build txn index (by ClientID or ClientName)
      const by = groupByClient(tx);

      // Score
      const now = new Date();
      const scored = clients
        .map((c) => {
          const t = by.get(c.ClientID) || by.get(c.Name) || [];
          const r = scoreClient(c, t, now);
          return {
            ClientID: c.ClientID,
            Name: c.Name,
            Band: r.band,
            Score: r.score,
            Reasons: r.reasons,
            // (Optional) show a couple of recent txn stats in UI later if needed
            Country: c.Country || "",
            PEP: c.PEP || "N",
            KYCStatus: c.KYCStatus || "",
            DeliveryChannel: c.DeliveryChannel || "",
            ServicesUsed: c.ServicesUsed || "",
          };
        })
        .sort((a, b) => b.Score - a.Score);

      // Build monitoring cases (simple narrations)
      const cases = [];
      const openai = getOpenAI();
      for (const c of scored) {
        const t = by.get(c.ClientID) || by.get(c.Name) || [];
        const s = detectStructuring(t, now);
        if (s.hit)
          cases.push({
            rule: "R_STRUCTURING",
            client: c.Name,
            amount: null,
            detail: `Structuring pattern (${s.maxRun}+ near-threshold cash deposits)`,
          });
        const hr = detectCorridors(t, now);
        if (hr.hit)
          cases.push({
            rule: "R_HIGH_RISK_CORRIDORS",
            client: c.Name,
            amount: null,
            detail: `Intl transfers to high-risk corridors (n=${hr.total})`,
          });
        const ld = detectLargeDomestic(t, now);
        if (ld.hit)
          cases.push({
            rule: "R_LARGE_DOMESTIC",
            client: c.Name,
            amount: null,
            detail: `Large domestic transfer(s) ≥ ${LARGE_DOMESTIC.toLocaleString()}`,
          });
      }

      // Optional: short AI narratives (gracefully skip if no key)
      async function narrateCase(k) {
        if (!openai) return `${k.rule}: ${k.detail}. Client ${k.client}.`;
        try {
          const r = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            temperature: 0.2,
            messages: [
              { role: "system", content: "Write concise AML monitoring narratives (≤2 sentences)." },
              { role: "user", content: `Create a short narrative for: ${JSON.stringify(k).slice(0, 1200)}` },
            ],
          });
          return r.choices[0]?.message?.content?.trim() || `${k.rule}: ${k.detail}.`;
        } catch {
          return `${k.rule}: ${k.detail}.`;
        }
      }
      for (const k of cases) k.narrative = await narrateCase(k);

      // Save run (JSON, program.html, manifest, ZIP)
      const runId = uuidv4();
      const runDir = path.join(__dirname, "runs", runId);
      ensureDir(runDir);

      fs.writeFileSync(path.join(runDir, "clients.json"), JSON.stringify(scored, null, 2));
      fs.writeFileSync(path.join(runDir, "transactions.json"), JSON.stringify(tx, null, 2));
      fs.writeFileSync(path.join(runDir, "cases.json"), JSON.stringify(cases, null, 2));
      const programHtml = programDocHtml({ org: "Your Org" }, "generic");
      fs.writeFileSync(path.join(runDir, "program.html"), programHtml, "utf8");

      const manifest = {};
      for (const f of ["clients.json", "transactions.json", "cases.json", "program.html"]) {
        const p = path.join(runDir, f);
        manifest[f] = { sha256: sha256OfFile(p), bytes: fs.statSync(p).size };
      }
      fs.writeFileSync(path.join(runDir, "manifest.json"), JSON.stringify(manifest, null, 2));

      const zipPath = path.join(runDir, "evidence_pack.zip");
      await new Promise((resolve, reject) => {
        const output = fs.createWriteStream(zipPath);
        const zip = archiver("zip", { zlib: { level: 9 } });
        output.on("close", resolve);
        zip.on("error", reject);
        zip.pipe(output);
        zip.directory(runDir, false);
        zip.finalize();
      });

      const token = uuidv4().replace(/-/g, "");
      fs.writeFileSync(path.join(runDir, "share.txt"), token, "utf8");

      // Render results
      res.render("results", {
        runId,
        token,
        clients: scored.map((x) => ({ ...x, Color: bandColor(x.Band) })),
        cases,
        hasKey: !!process.env.OPENAI_API_KEY,
      });
    } catch (e) {
      console.error(e);
      res.status(500).send("Processing error: " + e.message);
    }
  }
);

// Evidence download + verify
app.get("/download/:runId", (req, res) => {
  const runDir = path.join(__dirname, "runs", req.params.runId);
  const zipPath = path.join(runDir, "evidence_pack.zip");
  if (!fs.existsSync(zipPath)) return res.status(404).send("Not found.");
  res.download(zipPath, `TrancheReady_${req.params.runId}.zip`);
});

app.get("/share/:token", (req, res) => {
  const runsRoot = path.join(__dirname, "runs");
  const dirs = fs.existsSync(runsRoot) ? fs.readdirSync(runsRoot) : [];
  for (const d of dirs) {
    const p = path.join(runsRoot, d, "share.txt");
    if (fs.existsSync(p) && fs.readFileSync(p, "utf8") === req.params.token) {
      const manifest = JSON.parse(fs.readFileSync(path.join(runsRoot, d, "manifest.json"), "utf8"));
      return res.render("verify", { runId: d, manifest });
    }
  }
  res.status(404).send("Invalid token.");
});

// Export clients table as CSV (for offline review)
app.get("/export/clients.csv", (req, res) => {
  const { runId } = req.query;
  if (!runId) return res.status(400).send("Missing runId");
  const p = path.join(__dirname, "runs", runId, "clients.json");
  if (!fs.existsSync(p)) return res.status(404).send("Run not found");
  const arr = JSON.parse(fs.readFileSync(p, "utf8"));
  const csv = Papa.unparse(arr.map(({ Reasons, ...rest }) => rest));
  res.setHeader("Content-Type", "text/csv");
  res.setHeader("Content-Disposition", `attachment; filename="clients_${runId}.csv"`);
  res.send(csv);
});

app.listen(PORT, () => console.log(`TrancheReady running on ${PORT}`));
