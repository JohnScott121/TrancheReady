// server.js — TrancheReady
// Node/Express app: CSV upload → risk scoring → monitoring cases → evidence pack (ZIP) with SHA-256 manifest.
// Works with or without OPENAI_API_KEY (nicer narratives if set).

const express = require("express");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const multer = require("multer");
const Papa = require("papaparse");
const dayjs = require("dayjs");
const archiver = require("archiver");
const { v4: uuidv4 } = require("uuid");
const { OpenAI } = require("openai");

// --- App setup --------------------------------------------------------------
const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));
app.use(express.static("public"));

const upload = multer({
  dest: "uploads/",
  limits: { fileSize: 50 * 1024 * 1024 } // 50MB per file
});

const PORT = process.env.PORT || 3000;

// Ensure runtime dirs
function ensureDir(p) { if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true }); }
ensureDir(path.join(__dirname, "uploads"));
ensureDir(path.join(__dirname, "runs"));

// Optional AI (narratives)
function getOpenAI() {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return null;
  try { return new OpenAI({ apiKey: key }); } catch { return null; }
}

// --- Risk constants / rules -------------------------------------------------
const HIGH_RISK_COUNTRIES = new Set(["IR","KP","SY","AF","YE","SS","CU","RU"]);
const COUNTRY_TIER = (c) => HIGH_RISK_COUNTRIES.has((c||"").toUpperCase()) ? 3 : 1;

const SERVICE_RISK = {
  "conveyancing": 2,
  "trust_account": 3,
  "company_setup": 2,
  "real_estate_purchase": 2,
  "cash_intensive": 3
};

const CHANNEL_RISK = { "face_to_face": 1, "non_face_to_face": 2, "introduced": 2 };

const THRESHOLDS = {
  CASH_LARGE_AUD: 10000,
  MULTI_TXN_DAYS: 7,
  MULTI_TXN_COUNT: 3
};

// --- Helpers ----------------------------------------------------------------
function parseCSV(filePath) {
  const text = fs.readFileSync(filePath, "utf8");
  const parsed = Papa.parse(text, { header: true, skipEmptyLines: true });
  return parsed.data.map(row => Object.fromEntries(
    Object.entries(row).map(([k,v]) => [k.trim(), typeof v === "string" ? v.trim() : v])
  ));
}

function sha256OfFile(filePath) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("hex");
}

function scoreClient(client) {
  // normalized: name, country, type, channel, pep (Y/N), sector, services:[]
  let score = 0;
  const notes = [];

  const tier = COUNTRY_TIER(client.country);
  score += tier; notes.push(`Country risk tier: +${tier} (${client.country || "unknown"})`);

  const ch = (client.channel || "face_to_face").toLowerCase();
  if (CHANNEL_RISK[ch]) { score += CHANNEL_RISK[ch]; notes.push(`Channel: +${CHANNEL_RISK[ch]} (${ch})`); }

  const pep = String(client.pep || "").toLowerCase();
  if (pep === "y" || pep === "yes" || pep === "true") { score += 3; notes.push("PEP: +3"); }

  const services = client.services || [];
  for (const s of services) {
    const key = (s || "").toLowerCase();
    if (SERVICE_RISK[key]) { score += SERVICE_RISK[key]; notes.push(`Service ${key}: +${SERVICE_RISK[key]}`); }
  }

  const t = (client.type || "").toLowerCase();
  if (t === "company" || t === "trust") { score += 1; notes.push(`Entity type (${t}): +1`); }

  return { score, notes };
}

function band(score) {
  if (score >= 7) return { band: "High", color: "#b71c1c" };
  if (score >= 4) return { band: "Medium", color: "#f57c00" };
  return { band: "Low", color: "#2e7d32" };
}

function monitoringCases(clients, txns) {
  const out = [];
  const byClient = new Map();
  for (const t of txns) {
    const k = t.client_id || t.client || t.client_name || "";
    if (!byClient.has(k)) byClient.set(k, []);
    byClient.get(k).push(t);
  }

  for (const [k, arr] of byClient) {
    const client = clients.find(c => c.id === k || c.name === k) || { name: k || "(unknown)" };

    // R1: Single large cash
    for (const t of arr) {
      const amt = Number(t.amount || 0);
      const isCash = String(t.cash || t.method || "").toLowerCase().includes("cash");
      if (isCash && amt >= THRESHOLDS.CASH_LARGE_AUD) {
        out.push({ rule:"R1_LARGE_CASH", client:client.name, date:t.date, amount:amt,
          detail:"Cash transaction ≥ $10,000", evidence:t });
      }
    }

    // R2: Structuring (≥N txns in ≤D days totaling ≥ threshold)
    const sorted = arr.slice().sort((a,b)=> new Date(a.date)-new Date(b.date));
    for (let i=0; i<sorted.length; i++) {
      let count = 1, total = Number(sorted[i].amount||0);
      const start = dayjs(sorted[i].date);
      for (let j=i+1; j<sorted.length; j++) {
        const dd = dayjs(sorted[j].date);
        if (dd.diff(start, "day") <= THRESHOLDS.MULTI_TXN_DAYS) {
          count++; total += Number(sorted[j].amount||0);
        }
      }
      if (count >= THRESHOLDS.MULTI_TXN_COUNT && total >= THRESHOLDS.CASH_LARGE_AUD) {
        out.push({ rule:"R2_STRUCTURING", client:client.name, date:sorted[i].date+" …", amount:total,
          detail:`≥${THRESHOLDS.MULTI_TXN_COUNT} txns within ${THRESHOLDS.MULTI_TXN_DAYS} days totaling ≥ $${THRESHOLDS.CASH_LARGE_AUD}`,
          evidence: sorted.slice(i, i+THRESHOLDS.MULTI_TXN_COUNT) });
      }
    }

    // R3: High-risk corridor
    for (const t of arr) {
      const cc = (t.country || t.counterparty_country || "").toUpperCase();
      if (COUNTRY_TIER(cc) === 3) {
        out.push({ rule:"R3_HIGH_RISK_JURISDICTION", client:client.name, date:t.date,
          amount:Number(t.amount||0), detail:`Counterparty in high-risk country (${cc})`, evidence:t });
      }
    }
  }
  return out;
}

// AI narrative helper (optional)
async function narrateCase(c) {
  const openai = getOpenAI();
  if (!openai) return `Rule ${c.rule}: ${c.detail}. Client ${c.client}. Amount ${c.amount ? "$"+c.amount.toFixed(2) : "n/a"}.`;
  try {
    const r = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      temperature: 0.2,
      messages: [
        { role:"system", content:"You write concise AML monitoring narratives (<=2 sentences)." },
        { role:"user", content:`Create a short narrative for: ${JSON.stringify(c).slice(0,1500)}` }
      ]
    });
    return r.choices[0].message.content?.trim() || `Rule ${c.rule}: ${c.detail}.`;
  } catch { return `Rule ${c.rule}: ${c.detail}.`; }
}

// Program document (sector-aware stub)
function programDocHtml(meta, sector) {
  const today = dayjs().format("YYYY-MM-DD");
  const sectorTitle = (sector||"generic").replace(/_/g," ");
  return `
  <html><head><meta charset="utf-8"/>
  <style>
  body{font-family:Segoe UI,Arial,sans-serif;max-width:860px;margin:24px auto;padding:0 12px;color:#222}
  h1{margin:0 0 8px} h2{margin-top:24px} code{background:#f2f2f2;padding:2px 4px}
  .box{border:1px solid #e0e0e0;padding:12px;margin:12px 0;border-radius:6px}
  </style></head><body>
    <h1>AML/CTF Program — ${meta.org || "Your Organisation"}</h1>
    <div class="box">Version: 1.0 • Sector: ${sectorTitle} • Date: ${today}</div>
    <h2>1. Governance</h2>
    <p>Compliance Officer: ${meta.compliance || "(assign)"} • Reports to ${meta.board || "(board/owner)"}.</p>
    <h2>2. ML/TF Risk Assessment</h2>
    <p>Risk factors: customer type, geography, products/services, channels, delivery methods.</p>
    <h2>3. CDD</h2>
    <p>Standard CDD for Low; EDD for High risk/PEPs. Verify identity before service delivery.</p>
    <h2>4. Ongoing Monitoring</h2>
    <p>Rules: large cash ≥ $10,000; structuring within 7 days; high-risk corridors; unusual patterns.</p>
    <h2>5. Reporting</h2>
    <p>SMRs lodged promptly; internal escalation to the Compliance Officer.</p>
    <h2>6. Record Keeping</h2>
    <p>Retain CDD and transaction records ≥ 7 years. Maintain evidence packs with SHA-256 manifests.</p>
    <h2>7. Training & Review</h2>
    <p>Annual AML training; independent review at least every two years.</p>
  </body></html>`;
}

// --- Routes -----------------------------------------------------------------
app.get("/", (req, res) => {
  res.render("index", { hasKey: !!process.env.OPENAI_API_KEY });
});

// Expect two CSVs: clients.csv, transactions.csv
app.post("/ingest", upload.fields([{ name:"clients" }, { name:"transactions" }]), async (req, res) => {
  try {
    const cFile = req.files?.clients?.[0];
    const tFile = req.files?.transactions?.[0];
    if (!cFile || !tFile) return res.status(400).send("Upload both Clients and Transactions CSV.");

    const clientsRaw = parseCSV(cFile.path);
    const txnsRaw = parseCSV(tFile.path);

    const clients = clientsRaw.map((r,i)=>({
      id: r.id || r.client_id || String(i+1),
      name: r.name || r.client_name || r.customer || `Client ${i+1}`,
      type: (r.type || r.entity_type || "individual").toLowerCase(),
      country: r.country || r.residence_country || "AU",
      pep: r.pep || r.is_pep || "N",
      channel: (r.channel || "face_to_face").toLowerCase(),
      sector: (r.sector || "legal").toLowerCase(),
      services: (r.services || "").split(/[;,]/).map(s=>s.trim()).filter(Boolean)
    }));

    const txns = txnsRaw.map(r=>({
      date: r.date || r.txn_date || r.timestamp || "",
      client: r.client || r.client_name || r.name || "",
      client_id: r.client_id || r.id || "",
      amount: Number(r.amount || r.aud_amount || r.value || 0),
      currency: r.currency || "AUD",
      country: r.country || r.counterparty_country || "",
      method: r.method || r.channel || "",
      cash: r.cash || ""
    }));

    // Risk scoring
    const scored = clients.map(c=>{
      const sc = scoreClient(c);
      const b = band(sc.score);
      return { ...c, riskScore: sc.score, riskBand: b.band, riskColor: b.color, notes: sc.notes };
    }).sort((a,b)=> b.riskScore - a.riskScore);

    // Monitoring
    const cases = monitoringCases(scored, txns);
    await Promise.all(cases.map(async k => { k.narrative = await narrateCase(k); }));

    // Persist a "run"
    const runId = uuidv4();
    const runDir = path.join(__dirname, "runs", runId);
    ensureDir(runDir);

    // Save JSONs
    fs.writeFileSync(path.join(runDir, "clients.json"), JSON.stringify(scored, null, 2));
    fs.writeFileSync(path.join(runDir, "transactions.json"), JSON.stringify(txns, null, 2));
    fs.writeFileSync(path.join(runDir, "cases.json"), JSON.stringify(cases, null, 2));

    // Program doc
    const programHtml = programDocHtml({ org: "Your Org" }, scored[0]?.sector || "generic");
    fs.writeFileSync(path.join(runDir, "program.html"), programHtml, "utf8");

    // Manifest with SHA-256
    const manifest = {};
    for (const f of ["clients.json","transactions.json","cases.json","program.html"]) {
      const p = path.join(runDir, f);
      manifest[f] = { sha256: sha256OfFile(p), bytes: fs.statSync(p).size };
    }
    fs.writeFileSync(path.join(runDir, "manifest.json"), JSON.stringify(manifest, null, 2));

    // Build ZIP inside the run directory (persists on Render disk)
    const zipPath = path.join(runDir, "evidence_pack.zip");
    await new Promise((resolve, reject)=>{
      const output = fs.createWriteStream(zipPath);
      const zip = archiver("zip", { zlib: { level: 9 }});
      output.on("close", resolve);
      zip.on("error", reject);
      zip.pipe(output);
      zip.directory(runDir, false);
      zip.finalize();
    });

    // Share token
    const token = uuidv4().replace(/-/g,"");
    fs.writeFileSync(path.join(runDir, "share.txt"), token, "utf8");

    // Cleanup uploads
    try { fs.unlinkSync(cFile.path); } catch {}
    try { fs.unlinkSync(tFile.path); } catch {}

    res.render("results", {
      runId,
      token,
      clients: scored,
      cases,
      hasKey: !!process.env.OPENAI_API_KEY
    });
  } catch (e) {
    console.error(e);
    res.status(500).send("Processing error.");
  }
});

app.get("/download/:runId", (req,res)=>{
  const runDir = path.join(__dirname, "runs", req.params.runId);
  const zipPath = path.join(runDir, "evidence_pack.zip");
  if (!fs.existsSync(zipPath)) return res.status(404).send("Not found.");
  res.download(zipPath, `TrancheReady_${req.params.runId}.zip`);
});

app.get("/share/:token", (req,res)=>{
  const runsRoot = path.join(__dirname, "runs");
  const dirs = fs.existsSync(runsRoot) ? fs.readdirSync(runsRoot) : [];
  for (const d of dirs) {
    const p = path.join(runsRoot, d, "share.txt");
    if (fs.existsSync(p) && fs.readFileSync(p,"utf8") === req.params.token) {
      const manifest = JSON.parse(fs.readFileSync(path.join(runsRoot, d, "manifest.json"),"utf8"));
      return res.render("verify", { runId: d, manifest });
    }
  }
  res.status(404).send("Invalid token.");
});

app.listen(PORT, ()=> console.log(`TrancheReady running on ${PORT}`));
