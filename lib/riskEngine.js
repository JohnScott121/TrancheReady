// lib/riskEngine.js
const HIGH_RISK_COUNTRIES = new Set(["RU","IR"]);
const MED_RISK_COUNTRIES  = new Set(["CN","HK","AE","IN"]);
const HIGH_RISK_CORRIDORS = new Set(["RU","IR","CN","HK","AE","IN"]);
const CASH_STRUCTURING_MIN = 9600;
const CASH_STRUCTURING_MAX = 10000;
const LARGE_DOMESTIC = 100000;
const LOOKBACK_MONTHS = 18;
const MS_PER_DAY = 86400000;

const toNum = (x)=> {
  if (x == null) return NaN;
  if (typeof x === "number") return x;
  const n = parseFloat(String(x).replace(/[, ]/g,""));
  return isNaN(n) ? NaN : n;
};
const toUpper = (x)=> (x ?? "").toString().trim().toUpperCase();
const toBoolYN = (x)=> ["Y","YES","TRUE"].includes(toUpper(x));

function parseDate(x){
  const d = new Date(String(x ?? "").replace(/\//g,"-"));
  return isNaN(d.getTime()) ? null : d;
}
function monthsAgo(a,b){
  if (!(a instanceof Date)) a = parseDate(a);
  if (!(b instanceof Date)) b = parseDate(b);
  if (!a || !b) return Infinity;
  return (b.getFullYear()-a.getFullYear())*12 + (b.getMonth()-a.getMonth());
}

// header normalizers (case-insensitive)
const CLIENT_KEYMAP = new Map(Object.entries({
  clientid:"ClientID", client_id:"ClientID", id:"ClientID",
  name:"Name", entitytype:"EntityType", country:"Country", state:"State", suburb:"Suburb", postcode:"Postcode",
  residencystatus:"ResidencyStatus", pep:"PEP", kycstatus:"KYCStatus",
  onboarddate:"OnboardDate", lastkycreview:"LastKYCReview",
  deliverychannel:"DeliveryChannel", servicesused:"ServicesUsed",
  industry:"Industry", annualturnoveraud:"AnnualTurnoverAUD",
  sourceoffunds:"SourceOfFunds", sanctionsmatch:"SanctionsMatch", riskcountryexposure:"RiskCountryExposure"
}));
const TX_KEYMAP = new Map(Object.entries({
  txnid:"TxnID", id:"TxnID", clientid:"ClientID", client_id:"ClientID",
  date:"Date", amount:"Amount", currency:"Currency", type:"Type",
  channel:"Channel", location:"Location", counterpartyname:"CounterpartyName",
  counterpartycountry:"CounterpartyCountry", notes:"Notes"
}));

function normalizeRow(row, keymap){
  const out = {};
  for (const [k,v] of Object.entries(row)){
    const canon = keymap.get(k.toLowerCase());
    if (canon) out[canon] = v;
  }
  return out;
}
function groupByClient(tx){
  const m = new Map();
  for (const t of tx){
    const id = t.ClientID; if (!id) continue;
    if (!m.has(id)) m.set(id, []);
    m.get(id).push(t);
  }
  return m;
}

function detectStructuring(tx, now){
  const cash = tx
    .filter(t => (t.Type||"").toLowerCase().includes("cash deposit"))
    .map(t => ({ d: parseDate(t.Date), amt: toNum(t.Amount) }))
    .filter(t => t.d && monthsAgo(t.d, now) <= LOOKBACK_MONTHS && t.amt >= CASH_STRUCTURING_MIN && t.amt < CASH_STRUCTURING_MAX)
    .sort((a,b)=>a.d-b.d);
  if (cash.length < 4) return { hit:false };
  let run=[cash[0]], runs=[];
  for (let i=1;i<cash.length;i++){
    const prev=cash[i-1], cur=cash[i];
    if ((cur.d - prev.d) <= 7*MS_PER_DAY) run.push(cur);
    else { runs.push(run); run=[cur]; }
  }
  runs.push(run);
  const maxRun = Math.max(...runs.map(r=>r.length));
  return { hit:maxRun>=4, maxRun, count:cash.length };
}
function detectCorridors(tx, now){
  const intl = tx.filter(t => (t.Type||"").toLowerCase().includes("international"))
    .map(t => ({country: toUpper(t.CounterpartyCountry), amt: toNum(t.Amount), d: parseDate(t.Date)}))
    .filter(t => t.d && monthsAgo(t.d, now) <= LOOKBACK_MONTHS);
  const risky = intl.filter(t => HIGH_RISK_CORRIDORS.has(t.country));
  const total = risky.length, big = risky.filter(t => t.amt >= 20000).length;
  return { hit: total>=2 && big>=1, total, big };
}
function detectLargeDomestic(tx, now){
  const dom = tx.filter(t => (t.Type||"").toLowerCase().includes("domestic"))
    .map(t=>({ amt: toNum(t.Amount), d: parseDate(t.Date) }))
    .filter(t => t.d && monthsAgo(t.d, now) <= LOOKBACK_MONTHS && t.amt >= LARGE_DOMESTIC);
  return { hit: dom.length>0, count: dom.length };
}

function scoreClient(client, tx, now=new Date()){
  let score = 0;
  const reasons = [];

  // Profile
  if (toBoolYN(client.PEP))               { score+=30; reasons.push("PEP flagged (+30)"); }
  if (toBoolYN(client.SanctionsMatch))    { score+=40; reasons.push("Sanctions match (+40)"); }
  const last = parseDate(client.LastKYCReview || client.OnboardDate);
  if (last && monthsAgo(last, now) > 24)  { score+=6;  reasons.push("KYC review stale (>24mo) (+6)"); }
  if (toUpper(client.ResidencyStatus)==="NON-RESIDENT") { score+=5; reasons.push("Non-resident (+5)"); }
  const services = (client.ServicesUsed||"").toLowerCase();
  if (services.includes("remittance"))    { score+=10; reasons.push("Uses remittance (+10)"); }
  if (services.includes("property"))      { score+=5;  reasons.push("Property settlements (+5)"); }
  const ch = (client.DeliveryChannel||"").toLowerCase();
  if (ch.includes("mixed")||ch.includes("broker")||ch.includes("in-branch")) { score+=4; reasons.push("Higher-risk delivery channel (+4)"); }
  const exposure = (client.RiskCountryExposure||"") + "," + (client.Country||"");
  let highExp=0, medExp=0;
  for (const tag of exposure.split(",").map(s=>s.trim()).filter(Boolean)){
    const c2 = tag.replace(/^HighRisk:|^MedRisk:/i,"").toUpperCase();
    if (HIGH_RISK_COUNTRIES.has(c2)) highExp++;
    if (MED_RISK_COUNTRIES.has(c2))  medExp++;
  }
  if (highExp>0) { score+=12; reasons.push(`Exposure to high-risk countries (${highExp}) (+12)`); }
  if (medExp>0)  { score+=6;  reasons.push(`Exposure to medium-risk countries (${medExp}) (+6)`); }

  // Transactions (last 18 months)
  const s = detectStructuring(tx, now);
  if (s.hit) { score+=15; reasons.push(`Structuring pattern: ${s.maxRun}+ near-threshold cash deposits (+15)`); }
  const c = detectCorridors(tx, now);
  if (c.hit) { score+=12; reasons.push(`High-risk corridors: ${c.total} intl to RU/CN/HK/AE/IN/IR (+12)`); }
  const d = detectLargeDomestic(tx, now);
  if (d.hit) { score+=8;  reasons.push(`Large domestic transfer(s) ≥ ${LARGE_DOMESTIC.toLocaleString()} (+8)`); }

  // EDD bump
  if (toUpper(client.KYCStatus).includes("ENHANCED")) { score+=5; reasons.push("EDD in place (+5)"); }

  const band = score >= 30 ? "High" : score >= 15 ? "Medium" : "Low";
  return { score, band, reasons };
}

module.exports = { CLIENT_KEYMAP, TX_KEYMAP, normalizeRow, groupByClient, scoreClient };
