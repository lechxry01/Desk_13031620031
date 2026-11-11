const $ = id => document.getElementById(id);
const money = n => "$" + Number(n || 0).toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2});
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
const pct = n => (n >= 0 ? "+" : "") + (n * 100).toFixed(2) + "%";
let KEY = localStorage.getItem("appkey") || "";
let SEEN = null, CHATLAST = 0, ACCOUNT = null, PRICES = {}, PICK = null, busy = false, asking = null, tick = 0;

function toast(msg, kind = "info") {
  const t = $("toast");
  t.textContent = msg;
  t.className = "show " + kind;
  clearTimeout(toast.t);
  toast.t = setTimeout(() => (t.className = ""), 3800);
}

function askKey() {
  if (asking) return asking;
  asking = new Promise(res => setTimeout(() => {
    const k = prompt("ใส่รหัสเข้าใช้งาน (APP_KEY)");
    if (k) { KEY = k; localStorage.setItem("appkey", k); }
    asking = null;
    res(!!k);
  }, 0));
  return asking;
}

async function api(url, opts = {}, retried = false) {
  opts.headers = {...(opts.headers || {}), "X-Key": KEY};
  const r = await fetch(url, opts);
  const d = await r.json().catch(() => ({}));
  if (r.status === 401) {
    if (retried) { localStorage.removeItem("appkey"); KEY = ""; throw Error("รหัสไม่ถูกต้อง"); }
    if (await askKey()) return api(url, opts, true);
    throw Error("ต้องใส่รหัสเข้าใช้งาน");
  }
  if (!r.ok) throw Error(typeof d.detail === "string" ? d.detail : "คำสั่งไม่ถูกต้อง");
  return d;
}

const BADGE = {
  live: "", none: "ยังไม่มีราคา",
  closed: "ตลาดปิด/ล่าช้า · แสดงราคาล่าสุด",
  stale: "ราคาไม่สด · งดส่งคำสั่ง",
};

function renderMarket(items) {
  const old = PRICES;
  PRICES = Object.fromEntries(items.map(x => [x.symbol, x]));
  const dir = x => !old[x.symbol] || old[x.symbol].price == null || x.price == null ? "" : x.price > old[x.symbol].price ? "up" : x.price < old[x.symbol].price ? "down" : "";
  $("market").innerHTML = items.map(x => `
    <div class="marketrow"><b>${x.symbol}</b><strong class="${dir(x)}">${x.price == null ? "—" : money(x.price)}</strong>
    <span class="muted">${esc(x.source)}</span>
    ${BADGE[x.status] ? `<small class="badge ${x.status}">${BADGE[x.status]}</small>` : ""}</div>`).join("");
  const sel = $("symbol");
  if (sel.options.length !== items.length) {
    const cur = sel.value;
    sel.innerHTML = items.map(x => `<option>${esc(x.symbol)}</option>`).join("");
    if (cur) sel.value = cur;
  }
}

const BOTC = {Chief: ["#f26a1b", "50% 0 50% 50%"], Scout: ["#f7931e", "42%"], News: ["#2b7bff", "14px"],
  Sentiment: ["#e5384f", "38% 62% 45% 55%"], Charts: ["#8b5cf6", "50% 50% 40% 60%"], Skeptic: ["#8b95a5", "24px 24px 10px 10px"]};
const blob = (b, sm) => `<span class="blob${sm ? " sm" : ""}" style="--c:${(BOTC[b] || ["#888"])[0]};--r:${(BOTC[b] || [0, "50%"])[1]}"></span>`;

function hms(s) { return Math.floor(s / 3600) + "h " + String(Math.floor(s % 3600 / 60)).padStart(2, "0") + "m"; }

function renderDesk(d) {
  const wr = d.win_rate == null ? "—" : (d.win_rate * 100).toFixed(1) + "%";
  $("surv").innerHTML = `<div><small>BALANCE</small><strong>${money(d.equity)}</strong></div>
    <div><small>TOTAL P/L</small><strong class="${d.pnl >= 0 ? "buy" : "sell"}">${(d.pnl >= 0 ? "+" : "") + money(d.pnl)}</strong></div>
    <div><small>WIN RATE</small><strong>${wr}</strong><small>${d.closed} ไม้ปิด</small></div>
    <div><small>UPTIME · CYCLE</small><strong>${hms(d.uptime)}</strong><small>รอบ ${d.cycle}</small></div>`;
  $("bots").innerHTML = d.bots.map(x => `<div class="bot">${blob(x.bot)}<b>${x.bot.toUpperCase()}</b><i class="${x.state}">${x.state}</i></div>`).join("");
  const box = $("chat"), last = d.messages.length ? d.messages[d.messages.length - 1].id : 0;
  if (!d.messages.length) return;
  if (last !== CHATLAST) {
    const near = box.scrollHeight - box.scrollTop - box.clientHeight < 80 || !CHATLAST;
    box.innerHTML = d.messages.map(m => `<div class="msg">${blob(m.bot, true)}<div class="bub ${esc(m.tone)}" style="--c:${(BOTC[m.bot] || ["#aaa"])[0]}">
      <b>${esc(m.bot)}</b>${m.symbol ? ` · <span class="muted">${esc(m.symbol)}</span>` : ""} <span class="muted">· ${ago(m.ts)}</span><div>${esc(m.text)}</div></div></div>`).join("");
    if (near) box.scrollTop = box.scrollHeight;
    CHATLAST = last;
  }
  $("floorage").textContent = d.bots.filter(x => x.state === "online").length + " online · รอบ " + d.cycle;
}

function fillCard(t) {
  const el = $("fill"), sell = t.side === "SELL";
  el.innerHTML = `<small>${sell ? "POSITION CLOSED" : "BIG FILL"} · ${esc(t.side)}</small><strong>${esc(t.symbol)}</strong>` +
    (sell ? `<span class="${t.pnl >= 0 ? "pos" : "neg"}">${t.pnl >= 0 ? "+" : ""}${money(t.pnl)}</span>` : `<span>${t.qty} @ ${money(t.price)}</span>`);
  el.className = "show";
  clearTimeout(fillCard.t);
  fillCard.t = setTimeout(() => (el.className = ""), 3500);
}

const VERDICT = {bought: "🟢 ซื้อแล้ว", exit: "🔴 ปิดสถานะ", held: "📌 ถืออยู่", passed: "✅ ผ่าน", killed: "☠️ ตัดออก"};
const RSTAT = {pass: "✅", fail: "❌", skip: "➖", unknown: "❔"};

function prow(r) {
  return `<details class="prow"><summary><b>${esc(r.symbol)}</b>
    <span>${VERDICT[r.verdict] || esc(r.verdict)}${r.killed_by ? " · " + esc(r.killed_by) : ""}${r.score != null ? " · " + Number(r.score).toFixed(2) : ""}</span></summary>
    <div class="pnote">${esc(r.note) || "—"}</div>
    ${(r.rules || []).map(x => `<div class="rule">${RSTAT[x.status] || "?"} <b>${esc(x.rule)}</b> · ${esc(x.detail)}</div>`).join("")}</details>`;
}

function renderPipeline(p) {
  const c = p.counts;
  const chip = (name, n, sub) => `<div class="chip"><b>${n}</b><small>${name}</small>${sub ? `<em>${sub}</em>` : ""}</div>`;
  $("pipeflow").innerHTML = chip("Scout", c.scout, "สแกน") + "<span>›</span>" + chip("Filter", c.filter, "ผ่าน") + "<span>›</span>" +
    chip("Analyst", c.analyst, "≥ " + p.analyst_min) + "<span>›</span>" + chip("Executor", c.executor, "รอบนี้");
  $("pipeage").textContent = p.age == null ? "กำลังเริ่มรอบแรก…" : `รอบที่ ${p.cycle} · ${p.age} วิที่แล้ว`;
  const live = p.rows.filter(r => r.verdict !== "killed"), dead = p.rows.filter(r => r.verdict === "killed");
  const byAgent = {};
  dead.forEach(r => byAgent[r.killed_by] = (byAgent[r.killed_by] || 0) + 1);
  $("pipelist").innerHTML =
    (p.scout_error ? `<div class="empty">Scout ดึงรายชื่อเหรียญไม่ได้: ${esc(p.scout_error)}</div>` : "") +
    (live.length ? live.map(prow).join("") : '<div class="empty">ยังไม่มีตัวที่ผ่านทุกด่านในรอบนี้</div>') +
    (dead.length ? `<details class="deadbox"><summary>☠️ ถูกตัดออก ${dead.length} ตัว (${Object.entries(byAgent).map(([k, v]) => k + " " + v).join(" · ")})</summary>${dead.map(prow).join("")}</details>` : "");
}

function ago(iso) {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  return s < 60 ? s + " วิ" : s < 3600 ? Math.floor(s / 60) + " นาที" : Math.floor(s / 3600) + " ชม.";
}
const ACT = {BUY: "🟢 ซื้อ", SELL: "🔴 ขาย", kill: "☠️ ตัดออก", "stop-loss": "🛑 Stop-loss", "trailing-stop": "📉 Trailing stop", trail: "📈 เริ่ม trailing", skip: "⚠️ ข้าม", system: "⚙️ ระบบ"};

function renderBot(b) {
  $("autoSwitch").checked = b.enabled;
  const p = b.params;
  $("botstatus").textContent = (b.enabled ? "● ทำงานอยู่ (บัญชีจำลอง)" : "○ ปิดอยู่") +
    ` · ไม้ละไม่เกิน ${p.trade_pct * 100}% ของพอร์ต · สูงสุด ${p.max_positions} สถานะ · Analyst ≥ ${p.analyst_min} · พักต่อสัญลักษณ์ ${p.cooldown_min} นาที` +
    (b.analysis_age != null ? ` · รอบล่าสุด ${b.analysis_age} วิที่แล้ว (ทุก ${p.interval_sec} วิ)` : " · กำลังเริ่มรอบแรก…");
  $("botlog").innerHTML = b.log.length ? b.log.map(l => `
    <div class="traderow"><b>${ACT[l.action] || esc(l.action)} ${l.symbol === "-" ? "" : esc(l.symbol)}</b><span>${esc(l.msg)}</span><span>${esc(l.agent || "")} · ${ago(l.ts)}ที่แล้ว</span></div>`).join("")
    : '<div class="empty">ยังไม่มีกิจกรรมจากบอท</div>';
}

async function toggleAuto(el) {
  const want = el.checked;
  if (want && !confirm("เปิด Auto Trading?\nทีม Agent จะซื้อ/ขายในบัญชีจำลองให้เอง (ไม่ใช่เงินจริง และไม่เชื่อมกับ MT5/XM)")) { el.checked = false; return; }
  try { await api("/api/bot", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({enabled: want})}); toast(want ? "เปิด Auto Trading แล้ว" : "ปิด Auto Trading แล้ว", "ok"); refresh(true); }
  catch (err) { el.checked = !want; toast(err.message, "err"); }
}

function spark(pts) {
  if (pts.length < 2) { $("spark").innerHTML = '<div class="empty">กำลังสะสมข้อมูลกราฟ…</div>'; return; }
  const v = pts.map(p => p.equity), mn = Math.min(...v), mx = Math.max(...v), r = (mx - mn) || 1, W = 300, H = 70;
  const path = v.map((y, i) => `${(i / (v.length - 1) * W).toFixed(1)},${(H - 4 - (y - mn) / r * (H - 8)).toFixed(1)}`).join(" ");
  const up = v[v.length - 1] >= v[0];
  $("spark").innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"><polyline fill="none" stroke="${up ? "#5be0b4" : "#ff8585"}" stroke-width="2" vector-effect="non-scaling-stroke" points="${path}"/></svg><div class="sparkrange"><span>${money(mn)}</span><span>${money(mx)}</span></div>`;
}

function renderAccount(a) {
  ACCOUNT = a;
  $("equity").textContent = money(a.equity);
  $("cash").textContent = money(a.cash);
  $("pnl").textContent = money(a.pnl);
  $("pnl").className = a.pnl >= 0 ? "buy" : "sell";
  $("halt").hidden = !a.halted;
  const top = a.trades.length ? a.trades[0].id : 0;
  if (SEEN === null || top < SEEN) SEEN = top;
  else if (top > SEEN) { fillCard(a.trades.find(t => t.id > SEEN)); SEEN = top; }
  const L = a.limits;
  $("limits").textContent = `ค่าธรรมเนียมจำลอง ${(L.fee_rate * 100).toFixed(1)}% · ไม้ละ ≤ ${L.max_trade_pct * 100}% · รวมต่อตัว ≤ ${L.max_position_pct * 100}% · Stop -${L.stop_loss_pct * 100}% ตั้งก่อนเข้า · Trailing ${L.trail_pct * 100}% เมื่อกำไร +${L.trail_activate_pct * 100}% · ไม่มีคำสั่งเงินจริง`;
  $("positions").innerHTML = a.positions.length ? a.positions.map(p => `
    <div class="posrow"><b>${esc(p.symbol)}</b><span>${p.qty.toFixed(6)} @ ${money(p.avg)}${p.stop ? ` · stop ${money(p.stop)}${p.trail ? " (trailing)" : ""}` : ""}</span>
    <strong class="${p.unrealized >= 0 ? "buy" : "sell"}">${money(p.unrealized)} (${pct(p.pct)})</strong></div>`).join("")
    : '<div class="empty">ยังไม่มีสถานะ</div>';
  $("trades").innerHTML = a.trades.length ? a.trades.map(t => `
    <div class="traderow"><b>${t.side} ${t.symbol}</b><span>${t.qty} @ ${money(t.price)}${t.side === "SELL" ? ` · P/L ${money(t.pnl)}` : ""}</span>
    <span>${new Date(t.ts).toLocaleString()} · ${t.strategy || ""}</span></div>`).join("")
    : '<div class="empty">ยังไม่มีรายการ</div>';
}

async function refresh(manual = false) {
  if (busy || (document.hidden && !manual)) return;
  busy = true;
  try {
    const calls = [api("/api/market"), api("/api/pipeline"), api("/api/account"), api("/api/bot"), api("/api/desk")];
    if (tick++ % 6 === 0 || manual) calls.push(api("/api/equity"));
    const [m, pl, a, b, dk, e] = await Promise.all(calls);
    renderMarket(m.items); renderPipeline(pl); renderAccount(a); renderBot(b); renderDesk(dk);
    if (e) spark(e.points);
  } catch (err) { toast(err.message, "err"); }
  finally { busy = false; }
}

async function order() {
  const symbol = $("symbol").value, side = $("side").value, qty = Number($("qty").value);
  const p = PRICES[symbol];
  if (p && p.status === "closed" && !confirm(`${symbol} ตลาดปิด/ราคาล่าช้า จะใช้ราคาล่าสุด ${money(p.price)} ต่อไหม?`)) return;
  const strategy = "manual";
  const btn = $("orderBtn");
  btn.disabled = true;
  try {
    const d = await api("/api/trade", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({symbol, side, qty, strategy})});
    toast(`บันทึกคำสั่งจำลองแล้ว @ ${money(d.price)} · fee ${money(d.fee)}`, "ok");
    await refresh(true);
  } catch (err) { toast(err.message, "err"); }
  finally { btn.disabled = false; }
}

async function resetAccount() {
  if (!confirm("ลบบัญชีจำลองและประวัติทั้งหมด?")) return;
  try { await api("/api/reset", {method: "POST"}); await refresh(true); toast("รีเซ็ตแล้ว", "ok"); }
  catch (err) { toast(err.message, "err"); }
}

refresh(true);
setInterval(refresh, 5000);  // prices come from a live cache, so 5s polling is cheap
document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(true); });
if ("serviceWorker" in navigator) navigator.serviceWorker.register("/sw.js").catch(() => {});
