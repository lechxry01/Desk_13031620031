const $ = id => document.getElementById(id);
const money = n => "$" + Number(n || 0).toLocaleString("en-US", {minimumFractionDigits: 2, maximumFractionDigits: 2});
const pct = n => (n >= 0 ? "+" : "") + (n * 100).toFixed(2) + "%";
let KEY = localStorage.getItem("appkey") || "";
let ACCOUNT = null, PRICES = {}, PICK = null, busy = false, asking = null;

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
  PRICES = Object.fromEntries(items.map(x => [x.symbol, x]));
  $("market").innerHTML = items.map(x => `
    <div class="marketrow"><b>${x.symbol}</b><strong>${x.price == null ? "—" : money(x.price)}</strong>
    <span class="muted">${x.source}</span>
    ${BADGE[x.status] ? `<small class="badge ${x.status}">${BADGE[x.status]}</small>` : ""}</div>`).join("");
  const sel = $("symbol");
  if (!sel.options.length) sel.innerHTML = items.map(x => `<option>${x.symbol}</option>`).join("");
}

function renderSignals(list) {
  $("signals").innerHTML = list.map((x, i) => `
    <div class="sigrow"><b>${x.symbol}</b>
    <strong class="${x.signal.toLowerCase()}">${x.signal} · ${x.confidence}%</strong><span>${x.strategy}</span>
    <div class="reason">
      <div class="agent">Scout: ${x.agents.scout} · Analyst: ${x.agents.analyst}</div>
      <div class="agent">Strategy: ${x.agents.strategy} · Risk: <span class="${x.risk_ok ? "buy" : "sell"}">${x.agents.risk}</span></div>
      <small>${x.reason}</small>
      ${x.signal !== "WAIT" && x.risk_ok ? `<div><button class="mini" onclick="useSignal(${i})">ใช้สัญญาณนี้</button></div>` : ""}
    </div></div>`).join("");
  window.SIGNALS = list;
}

function useSignal(i) {
  const s = window.SIGNALS[i], p = PRICES[s.symbol];
  if (!p || !p.price) return toast("ยังไม่มีราคา", "err");
  $("symbol").value = s.symbol;
  $("side").value = s.signal;
  let qty;
  if (s.signal === "SELL") {
    const pos = ACCOUNT && ACCOUNT.positions.find(x => x.symbol === s.symbol);
    qty = pos ? pos.qty : 0;
  } else {
    qty = 0.05 * (ACCOUNT ? ACCOUNT.equity : 10000) / p.price;  // 5% of equity
  }
  $("qty").value = Number(qty.toPrecision(3));
  PICK = {symbol: s.symbol, side: s.signal, strategy: s.strategy};
  toast(`เติมคำสั่งจากสัญญาณ ${s.signal} ${s.symbol} แล้ว กด "ส่งคำสั่งจำลอง" เพื่อยืนยัน`);
  $("symbol").scrollIntoView({behavior: "smooth", block: "center"});
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
  const L = a.limits;
  $("limits").textContent = `ค่าธรรมเนียมจำลอง ${(L.fee_rate * 100).toFixed(1)}% · สูงสุด ${L.max_position_pct * 100}%/สัญลักษณ์ · SL -${L.stop_loss_pct * 100}% · TP +${L.take_profit_pct * 100}% (ปิดสถานะอัตโนมัติ) · ไม่มีคำสั่งเงินจริง`;
  $("positions").innerHTML = a.positions.length ? a.positions.map(p => `
    <div class="posrow"><b>${p.symbol}</b><span>${p.qty.toFixed(6)} @ ${money(p.avg)}</span>
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
    const [m, s, a, e] = await Promise.all([api("/api/market"), api("/api/signals"), api("/api/account"), api("/api/equity")]);
    renderMarket(m.items); renderSignals(s.signals); renderAccount(a); spark(e.points);
  } catch (err) { toast(err.message, "err"); }
  finally { busy = false; }
}

async function order() {
  const symbol = $("symbol").value, side = $("side").value, qty = Number($("qty").value);
  const p = PRICES[symbol];
  if (p && p.status === "closed" && !confirm(`${symbol} ตลาดปิด/ราคาล่าช้า จะใช้ราคาล่าสุด ${money(p.price)} ต่อไหม?`)) return;
  const strategy = PICK && PICK.symbol === symbol && PICK.side === side ? "signal:" + PICK.strategy : "manual";
  const btn = $("orderBtn");
  btn.disabled = true;
  try {
    const d = await api("/api/trade", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({symbol, side, qty, strategy})});
    toast(`บันทึกคำสั่งจำลองแล้ว @ ${money(d.price)} · fee ${money(d.fee)}`, "ok");
    PICK = null;
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
setInterval(refresh, 15000);
document.addEventListener("visibilitychange", () => { if (!document.hidden) refresh(true); });
if ("serviceWorker" in navigator) navigator.serviceWorker.register("/sw.js").catch(() => {});
