"""AI Trading Desk PRO - Phase 3 (paper trading only)."""
import asyncio
import hmac
import json
import logging
import math
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yfinance as yf
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

log = logging.getLogger("trading-desk")
ROOT = Path(__file__).parent
DB = Path(os.getenv("DB_PATH", ROOT / "trading.db"))
DB.parent.mkdir(parents=True, exist_ok=True)

SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "AAPL", "MSFT", "NVDA", "SPY", "TSLA"]
CRYPTO = {"BTC-USD": "BTCUSDT", "ETH-USD": "ETHUSDT", "SOL-USD": "SOLUSDT"}
BINANCE_REV = {v: k for k, v in CRYPTO.items()}
BINANCE_HOSTS = ["wss://stream.binance.com:9443", "wss://stream.binance.us:9443"]

# ---- paper account & risk configuration ----
INITIAL_CASH = 10000.0
FEE_RATE = 0.001            # 0.1% simulated fee
MAX_POSITION_PCT = 0.25     # one symbol may not exceed 25% of equity
STOP_LOSS_PCT = 0.05        # auto-close at -5% vs average cost
TAKE_PROFIT_PCT = 0.10      # auto-close at +10% vs average cost
MAX_DRAWDOWN_PCT = 0.20     # no new BUY once equity is down 20%

# ---- data freshness ----
WS_FRESH_SEC = 45           # Binance tick considered fresh
CRYPTO_STALE_SEC = 300      # crypto price older than this -> stale, trades blocked
STOCK_CLOSED_SEC = 1800     # stock bar older than this -> market closed / delayed
TICK_SEC = 5                # worker heartbeat: SL/TP is checked this often on live prices
PRICE_LOOP_SEC = 30         # Yahoo refresh (stocks + crypto fallback)
SIGNAL_LOOP_SEC = 60        # re-analysis interval
SNAPSHOT_SEC = 300

# ---- auto trading (paper) ----
AUTO_TRADE_PCT = 0.05       # each auto entry uses 5% of equity
AUTO_MAX_POSITIONS = 4
AUTO_COOLDOWN_SEC = 1800    # no new auto trade on a symbol within 30 min of its last trade
MIN_EMA_GAP = 0.0005        # EMA9 vs EMA21 must differ by 0.05% to count as a trend

CACHE = {}       # symbol -> {price, source, bar_ts}
SIGNALS = {}     # symbol -> cached agent decision
LAST_ANALYSIS = {"ts": 0.0}
LOCK = threading.Lock()
TRADE_LOCK = threading.Lock()
STOP = threading.Event()


# --------------------------------------------------------------------------- db
@contextmanager
def db():
    c = sqlite3.connect(DB, timeout=10)
    c.row_factory = sqlite3.Row
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def init():
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS account(id INTEGER PRIMARY KEY CHECK(id=1), cash REAL, initial REAL)")
        c.execute("INSERT OR IGNORE INTO account VALUES(1,?,?)", (INITIAL_CASH, INITIAL_CASH))
        c.execute("""CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT,
                     side TEXT, qty REAL, price REAL, fee REAL, pnl REAL DEFAULT 0, strategy TEXT)""")
        c.execute("CREATE TABLE IF NOT EXISTS positions(symbol TEXT PRIMARY KEY, qty REAL, avg REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS equity(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, equity REAL)")
        c.execute("CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS bot_log(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, action TEXT, msg TEXT)")


init()


def get_setting(k, default=""):
    with db() as c:
        r = c.execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return r["v"] if r else default


def set_setting(k, v):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, str(v)))


def bot_log(symbol, action, msg):
    with db() as c:
        c.execute("INSERT INTO bot_log(ts,symbol,action,msg) VALUES(?,?,?,?)",
                  (datetime.now(timezone.utc).isoformat(), symbol, action, msg))
        c.execute("DELETE FROM bot_log WHERE id NOT IN (SELECT id FROM bot_log ORDER BY id DESC LIMIT 200)")


# ----------------------------------------------------------------------- prices
def set_quote(sym, price, source, bar_ts):
    """Store a quote unless we already hold a newer one."""
    with LOCK:
        old = CACHE.get(sym)
        if old and old["bar_ts"] > bar_ts:
            return
        CACHE[sym] = {"price": float(price), "source": source, "bar_ts": float(bar_ts)}


def quote(sym):
    with LOCK:
        q = CACHE.get(sym)
    if not q:
        return None
    age = time.time() - q["bar_ts"]
    is_crypto = sym in CRYPTO
    if age <= (CRYPTO_STALE_SEC if is_crypto else STOCK_CLOSED_SEC):
        status = "live"
    else:
        status = "stale" if is_crypto else "closed"
    return {"price": q["price"], "source": q["source"], "age": int(age), "status": status,
            "updated": datetime.fromtimestamp(q["bar_ts"], timezone.utc).isoformat()}


def yahoo_price(symbol):
    try:
        h = yf.Ticker(symbol).history(period="5d", interval="1m")
        if h.empty:
            return None
        return {"price": float(h["Close"].iloc[-1]), "bar_ts": h.index[-1].timestamp()}
    except Exception as e:
        log.warning("yahoo %s failed: %s", symbol, e)
        return None


def refresh_prices():
    for s in SYMBOLS:
        q = quote(s)
        if s in CRYPTO and q and q["source"].startswith("Binance") and q["age"] < WS_FRESH_SEC:
            continue  # WebSocket is healthy, no need for the fallback
        y = yahoo_price(s)
        if y:
            label = "Yahoo Finance · 1m" + (" (fallback)" if s in CRYPTO else "")
            set_quote(s, y["price"], label, y["bar_ts"])


async def ws_runner():
    """Binance public stream; alternates between .com and .us if one is blocked."""
    import websockets
    streams = "/".join(v.lower() + "@ticker" for v in CRYPTO.values())
    i = 0
    while True:
        host = BINANCE_HOSTS[i % len(BINANCE_HOSTS)]
        label = "Binance WebSocket" + (".US" if host.endswith("binance.us:9443") else "")
        try:
            async with websockets.connect(f"{host}/stream?streams={streams}", ping_interval=20) as ws:
                async for msg in ws:
                    d = json.loads(msg).get("data", {})
                    sym = BINANCE_REV.get(d.get("s"))
                    if sym and d.get("c"):
                        set_quote(sym, d["c"], label, time.time())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ws %s failed: %s", host, e)
        i += 1
        await asyncio.sleep(5)


def market_snapshot():
    out = []
    for s in SYMBOLS:
        q = quote(s)
        if q:
            out.append({"symbol": s, **q})
        else:
            out.append({"symbol": s, "price": None, "source": "ยังไม่มีข้อมูลราคา", "updated": None, "age": None, "status": "none"})
    return out


# ---------------------------------------------------------------------- signals
def indicators(symbol):
    """5-minute bars, blended with the latest live price so the analysis reacts in near real time."""
    try:
        hist = yf.Ticker(symbol).history(period="2d", interval="5m")
        if len(hist) < 30:
            return None
        close = hist["Close"]
        delta = close.diff()
        gain = float(delta.clip(lower=0).rolling(14).mean().iloc[-1])
        loss = float((-delta.clip(upper=0)).rolling(14).mean().iloc[-1])
        rsi = 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
        ema9 = float(close.ewm(span=9).mean().iloc[-1])
        ema21 = float(close.ewm(span=21).mean().iloc[-1])
        last = float(close.iloc[-1])
        q = quote(symbol)
        if q and q["status"] == "live":  # fold the live tick into the EMAs
            last = q["price"]
            ema9 = last * (2 / 10) + ema9 * (1 - 2 / 10)
            ema21 = last * (2 / 22) + ema21 * (1 - 2 / 22)
        return {"last": last, "ema9": ema9, "ema21": ema21, "rsi": rsi}
    except Exception as e:
        log.warning("indicators %s failed: %s", symbol, e)
        return None


def rule_signal(ind):
    """Trend filter: BUY on a clear uptrend that is not overbought; SELL (exit) on a downtrend or RSI>75."""
    gap = (ind["ema9"] - ind["ema21"]) / ind["ema21"] if ind["ema21"] else 0.0
    rsi = ind["rsi"]
    trend = "ขาขึ้น" if gap > MIN_EMA_GAP else "ขาลง" if gap < -MIN_EMA_GAP else "ไม่ชัด"
    if gap > MIN_EMA_GAP and ind["last"] > ind["ema21"] and 45 <= rsi <= 68:
        sig, why = "BUY", "แนวโน้มขาขึ้น และ RSI ยังไม่ Overbought"
    elif gap < -MIN_EMA_GAP or rsi > 75:
        sig, why = "SELL", "แนวโน้มขาลง" if gap < -MIN_EMA_GAP else "RSI สูงกว่า 75 (Overbought)"
    else:
        sig, why = "WAIT", "ยังไม่เข้าเงื่อนไข (แนวโน้ม/RSI)"
    conf = 50 if sig == "WAIT" else 55 + min(30, int(abs(gap) * 10000))
    return sig, conf, f"{why} · EMA gap {gap * 100:+.2f}% · RSI {rsi:.0f}", trend


def agent_decision(symbol):
    ind = indicators(symbol)
    now = time.time()
    if not ind:
        return {"symbol": symbol, "signal": "WAIT", "confidence": 0, "strategy": "none", "computed_at": now,
                "reason": "ข้อมูลย้อนหลังไม่เพียงพอ",
                "agents": {"scout": "รอข้อมูล", "analyst": "ไม่พบข้อมูลพอ", "strategy": "ยังไม่เลือก", "risk": ""}}
    sig, conf, reason, trend = rule_signal(ind)
    return {"symbol": symbol, "signal": sig, "confidence": conf, "strategy": "trend-follow 5m", "computed_at": now,
            "reason": reason,
            "indicators": {"rsi": round(ind["rsi"], 1), "ema9": round(ind["ema9"], 4), "ema21": round(ind["ema21"], 4)},
            "agents": {"scout": "ดึงแท่ง 5 นาที + ราคา live", "analyst": f"RSI {ind['rsi']:.0f} · แนวโน้ม{trend}",
                       "strategy": "Trend-follow (EMA9/21 + RSI)", "risk": ""}}


def refresh_signals():
    for s in SYMBOLS:
        SIGNALS[s] = agent_decision(s)
    LAST_ANALYSIS["ts"] = time.time()


# -------------------------------------------------------------------- portfolio
def portfolio(c):
    a = c.execute("SELECT * FROM account WHERE id=1").fetchone()
    equity = float(a["cash"])
    positions = []
    for p in c.execute("SELECT * FROM positions").fetchall():
        q = quote(p["symbol"])
        px = q["price"] if q else p["avg"]
        val = p["qty"] * px
        equity += val
        positions.append({"symbol": p["symbol"], "qty": p["qty"], "avg": p["avg"], "price": px, "value": val,
                          "unrealized": (px - p["avg"]) * p["qty"], "pct": (px / p["avg"] - 1) if p["avg"] else 0,
                          "status": q["status"] if q else "none"})
    return {"cash": float(a["cash"]), "initial": float(a["initial"]), "equity": equity, "positions": positions}


def drawdown_hit(pf):
    return pf["equity"] < pf["initial"] * (1 - MAX_DRAWDOWN_PCT)


def risk_check_buy(c, symbol, qty, px):
    pf = portfolio(c)
    if drawdown_hit(pf):
        raise HTTPException(400, f"Risk: หยุดเปิดสถานะใหม่ เพราะขาดทุนสะสมเกิน {MAX_DRAWDOWN_PCT:.0%}")
    held = next((p["qty"] for p in pf["positions"] if p["symbol"] == symbol), 0)
    limit = pf["equity"] * MAX_POSITION_PCT
    if (held + qty) * px > limit:
        room = max(0.0, (limit - held * px) / px)
        raise HTTPException(400, f"Risk: เกินกรอบ {MAX_POSITION_PCT:.0%} ของพอร์ตต่อสัญลักษณ์ (ซื้อเพิ่มได้สูงสุด ≈ {room:.6g})")


def risk_text(sig, symbol, pf):
    if sig == "WAIT":
        return "ไม่เปิดสถานะใหม่", True
    held = next((p for p in pf["positions"] if p["symbol"] == symbol), None)
    if sig == "SELL":
        return ("ผ่าน · ปิดสถานะที่ถืออยู่" if held else "ไม่มีสถานะให้ขาย (ระบบไม่เปิด short)"), bool(held)
    if drawdown_hit(pf):
        return f"ไม่อนุมัติ · ขาดทุนสะสมเกิน {MAX_DRAWDOWN_PCT:.0%}", False
    if held and held["value"] >= pf["equity"] * MAX_POSITION_PCT:
        return f"ไม่อนุมัติ · ถือครบกรอบ {MAX_POSITION_PCT:.0%} แล้ว", False
    return f"อนุมัติ · กรอบ {MAX_POSITION_PCT:.0%}/สัญลักษณ์ · SL -{STOP_LOSS_PCT:.0%} / TP +{TAKE_PROFIT_PCT:.0%}", True


def execute_trade(symbol, side, qty, strategy, px, enforce_risk=True):
    fee = px * qty * FEE_RATE
    with TRADE_LOCK, db() as c:
        a = c.execute("SELECT * FROM account WHERE id=1").fetchone()
        p = c.execute("SELECT * FROM positions WHERE symbol=?", (symbol,)).fetchone()
        pnl = 0.0
        if side == "BUY":
            cost = px * qty + fee
            if cost > a["cash"]:
                raise HTTPException(400, "เงินจำลองไม่พอ")
            if enforce_risk:
                risk_check_buy(c, symbol, qty, px)
            oldq = p["qty"] if p else 0.0
            oldavg = p["avg"] if p else 0.0
            avg = (oldq * oldavg + cost) / (oldq + qty)  # average cost includes the fee
            c.execute("UPDATE account SET cash=cash-? WHERE id=1", (cost,))
            c.execute("INSERT OR REPLACE INTO positions VALUES(?,?,?)", (symbol, oldq + qty, avg))
        else:
            if not p or p["qty"] + 1e-12 < qty:
                raise HTTPException(400, "จำนวนที่ขายมากกว่าที่ถือ")
            pnl = (px - p["avg"]) * qty - fee
            c.execute("UPDATE account SET cash=cash+? WHERE id=1", (px * qty - fee,))
            left = p["qty"] - qty
            if left < 1e-9:
                c.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            else:
                c.execute("UPDATE positions SET qty=? WHERE symbol=?", (left, symbol))
        c.execute("INSERT INTO trades(ts,symbol,side,qty,price,fee,pnl,strategy) VALUES(?,?,?,?,?,?,?,?)",
                  (datetime.now(timezone.utc).isoformat(), symbol, side, qty, px, fee, pnl, strategy))
    return {"ok": True, "price": px, "fee": fee, "pnl": pnl}


def check_exits():
    """Auto stop-loss / take-profit, only on fresh prices."""
    with db() as c:
        pf = portfolio(c)
    for p in pf["positions"]:
        if p["status"] != "live" or not p["avg"]:
            continue
        reason = "stop-loss" if p["pct"] <= -STOP_LOSS_PCT else "take-profit" if p["pct"] >= TAKE_PROFIT_PCT else None
        if reason:
            try:
                execute_trade(p["symbol"], "SELL", p["qty"], reason, p["price"], enforce_risk=False)
                bot_log(p["symbol"], reason, f"ปิดสถานะ {p['symbol']} @ {p['price']:.4f} ({p['pct'] * 100:+.2f}%)")
            except HTTPException as e:
                log.warning("exit failed %s: %s", p["symbol"], e.detail)


def snapshot_equity():
    with db() as c:
        pf = portfolio(c)
        c.execute("INSERT INTO equity(ts,equity) VALUES(?,?)", (datetime.now(timezone.utc).isoformat(), pf["equity"]))
        c.execute("DELETE FROM equity WHERE id NOT IN (SELECT id FROM equity ORDER BY id DESC LIMIT 2000)")


def auto_trade():
    """Paper-only: open/close positions from the latest fresh signals. Does nothing unless the switch is on."""
    if get_setting("auto_enabled", "0") != "1":
        return
    with db() as c:
        pf = portfolio(c)
        last = {r["symbol"]: r["ts"] for r in c.execute("SELECT symbol, MAX(ts) ts FROM trades GROUP BY symbol")}
    held = {p["symbol"]: p for p in pf["positions"]}
    cash = pf["cash"]
    for sym in SYMBOLS:
        d = SIGNALS.get(sym)
        if not d or d["signal"] == "WAIT" or time.time() - d.get("computed_at", 0) > 3 * SIGNAL_LOOP_SEC:
            continue
        q = quote(sym)
        if not q or q["status"] != "live":
            continue
        lt = last.get(sym)
        if lt and time.time() - datetime.fromisoformat(lt).timestamp() < AUTO_COOLDOWN_SEC:
            continue
        try:
            if d["signal"] == "BUY" and sym not in held:
                if len(held) >= AUTO_MAX_POSITIONS:
                    continue
                spend = min(pf["equity"] * AUTO_TRADE_PCT, cash * 0.98)
                if spend < 10:
                    continue
                qty = spend / q["price"]
                execute_trade(sym, "BUY", qty, "auto:" + d["strategy"], q["price"])
                held[sym] = {"qty": qty}
                cash -= spend * (1 + FEE_RATE)
                bot_log(sym, "BUY", f"ซื้อ {qty:.6g} @ {q['price']:.4f} · {d['reason']}")
            elif d["signal"] == "SELL" and sym in held:
                execute_trade(sym, "SELL", held[sym]["qty"], "auto:exit", q["price"], enforce_risk=False)
                del held[sym]
                bot_log(sym, "SELL", f"ปิดสถานะ @ {q['price']:.4f} · {d['reason']}")
        except HTTPException as e:
            bot_log(sym, "skip", f"ไม่ส่งคำสั่ง: {e.detail}")


def worker():
    t_price = t_sig = t_snap = 0.0
    while not STOP.is_set():
        try:
            if time.time() - t_price >= PRICE_LOOP_SEC:
                refresh_prices(); t_price = time.time()
            check_exits()
            if time.time() - t_snap >= SNAPSHOT_SEC:
                snapshot_equity(); t_snap = time.time()
            if time.time() - t_sig >= SIGNAL_LOOP_SEC:
                refresh_signals(); t_sig = time.time()
                auto_trade()
        except Exception:
            log.exception("worker error")
        STOP.wait(TICK_SEC)


# -------------------------------------------------------------------------- api
@asynccontextmanager
async def lifespan(_app):
    STOP.clear()
    threading.Thread(target=worker, daemon=True).start()
    task = asyncio.create_task(ws_runner())
    yield
    STOP.set()
    task.cancel()


app = FastAPI(title="AI Trading Desk PRO Phase 3", lifespan=lifespan)
# Works with either layout: files inside ./static/ or all files flat next to app.py
# (flat is easier to upload from a phone). Only this allowlist is ever served.
STATIC_DIR = ROOT / "static" if (ROOT / "static").is_dir() else ROOT
STATIC_FILES = {"index.html", "app.js", "style.css", "sw.js", "manifest.json", "icon-192.png", "icon-512.png",
                "icon-maskable-512.png", "apple-touch-icon.png"}


@app.get("/static/{name}")
def static_file(name: str):
    if name not in STATIC_FILES or not (STATIC_DIR / name).is_file():
        raise HTTPException(404, "not found")
    return FileResponse(STATIC_DIR / name)


def auth(x_key: Optional[str] = Header(default=None)):
    key = os.getenv("APP_KEY", "")
    if key and not hmac.compare_digest((x_key or "").encode(), key.encode()):
        raise HTTPException(401, "ต้องใส่รหัสเข้าใช้งาน")


api = APIRouter(prefix="/api", dependencies=[Depends(auth)])


class Order(BaseModel):
    symbol: str
    side: str
    qty: float = Field(gt=0)
    strategy: str = "manual"


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/sw.js")
def sw():  # served from root so the service worker can control "/"
    return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript")


@app.get("/healthz")
def healthz():
    return {"ok": True}


@api.get("/market")
def market():
    return {"items": market_snapshot(), "asof": datetime.now(timezone.utc).isoformat()}


@api.get("/signals")
def signals():
    with db() as c:
        pf = portfolio(c)
    out = []
    for s in SYMBOLS:
        d = dict(SIGNALS.get(s) or {"symbol": s, "signal": "WAIT", "confidence": 0, "strategy": "none",
                                    "reason": "กำลังโหลดข้อมูล…",
                                    "agents": {"scout": "กำลังโหลด", "analyst": "-", "strategy": "-", "risk": ""}})
        d["agents"] = dict(d["agents"])
        d["age"] = int(time.time() - d["computed_at"]) if d.get("computed_at") else None
        d["agents"]["risk"], d["risk_ok"] = risk_text(d["signal"], s, pf)
        out.append(d)
    return {"signals": out}


@api.get("/account")
def account():
    with db() as c:
        pf = portfolio(c)
        ts = c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 30").fetchall()
    return {**pf, "pnl": pf["equity"] - pf["initial"], "trades": [dict(t) for t in ts],
            "limits": {"max_position_pct": MAX_POSITION_PCT, "stop_loss_pct": STOP_LOSS_PCT,
                       "take_profit_pct": TAKE_PROFIT_PCT, "max_drawdown_pct": MAX_DRAWDOWN_PCT, "fee_rate": FEE_RATE},
            "halted": drawdown_hit(pf)}


@api.get("/equity")
def equity_curve():
    with db() as c:
        rows = c.execute("SELECT ts,equity FROM equity ORDER BY id DESC LIMIT 300").fetchall()
    return {"points": [dict(r) for r in reversed(rows)]}


class BotSwitch(BaseModel):
    enabled: bool


@api.get("/bot")
def bot_status():
    with db() as c:
        rows = c.execute("SELECT ts,symbol,action,msg FROM bot_log ORDER BY id DESC LIMIT 30").fetchall()
    la = LAST_ANALYSIS["ts"]
    return {"enabled": get_setting("auto_enabled", "0") == "1",
            "analysis_age": int(time.time() - la) if la else None,
            "params": {"trade_pct": AUTO_TRADE_PCT, "max_positions": AUTO_MAX_POSITIONS,
                       "cooldown_min": AUTO_COOLDOWN_SEC // 60, "interval_sec": SIGNAL_LOOP_SEC},
            "log": [dict(r) for r in rows]}


@api.post("/bot")
def bot_switch(b: BotSwitch):
    set_setting("auto_enabled", "1" if b.enabled else "0")
    bot_log("-", "system", "เปิด Auto Trading (บัญชีจำลอง)" if b.enabled else "ปิด Auto Trading")
    return {"enabled": b.enabled}


@api.post("/trade")
def trade(o: Order):
    side = o.side.upper()
    if o.symbol not in SYMBOLS or side not in ("BUY", "SELL") or not math.isfinite(o.qty):
        raise HTTPException(400, "คำสั่งไม่ถูกต้อง")
    q = quote(o.symbol)
    if not q:
        raise HTTPException(503, "ไม่มีราคาล่าสุด จึงไม่ส่งคำสั่งจำลอง")
    if q["status"] == "stale":
        raise HTTPException(503, f"ราคา {o.symbol} ไม่สด (อายุ {q['age']} วินาที) จึงไม่ส่งคำสั่งจำลอง")
    return execute_trade(o.symbol, side, o.qty, o.strategy[:40], q["price"])


@api.post("/reset")
def reset():
    with TRADE_LOCK, db() as c:
        c.execute("UPDATE account SET cash=?,initial=? WHERE id=1", (INITIAL_CASH, INITIAL_CASH))
        c.execute("DELETE FROM positions")
        c.execute("DELETE FROM trades")
        c.execute("DELETE FROM equity")
        c.execute("DELETE FROM bot_log")
    return {"ok": True}


app.include_router(api)
