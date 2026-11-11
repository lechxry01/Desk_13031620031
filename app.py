"""AI Trading Desk PRO - Phase 4 (paper trading only).

Six agents with hard hand-offs:
  Scout    -> finds candidates (BTC/ETH/SOL, top alts by volume, US stocks incl. pre/post-market)
  Filter   -> six hard rules; anything that fails is killed
  Analyst  -> scores survivors 0..1; below ANALYST_MIN is killed
  Executor -> sends the paper order (max MAX_TRADE_PCT of equity per trade)
  Risk     -> fixes the stop BEFORE entry, trails winners after +15%, caps exposure
  Logger   -> records every kill and every action
"""
import asyncio
import hmac
import json
import logging
import math
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Optional

import pandas as pd
import yfinance as yf
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

log = logging.getLogger("trading-desk")
ROOT = Path(__file__).parent
DB = Path(os.getenv("DB_PATH", ROOT / "trading.db"))
DB.parent.mkdir(parents=True, exist_ok=True)

# ---- universe ----
CORE = ["BTC-USD", "ETH-USD", "SOL-USD"]
STOCKS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "NFLX", "AVGO", "SPY", "QQQ", "COIN", "PLTR", "JPM"]
ETFS = {"SPY", "QQQ"}
STATIC = CORE + STOCKS
ALTS = []        # filled by Scout: top USDT pairs by 24h volume
ALT_COUNT = 12
MIN_ALT_VOL_USD = 2_000_000
STABLES = {"USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDP", "BUSD", "EUR", "GBP", "AEUR", "PAXG", "XAUT"}
CRYPTO = {"BTC-USD": "BTCUSDT", "ETH-USD": "ETHUSDT", "SOL-USD": "SOLUSDT"}
BINANCE_REV = {v: k for k, v in CRYPTO.items()}
BINANCE_HOSTS = ["wss://stream.binance.com:9443", "wss://stream.binance.us:9443"]
BINANCE_REST = ["https://api.binance.us", "https://api.binance.com"]   # .US first: .com is often geo-blocked
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TradingDesk/4.0)"}

# ---- paper account & risk configuration ----
INITIAL_CASH = float(os.getenv("START_CASH", "10000"))
FEE_RATE = 0.001
MAX_TRADE_PCT = 0.10        # Executor: max 10% of equity per trade
MAX_POSITION_PCT = 0.25     # Risk: one symbol may not exceed 25% of equity
STOP_LOSS_PCT = 0.08        # Risk: stop set at -8% BEFORE entry
TRAIL_ACTIVATE_PCT = 0.15   # Risk: trailing starts once a position is +15%
TRAIL_PCT = 0.08            # trailing stop sits 8% below the peak
MAX_DRAWDOWN_PCT = 0.20     # no new BUY once equity is down 20%
AUTO_MAX_POSITIONS = 5
AUTO_COOLDOWN_SEC = 1800
MIN_ORDER_USD = 1.0
TARGET_PCT = 0.15           # Charts bot: indicative target shown in chat (+15%, same level trailing starts)

# ---- filter / analyst thresholds ----
MAX_SPREAD_PCT = 0.001      # 0.10%
MIN_DEPTH_USD = 20_000      # liquidity within +-0.5% of mid
VOL_RATIO_MIN = 1.0         # volume must be at least the 30-day average
EARNINGS_WINDOW_H = 48      # earnings within 48h (date-only data) => kill
NEWS_LOOKBACK_H = 12
NEWS_KEYWORDS = ["halt", "suspend", "investigat", "lawsuit", "sues", "fraud", "subpoena", "sec charge", "probe",
                 "hack", "exploit", "breach", "delist", "bankrupt", "recall", "outage", "default", "plunge", "crash"]
MIN_EMA_GAP = 0.0005
ANALYST_MIN = 0.6

# ---- data freshness / loops ----
WS_FRESH_SEC = 45
CRYPTO_STALE_SEC = 300
STOCK_CLOSED_SEC = 1800
TICK_SEC = 5
PRICE_LOOP_SEC = 30
PIPELINE_SEC = 120
SNAPSHOT_SEC = 300

CACHE = {}        # symbol -> {price, source, bar_ts}
CACHE2 = {}       # generic TTL cache
BOOK = {}         # binance pair -> (bid, ask)
LAST_TRY = {}
LAST_KILL = {}
PRICE_ERR = {}
SERIES_ERR = {}
SCOUT_ERR = {}
PIPE = {"ts": 0.0, "cycle": 0, "auto": False, "counts": {"scout": 0, "filter": 0, "analyst": 0, "executor": 0}, "rows": []}
LOCK = threading.Lock()
TRADE_LOCK = threading.Lock()
STOP = threading.Event()

# ---- Trading Floor: six bots talking in one group chat (all paper trading) ----
BOTS = ["Chief", "Scout", "News", "Sentiment", "Charts", "Skeptic"]
DESK = {"started": time.time(), "seq": 0, "msgs": deque(maxlen=150), "last": {}, "sum": None}
DESK_LOCK = threading.Lock()
DESK_REPEAT_SEC = 900       # same symbol is discussed again after 15 min (or when the verdict flips)
SKEPTIC_VETO_AT = 3         # Skeptic blocks an entry when it finds this many objections


def say(bot, text, symbol="", tone=""):
    with DESK_LOCK:
        DESK["seq"] += 1
        DESK["msgs"].append({"id": DESK["seq"], "ts": datetime.now(timezone.utc).isoformat(), "bot": bot,
                             "symbol": symbol, "text": text, "tone": tone})


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
        for table, col, ddl in [("positions", "stop", "REAL"), ("positions", "peak", "REAL"),
                                ("positions", "trail", "INTEGER DEFAULT 0"), ("bot_log", "agent", "TEXT")]:
            if col not in [r["name"] for r in c.execute(f"PRAGMA table_info({table})")]:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")


init()


def get_setting(k, default=""):
    with db() as c:
        r = c.execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return r["v"] if r else default


def set_setting(k, v):
    with db() as c:
        c.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, str(v)))


def bot_log(agent, symbol, action, msg):
    """Logger agent: one line per kill / action."""
    with db() as c:
        c.execute("INSERT INTO bot_log(ts,agent,symbol,action,msg) VALUES(?,?,?,?,?)",
                  (datetime.now(timezone.utc).isoformat(), agent, symbol, action, msg))
        c.execute("DELETE FROM bot_log WHERE id NOT IN (SELECT id FROM bot_log ORDER BY id DESC LIMIT 500)")


def held_symbols():
    with db() as c:
        return [r["symbol"] for r in c.execute("SELECT symbol FROM positions")]


def price_symbols():
    out = list(STATIC)
    for s in held_symbols():
        if s not in out:
            out.append(s)
        if s.endswith("-USD") and s not in CRYPTO:
            CRYPTO[s] = s[:-4] + "USDT"
    return out


def tracked_symbols():
    return list(dict.fromkeys(price_symbols() + ALTS))


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


def short_err(e):
    if isinstance(e, urllib.error.HTTPError):
        return f"HTTP {e.code}"
    return (str(e) or type(e).__name__)[:60]


def http_json(url, timeout=8):
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def binance_get(path, timeout=8):
    last = None
    for host in BINANCE_REST:
        try:
            return http_json(host + path, timeout)
        except Exception as e:
            last = e
    raise last


def binance_series(sym, limit=120):
    rows = binance_get(f"/api/v3/klines?symbol={CRYPTO[sym]}&interval=5m&limit={limit}")
    return [(r[0] / 1000, float(r[4])) for r in rows]


def binance_last(sym):
    return float(binance_get(f"/api/v3/ticker/price?symbol={CRYPTO[sym]}")["price"]), time.time()


def yahoo_chart(symbol, interval, rng):
    """Yahoo chart API called directly (no crumb needed), incl. pre/post-market; returns [(ts, close)]."""
    d = http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={interval}&range={rng}&includePrePost=true")
    res = d["chart"]["result"][0]
    closes = res["indicators"]["quote"][0]["close"]
    return [(t, c) for t, c in zip(res.get("timestamp") or [], closes) if c is not None]


def yahoo_daily_volumes(symbol):
    d = http_json(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=2mo")
    return [v for v in d["chart"]["result"][0]["indicators"]["quote"][0]["volume"] if v is not None]


def yf_series(symbol, interval, period):
    h = yf.Ticker(symbol).history(period=period, interval=interval, prepost=True)
    return [(i.timestamp(), float(c)) for i, c in zip(h.index, h["Close"]) if c == c]


def try_sources(sources, min_points, err_store, symbol):
    errs = []
    for name, fn in sources:
        try:
            pts = fn()
            if len(pts) >= min_points:
                err_store.pop(symbol, None)
                return name, pts
            errs.append(f"{name}: ข้อมูลน้อย ({len(pts)})")
        except Exception as e:
            errs.append(f"{name}: {short_err(e)}")
    err_store[symbol] = " | ".join(errs)
    return None, None


def series_5m(symbol):
    src = []
    if symbol in CRYPTO:
        src.append(("Binance", lambda: binance_series(symbol)))
    src += [("Yahoo", lambda: yahoo_chart(symbol, "5m", "2d")), ("yfinance", lambda: yf_series(symbol, "5m", "2d"))]
    return try_sources(src, 30, SERIES_ERR, symbol)[1]


def latest_price(symbol):
    src = []
    if symbol in CRYPTO:
        src.append(("Binance REST", lambda: [(time.time(), binance_last(symbol)[0])]))
    src += [("Yahoo Finance · 1m", lambda: yahoo_chart(symbol, "1m", "1d")),
            ("Yahoo Finance · 5m", lambda: yahoo_chart(symbol, "5m", "5d")),
            ("yfinance · 1m", lambda: yf_series(symbol, "1m", "5d"))]
    name, pts = try_sources(src, 1, PRICE_ERR, symbol)
    return (name, pts[-1][0], pts[-1][1]) if pts else None


def cached(key, ttl, fn):
    """TTL cache that also remembers failures (so a blocked source is not hammered)."""
    now = time.time()
    e = CACHE2.get(key)
    if e and now - e[0] < ttl:
        if isinstance(e[1], Exception):
            raise e[1]
        return e[1]
    try:
        v = fn()
    except Exception as ex:
        CACHE2[key] = (now, ex)
        raise
    CACHE2[key] = (now, v)
    return v


def _safe(fn, *a):
    try:
        return fn(*a)
    except Exception:
        log.exception("%s failed", getattr(fn, "__name__", "task"))


def _refresh_one_price(s):
    q = quote(s)
    if s in CRYPTO and q and q["source"].startswith("Binance WebSocket") and q["age"] < WS_FRESH_SEC:
        return  # WebSocket is healthy
    live = bool(q and q["status"] == "live")
    wait = (PRICE_LOOP_SEC if s in CRYPTO else 60) if live else (60 if s in CRYPTO else 300)
    if time.time() - LAST_TRY.get(s, 0) < wait - 1:
        return
    LAST_TRY[s] = time.time()
    r = latest_price(s)
    if r:
        name, ts, price = r
        set_quote(s, price, name + (" (fallback)" if s in CRYPTO else ""), ts)


def refresh_prices():
    with ThreadPoolExecutor(max_workers=4) as ex:
        list(ex.map(lambda s: _safe(_refresh_one_price, s), price_symbols()))


async def ws_runner():
    """Binance public stream for BTC/ETH/SOL; alternates between .com and .us if one is blocked."""
    import websockets
    streams = "/".join(v.lower() + "@ticker" for v in BINANCE_REV)
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
    for s in price_symbols():
        q = quote(s)
        if q:
            out.append({"symbol": s, **q})
        else:
            why = PRICE_ERR.get(s)
            out.append({"symbol": s, "price": None, "updated": None, "age": None, "status": "none",
                        "source": f"ดึงราคาไม่ได้ · {why}" if why else "กำลังดึงราคา…"})
    return out


# -------------------------------------------------------------------- indicators
def indicators(symbol):
    """5-minute bars blended with the latest live price (near real-time)."""
    try:
        pts = series_5m(symbol)
        if not pts:
            return None
        close = pd.Series([c for _, c in pts], dtype="float64")
        delta = close.diff()
        gain = float(delta.clip(lower=0).rolling(14).mean().iloc[-1])
        loss = float((-delta.clip(upper=0)).rolling(14).mean().iloc[-1])
        rsi = 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
        ema9 = float(close.ewm(span=9).mean().iloc[-1])
        ema21 = float(close.ewm(span=21).mean().iloc[-1])
        last = float(close.iloc[-1])
        q = quote(symbol)
        if q and q["status"] == "live":
            last = q["price"]
            ema9 = last * (2 / 10) + ema9 * (1 - 2 / 10)
            ema21 = last * (2 / 22) + ema21 * (1 - 2 / 22)
        return {"last": last, "ema9": ema9, "ema21": ema21, "rsi": rsi}
    except Exception as e:
        SERIES_ERR[symbol] = short_err(e)
        return None


def get_ind(symbol):
    return cached(("ind", symbol), 60 if symbol in CRYPTO else 240, lambda: indicators(symbol))


def rule_signal(ind):
    """Used for exits: SELL on a clear downtrend or RSI > 75."""
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


# ---------------------------------------------------------------------- agents
def scout():
    """Agent 1 - Scout: crypto (core + top alts by 24h volume) and US stocks; held positions always included."""
    stats = {}
    try:
        for t in binance_get("/api/v3/ticker/24hr", 15):
            if t["symbol"].endswith("USDT"):
                stats[t["symbol"]] = t
        for b in binance_get("/api/v3/ticker/bookTicker", 15):
            if b["symbol"].endswith("USDT") and float(b["bidPrice"]) > 0 and float(b["askPrice"]) > 0:
                BOOK[b["symbol"]] = (float(b["bidPrice"]), float(b["askPrice"]))
        SCOUT_ERR.pop("crypto", None)
        alts = []
        core_pairs = {CRYPTO[s] for s in CORE}
        for pair, t in sorted(stats.items(), key=lambda kv: -float(kv[1]["quoteVolume"])):
            base = pair[:-4]
            if base in STABLES or base.endswith(("UP", "DOWN")) or pair in core_pairs:
                continue
            if float(t["quoteVolume"]) < MIN_ALT_VOL_USD:
                break
            alts.append(base)
            if len(alts) >= ALT_COUNT:
                break
        ALTS[:] = [f"{b}-USD" for b in alts]
        for b in alts:
            CRYPTO[f"{b}-USD"] = f"{b}USDT"
    except Exception as e:
        SCOUT_ERR["crypto"] = short_err(e)
    cands = []
    for s in CORE + ALTS:
        t = stats.get(CRYPTO.get(s, ""))
        if t:
            price = float(t["lastPrice"])
            if s in ALTS:
                set_quote(s, price, "Binance REST", time.time())
            cands.append({"symbol": s, "kind": "crypto", "price": price, "chg24": float(t["priceChangePercent"]),
                          "vol24": float(t["quoteVolume"])})
        else:
            q = quote(s)
            cands.append({"symbol": s, "kind": "crypto", "price": q["price"] if q else None, "chg24": None, "vol24": None})
    for s in STOCKS:
        q = quote(s)
        cands.append({"symbol": s, "kind": "stock", "price": q["price"] if q else None, "chg24": None, "vol24": None})
    have = {c["symbol"] for c in cands}
    for s in held_symbols():
        if s not in have:
            q = quote(s)
            cands.append({"symbol": s, "kind": "crypto" if s in CRYPTO else "stock", "price": q["price"] if q else None,
                          "chg24": None, "vol24": None})
    return cands


def depth_usd(pair):
    j = binance_get(f"/api/v3/depth?symbol={pair}&limit=100")
    bids = [(float(p), float(q)) for p, q in j["bids"]]
    asks = [(float(p), float(q)) for p, q in j["asks"]]
    mid = (bids[0][0] + asks[0][0]) / 2
    return min(sum(p * q for p, q in bids if p >= mid * 0.995), sum(p * q for p, q in asks if p <= mid * 1.005))


def news_hits(symbol):
    items = cached(("news", symbol), 900, lambda: yf.Ticker(symbol).news or [])
    cutoff = time.time() - NEWS_LOOKBACK_H * 3600
    hits = []
    for it in items:
        c = it.get("content") or it
        title = (c.get("title") or "").strip()
        ts = None
        try:
            if c.get("pubDate"):
                ts = datetime.fromisoformat(c["pubDate"].replace("Z", "+00:00")).timestamp()
            elif it.get("providerPublishTime"):
                ts = float(it["providerPublishTime"])
        except Exception:
            ts = None
        if title and not (ts and ts < cutoff) and any(k in title.lower() for k in NEWS_KEYWORDS):
            hits.append(title[:90])
    return hits


def earnings_hours(symbol):
    """Hours until the next earnings date (None when none is known)."""
    cal = yf.Ticker(symbol).calendar
    dates = cal.get("Earnings Date") if isinstance(cal, dict) else None
    now = datetime.now(timezone.utc)
    hs = []
    for d in dates or []:
        dt = d if isinstance(d, datetime) else datetime(d.year, d.month, d.day)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        h = (dt - now).total_seconds() / 3600
        if h > -12:
            hs.append(h)
    return min(hs) if hs else None


def gather_data(cand):
    """Data the Filter and Analyst need. None = could not be verified."""
    sym = cand["symbol"]
    d = {"spread": None, "vol_ratio": None, "depth": None, "earn": {"known": False, "hours": None}, "news": None,
         "etf": sym in ETFS}
    if cand["kind"] == "crypto":
        pair = CRYPTO[sym]
        bk = BOOK.get(pair)
        if bk:
            d["spread"] = (bk[1] - bk[0]) / ((bk[1] + bk[0]) / 2)
        try:
            rows = cached(("1d", sym), 3600, lambda: binance_get(f"/api/v3/klines?symbol={pair}&interval=1d&limit=32"))
            avg = mean(float(r[7]) for r in rows[-31:-1])
            if avg > 0 and cand.get("vol24"):
                d["vol_ratio"] = cand["vol24"] / avg
        except Exception:
            pass
        try:
            d["depth"] = cached(("depth", sym), 60, lambda: depth_usd(pair))
        except Exception:
            pass
    else:
        try:  # previous full session vs the 30 sessions before it
            vols = cached(("1dv", sym), 3600, lambda: yahoo_daily_volumes(sym))
            avg = mean(vols[-32:-2])
            if avg > 0:
                d["vol_ratio"] = vols[-2] / avg
        except Exception:
            pass
        if sym not in ETFS:
            try:
                d["earn"] = {"known": True, "hours": cached(("earn", sym), 21600, lambda: earnings_hours(sym))}
            except Exception:
                pass
    try:
        d["news"] = news_hits(sym)
    except Exception:
        pass
    return d


def apply_filter(cand, d):
    """Agent 2 - Filter: six hard rules. status: pass | fail | skip (n/a) | unknown (could not verify)."""
    crypto = cand["kind"] == "crypto"
    R = []

    def add(rule, status, detail):
        R.append({"rule": rule, "status": status, "detail": detail})

    sp = d.get("spread")
    if not crypto:
        add("spread", "unknown", "ไม่มีข้อมูล bid/ask ของหุ้น")
    elif sp is None:
        add("spread", "unknown", "ไม่มีข้อมูล bid/ask")
    elif sp > MAX_SPREAD_PCT:
        add("spread", "fail", f"กว้างเกิน {sp * 100:.3f}% (สูงสุด {MAX_SPREAD_PCT * 100:.2f}%)")
    else:
        add("spread", "pass", f"{sp * 100:.3f}%")

    if crypto or d.get("etf"):
        add("earnings", "skip", "ไม่มีงบประกาศ")
    elif not d["earn"]["known"]:
        add("earnings", "unknown", "ดึงวันประกาศงบไม่ได้")
    elif d["earn"]["hours"] is not None and d["earn"]["hours"] <= EARNINGS_WINDOW_H:
        add("earnings", "fail", f"งบประกาศในอีก ~{max(0, d['earn']['hours']):.0f} ชม.")
    else:
        add("earnings", "pass", "ไม่มีงบใกล้ๆ")

    add("funding", "skip", "ใช้กับ perp เท่านั้น (ระบบเทรด spot)")

    vr = d.get("vol_ratio")
    if vr is None:
        add("volume", "unknown", "ไม่มีข้อมูลวอลุ่ม 30 วัน")
    elif vr < VOL_RATIO_MIN:
        add("volume", "fail", f"วอลุ่ม {vr:.2f}× ของค่าเฉลี่ย 30 วัน (ต้อง ≥ {VOL_RATIO_MIN:.1f}×)")
    else:
        add("volume", "pass", f"{vr:.2f}× ของค่าเฉลี่ย 30 วัน")

    nw = d.get("news")
    if nw is None:
        add("news", "unknown", "ดึงข่าวไม่ได้")
    elif nw:
        add("news", "fail", "ข่าวเสี่ยง: " + nw[0])
    else:
        add("news", "pass", f"ไม่พบข่าวเสี่ยงใน {NEWS_LOOKBACK_H} ชม.")

    dp = d.get("depth")
    if not crypto:
        add("book", "unknown", "ไม่มีข้อมูล order book ของหุ้น")
    elif dp is None:
        add("book", "unknown", "ดึง order book ไม่ได้")
    elif dp < MIN_DEPTH_USD:
        add("book", "fail", f"สภาพคล่อง ±0.5% แค่ ${dp:,.0f} (ต้อง ≥ ${MIN_DEPTH_USD:,})")
    else:
        add("book", "pass", f"${dp:,.0f} ภายใน ±0.5%")
    return any(r["status"] == "fail" for r in R), R


def analyst_score(cand, d, ind, rules):
    """Agent 3 - Analyst: score 0..1 (rule-based ranking, NOT a probability of profit). Returns (score|None, why)."""
    gap = (ind["ema9"] - ind["ema21"]) / ind["ema21"] if ind["ema21"] else 0.0
    if gap <= MIN_EMA_GAP or ind["last"] <= ind["ema21"]:
        return None, f"ไม่ใช่แนวโน้มขาขึ้น (EMA gap {gap * 100:+.2f}%)"
    rsi = ind["rsi"]
    if rsi > 75:
        return None, f"RSI {rsi:.0f} Overbought"
    trend = min(1.0, 0.5 + gap * 150)
    rsi_s = 1.0 if 50 <= rsi <= 62 else 0.6 if (45 <= rsi < 50 or 62 < rsi <= 68) else 0.2 if rsi <= 75 and rsi > 68 else 0.0
    vr = d.get("vol_ratio")
    vol_s = 0.5 if vr is None else min(1.0, vr / 2)
    sp = d.get("spread")
    liq_s = 0.5 if sp is None else max(0.0, 1 - sp / MAX_SPREAD_PCT)
    checked = [r for r in rules if r["status"] in ("pass", "unknown")]
    qual = (sum(r["status"] == "pass" for r in checked) / len(checked)) if checked else 0.0
    score = 0.35 * trend + 0.20 * rsi_s + 0.20 * vol_s + 0.10 * liq_s + 0.15 * qual
    why = f"เทรนด์ {trend:.2f} · RSI {rsi_s:.2f} · วอลุ่ม {vol_s:.2f} · สภาพคล่อง {liq_s:.2f} · ข้อมูลครบ {qual:.2f}"
    return round(score, 2), why


def risk_plan(pf, symbol, px):
    """Agent 5 - Risk: approves the order and fixes the stop BEFORE entry."""
    if drawdown_hit(pf):
        return None, f"ขาดทุนสะสมเกิน {MAX_DRAWDOWN_PCT:.0%} หยุดเปิดสถานะใหม่"
    held_val = next((p["value"] for p in pf["positions"] if p["symbol"] == symbol), 0.0)
    room = pf["equity"] * MAX_POSITION_PCT - held_val
    if room <= 0:
        return None, f"ถือครบกรอบ {MAX_POSITION_PCT:.0%} ของพอร์ตแล้ว"
    return {"stop": px * (1 - STOP_LOSS_PCT), "max_value": min(pf["equity"] * MAX_TRADE_PCT, room)}, ""


def skeptic_review(cand, d, ind, score, rules):
    """Skeptic bot: argues against every trade. Returns (objections, veto)."""
    obj = []
    if ind["rsi"] > 65:
        obj.append(f"RSI {ind['rsi']:.0f} เริ่มร้อนแรง")
    vr = d.get("vol_ratio")
    if vr is not None and vr < 1.3:
        obj.append(f"วอลุ่มแค่ {vr:.2f}× ยังไม่หนุนแรง")
    unk = sum(r["status"] == "unknown" for r in rules)
    if unk >= 3:
        obj.append(f"ตรวจข้อมูลไม่ได้ {unk} จาก 6 กฎ")
    if score < 0.7:
        obj.append(f"คะแนน {score:.2f} ไม่สูงมาก")
    chg = cand.get("chg24")
    if chg is not None and chg > 8:
        obj.append(f"ขึ้นมาแล้ว {chg:+.1f}% ใน 24 ชม. เสี่ยงไล่ราคา")
    return obj, len(obj) >= SKEPTIC_VETO_AT


def desk_talk(cand, d, ind, score, obj, veto, auto, px, talk):
    """The six bots discuss one candidate in the group chat (rate-limited per symbol)."""
    sym = cand["symbol"]
    prev = DESK["last"].get(sym)
    if not talk or (prev and time.time() - prev[0] < DESK_REPEAT_SEC and prev[1] == veto):
        return
    DESK["last"][sym] = (time.time(), veto)
    chg, vol, vr = cand.get("chg24"), cand.get("vol24"), d.get("vol_ratio")
    say("Scout", f"เจอ {sym} @ {px:.6g}" + (f" · 24 ชม. {chg:+.1f}%" if chg is not None else "") +
        (f" · วอลุ่ม ${vol / 1e6:.1f}M" if vol else ""), sym)
    nw = d.get("news")
    if nw is None:
        say("News", "ดึงข่าวไม่ได้ ยังยืนยันไม่ได้ว่าปลอดภัย", sym, "warn")
    else:
        say("News", f"ไม่พบข่าวเสี่ยงใน {NEWS_LOOKBACK_H} ชม. (จับคำสำคัญในพาดหัว ไม่ได้อ่านเนื้อข่าว)", sym, "ok")
    if vr is not None and vr >= 2 and chg is not None and chg > 3:
        mood, tone = f"ฝูงชนกำลังแห่ซื้อ (วอลุ่ม {vr:.1f}×, {chg:+.1f}%) ระวัง FOMO", "warn"
    elif vr is not None and vr >= 1:
        mood, tone = f"ความสนใจปกติถึงเพิ่มขึ้น (วอลุ่ม {vr:.1f}×)", "ok"
    else:
        mood, tone = "ฝูงชนยังเงียบ ไม่มีข้อมูลวอลุ่มเทียบ", "warn"
    say("Sentiment", mood + " · ประเมินจากวอลุ่ม/ราคา ไม่ได้อ่านโซเชียล", sym, tone)
    gap = (ind["ema9"] - ind["ema21"]) / ind["ema21"] if ind["ema21"] else 0.0
    say("Charts", f"เข้า {px:.6g} · stop {px * (1 - STOP_LOSS_PCT):.6g} (-{STOP_LOSS_PCT:.0%}) · "
        f"เป้า {px * (1 + TARGET_PCT):.6g} (+{TARGET_PCT:.0%}) · EMA gap {gap * 100:+.2f}% · RSI {ind['rsi']:.0f}", sym)
    if obj:
        say("Skeptic", "ค้านนะ: " + "; ".join(obj) + (" → VETO" if veto else " → ยังพอรับได้"), sym, "bad" if veto else "warn")
    else:
        say("Skeptic", "หาจุดค้านไม่เจอรอบนี้", sym, "ok")
    if veto:
        say("Chief", f"ตัดสิน: ข้าม {sym} ตาม Skeptic (ค้าน {len(obj)} ข้อ)", sym, "bad")
    elif auto:
        say("Chief", f"ตัดสิน: เข้า {sym} คะแนน {score:.2f} → ส่งให้ Risk/Executor", sym, "ok")
    else:
        say("Chief", f"ตัดสิน: {sym} ผ่านทีม (คะแนน {score:.2f}) แต่ Auto ปิดอยู่ จึงยังไม่ส่งคำสั่ง", sym)


def _kill(row, agent, reason, log_it=True):
    row["verdict"] = "killed"
    row["killed_by"] = agent
    row["note"] = reason
    if log_it and LAST_KILL.get(row["symbol"]) != (agent, reason):
        LAST_KILL[row["symbol"]] = (agent, reason)
        bot_log(agent, row["symbol"], "kill", reason)


def run_cycle(cands=None, gather=None, ind_fn=None):
    """One pass of the pipeline: Scout -> Filter -> Analyst -> Risk -> Executor (Logger records everything)."""
    cands = scout() if cands is None else cands
    gather = gather or gather_data
    ind_fn = ind_fn or get_ind
    auto = get_setting("auto_enabled", "0") == "1"
    with db() as c:
        pf = portfolio(c)
        last = {r["symbol"]: r["ts"] for r in c.execute("SELECT symbol, MAX(ts) ts FROM trades GROUP BY symbol")}
    held = {p["symbol"]: p for p in pf["positions"]}

    def fetch(cand):
        if cand["symbol"] in held:
            return {}
        try:
            return gather(cand)
        except Exception as e:
            log.warning("gather %s failed: %s", cand["symbol"], e)
            return {}

    with ThreadPoolExecutor(max_workers=4) as ex:
        datas = list(ex.map(fetch, cands))

    rows, ranked = [], []
    n_filter = n_analyst = n_exec = 0
    for cand, d in zip(cands, datas):
        sym = cand["symbol"]
        row = {"symbol": sym, "kind": cand["kind"], "price": cand.get("price"), "stage": "scout",
               "verdict": "passed", "score": None, "rules": [], "note": ""}
        rows.append(row)
        q = quote(sym)
        if not q or q["status"] != "live":
            _kill(row, "Scout", "ไม่มีราคา" if not q else "ราคาไม่สด/ตลาดปิด", log_it=False)
            continue
        if sym in held:
            ind = ind_fn(sym)
            p = held[sym]
            row.update(stage="risk", verdict="held",
                       note=f"ถืออยู่ · stop {p['stop']:.6g}" + (" · trailing" if p["trail"] else ""))
            if ind and auto and rule_signal(ind)[0] == "SELL":
                try:
                    execute_trade(sym, "SELL", p["qty"], "auto:trend-exit", q["price"], enforce_risk=False)
                    bot_log("Executor", sym, "SELL", f"ปิดสถานะ @ {q['price']:.6g} · {rule_signal(ind)[2]}")
                    say("Chief", f"ปิด {sym} @ {q['price']:.6g} · {rule_signal(ind)[2]}", sym, "warn")
                    row.update(verdict="exit", note="ปิดสถานะตามสัญญาณ")
                    n_exec += 1
                    held.pop(sym)
                except HTTPException as e:
                    bot_log("Executor", sym, "skip", str(e.detail))
            continue
        killed, rules = apply_filter(cand, d)
        row.update(stage="filter", rules=rules)
        fails = [r for r in rules if r["status"] == "fail"]
        if killed:
            _kill(row, "Filter", "; ".join(f"{r['rule']}: {r['detail']}" for r in fails))
            continue
        n_filter += 1
        ind = ind_fn(sym)  # only candidates that survived the Filter cost a candle download
        if not ind:
            _kill(row, "Analyst", "ไม่มีแท่งเทียนให้วิเคราะห์ · " + (SERIES_ERR.get(sym) or "ไม่ทราบสาเหตุ"))
            continue
        row["stage"] = "analyst"
        score, why = analyst_score(cand, d, ind, rules)
        if score is None:
            _kill(row, "Analyst", why)
            continue
        row["score"] = score
        if score < ANALYST_MIN:
            _kill(row, "Analyst", f"คะแนน {score:.2f} < {ANALYST_MIN} · {why}")
            continue
        row["note"] = why
        n_analyst += 1
        LAST_KILL.pop(sym, None)
        ranked.append((score, cand, row, d, ind))

    cash = pf["cash"]
    for n_rank, (score, cand, row, d, ind) in enumerate(sorted(ranked, key=lambda x: -x[0])):
        sym = cand["symbol"]
        q = quote(sym)
        objections, veto = skeptic_review(cand, d, ind, score, row["rules"])
        desk_talk(cand, d, ind, score, objections, veto, auto, q["price"], n_rank < 4)
        if veto:
            row["stage"] = "skeptic"
            _kill(row, "Skeptic", "ค้าน: " + "; ".join(objections))
            continue
        row["stage"] = "executor"
        if not auto:
            row["note"] += " · Auto ปิดอยู่ จึงไม่ส่งคำสั่ง"
            continue
        if len(held) >= AUTO_MAX_POSITIONS:
            row["note"] += f" · ถือครบ {AUTO_MAX_POSITIONS} สถานะแล้ว"
            continue
        lt = last.get(sym)
        if lt and time.time() - datetime.fromisoformat(lt).timestamp() < AUTO_COOLDOWN_SEC:
            row["note"] += " · พักหลังเทรดล่าสุด"
            continue
        plan, why = risk_plan(pf, sym, q["price"])
        if not plan:
            _kill(row, "Risk", why)
            continue
        spend = min(plan["max_value"], cash * 0.98)
        if spend < MIN_ORDER_USD:
            row["note"] += " · เงินสดไม่พอ"
            continue
        qty = spend / q["price"]
        try:
            execute_trade(sym, "BUY", qty, f"auto:analyst {score:.2f}", q["price"], stop=plan["stop"])
        except HTTPException as e:
            _kill(row, "Risk", str(e.detail))
            continue
        cash -= spend * (1 + FEE_RATE)
        held[sym] = {"qty": qty}
        n_exec += 1
        row.update(verdict="bought", note=f"ซื้อ {qty:.6g} @ {q['price']:.6g} · stop {plan['stop']:.6g} (-{STOP_LOSS_PCT:.0%})")
        bot_log("Executor", sym, "BUY", f"คะแนน {score:.2f} · ซื้อ {qty:.6g} @ {q['price']:.6g} · Risk ตั้ง stop {plan['stop']:.6g}")
        say("Chief", f"ซื้อ {sym} {qty:.6g} @ {q['price']:.6g} · stop {plan['stop']:.6g}", sym, "ok")

    order = {"bought": 0, "exit": 1, "held": 2, "passed": 3, "killed": 4}
    rows.sort(key=lambda r: (order.get(r["verdict"], 5), -(r["score"] or 0)))
    PIPE.update(ts=time.time(), cycle=PIPE["cycle"] + 1, auto=auto, rows=rows,
                counts={"scout": len(cands), "filter": n_filter, "analyst": n_analyst, "executor": n_exec})
    sig = (len(cands), n_filter, n_analyst, n_exec)
    if DESK["sum"] != sig or PIPE["cycle"] % 10 == 1:
        say("Chief", f"รอบ #{PIPE['cycle']}: สแกน {sig[0]} → ผ่าน Filter {sig[1]} → ผ่าน Analyst {sig[2]} → ส่งคำสั่ง {sig[3]}" +
            ("" if auto else " · Auto ปิดอยู่"))
        DESK["sum"] = sig
    return PIPE


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
                          "status": q["status"] if q else "none", "stop": p["stop"], "peak": p["peak"],
                          "trail": p["trail"] or 0})
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


def execute_trade(symbol, side, qty, strategy, px, enforce_risk=True, stop=None):
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
            new_stop = stop if stop is not None else px * (1 - STOP_LOSS_PCT)  # every position always has a stop
            if p and p["stop"]:
                new_stop = max(new_stop, p["stop"])
            peak = max(px, (p["peak"] or 0) if p else 0)
            c.execute("UPDATE account SET cash=cash-? WHERE id=1", (cost,))
            c.execute("INSERT OR REPLACE INTO positions(symbol,qty,avg,stop,peak,trail) VALUES(?,?,?,?,?,?)",
                      (symbol, oldq + qty, avg, new_stop, peak, (p["trail"] or 0) if p else 0))
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
    """Risk agent, every 5s on live prices: fixed stop, then a trailing stop once a position is +15%."""
    with db() as c:
        rows = c.execute("SELECT * FROM positions").fetchall()
    for p in rows:
        q = quote(p["symbol"])
        if not q or q["status"] != "live" or not p["avg"]:
            continue
        px = q["price"]
        peak = max(p["peak"] or px, px)
        trail = p["trail"] or 0
        stop = p["stop"] or p["avg"] * (1 - STOP_LOSS_PCT)
        if not trail and px / p["avg"] - 1 >= TRAIL_ACTIVATE_PCT:
            trail = 1
            bot_log("Risk", p["symbol"], "trail", f"กำไรถึง +{TRAIL_ACTIVATE_PCT:.0%} เริ่ม trailing stop")
        if trail:
            stop = max(stop, peak * (1 - TRAIL_PCT))
        if (peak, trail, stop) != (p["peak"], p["trail"] or 0, p["stop"]):
            with db() as c:
                c.execute("UPDATE positions SET peak=?, trail=?, stop=? WHERE symbol=?", (peak, trail, stop, p["symbol"]))
        if px <= stop:
            try:
                execute_trade(p["symbol"], "SELL", p["qty"], "trailing-stop" if trail else "stop-loss", px, enforce_risk=False)
                bot_log("Risk", p["symbol"], "trailing-stop" if trail else "stop-loss",
                        f"ปิดสถานะ {p['symbol']} @ {px:.6g} (stop {stop:.6g})")
                say("Chief", f"{'Trailing stop' if trail else 'Stop-loss'} ทำงาน ปิด {p['symbol']} @ {px:.6g}", p["symbol"], "bad")
            except HTTPException as e:
                log.warning("exit failed %s: %s", p["symbol"], e.detail)


def snapshot_equity():
    with db() as c:
        pf = portfolio(c)
        c.execute("INSERT INTO equity(ts,equity) VALUES(?,?)", (datetime.now(timezone.utc).isoformat(), pf["equity"]))
        c.execute("DELETE FROM equity WHERE id NOT IN (SELECT id FROM equity ORDER BY id DESC LIMIT 2000)")


def price_loop():
    while not STOP.is_set():
        _safe(refresh_prices)
        STOP.wait(PRICE_LOOP_SEC)


def pipeline_loop():
    STOP.wait(4)  # let the first prices arrive
    while not STOP.is_set():
        _safe(run_cycle)
        STOP.wait(PIPELINE_SEC)


def worker():
    """Fast loop: Risk agent (stops / trailing) on live prices and equity snapshots."""
    t_snap = 0.0
    while not STOP.is_set():
        _safe(check_exits)
        if time.time() - t_snap >= SNAPSHOT_SEC:
            _safe(snapshot_equity)
            t_snap = time.time()
        STOP.wait(TICK_SEC)


# -------------------------------------------------------------------------- api
@asynccontextmanager
async def lifespan(_app):
    STOP.clear()
    for fn in (worker, price_loop, pipeline_loop):
        threading.Thread(target=fn, daemon=True).start()
    task = asyncio.create_task(ws_runner())
    yield
    STOP.set()
    task.cancel()


app = FastAPI(title="AI Trading Desk PRO Phase 5", lifespan=lifespan)

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


class BotSwitch(BaseModel):
    enabled: bool


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


@api.get("/pipeline")
def pipeline():
    return {"cycle": PIPE["cycle"], "age": int(time.time() - PIPE["ts"]) if PIPE["ts"] else None, "auto": PIPE["auto"],
            "counts": PIPE["counts"], "rows": PIPE["rows"], "scout_error": SCOUT_ERR.get("crypto"),
            "analyst_min": ANALYST_MIN}


@api.get("/desk")
def desk():
    now = time.time()
    with db() as c:
        pf = portfolio(c)
        sells = [r["pnl"] for r in c.execute("SELECT pnl FROM trades WHERE side='SELL'")]
    spoke = {}
    for m in list(DESK["msgs"]):
        spoke[m["bot"]] = datetime.fromisoformat(m["ts"]).timestamp()
    alive = bool(PIPE["ts"]) and now - PIPE["ts"] < PIPELINE_SEC * 3
    bots = [{"bot": b, "state": "online" if (b == "Chief" and alive) or now - spoke.get(b, 0) < 120 else "idle"} for b in BOTS]
    return {"bots": bots, "messages": list(DESK["msgs"])[-60:], "uptime": int(now - DESK["started"]),
            "cycle": PIPE["cycle"], "equity": pf["equity"], "pnl": pf["equity"] - pf["initial"],
            "closed": len(sells), "win_rate": (sum(1 for x in sells if x > 0) / len(sells)) if sells else None}


@api.get("/account")
def account():
    with db() as c:
        pf = portfolio(c)
        ts = c.execute("SELECT * FROM trades ORDER BY id DESC LIMIT 30").fetchall()
    return {**pf, "pnl": pf["equity"] - pf["initial"], "trades": [dict(t) for t in ts],
            "limits": {"max_trade_pct": MAX_TRADE_PCT, "max_position_pct": MAX_POSITION_PCT,
                       "stop_loss_pct": STOP_LOSS_PCT, "trail_activate_pct": TRAIL_ACTIVATE_PCT,
                       "trail_pct": TRAIL_PCT, "max_drawdown_pct": MAX_DRAWDOWN_PCT, "fee_rate": FEE_RATE},
            "halted": drawdown_hit(pf)}


@api.get("/equity")
def equity_curve():
    with db() as c:
        rows = c.execute("SELECT ts,equity FROM equity ORDER BY id DESC LIMIT 300").fetchall()
    return {"points": [dict(r) for r in reversed(rows)]}


@api.get("/bot")
def bot_status():
    with db() as c:
        rows = c.execute("SELECT ts,agent,symbol,action,msg FROM bot_log ORDER BY id DESC LIMIT 40").fetchall()
    return {"enabled": get_setting("auto_enabled", "0") == "1",
            "analysis_age": int(time.time() - PIPE["ts"]) if PIPE["ts"] else None,
            "params": {"trade_pct": MAX_TRADE_PCT, "max_positions": AUTO_MAX_POSITIONS,
                       "cooldown_min": AUTO_COOLDOWN_SEC // 60, "interval_sec": PIPELINE_SEC,
                       "analyst_min": ANALYST_MIN},
            "log": [dict(r) for r in rows]}


@api.post("/bot")
def bot_switch(b: BotSwitch):
    set_setting("auto_enabled", "1" if b.enabled else "0")
    bot_log("Logger", "-", "system", "เปิด Auto Trading (บัญชีจำลอง)" if b.enabled else "ปิด Auto Trading")
    return {"enabled": b.enabled}


@api.post("/trade")
def trade(o: Order):
    side = o.side.upper()
    if o.symbol not in tracked_symbols() or side not in ("BUY", "SELL") or not math.isfinite(o.qty):
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
    LAST_KILL.clear()
    return {"ok": True}


app.include_router(api)
