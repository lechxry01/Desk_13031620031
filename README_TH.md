# AI Trading Desk PRO – Phase 2.1

ระบบทดลอง Paper Trading สำหรับมือถือ รองรับ BTC/ETH/SOL และ AAPL/MSFT/NVDA/SPY/TSLA
เงินจำลองเริ่มต้น $10,000 · ไม่มีการเชื่อมต่อบัญชีเงินจริง

## เริ่มใช้งานในเครื่อง
1. Python 3.10+
2. `pip install -r requirements.txt`
3. `uvicorn app:app --host 0.0.0.0 --port 8000`
4. เปิด `http://localhost:8000`
5. (ถ้าต้องการ) ตั้งรหัสเข้าใช้งาน: `APP_KEY=รหัสของคุณ uvicorn app:app ...`

รันเทสต์: `pip install -r requirements-dev.txt && pytest`

## Deploy (Render)
อัปโหลดขึ้น GitHub แล้วเชื่อม Render ด้วย `render.yaml` — ระบบสร้าง `APP_KEY` ให้อัตโนมัติ
(ดูค่าได้ที่ Render > Environment) เปิด URL แล้วใส่รหัสครั้งแรก จากนั้น Add to Home Screen
ใช้ `--workers 1` เสมอ เพราะราคา/สัญญาณเก็บในหน่วยความจำของ process เดียว

## สิ่งที่แก้ใน Phase 2.1
- ราคาหุ้นรีเฟรชทุก 30 วินาทีโดย background worker (เดิมค้างค่าแรกตลอด)
- คริปโต: Binance.com → Binance.US → fallback Yahoo อัตโนมัติ เมื่อ WebSocket ใช้ไม่ได้
- สถานะราคา live / closed (ตลาดหุ้นปิด) / stale (คริปโตไม่สด) — คริปโตที่ไม่สดจะไม่รับคำสั่ง
- สัญญาณคำนวณเบื้องหลังทุก 5 นาทีแล้ว cache (เดิมเรียก yfinance 8 ครั้งทุกการรีเฟรช)
- Risk engine จริง: ≤25% ของพอร์ตต่อสัญลักษณ์, หยุดเปิดสถานะใหม่เมื่อขาดทุนสะสม ≥20%,
  Stop-loss −5% / Take-profit +10% ปิดสถานะอัตโนมัติ (เฉพาะราคาสด)
- ปุ่ม "ใช้สัญญาณนี้" เติมคำสั่งจากสัญญาณ (ผู้ใช้ยังต้องกดยืนยันเอง)
- ราคาเฉลี่ยรวมค่าธรรมเนียม, กราฟ equity curve, toast แทน alert
- dropdown ไม่เด้งกลับทุกรอบรีเฟรช
- PWA: ไอคอน 192/512/maskable + apple-touch-icon, service worker ที่ root, ไม่ cache /api
- Auth ด้วย `APP_KEY` (header `X-Key`), `/healthz`, `.gitignore`, pytest 10 เคส

## ปรับกรอบความเสี่ยง
แก้ค่าคงที่ต้นไฟล์ `app.py` (`MAX_POSITION_PCT`, `STOP_LOSS_PCT`, `TAKE_PROFIT_PCT`, `MAX_DRAWDOWN_PCT`, `FEE_RATE`)

## ข้อจำกัดสำคัญ
- Prototype สำหรับทดลอง ไม่ใช่คำแนะนำลงทุน และไม่ได้ใช้ LLM (Agent เป็นกฎเชิงเทคนิค)
- Render free: เซิร์ฟเวอร์หลับเมื่อไม่มีผู้ใช้ (worker หยุด SL/TP ไม่ทำงานช่วงนั้น) และไฟล์ SQLite หายเมื่อ redeploy
  → ใช้ Render disk/Postgres ถ้าต้องการเก็บข้อมูลถาวร (ตั้ง `DB_PATH` ชี้ไปที่ disk ได้)
- ราคาหุ้นจาก Yahoo ไม่รับประกัน real-time; นอกเวลาตลาดแสดงราคาปิดล่าสุดพร้อมป้ายเตือน
- Backtest ยังไม่มี, ไม่มี slippage model, ไม่มี short selling
- ห้ามนำไปต่อเงินจริงโดยไม่ทำ security review, tests, risk validation และ compliance review

## ความปลอดภัย
ไม่เก็บ API keys ของ broker/exchange, ไม่มี order endpoint เงินจริง, ไม่มีการถอนเงิน
