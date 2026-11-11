# AI Trading Desk PRO – Phase 3

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

## Phase 3: Auto Trading + วิเคราะห์เกือบเรียลไทม์
- สวิตช์ **Auto Trading** (ค่าเริ่มต้นปิด) บอทซื้อ/ขายในบัญชีจำลองให้เองตามสัญญาณ
  - เข้า: สัญญาณ BUY และยังไม่ถือสัญลักษณ์นั้น · ไม้ละ 5% ของพอร์ต · ถือพร้อมกันสูงสุด 4 สถานะ
  - ออก: สัญญาณ SELL, Stop-loss −5%, Take-profit +10%
  - พัก 30 นาทีต่อสัญลักษณ์หลังมีรายการล่าสุด กันซื้อขายรัว
  - ทำงานเฉพาะเมื่อราคา "live" และสัญญาณสดเท่านั้น (หุ้นตอนตลาดปิดจะไม่เทรด)
- วิเคราะห์ทุก 1 นาทีจากแท่ง 5 นาที + ราคา live (เดิมทุก 5 นาที/แท่ง 15 นาที), เช็ก SL/TP ทุก 5 วินาที, หน้าจออัปเดตทุก 5 วินาที
- Bot Activity แสดงว่าบอททำอะไรและเพราะอะไร
- กฎสัญญาณใหม่ (Trend-follow): BUY เมื่อ EMA9 > EMA21 ชัดเจน, ราคาเหนือ EMA21 และ RSI 45–68 ·
  SELL เมื่อ EMA9 < EMA21 หรือ RSI > 75
- **ยังไม่เคย backtest** ไม่รับประกันว่าทำกำไรได้ ใช้ดูผลในบัญชีจำลองก่อน
- ไม่เชื่อมกับ MT5/XM — เป็นบัญชีจำลองในแอปนี้เท่านั้น

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
- Render free: เซิร์ฟเวอร์หลับเมื่อไม่มีผู้ใช้ (บอท Auto และ SL/TP หยุดทำงานช่วงนั้น ต้องใช้แพ็กเกจที่ไม่หลับถ้าอยากให้เทรดต่อเนื่อง) และไฟล์ SQLite หายเมื่อ redeploy
  → ใช้ Render disk/Postgres ถ้าต้องการเก็บข้อมูลถาวร (ตั้ง `DB_PATH` ชี้ไปที่ disk ได้)
- ราคาหุ้นจาก Yahoo ไม่รับประกัน real-time; นอกเวลาตลาดแสดงราคาปิดล่าสุดพร้อมป้ายเตือน
- Backtest ยังไม่มี, ไม่มี slippage model, ไม่มี short selling
- ห้ามนำไปต่อเงินจริงโดยไม่ทำ security review, tests, risk validation และ compliance review

## ความปลอดภัย
ไม่เก็บ API keys ของ broker/exchange, ไม่มี order endpoint เงินจริง, ไม่มีการถอนเงิน
