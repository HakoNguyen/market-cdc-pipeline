# Plan: CDC Market Data Pipeline

Project portfolio cho vị trí Fresher/Junior Data Engineer.
Mục tiêu: chứng minh năng lực engineering (CDC, idempotency, data quality, backfill)
bằng một artifact public — bù cho việc code production ở Zenify không show được.

---

## 0. Chốt phương án

**Stack**

```
yfinance poll (Airflow) ──→ Postgres (daily_bar, UPSERT)
                                  │
                                  ↓ Debezium (Kafka Connect)
                            Kafka (KRaft, KHÔNG Zookeeper)
                                  │
Binance WS ───────────────────────┤
                                  ↓
                            StarRocks (Primary Key model)
                                  │
                                  ↓ dbt (staging → dim/fact → mart)
                               BI / query
```

**Vì sao từng thành phần tồn tại** (phải trả lời được khi phỏng vấn)

| Thành phần | Lý do kỹ thuật |
|---|---|
| Postgres | OLTP source, `UPSERT` sinh UPDATE thật trên `daily_bar` |
| Debezium | Bắt `c`/`u`/`d`; keyword mạnh; mirror việc đang làm ở Zenify |
| Kafka KRaft | Transport của Debezium — không phải để trang trí |
| Binance WS | Stream thật, 24/7, không key → biện minh cho Kafka |
| StarRocks | Primary Key model xử lý upsert CDC native; verify kinh nghiệm Zenify |
| dbt | Cho sẵn test / incremental / snapshot / docs — 3/4 điểm khác biệt |
| Airflow | Orchestration (đã setup) |

**Không dùng và vì sao**

- Zookeeper — Kafka 3.x+ dùng KRaft, để Zookeeper là dấu hiệu theo tutorial cũ
- Spark — transform nằm trong dbt (ELT); giữ Spark chỉ nếu cho nó việc thật
  (dedup theo `(pk, op_ts)` + watermark). Nếu bỏ, để Spark ở mục Skills
- Snowflake — trial 30 ngày, project cần sống lâu hơn
- Redpanda — nhẹ hơn nhưng mất từ khoá "Kafka" ở vòng lọc CV
- vnstock / TCBS — cần credential, bind thiết bị → người clone repo không chạy được
- vietnam_ecommerce dataset — data tĩnh, download 1 lần

---

## Phase 0 — Verify data source (nửa ngày, làm TRƯỚC tiên)

Câu chuyện chính của project là **retroactive price adjustment**. Phải xác nhận
Yahoo có data corporate action cho mã VN trước khi cam kết.

```python
import yfinance as yf

for sym in ["FPT.VN", "VNM.VN", "HPG.VN", "VCB.VN"]:
    t = yf.Ticker(sym)
    h = t.history(period="max", auto_adjust=False)
    print(sym, h.index.min(), h.index.max(), len(h))
    print(t.actions)                      # có dividend/split không?

    adj = t.history(period="2y", auto_adjust=True)
    raw = t.history(period="2y", auto_adjust=False)
    print("delta:", (adj["Close"] - raw["Close"]).abs().sum())
```

**Quyết định:**

- `t.actions` có data và `delta > 0` → Yahoo áp adjustment. Giữ nguyên plan.
- `t.actions` rỗng hoặc `delta == 0` → **tự tính adjustment factor trong dbt**
  từ cổ tức/split. Mạnh hơn: tự implement logic thay vì tiêu thụ cột có sẵn.
- Thiếu quá nhiều mã → dùng ticker Mỹ (AAPL, MSFT...) làm chính, mã VN làm phụ.

Ghi lại kết quả vào `docs/data-source-notes.md`. Đây là input cho README.

---

## Phase 1 — OLTP schema + crawler (1–2 ngày)

**Postgres schema**

```sql
-- Nến ngày. Đây là bảng sinh mutation: nến hôm nay bị UPSERT liên tục
-- trong phiên (high/low/close/volume đổi), rồi đóng băng khi hết phiên.
CREATE TABLE daily_bar (
    symbol       TEXT        NOT NULL,
    trade_date   DATE        NOT NULL,
    open         NUMERIC(18,4),
    high         NUMERIC(18,4),
    low          NUMERIC(18,4),
    close        NUMERIC(18,4),
    adj_close    NUMERIC(18,4),
    volume       BIGINT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date)
);

-- Đổi chậm → ứng viên cho dbt snapshot (SCD2). Delist = DELETE thật.
CREATE TABLE instrument (
    symbol       TEXT PRIMARY KEY,
    name         TEXT,
    exchange     TEXT,
    sector       TEXT,
    is_listed    BOOLEAN     NOT NULL DEFAULT true,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Nguồn của retroactive adjustment
CREATE TABLE corporate_action (
    symbol       TEXT        NOT NULL,
    action_date  DATE        NOT NULL,
    action_type  TEXT        NOT NULL,   -- dividend | split
    value        NUMERIC(18,6),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, action_date, action_type)
);
```

**Adapter interface** — quan trọng, làm ngay từ đầu

```python
class MarketDataSource(Protocol):
    def get_daily_bars(self, symbol: str, start: date, end: date) -> pd.DataFrame: ...
    def get_corporate_actions(self, symbol: str) -> pd.DataFrame: ...

class YFinanceSource:  ...   # implement đầu tiên
class TCBSSource:      ...   # để trống, cắm sau nếu muốn
```

Downstream không biết nguồn nào. Đổi source = sửa một class.

**Crawler**

- Airflow DAG poll yfinance, `INSERT ... ON CONFLICT DO UPDATE`
- Retry + exponential backoff, `User-Agent` thật, giới hạn concurrency
- Validate payload bằng Pydantic → API đổi format thì fail fast, không ghi `null`
- Lưu raw response xuống disk/MinIO theo `date/symbol` trước khi parse
  → upstream đổi format thì replay được từ raw

---

## Phase 2 — CDC path (2–3 ngày)

- Postgres: `wal_level=logical`, tạo publication
- Debezium Postgres connector → Kafka KRaft (1 broker, `mem_limit` thấp)
- Sink vào StarRocks Primary Key table, **giữ nguyên hình dạng CDC**:
  cột `op`, `op_ts`, `_ingested_at` — đừng dedup ở tầng này
- Binance WS consumer → Kafka topic riêng → StarRocks
- Mục tiêu phase: `c` và `u` chạy end-to-end. Chưa cần đẹp.

**StarRocks tuning** (nếu RAM căng)

- `be.conf`: `mem_limit` cứng 2–3GB
- FE: `-Xmx1g`
- 1 FE + 1 BE, hoặc bản allin1
- Chia compose profile `ingest` / `warehouse`, bật cả hai chỉ khi test end-to-end

---

## Phase 3 — dbt (3–4 ngày) — PHẦN ĂN ĐIỂM

**Model layers**

```
staging/
  stg_daily_bar          -- dedup theo (symbol, trade_date, op_ts), lấy bản mới nhất
  stg_instrument
  stg_corporate_action
marts/
  dim_instrument         -- snapshot SCD2
  fact_daily_bar         -- incremental, unique_key = symbol||trade_date
  fct_adjusted_price     -- tự tính adjustment factor từ corporate_action
mart/
  mart_price_summary     -- OHLCV theo tuần/tháng
  mart_volatility        -- rolling stddev, phát hiện bất thường
  mart_adjustment_impact -- adjustment ảnh hưởng bao nhiêu % giá lịch sử
```

Giữ mart ở 3–4 model. Mart để **chứng minh pipeline chạy đúng**, không phải để
làm phân tích. Nếu mart phình ra mà CDC/test/backfill mỏng thì project lệch sang DA.

**Test — viết từ model đầu tiên, không để cuối**

```yaml
# generic tests
- unique: [symbol, trade_date]
- not_null: [symbol, trade_date, close]
- accepted_values: exchange in [HOSE, HNX, UPCOM]
- relationships: fact_daily_bar.symbol -> dim_instrument.symbol
```

```sql
-- singular tests (tests/*.sql) — trả về row là fail
-- 1. giá không được âm
select * from {{ ref('fact_daily_bar') }} where close <= 0

-- 2. high phải >= low
select * from {{ ref('fact_daily_bar') }} where high < low

-- 3. không có gap ngày giao dịch quá dài với mã đang listed
-- 4. adj_close không được lệch raw quá X% mà không có corporate_action tương ứng
```

Test 4 là test hay nhất — nó bắt đúng bài toán retroactive adjustment.

---

## Phase 4 — Cái làm nên khác biệt (2 ngày)

**Idempotency**

```sql
{{ config(
    materialized='incremental',
    unique_key=['symbol', 'trade_date'],
    incremental_strategy='merge'
) }}
```

Chạy lại cùng một DAG run không sinh bản trùng. Verify bằng cách chạy 2 lần
rồi `COUNT(*)` — ghi kết quả vào README.

**Backfill**

```bash
dbt build --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
```

Chạy lại một tháng quá khứ mà không phá dữ liệu hiện có. Test: backfill tháng 1
sau khi đã có data tháng 1–6, verify tháng 2–6 không đổi.

**Retroactive adjustment — câu chuyện chính**

Khi có split/cổ tức mới, toàn bộ `adj_close` lịch sử của mã đó thay đổi.
Pipeline phải: phát hiện corporate_action mới → xác định phạm vi ảnh hưởng →
backfill đúng phạm vi đó → không touch mã khác. Ghi lại flow này trong README.

**Airflow DAG**

```
crawl_yfinance → dbt seed → dbt run → dbt test → (fail DAG nếu test fail)
```

Dùng `dbt build` để test chạy xen kẽ với run, fail sớm.

**README** — quan trọng ngang code

- Sơ đồ kiến trúc (bỏ Zookeeper, đúng luồng)
- Data dictionary: cột nào nghĩa gì
- **Quyết định thiết kế kèm lý do**: vì sao CDC, vì sao StarRocks, vì sao KRaft,
  vì sao không Spark, trade-off của từng lựa chọn
- Cách chạy lại từ đầu: `docker compose up` → xong. Không credential.
- Kết quả verify idempotency và backfill (số liệu thật)

Mở đầu README bằng năng lực, không bằng nguồn data:

> Pipeline CDC end-to-end xử lý dữ liệu bị điều chỉnh ngược về quá khứ
> (retroactive price adjustment), có data quality gate và backfill idempotent.
> Nguồn: thị trường chứng khoán.

---

## Timeline

| Phase | Thời gian | Ưu tiên |
|---|---|---|
| 0 — Verify source | 0.5 ngày | Bắt buộc trước tiên |
| 1 — OLTP + crawler | 1–2 ngày | Cao |
| 2 — CDC path | 2–3 ngày | Trung bình (lấy keyword) |
| 3 — dbt + test | 3–4 ngày | **Cao nhất** |
| 4 — Idempotency/backfill/README | 2 ngày | **Cao nhất** |

Tổng ~9–12 ngày làm thật.

**Lỗi phổ biến cần tránh:** để Phase 2 ngốn hết thời gian (debug Debezium rất
dễ mất ngày) rồi bỏ dở Phase 3–4. Nếu deadline gấp, cắt Phase 2 xuống mức
tối thiểu (một bảng, chỉ `c` và `u`) và giữ nguyên Phase 3–4.

---

## Định vị trong CV

- **Zenify** = bằng chứng kinh nghiệm production. Bullet dạng
  "action + scale + kết quả", không cần code. Đây là thứ khiến họ gọi.
- **Project này** = bằng chứng code + phán đoán kỹ thuật. Public repo, README.
- **Blog Viblo** = bóc pattern kỹ thuật ra khỏi context công ty
  ("dedup CDC event trong StarRocks", "khi nào MV rẻ hơn query trực tiếp").
  Không code công ty, không data công ty — chỉ pattern + ví dụ tự dựng.
  Với dev VN thì bài về StarRocks/CDC là cực hiếm.

Ba thứ này cộng lại mạnh hơn nhiều so với dồn hết vào project.

**Ưu tiên song song:** chuẩn bị tiếng Anh (nếu apply FPT — vòng test loại
nhiều nhất và không hỏi gì về project) và luyện kể 3 quyết định kỹ thuật ở
Zenify theo format *vấn đề → phương án cân nhắc → chọn gì, vì sao → trade-off*.
Hai cái đó đổi ra offer nhanh hơn việc thêm layer cho pipeline.

---

## Nguyên tắc

Đã đổi hướng 5 lần trước khi chốt. Từ giờ **không đổi data source hay stack
nữa** cho tới khi Phase 2 chạy được end-to-end. Chi phí của việc chọn thêm
đã lớn hơn lợi ích của việc chọn hoàn hảo.

Bắt đầu: chạy script Phase 0.
