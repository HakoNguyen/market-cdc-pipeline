# Project Overview

## Tên

**Repo:** `market-cdc-pipeline`

Ngắn, mô tả đúng thứ nó làm, không sáo. Tránh những tên t đã cân nhắc và loại:
`stock-data-pipeline` (chung chung, GitHub có hàng nghìn cái),
`market-lakehouse` (sai — không có table format, không có lake query engine),
`vn-stock-etl` (đây là ELT, không phải ETL).

**Tên trên CV:**

> **Market CDC Pipeline** — Real-time CDC pipeline xử lý retroactive price
> adjustment, có data quality gate và idempotent backfill.
> *Postgres · Debezium · Kafka · StarRocks · dbt · Airflow · Docker*

**One-liner cho README:**

> End-to-end CDC pipeline for equity market data — handles retroactive price
> adjustments from corporate actions, with data quality gates and idempotent backfill.

---

## Nghiệp vụ

**Bài toán:** Giá cổ phiếu không phải append-only như người ta tưởng. Khi doanh
nghiệp chia cổ tức hoặc split, **toàn bộ giá lịch sử của mã đó bị điều chỉnh lại**.
Một sự kiện hôm nay làm thay đổi dữ liệu của 5 năm trước.

Đây là loại bài toán mà pipeline "load thêm data mới mỗi ngày" xử lý sai hoàn toàn:
nó không phát hiện được thay đổi ngược, và nếu backfill thô thì phá dữ liệu đang có.

**Pipeline giải quyết:**

1. Phát hiện thay đổi ở source (kể cả thay đổi trên bản ghi cũ) — CDC
2. Phân loại: data mới vs. data cũ bị điều chỉnh
3. Backfill đúng phạm vi ảnh hưởng, không touch phần còn lại
4. Chặn dữ liệu sai trước khi vào mart — data quality gate

**Câu hỏi mart trả lời:**

- OHLCV theo tuần/tháng cho từng mã
- Rolling volatility — phát hiện biến động bất thường
- Adjustment ảnh hưởng bao nhiêu % giá lịch sử, mã nào bị nhiều nhất
- Lệch giữa giá thô và giá điều chỉnh theo thời gian

**Vì sao bài toán này đáng chọn** (theo 4 câu sàng lọc)

| | |
|---|---|
| Data ở đâu | API public, không key, cập nhật hàng ngày + stream 24/7 |
| Ai quan tâm | Theo dõi giá, cảnh báo bất thường, tính đúng lợi nhuận lịch sử |
| Pipeline có gì kể | Ingest → CDC → transform → store, có retry, test, backfill, scheduling |
| Giải thích 30 giây | "Xử lý dữ liệu bị điều chỉnh ngược về quá khứ" — cụ thể, không chung chung |

---

## Nguồn dataset

| Nguồn | Loại | Auth | Vai trò |
|---|---|---|---|
| **yfinance** | REST, batch | Không | Nến ngày, corporate action. Luồng CDC chính |
| **Binance WebSocket** | Stream, 24/7 | Không | Luồng event thật, biện minh cho Kafka |
| *TCBS/VCI (tuỳ chọn)* | REST không docs | Không | Data VN, cắm sau qua adapter interface |

**Nguyên tắc chọn:** zero-credential. Ai clone repo về cũng `docker compose up`
là chạy. Đây là property mà phần lớn portfolio project trên GitHub không có.

Ticker: mã VN qua suffix `.VN` (FPT.VN, VNM.VN, HPG.VN, VCB.VN) — verify độ phủ
và corporate action ở Phase 0 trước khi cam kết.

---

## Tech stack

| Layer | Công cụ | Vì sao |
|---|---|---|
| Source | yfinance, Binance WS | Public, không key, có mutation thật |
| OLTP | PostgreSQL | `UPSERT` sinh UPDATE; logical replication cho CDC |
| CDC | Debezium (Kafka Connect) | Bắt `c`/`u`/`d` từ WAL |
| Streaming | Kafka (KRaft mode) | Transport của Debezium + Binance stream |
| Warehouse | StarRocks | Primary Key model xử lý upsert CDC native |
| Transform | dbt | Test, incremental, snapshot, docs — ELT in-warehouse |
| Orchestration | Airflow | Schedule crawl + dbt build, fail khi test fail |
| Infra | Docker Compose | Reproducible, chia profile để nhẹ RAM |

**Không dùng:** Zookeeper (Kafka 3.x+ dùng KRaft), Spark (transform ở dbt),
Snowflake (trial 30 ngày), Redpanda (mất keyword Kafka).

---

## Kiến trúc

```
┌─────────────┐
│  yfinance   │──poll──┐
└─────────────┘        │
                       ▼
              ┌─────────────────┐
              │    Postgres     │  daily_bar (PK: symbol, trade_date)
              │     (OLTP)      │  instrument, corporate_action
              └────────┬────────┘
                       │ WAL → Debezium
                       ▼
              ┌─────────────────┐
┌─────────────┐│  Kafka (KRaft)  │
│ Binance WS  │├────────────────►│
└─────────────┘└────────┬────────┘
                        ▼
              ┌─────────────────┐
              │   StarRocks     │  raw CDC (op, op_ts)
              │  (Primary Key)  │
              └────────┬────────┘
                       │ dbt
                       ▼
         staging → dim/fact → mart

        Airflow: crawl → dbt build → test gate
```

---

## Folder structure

```
market-cdc-pipeline/
├── README.md                    # sơ đồ, design decisions, cách chạy
├── docker-compose.yml           # Kafka KRaft, Postgres, StarRocks, Airflow
├── docker-compose.override.yml  # profile nhẹ cho dev
├── .env.example
│
├── docs/
│   ├── architecture.md          # sơ đồ + vì sao từng thành phần
│   ├── data-dictionary.md       # cột nào nghĩa gì
│   ├── design-decisions.md      # trade-off từng lựa chọn
│   └── data-source-notes.md     # kết quả verify Phase 0
│
├── ingestion/
│   ├── sources/
│   │   ├── base.py              # MarketDataSource protocol
│   │   ├── yfinance_source.py
│   │   └── tcbs_source.py       # để trống, cắm sau
│   ├── schemas.py               # Pydantic contract
│   ├── loader.py                # UPSERT vào Postgres
│   └── stream/
│       └── binance_ws.py
│
├── postgres/
│   └── init/
│       ├── 01_schema.sql
│       └── 02_replication.sql   # wal_level, publication
│
├── debezium/
│   └── connectors/
│       ├── postgres-source.json
│       └── starrocks-sink.json
│
├── starrocks/
│   └── ddl/
│       └── raw_tables.sql       # Primary Key model
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/             # stg_daily_bar, stg_instrument, ...
│   │   ├── marts/               # dim_instrument, fact_daily_bar, ...
│   │   └── mart/                # mart_price_summary, mart_volatility, ...
│   ├── tests/                   # singular tests
│   ├── snapshots/               # SCD2 cho instrument
│   └── macros/
│
├── airflow/
│   └── dags/
│       ├── ingest_daily.py
│       └── backfill.py
│
└── scripts/
    └── verify_source.py         # script Phase 0
```

**Nguyên tắc:** folder theo **data flow order** (ingestion → postgres → debezium
→ starrocks → dbt → airflow), không theo loại file. Đọc từ trên xuống là hiểu
luồng dữ liệu chảy thế nào.

---

## Điểm khác biệt (thứ người phỏng vấn thật sự hỏi)

Danh sách công cụ thì ai cũng có. Bốn thứ này ít portfolio nào làm:

1. **Idempotency** — chạy lại DAG run không sinh bản trùng
   (`incremental` + `unique_key` + `merge`)
2. **Retroactive update & backfill** — chạy lại một khoảng quá khứ không phá
   dữ liệu hiện có; xác định đúng phạm vi ảnh hưởng khi có corporate action mới
3. **Data quality gate** — dbt test fail thì DAG fail, không âm thầm ghi data sai
4. **README tử tế** — sơ đồ, design decisions kèm lý do, cách chạy lại từ đầu

Một project vừa phải có bốn thứ trên ăn điểm cao hơn một project mười container
không có gì trong số đó.
