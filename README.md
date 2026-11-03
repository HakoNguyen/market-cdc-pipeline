# Market CDC Pipeline

Change-data-capture pipeline for Vietnamese equities (VN30). Raw daily prices and corporate actions live in PostgreSQL; Debezium streams every change through Kafka into StarRocks; dbt turns them into **corporate-action-adjusted** prices and indicators; Airflow orchestrates the daily run.

```
VNDirect API → PostgreSQL → Debezium → Kafka → StarRocks → dbt → analytics marts
                                  orchestrated by Airflow
```

## The problem

Vietnamese stock prices "break" on every ex-date (ngày GDKHQ): cash dividends, stock dividends, bonus shares and rights issues all push the reference price down overnight. Compute returns or moving averages on raw prices and you get crashes that never happened.

FPT, 15% bonus shares, ex-date 2025-07-21 (actual output of this pipeline):

| Date | Raw close (VND) | Raw return | Adjusted close | Adjusted return |
|---|---:|---:|---:|---:|
| 2025-07-18 | 126,030 | −0.76% | 99,628 | −0.76% |
| 2025-07-21 | 110,300 | **−12.48%** | 100,273 | **+0.65%** |

Adjusted prices are in today's share terms, so they also include FPT's later 10% bonus (2026-09-21).

Two things make this a data engineering problem rather than a one-off calculation:

1. **One event rewrites the whole history.** A new corporate action changes the adjusted price of every earlier session of that stock.
2. **Events change after they are announced.** Corporate actions get corrected, rescheduled or cancelled, and prices occasionally get corrected too.

So the pipeline keeps **raw prices and events as the immutable source of truth** in Postgres, captures every change from the write-ahead log instead of reloading tables, and recomputes adjusted history in the warehouse.

## Architecture

```mermaid
flowchart LR
    src["VNDirect finfo API"]
    ing["Python ingestion<br/>Pydantic contracts"]
    arc[("data/raw/<br/>JSON archive")]
    pg[("PostgreSQL 16<br/>source of truth")]
    dbz["Debezium 2.5<br/>pgoutput"]
    kafka["Kafka 3.7 (KRaft)<br/>market.public.*"]
    sr[("StarRocks 4.1<br/>market_db.raw_*")]
    dbt["dbt<br/>staging → marts"]
    marts[("StarRocks<br/>analytics.*")]
    af{{"Airflow 2.8"}}

    src --> ing --> pg
    ing --> arc
    pg -->|"logical replication (WAL)"| dbz --> kafka
    kafka -->|"Routine Load"| sr --> dbt --> marts
    af -.->|ingest| ing
    af -.->|"dbt build"| dbt
```

| Layer | Tech | What it does |
|---|---|---|
| Source | VNDirect finfo API | Raw (unadjusted) daily OHLCV, corporate action events, instrument info |
| Ingestion | Python, Pydantic, psycopg2 | Validates records, archives raw JSON, upserts into Postgres only when a value actually changed |
| OLTP | PostgreSQL 16 | `daily_bar`, `corporate_action`, `instrument`; `wal_level=logical`, publication for Debezium |
| CDC | Debezium 2.5 + Kafka 3.7 | One topic per table (`market.public.<table>`) |
| Warehouse | StarRocks 4.1 | Routine Load jobs pull each topic into Primary Key tables |
| Transform | dbt (dbt-starrocks) | Staging views, adjustment factors, adjusted prices, SMA and returns |
| Orchestration | Airflow 2.8 (LocalExecutor) | `market_daily` (17:00 Mon–Fri, `Asia/Ho_Chi_Minh`): ingest → wait for CDC sync (sensor) → source freshness → dbt build. `market_backfill`: same ingestion function, manual trigger with parameters |

## How price adjustment works

Each event yields a factor that applies to every session **before** its ex-date. The cumulative factor for a session is the product of the factors of all later events, and `adj_price = raw_price × cumulative_factor`.

| Event | Factor | Status |
|---|---|---|
| Stock split, stock dividend, bonus shares (ratio *r*) | `1 / (1 + r)` | ✅ implemented |
| Cash dividend *D* VND/share | `(P_prev − D) / P_prev` | ingested, not yet applied |
| Rights issue (ratio *r*, price *S*) | `TERP / P_prev`, `TERP = (P_prev + r·S) / (1 + r)` | planned |

`P_prev` is the raw close of the session before the ex-date. Several events on the same day multiply. Only events whose ex-date has passed are applied, because they are announced weeks in advance. See [fct_adjusted_price.sql](market_cdc/models/marts/fct_adjusted_price.sql).

## Quick start

Requirements: Docker with Compose v2, plus `make` and `bash` (on Windows, use Git Bash and the commands in the second block).

```bash
make init     # build + start the stack, then create topics, tables, connector, Routine Load jobs, unpause the DAG
make status   # services, connector state, Routine Load state, row counts Postgres vs StarRocks
```

Without `make`:

```bash
cp .env.example .env
docker compose up -d --build --wait --wait-timeout 300
bash scripts/init.sh
```

`init` is idempotent: run it again at any time, finished steps are skipped or updated. Once the DAG is unpaused, Airflow runs it once straight away.

| Service | Address | Login |
|---|---|---|
| Airflow | http://localhost:8088 | `admin` / `admin` |
| Kafka UI (topics + Debezium connector) | http://localhost:8085 | – |
| Kafka Connect REST | http://localhost:8083 | – |
| StarRocks FE UI | http://localhost:8030 | `root`, no password |
| StarRocks SQL (MySQL protocol) | `localhost:9030` | `root`, no password |
| PostgreSQL (source) | `localhost:5435`, db `market_db` | `postgres` / `postgres` |
| Kafka (from host) | `localhost:9092` | – |

Credentials come from `.env` (copied from [.env.example](.env.example)). Postgres reads them only when its volume is first created; after changing them, run `make reset`.

| Command | Effect |
|---|---|
| `make up` | Build and start the stack, wait until healthy |
| `make init` | `up` + initialise the pipeline |
| `make status` | Pipeline health summary |
| `make ingest` | Trigger the daily DAG now |
| `make down` | Stop the stack, keep data |
| `make reset` | Stop the stack and **delete all volumes** |

## Loading data

`market_daily` reloads the last 5 days for the whole VN30 basket, counted back from the run's `data_interval_end`, so a missed session or a late correction at the source is picked up by the next run. It runs with `catchup=False`: the lookback window already covers missed runs. Re-running an old run reloads exactly the same window.

To load history, trigger `market_backfill` from the Airflow UI (*Trigger DAG w/ config*: symbols one per line, empty = VN30; `start_date` required; `end_date` empty = today) or from the CLI:

```bash
docker compose exec airflow airflow dags trigger market_backfill \
  -c '{"symbols": ["FPT", "HPG"], "start_date": "2025-07-01", "end_date": null}'
```

It runs the same ingestion function, waits for CDC, then runs `dbt build`. A task that fails after its retry is logged by `on_failure_callback`, and posted to Slack when `SLACK_WEBHOOK_URL` is set in `.env`.

The same function is available as a CLI from the host:

```bash
python -m venv .venv
source .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

python ingestion/batch/backfill_ingest.py                                       # VN30, resumes per symbol
python ingestion/batch/backfill_ingest.py --symbols FPT HPG --start-date 2025-07-01
```

Without dates, each symbol resumes from its latest `trade_date` minus 5 days, or loads one year if it has no data yet. Re-running a window is safe: unchanged rows produce no CDC events.

Then rebuild the models:

```bash
cd market_cdc
dbt build --profiles-dir .
```

And query the result in StarRocks:

```sql
SELECT trade_date, close, adj_close, daily_return_pct, sma_20
FROM analytics.fct_daily_bars
WHERE symbol = 'FPT' AND trade_date BETWEEN '2025-07-16' AND '2025-07-23'
ORDER BY trade_date;
```

## Data model

| Schema | Model | Materialization | Content |
|---|---|---|---|
| `market_db` | `raw_daily_bar`, `raw_corporate_action`, `raw_instrument` | Primary Key table | Latest row per key from CDC, plus `op`, `op_ts`, `_ingest_at` |
| `analytics` | `stg_daily_bar`, `stg_corporate_action`, `stg_instrument` | view | Typed columns, CDC metadata renamed, deletes are applied upstream (Routine Load maps them to StarRocks __op) |
| `analytics` | `fct_adjusted_price` | table | Raw OHLC, cumulative adjustment factor, adjusted OHLC |
| `analytics` | `int_daily_bar_indicators` | view | SMA 20/25/50 and daily return on adjusted close |
| `analytics` | `fct_daily_bars` | table | Raw OHLCV + adjusted close + indicators |
| `analytics` | `fct_corporate_actions`, `dim_instrument` | table | Events and instrument dimension |

Data quality: dbt `not_null`, `unique` and `relationships` tests, plus a custom test for non-positive prices and `high < low`. Pydantic rejects bad records at ingestion (prices > 0, `high >= low`, non-negative volume).

## Design decisions

- **Raw prices are never adjusted in place.** Writing adjusted prices back to the source would mean rewriting hundreds of rows per event, with no clean way to undo a cancelled event. Postgres keeps raw prices and events; dbt derives the adjusted history.
- **Log-based CDC instead of reloads.** Debezium reads the Postgres WAL, so corrections to events and prices reach the warehouse without full reloads or polling `updated_at`.
- **Upserts only write when something changed.** `ON CONFLICT … DO UPDATE … WHERE (…) IS DISTINCT FROM (…)` keeps overlapping re-runs from flooding Kafka with no-op updates.
- **StarRocks Routine Load instead of a Kafka Connect sink.** StarRocks pulls from Kafka itself, so there is no sink plugin to deploy, and each load task commits rows and offsets together. Primary Key tables make replays idempotent.
- **Bounded WAL retention.** `max_slot_wal_keep_size=2048MB`: if Debezium stays down too long, Postgres drops the replication slot instead of filling the disk. Recovery is to re-register the connector and let it re-snapshot.
- **The source must return raw prices.** yfinance (even with `auto_adjust=False`), DNSE and VCI all return prices already adjusted for stock dividends, which would be adjusted twice. VNDirect returns raw prices; [an integration test](tests/test_vndirect_source.py) checks the FPT gap at its ex-date against the live API.
- **No secrets in config files.** The connector JSON references `${env:POSTGRES_PASSWORD}` through Kafka Connect's `EnvVarConfigProvider`; all credentials come from `.env`.
- **dbt in its own virtualenv** inside the Airflow image, so dbt and Airflow dependencies never conflict.

## Tests

```bash
pytest -m "not integration"   # unit tests, no network
pytest                        # also hits the live VNDirect API
```

Unit tests use a fake HTTP session to check price units, pagination, event mapping (same-day events are combined) and instrument parsing.

## Status and roadmap

Working today: the full flow from the API to the marts, VN30 backfill, adjustment for share-based events, idempotent ingestion, one-command setup.

Known gaps, being worked on:

- **Corporate action coverage.** Cash dividends are stored but not yet applied, and rights issues are not ingested. Next: an explicit event schema (`cash_dividend`, `stock_dividend`, `bonus_share`, `rights_issue`, `split`) and the full set of factors above.
- **Deletes and ordering.** Routine Load reads `payload.after` from the Debezium envelope, which is empty for deletes, so a DELETE in Postgres does not yet remove the row in StarRocks. Next: Debezium's `ExtractNewRecordState` transform, mapping deletes to StarRocks' `__op`, `merge_condition` on LSN, decimals as strings.
- **Quality gate.** `verify_cdc_ingestion` is a placeholder. Next: a sensor that waits until StarRocks matches Postgres (row counts, `max(updated_at)`) before dbt runs, dbt source freshness, and a test that no adjusted return exceeds the exchange's daily price limit on an ex-date.
- **Presentation.** A before/after demo of a corporate action flowing through CDC, and CI (pytest, `dbt parse`, lint).

## Project structure

```
.
├── airflow/              # Airflow image (dbt in a separate venv) and DAGs
├── debezium/connectors/  # Postgres source connector config
├── ingestion/
│   ├── source/           # VNDirect client, Pydantic contracts, Postgres loader
│   └── batch/            # Backfill CLI
├── market_cdc/           # dbt project
├── postgres/init/        # OLTP schema, updated_at triggers, publication
├── scripts/              # init.sh, status.sh, ingestion entry point used by the DAG
├── starrocks/ddl/        # Raw tables, one Routine Load job per topic
├── tests/                # pytest: unit + live API
├── docker-compose.yml
└── Makefile
```
