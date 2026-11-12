#!/usr/bin/env bash
# Nạp lịch sử VN30 qua DAG market_backfill (ingest -> chờ CDC -> dbt build) và chờ tới khi xong.
# Dùng: bash scripts/seed.sh [start_date]   (mặc định 2025-07-01: demo cần FPT quanh GDKHQ 2025-07-21)
set -euo pipefail
cd "$(dirname "$0")/.."

START_DATE="${1:-2025-07-01}"
RUN_ID="seed_$(date +%Y%m%d_%H%M%S)"
af() { docker compose exec -T airflow airflow "$@" 2>/dev/null; }

af dags trigger market_backfill -r "$RUN_ID" \
  -c "{\"symbols\": [], \"start_date\": \"$START_DATE\", \"end_date\": null}" > /dev/null
echo "market_backfill $RUN_ID: VN30 từ $START_DATE (xem tiến độ ở http://localhost:8088)"

for _ in $(seq 1 120); do  # tối đa 30 phút
  state=$(af dags list-runs -d market_backfill -o plain | awk -v r="$RUN_ID" '$2 == r {print $3}')
  case "$state" in
    success) echo "Xong."; exit 0 ;;
    failed)  echo "FAILED: xem log task trong Airflow UI" >&2; exit 1 ;;
  esac
  sleep 15
done
echo "Quá 30 phút vẫn chưa xong (trạng thái: ${state:-unknown})" >&2
exit 1
