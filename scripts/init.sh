#!/usr/bin/env bash
# Khởi tạo pipeline: Kafka topics -> StarRocks DDL -> Debezium connector -> Routine Load -> bật DAG.
# Idempotent: chạy lại bao nhiêu lần cũng được, bước nào đã xong thì bỏ qua / cập nhật.
#
# Dùng: bash scripts/init.sh   (hoặc `make init`)
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash trên Windows: không tự đổi /opt/... thành C:/Program Files/Git/opt/...
cd "$(dirname "$0")/.."

CONNECT_URL="http://localhost:8083"
CDC_TOPICS="market.public.daily_bar market.public.instrument market.public.corporate_action"

log() { printf '\n==> %s\n' "$*"; }
sr()  { docker compose exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot "$@"; }

log "Khởi động stack và chờ các service healthy"
docker compose up -d --wait --wait-timeout 300

# Tạo topic trước Routine Load: job tạo trên topic chưa tồn tại sẽ bị PAUSED (unknown topic)
log "Kafka: tạo CDC topics"
for t in $CDC_TOPICS; do
  # Bỏ cảnh báo vô hại về '.' và '_' trong tên topic, nhưng vẫn in toàn bộ output nếu lệnh lỗi
  out=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 2>&1) || { echo "$out"; exit 1; }
  grep -v "limitations in metric names" <<< "$out" || true
done
echo "OK"

log "StarRocks: tạo database và raw tables"
sr < starrocks/ddl/raw_tables.sql
echo "OK"

# PUT /connectors/<name>/config: tạo mới nếu chưa có, cập nhật nếu đã có; offset được giữ nguyên
log "Debezium: tạo/cập nhật connectors"
for f in debezium/connectors/*.json; do
  name=$(basename "$f" .json)
  curl -fsS -X PUT -H "Content-Type: application/json" --data "@$f" \
    "$CONNECT_URL/connectors/$name/config" > /dev/null
  for _ in $(seq 1 30); do
    curl -fsS "$CONNECT_URL/connectors/$name/status" 2>/dev/null | grep -q '"tasks":\[{"id":0,"state":"RUNNING"' && break
    sleep 2
  done
  echo "$name: $(curl -fsS "$CONNECT_URL/connectors/$name/status" | grep -o '"state":"[A-Z]*"' | tail -1)"
done

# CREATE ROUTINE LOAD không có IF NOT EXISTS -> kiểm tra trước, mỗi file là một job cùng tên
log "StarRocks: tạo Routine Load jobs"
for f in starrocks/ddl/routine_load/*.sql; do
  job=$(basename "$f" .sql)
  if sr -N -e "SHOW ROUTINE LOAD FOR market_db.$job" 2>/dev/null | grep -q .; then
    echo "$job: đã tồn tại, bỏ qua"
  else
    sr market_db < "$f"
    echo "$job: đã tạo"
  fi
done

# Bật DAG sau cùng, khi pipeline đã sẵn sàng; scheduler sẽ tự chạy lần đầu (catchup=False)
log "Airflow: bật DAG market_daily_pipeline"
docker compose exec -T airflow airflow dags unpause market_daily_pipeline > /dev/null
echo "OK"

bash scripts/status.sh
