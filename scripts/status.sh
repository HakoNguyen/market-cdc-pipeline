#!/usr/bin/env bash
# Trạng thái nhanh của pipeline: services, Debezium connector, Routine Load, số dòng nguồn vs đích.
#
# Dùng: bash scripts/status.sh   (hoặc `make status`)
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."

sr() { docker compose exec -T starrocks mysql -h127.0.0.1 -P9030 -uroot "$@" 2>/dev/null; }
pg() { docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$0"' "$1" 2>/dev/null; }

printf '\n==> Services\n'
docker compose ps --format "table {{.Service}}\t{{.Status}}"

printf '\n==> Debezium connectors\n'
for name in $(curl -fsS http://localhost:8083/connectors 2>/dev/null | tr -d '[]"' | tr ',' ' '); do
  echo "$name: $(curl -fsS "http://localhost:8083/connectors/$name/status" | grep -o '"state":"[A-Z]*"' | tail -1)"
done

printf '\n==> Routine Load\n'
sr -e "SHOW ROUTINE LOAD FROM market_db\G" | grep -E "^ +(Name|State|Progress):" | sed -E 's/^ +//' | paste - - - | column -t

printf '\n==> Số dòng Postgres -> StarRocks raw\n'
for t in daily_bar instrument corporate_action; do
  printf "%-18s %6s -> %s\n" "$t" "$(pg "SELECT count(*) FROM $t")" "$(sr -N -e "SELECT count(*) FROM market_db.raw_$t")"
done
