#!/bin/bash
# User riêng cho Debezium: chỉ có quyền REPLICATION + SELECT, không dùng superuser postgres.
# Viết bằng .sh (không phải .sql) để đọc mật khẩu từ biến môi trường.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    create role ${DEBEZIUM_DB_USER} with login replication password '${DEBEZIUM_DB_PASSWORD}';
    grant select on all tables in schema public to ${DEBEZIUM_DB_USER};
    alter default privileges in schema public grant select on tables to ${DEBEZIUM_DB_USER};
EOSQL
