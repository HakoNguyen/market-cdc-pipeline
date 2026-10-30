# Lớp bọc mỏng quanh docker compose và scripts/.
# Windows không có make: chạy lệnh tương ứng trực tiếp trong Git Bash (vd. `bash scripts/init.sh`).
.DEFAULT_GOAL := help
.PHONY: help up init status ingest down reset

help: ## Liệt kê các lệnh
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  make %-8s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

up: .env ## Build và khởi động toàn bộ stack, chờ tới khi healthy
	docker compose up -d --build --wait --wait-timeout 300

init: up ## Khởi tạo topics, StarRocks DDL, Debezium connector, Routine Load, bật DAG (idempotent)
	bash scripts/init.sh

status: ## Trạng thái services, connector, Routine Load, số dòng nguồn vs đích
	bash scripts/status.sh

ingest: ## Trigger DAG market_daily_pipeline ngay
	docker compose exec airflow airflow dags trigger market_daily_pipeline

down: ## Dừng stack, GIỮ dữ liệu
	docker compose down

reset: ## Dừng stack và XOÁ toàn bộ dữ liệu (volumes)
	docker compose down -v
