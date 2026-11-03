"""Phần dùng chung của market_daily và market_backfill (không chứa DAG)."""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

from ingestion.monitoring.alerts import notify_failure
from ingestion.monitoring.cdc_check import cdc_caught_up

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
DBT_DIR = "/opt/airflow/market_cdc"

DEFAULT_ARGS = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,  # chỉ gọi khi đã hết retry
}


def wait_for_cdc_sync() -> PythonSensor:
    # CDC chạy bất đồng bộ: đợi StarRocks khớp Postgres rồi mới cho dbt chạy
    return PythonSensor(
        task_id="wait_for_cdc_sync",
        python_callable=cdc_caught_up,
        mode="reschedule",       # giữa các lần kiểm tra, nhả slot worker cho task khác
        poke_interval=15,        # kiểm tra lại sau mỗi 15 giây
        timeout=10 * 60,         # quá 10 phút vẫn chưa khớp -> FAIL
    )


def dbt_source_freshness() -> BashOperator:
    # Nguồn có dữ liệu mới không (API chết thì Postgres và StarRocks cùng cũ, sensor vẫn cho qua).
    # warn -> chỉ cảnh báo; error -> task fail, không build trên dữ liệu cũ.
    return BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --profiles-dir .",
    )


def dbt_build() -> BashOperator:
    # build = run + test theo thứ tự DAG của dbt: test của model nào fail thì các model phía sau bị SKIP
    return BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir .",
    )
