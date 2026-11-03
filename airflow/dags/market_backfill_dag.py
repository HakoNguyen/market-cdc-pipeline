from datetime import date

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from market_tasks import DEFAULT_ARGS, LOCAL_TZ, dbt_build, wait_for_cdc_sync
from ingestion.batch.backfill_ingest import run_backfill


def backfill_from_params(params, **_):
    end = date.fromisoformat(params["end_date"]) if params["end_date"] else pendulum.now(LOCAL_TZ).date()
    return run_backfill(params["symbols"], date.fromisoformat(params["start_date"]), end)


with DAG(
    "market_backfill",
    default_args=DEFAULT_ARGS,
    description="Nạp lại một khoảng ngày cho các mã chọn trước, rồi dbt build",
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    schedule=None,  # chỉ trigger tay
    catchup=False,
    max_active_runs=1,
    params={
        "symbols": Param(
            [], type="array", items={"type": "string"},
            description="Mỗi dòng 1 mã, vd. FPT. Để trống = cả rổ VN30.",
        ),
        # Không có default -> bắt buộc nhập khi trigger
        "start_date": Param(type="string", format="date", description="Ngày bắt đầu (YYYY-MM-DD)."),
        "end_date": Param(
            None, type=["null", "string"], format="date",
            description="Ngày kết thúc (YYYY-MM-DD). Để trống = hôm nay (giờ VN).",
        ),
    },
    tags=["market", "backfill"],
) as dag:

    backfill = PythonOperator(task_id="backfill_market_data", python_callable=backfill_from_params)

    # Không chạy source freshness: backfill dữ liệu cũ không nói gì về độ mới của nguồn
    backfill >> wait_for_cdc_sync() >> dbt_build()
