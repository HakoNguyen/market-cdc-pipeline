import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from market_tasks import DEFAULT_ARGS, LOCAL_TZ, dbt_build, dbt_source_freshness, wait_for_cdc_sync
from ingestion.batch.backfill_ingest import daily_window, run_backfill
from ingestion.config import VN30_TICKERS


def ingest_recent_window(data_interval_end, **_):
    # Run lúc 17:00 thứ 2 có data_interval_end = thứ 2 17:00 (giờ VN) -> nạp thứ 4 tuần trước .. thứ 2.
    # Cửa sổ tính từ data interval chứ không từ now(): chạy lại run cũ nạp đúng cửa sổ cũ.
    start, end = daily_window(data_interval_end.in_timezone(LOCAL_TZ).date())
    return run_backfill(VN30_TICKERS, start, end)


with DAG(
    "market_daily",
    default_args=DEFAULT_ARGS,
    description="VN30: nạp 5 ngày gần nhất -> chờ CDC -> dbt freshness + build",
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    # HOSE đóng cửa 14:45, thoả thuận tới 15:00; 17:00 để API nguồn kịp cập nhật
    schedule="0 17 * * 1-5",
    # Không catchup: cửa sổ 5 ngày đã lấp các phiên bị lỡ, không chồng thêm cơ chế thứ 2
    catchup=False,
    max_active_runs=1,
    tags=["market", "daily"],
) as dag:

    ingest = PythonOperator(task_id="ingest_market_data", python_callable=ingest_recent_window)

    ingest >> wait_for_cdc_sync() >> dbt_source_freshness() >> dbt_build()
