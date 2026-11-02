import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from airflow.sensors.python import PythonSensor
from ingestion.monitoring.cdc_check import cdc_caught_up
from scripts.test_ingestion import main as run_ingestion_script

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'market_daily_pipeline',
    default_args=default_args,
    description='End-to-End Market CDC Pipeline',
    schedule_interval='0 11 * * *',
    catchup=False,
    tags=['market', 'daily', 'pipeline'],
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_market_data_to_postgres',
        python_callable=run_ingestion_script,
    )

    # CDC chạy bất đồng bộ: đợi StarRocks khớp Postgres rồi mới cho dbt chạy
    wait_for_cdc_task = PythonSensor(
        task_id='wait_for_cdc_sync',
        python_callable=cdc_caught_up,
        mode='reschedule',       # giữa các lần kiểm tra, nhả slot worker cho task khác
        poke_interval=15,        # kiểm tra lại sau mỗi 15 giây
        timeout=10 * 60,         # quá 10 phút vẫn chưa khớp -> FAIL
    )

    # Nguồn có dữ liệu mới không (API chết thì Postgres và StarRocks cùng cũ, sensor vẫn cho qua).
    # warn -> chỉ cảnh báo; error -> task fail, không build trên dữ liệu cũ.
    dbt_freshness_task = BashOperator(
        task_id='dbt_source_freshness',
        bash_command='cd /opt/airflow/market_cdc && dbt source freshness --profiles-dir .',
    )

    # build = run + test theo thứ tự DAG của dbt: test của model nào fail thì các model phía sau bị SKIP
    dbt_build_task = BashOperator(
        task_id='dbt_build',
        bash_command='cd /opt/airflow/market_cdc && dbt build --profiles-dir .',
    )

    ingest_task >> wait_for_cdc_task >> dbt_freshness_task >> dbt_build_task

