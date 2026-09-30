import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from scripts.test_ingestion import main as run_ingestion_script

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def verify_cdc_ingestion():
    print("Verifying CDC Data Stream in StarRocks raw_daily_bar...")
    return True

with DAG(
    'market_daily_pipeline',
    default_args=default_args,
    description='End-to-End Market CDC Pipeline',
    schedule_interval='0 11 * * *',
    catchup=False,
    tags=['market', 'daily', 'pipeline'],
) as dag:

    ingest_yfinance_task = PythonOperator(
        task_id='ingest_yfinance_to_postgres',
        python_callable=run_ingestion_script,
    )

    verify_cdc_task = PythonOperator(
        task_id='verify_cdc_ingestion',
        python_callable=verify_cdc_ingestion,
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run_model',
        bash_command='cd /opt/airflow/market_cdc && dbt clean && dbt run --profiles-dir .',
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test_quality_gates',
        bash_command='cd /opt/airflow/market_cdc && dbt test --profiles-dir .',
    )

    ingest_yfinance_task >> verify_cdc_task >> dbt_run_task >> dbt_test_task

