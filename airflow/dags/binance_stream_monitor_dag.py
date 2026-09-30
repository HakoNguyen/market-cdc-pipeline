from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from ingestion.stream.binance_ws import stream_binance_trade


def run_binance_stream():
    print("Starting Binance WebSocket Streaming...")
    try: 
        asyncio.run(stream_binance_trade())
    except Exception as e:
        print(f"Error: {e}")

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='binance_stream_monitor',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['binance', 'stream', 'monitor'],
) as dag:
    monitor_binance_task = PythonOperator(
        task_id='monitor_binance_websocket',
        python_callable=run_binance_stream,
    )
    