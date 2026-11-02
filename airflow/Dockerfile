FROM apache/airflow:2.8.1-python3.11

# Ingestion deps vào môi trường Airflow; ghim apache-airflow để pip không nâng/hạ nó
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

# dbt trong venv riêng để không xung đột dependency với Airflow
# (image gốc đặt PIP_USER=true, phải tắt khi cài vào venv)
RUN python -m venv /home/airflow/dbt_venv \
 && PIP_USER=false /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.5 dbt-starrocks==1.12.2

# Thêm vào CUỐI PATH: `dbt` gọi được trực tiếp, `python` vẫn là python của Airflow
ENV PATH="${PATH}:/home/airflow/dbt_venv/bin"
