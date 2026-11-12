"""on_failure_callback cho các DAG: luôn ghi log, gửi Slack nếu có SLACK_WEBHOOK_URL."""
import logging
import os

import requests

log = logging.getLogger(__name__)


def format_failure(context: dict) -> str:
    ti = context["task_instance"]
    return (
        f"[Airflow] Task FAILED: {ti.dag_id}.{ti.task_id} "
        f"(run_id={context.get('run_id')}, try={ti.try_number - 1}/{ti.max_tries + 1})\n"
        f"Error: {context.get('exception')!r}\n"
        f"Log: {ti.log_url}"
    )


def notify_failure(context: dict) -> None:
    message = format_failure(context)
    log.error(message)

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return
    # Callback lỗi không được làm hỏng thêm gì: gửi Slack thất bại thì chỉ ghi log
    try:
        requests.post(webhook, json={"text": message}, timeout=10).raise_for_status()
    except requests.RequestException as e:
        log.warning("Gửi Slack thất bại: %s", e)
