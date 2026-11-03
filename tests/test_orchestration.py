from datetime import date
from types import SimpleNamespace

import pytest

from ingestion.batch import backfill_ingest
from ingestion.batch.backfill_ingest import daily_window, normalize_symbols, run_backfill
from ingestion.config import VN30_TICKERS
from ingestion.monitoring import alerts


def test_normalize_symbols_cleans_and_dedupes():
    assert normalize_symbols([" fpt", "HPG", "FPT", ""]) == ["FPT", "HPG"]


@pytest.mark.parametrize("empty", [None, [], ["", "  "]])
def test_normalize_symbols_empty_means_vn30(empty):
    assert normalize_symbols(empty) == VN30_TICKERS


def test_daily_window_is_five_days_back():
    assert daily_window(date(2026, 11, 2)) == (date(2026, 10, 28), date(2026, 11, 2))


def test_run_backfill_rejects_inverted_range(monkeypatch):
    # Kiểm tra tham số trước khi gọi API/DB
    monkeypatch.setattr(backfill_ingest, "VNDirectSource", lambda: None)
    monkeypatch.setattr(backfill_ingest, "PostgresLoader", lambda: None)
    with pytest.raises(ValueError):
        run_backfill(["FPT"], date(2026, 11, 2), date(2026, 11, 1))


def _context():
    ti = SimpleNamespace(dag_id="market_daily", task_id="dbt_build", try_number=3, max_tries=1,
                         log_url="http://localhost:8080/log")
    return {"task_instance": ti, "run_id": "scheduled__x", "exception": RuntimeError("boom")}


def test_notify_failure_without_webhook_only_logs(monkeypatch, caplog):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(alerts.requests, "post", lambda *a, **k: pytest.fail("không được gọi Slack"))
    alerts.notify_failure(_context())
    assert "market_daily.dbt_build" in caplog.text and "boom" in caplog.text


def test_notify_failure_swallows_slack_errors(monkeypatch, caplog):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.invalid/x")

    def broken_post(*a, **k):
        raise alerts.requests.ConnectionError("down")

    monkeypatch.setattr(alerts.requests, "post", broken_post)
    alerts.notify_failure(_context())  # không raise
    assert "Slack" in caplog.text
