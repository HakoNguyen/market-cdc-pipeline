from datetime import date
from types import SimpleNamespace

import pytest

from ingestion.batch import backfill_ingest
from ingestion.batch.backfill_ingest import (_load_symbols, check_quality, daily_window,
                                             normalize_symbols, run_backfill)
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


def ok(bars=100, rejected=0):
    return {"bars": bars, "actions": 0, "rejected": rejected}


def test_quality_passes_below_threshold():
    check_quality({"FPT": ok(rejected=1)}, {})  # 1/101 < 1%


def test_quality_passes_on_empty_window():
    check_quality({"FPT": ok(bars=0)}, {})  # kỳ nghỉ: không có phiên nào


def test_quality_raises_on_failed_symbol():
    with pytest.raises(RuntimeError, match="HPG"):
        check_quality({"FPT": ok()}, {"HPG": "TimeoutError()"})


def test_quality_raises_when_reject_rate_too_high():
    with pytest.raises(RuntimeError, match="10/100"):
        check_quality({"FPT": ok(bars=90, rejected=10)}, {})


def test_quality_raises_when_symbol_fully_rejected():
    # tỉ lệ tổng 3/1003 < 1%, nhưng VNM mất hết dòng
    with pytest.raises(RuntimeError, match="VNM"):
        check_quality({"FPT": ok(bars=1000), "VNM": ok(bars=0, rejected=3)}, {})


class FakeBar:
    def model_dump(self):
        return {}


class FakeSource:
    def get_instrument_info(self, symbol):
        if symbol == "BAD":
            raise TimeoutError("api down")
        return SimpleNamespace(symbol=symbol)

    def get_daily_bars(self, symbol, start, end):
        return [FakeBar()], []

    def get_corporate_actions(self, symbol):
        return []


class FakeLoader:
    def __init__(self):
        self.loaded = []

    def load_instruments(self, inst):
        pass

    def save_raw_json(self, symbol, data_type, data):
        self.current = symbol

    def load_daily_bars(self, bars):
        self.loaded.append(self.current)


def test_one_failing_symbol_does_not_block_others():
    loader = FakeLoader()
    with pytest.raises(RuntimeError, match="BAD"):
        _load_symbols(FakeSource(), loader, ["FPT", "BAD", "HPG"],
                      date(2026, 10, 1), date(2026, 10, 15), auto_catchup=False)
    assert loader.loaded == ["FPT", "HPG"]  # HPG đứng sau BAD vẫn được nạp
