from datetime import date

import pytest

from ingestion.source.vndirect_source import PAGE_SIZE, VNDirectSource


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Trả các trang dữ liệu dựng sẵn theo endpoint, ghi lại từng lần gọi."""

    def __init__(self, pages_by_path):
        self.pages_by_path = pages_by_path
        self.headers = {}
        self.calls = []

    def get(self, url, params, timeout):
        path = url.rsplit("/", 1)[-1]
        self.calls.append((path, params))
        pages = self.pages_by_path[path]
        page = params["page"]
        return FakeResponse({"data": pages[page - 1] if page <= len(pages) else []})


def bar(day, close=126.03, volume=6494600.0):
    return {"date": day, "open": 128.0, "high": 128.0, "low": 126.0, "close": close,
            "adClose": 97.266, "nmVolume": volume}


def test_daily_bars_are_raw_prices_in_vnd():
    session = FakeSession({"stock_prices": [[bar("2025-07-18")]]})

    bars = VNDirectSource(session=session).get_daily_bars("FPT", date(2025, 7, 18), date(2025, 7, 18))

    assert len(bars) == 1
    assert bars[0].close == 126030.0  # close thô x1000, không phải adClose
    assert bars[0].open == 128000.0
    assert bars[0].volume == 6494600
    assert bars[0].trade_date == date(2025, 7, 18)


def test_pagination_stops_at_first_short_page():
    full_page = [bar(f"2024-01-{i % 28 + 1:02d}") for i in range(PAGE_SIZE)]
    session = FakeSession({"stock_prices": [full_page, [bar("2025-01-02")]]})

    bars = VNDirectSource(session=session).get_daily_bars("FPT", date(2024, 1, 1), date(2025, 1, 2))

    assert len(bars) == PAGE_SIZE + 1
    assert [p["page"] for _, p in session.calls] == [1, 2]


def test_corporate_actions_mapping():
    events = [
        {"type": "DIVIDEND", "effectiveDate": "2024-06-12", "dividend": 1000.0, "ratio": 10.0},
        # cổ tức cổ phiếu + thưởng cùng ngày -> gộp tỷ lệ: 1 + (10 + 5)/100
        {"type": "KINDDIV", "effectiveDate": "2024-06-12", "ratio": 10.0},
        {"type": "STOCKDIV", "effectiveDate": "2024-06-12", "ratio": 5.0},
        {"type": "ISSUE", "effectiveDate": "2024-09-23", "ratio": 10.0, "price": 15000.0},  # chờ schema 1.2
        {"type": "MEETING", "effectiveDate": "2024-04-20"},
        {"type": "DIVIDEND", "dividend": 500.0},  # thiếu ngày GDKHQ -> bỏ
    ]
    session = FakeSession({"events": [events]})

    actions = VNDirectSource(session=session).get_corporate_actions("FPT")

    assert [(a.action_date, a.action_type, a.value) for a in actions] == [
        (date(2024, 6, 12), "dividend", 1000.0),
        (date(2024, 6, 12), "split", 1.15),
    ]


def test_instrument_info():
    session = FakeSession({
        "stocks": [[{"code": "FPT", "floor": "HOSE", "status": "listed",
                     "companyName": "Công ty Cổ phần FPT", "companyNameEng": "FPT Corporation"}]],
        "industry_classification": [[{"englishName": "TECHNOLOGY "}]],
    })

    inst = VNDirectSource(session=session).get_instrument_info("FPT")

    assert (inst.name, inst.exchange, inst.sector, inst.is_listed) == ("FPT Corporation", "HOSE", "Technology", True)


@pytest.mark.integration
def test_live_prices_are_unadjusted_at_ex_date():
    """FPT thưởng 15%, GDKHQ 2025-07-21: giá thô phải giảm ~13% (1/1.15), giá đã điều chỉnh thì không."""
    source = VNDirectSource()
    closes = {b.trade_date: b.close for b in source.get_daily_bars("FPT", date(2025, 7, 18), date(2025, 7, 21))}

    ratio = closes[date(2025, 7, 21)] / closes[date(2025, 7, 18)]

    # 1/1.15 = 0.8696, cộng/trừ biên độ dao động HOSE 7% trong phiên
    assert 0.8696 * 0.93 < ratio < 0.8696 * 1.07

    bonus = [a for a in source.get_corporate_actions("FPT") if a.action_date == date(2025, 7, 21)]
    assert [(a.action_type, a.value) for a in bonus] == [("split", 1.15)]
