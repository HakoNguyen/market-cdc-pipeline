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
            "adClose": 97.266, "nmVolume": volume,
            "basicPrice": 127.0, "ceilingPrice": 135.8, "floorPrice": 118.2}


def test_daily_bars_are_raw_prices_in_vnd():
    session = FakeSession({"stock_prices": [[bar("2025-07-18")]]})

    bars, _ = VNDirectSource(session=session).get_daily_bars("FPT", date(2025, 7, 18), date(2025, 7, 18))

    assert len(bars) == 1
    assert bars[0].close == 126030.0  # close thô x1000, không phải adClose
    assert bars[0].open == 128000.0
    assert bars[0].volume == 6494600
    assert bars[0].trade_date == date(2025, 7, 18)
    # giá tham chiếu / trần / sàn của sàn (HOSE ±7%)
    assert (bars[0].reference_price, bars[0].ceiling_price, bars[0].floor_price) == (127000.0, 135800.0, 118200.0)


def test_daily_bars_without_exchange_prices_keep_none():
    row = bar("2025-07-18")
    for key in ("basicPrice", "ceilingPrice", "floorPrice"):
        del row[key]
    session = FakeSession({"stock_prices": [[row]]})

    bars, _ = VNDirectSource(session=session).get_daily_bars("FPT", date(2025, 7, 18), date(2025, 7, 18))

    assert (bars[0].reference_price, bars[0].ceiling_price, bars[0].floor_price) == (None, None, None)


def test_pagination_stops_at_first_short_page():
    full_page = [bar(f"2024-01-{i % 28 + 1:02d}") for i in range(PAGE_SIZE)]
    session = FakeSession({"stock_prices": [full_page, [bar("2025-01-02")]]})

    bars, _ = VNDirectSource(session=session).get_daily_bars("FPT", date(2024, 1, 1), date(2025, 1, 2))

    assert len(bars) == PAGE_SIZE + 1
    assert [p["page"] for _, p in session.calls] == [1, 2]


def test_corporate_actions_mapping():
    events = [
        # VNM: 2 kỳ cổ tức cùng ngày GDKHQ -> cộng tiền, lấy ngày công bố sớm nhất
        {"type": "DIVIDEND", "effectiveDate": "2024-09-24", "dividend": 950.0, "disclosureDate": "2024-09-10"},
        {"type": "DIVIDEND", "effectiveDate": "2024-09-24", "dividend": 1500.0, "disclosureDate": "2024-09-12"},
        {"type": "KINDDIV", "effectiveDate": "2025-07-21", "ratio": 15.0, "disclosureDate": "2025-07-10"},
        {"type": "STOCKDIV", "effectiveDate": "2025-10-29", "ratio": 10.0, "disclosureDate": "2025-10-15"},
        {"type": "ISSUE", "effectiveDate": "2025-12-08", "ratio": 20.0, "price": 15000.0, "disclosureDate": "2025-11-20"},
        {"type": "MEETING", "effectiveDate": "2024-04-20"},   # không làm thay đổi giá -> bỏ
        {"type": "DIVIDEND", "dividend": 500.0},               # thiếu ngày GDKHQ -> bỏ
    ]
    session = FakeSession({"events": [events]})

    actions = VNDirectSource(session=session).get_corporate_actions("VNM")

    assert [(a.ex_date, a.action_type, a.cash_amount, a.ratio, a.subscription_price, a.announce_date)
            for a in actions] == [
        (date(2024, 9, 24), "cash_dividend", 2450.0, None, None, date(2024, 9, 10)),
        (date(2025, 7, 21), "bonus_share", None, 0.15, None, date(2025, 7, 10)),
        (date(2025, 10, 29), "stock_dividend", None, 0.1, None, date(2025, 10, 15)),
        (date(2025, 12, 8), "rights_issue", None, 0.2, 15000.0, date(2025, 11, 20)),
    ]


def test_conflicting_rights_issues_on_same_day_raise():
    events = [
        {"type": "ISSUE", "effectiveDate": "2025-12-08", "ratio": 20.0, "price": 15000.0, "disclosureDate": "2025-11-20"},
        {"type": "ISSUE", "effectiveDate": "2025-12-08", "ratio": 10.0, "price": 12000.0, "disclosureDate": "2025-11-20"},
    ]
    session = FakeSession({"events": [events]})

    with pytest.raises(ValueError):
        VNDirectSource(session=session).get_corporate_actions("SSI")


def test_instrument_info():
    session = FakeSession({
        "stocks": [[{"code": "FPT", "floor": "HOSE", "status": "listed",
                     "companyName": "Công ty Cổ phần FPT", "companyNameEng": "FPT Corporation"}]],
        "industry_classification": [[{"englishName": "TECHNOLOGY "}]],
    })

    inst = VNDirectSource(session=session).get_instrument_info("FPT")

    assert (inst.name, inst.exchange, inst.sector, inst.is_listed) == ("FPT Corporation", "HOSE", "Technology", True)

def test_bad_row_are_rejected_not_fatal():
    good = bar("2025-07-17")
    high_below_low = bar("2025-07-18")
    high_below_low["high"] = 100.0  # low = 126 -> Pydantic loại
    missing_open = bar("2025-07-21")
    missing_open["open"] = None  # None * 1000 -> TypeError
    session = FakeSession({"stock_prices": [[good, high_below_low, missing_open]]})

    bars, rejected = VNDirectSource(session=session).get_daily_bars("FPT", date(2025, 7, 17), date(2025, 7, 21))

    assert [b.trade_date for b in bars] == [date(2025, 7, 17)]    
    assert len(rejected) == 2
    assert rejected[0]["row"]["date"] == "2025-07-18"             
    assert "high" in rejected[0]["error"]       


def test_unknown_symbol_raises_clear_error():
    session = FakeSession({"stocks": [[]]})
    with pytest.raises(ValueError, match="ZZZZ"):
        VNDirectSource(session=session).get_instrument_info("ZZZZ")


@pytest.mark.integration
def test_live_prices_are_unadjusted_at_ex_date():
    """FPT thưởng 15%, GDKHQ 2025-07-21: giá thô phải giảm ~13% (1/1.15), giá đã điều chỉnh thì không."""
    source = VNDirectSource()
    bars, _ = source.get_daily_bars("FPT", date(2025, 7, 18), date(2025, 7, 21))
    closes = {b.trade_date: b.close for b in bars}

    ratio = closes[date(2025, 7, 21)] / closes[date(2025, 7, 18)]

    # 1/1.15 = 0.8696, cộng/trừ biên độ dao động HOSE 7% trong phiên
    assert 0.8696 * 0.93 < ratio < 0.8696 * 1.07

    bonus = [a for a in source.get_corporate_actions("FPT") if a.ex_date == date(2025, 7, 21)]
    assert [(a.action_type, a.ratio) for a in bonus] == [("bonus_share", 0.15)]
