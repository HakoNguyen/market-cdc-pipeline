"""VNDirect finfo API (public, không cần key).

Trả giá THÔ (chưa điều chỉnh): bước nhảy tại ngày GDKHQ còn nguyên, ví dụ FPT thưởng 15%
ngày 2025-07-21: close 126.03 -> 110.3. yfinance thì đã điều chỉnh sẵn nên không dùng được.
Đã đối chiếu: hệ số tự tính từ giá thô + events khớp adClose của VNDirect, lệch tối đa 0.005%
trên 953 phiên FPT (2023-2026).
"""
from collections import defaultdict
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.source.schemas import CorporateActionRecord, DailyBarRecord, InstrumentRecord

BASE_URL = "https://api-finfo.vndirect.com.vn/v4"
PAGE_SIZE = 500
PRICE_UNIT = 1000  # API trả giá theo nghìn VND, lưu theo VND

# Sự kiện chia thêm cổ phiếu: cổ tức bằng cổ phiếu, thưởng cổ phiếu (ratio = số cp mới / 100 cp cũ)
SHARE_EVENT_TYPES = {"STOCKDIV", "KINDDIV"}


class VNDirectSource:
    def __init__(self, session: requests.Session | None = None, timeout: int = 30):
        if session is None:
            # API public đôi khi phản hồi rất chậm/timeout: retry GET với backoff 1s, 2s, 4s, 8s
            session = requests.Session()
            retry = Retry(total=4, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
                          allowed_methods=("GET",))
            session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session = session
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36",
            "Accept": "application/json",
        })
        self.timeout = timeout

    def _get_all(self, path: str, query: str, sort: str | None = None) -> list[dict]:
        rows, page = [], 1
        while True:
            params = {"q": query, "size": PAGE_SIZE, "page": page}
            if sort:
                params["sort"] = sort
            resp = self.session.get(f"{BASE_URL}/{path}", params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            rows.extend(data)
            if len(data) < PAGE_SIZE:
                return rows
            page += 1

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBarRecord]:
        rows = self._get_all(
            "stock_prices", f"code:{symbol}~date:gte:{start_date}~date:lte:{end_date}", sort="date:asc"
        )
        return [
            DailyBarRecord(
                symbol=symbol,
                trade_date=r["date"],
                open=round(r["open"] * PRICE_UNIT, 2),
                high=round(r["high"] * PRICE_UNIT, 2),
                low=round(r["low"] * PRICE_UNIT, 2),
                close=round(r["close"] * PRICE_UNIT, 2),
                volume=int(r["nmVolume"]),  # khối lượng khớp lệnh, không gồm thoả thuận
            )
            for r in rows
        ]

    def get_corporate_actions(self, symbol: str) -> list[CorporateActionRecord]:
        # Tạm map vào schema hiện tại (dividend/split); quyền mua (ISSUE) cần schema mới ở bước 1.2.
        # Nhiều sự kiện cùng loại trong một ngày được gộp: sàn tính giá tham chiếu theo tổng.
        cash = defaultdict(float)
        share_ratio = defaultdict(float)
        for e in self._get_all("events", f"code:{symbol}~locale:EN_GB"):
            ex_date = e.get("effectiveDate")
            if not ex_date:
                continue
            if e["type"] == "DIVIDEND" and e.get("dividend"):
                cash[ex_date] += e["dividend"]
            elif e["type"] in SHARE_EVENT_TYPES and e.get("ratio"):
                share_ratio[ex_date] += e["ratio"]

        records = [
            CorporateActionRecord(symbol=symbol, action_date=d, action_type="dividend", value=v)
            for d, v in cash.items()
        ] + [
            CorporateActionRecord(symbol=symbol, action_date=d, action_type="split", value=1 + r / 100)
            for d, r in share_ratio.items()
        ]
        return sorted(records, key=lambda r: (r.action_date, r.action_type))

    def get_instrument_info(self, symbol: str) -> InstrumentRecord:
        stock = self._get_all("stocks", f"code:{symbol}")[0]
        industry = self._get_all("industry_classification", f"codeList:{symbol}~industryLevel:1")
        return InstrumentRecord(
            symbol=symbol,
            name=stock.get("companyNameEng") or stock.get("companyName"),
            exchange=stock["floor"],
            sector=industry[0]["englishName"].strip().title() if industry else None,
            is_listed=stock.get("status") == "listed",
        )
