from datetime import date
from typing import Protocol

from ingestion.source.schemas import CorporateActionRecord, DailyBarRecord, InstrumentRecord


class MarketDataSource(Protocol):
    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> tuple[list[DailyBarRecord], list[dict]]:
        """(bars hợp lệ, dòng bị loại [{'row', 'error'}])."""
        ...
    def get_corporate_actions(self, symbol: str) -> list[CorporateActionRecord]:
        ...
    def get_instrument_info(self, symbol: str) -> InstrumentRecord:
        ...
