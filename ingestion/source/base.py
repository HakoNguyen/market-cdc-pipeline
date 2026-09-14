from datetime import date
from typing import Protocol
import pandas as pd

class MarketDataSource(Protocol):
    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        ...
    def get_corporate_actions(self, symbol: str) -> pd.DataFrame:
        ...
    def get_instrument_info(self, symbol: str) -> dict:
        ...