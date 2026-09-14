from datetime import date
import pandas as pd
import yfinance as yf
from ingestion.source.schemas import DailyBarRecord, CorporateActionRecord, InstrumentRecord

class YFinanceSource:
    """Get data finance from yfinance"""
    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> list[DailyBarRecord]:
        ticker = yf.Ticker(symbol)

        df = ticker.history(
            start=start_date.strftime("%Y-%m-%d"), 
            end=end_date.strftime("%Y-%m-%d"), 
            auto_adjust=False
        )
        if df.empty:
            return []

        records = []
        for idx, row in df.iterrows():
            trade_date = idx.date()
            record = DailyBarRecord(
                symbol=symbol, 
                trade_date=trade_date,
                open=float(row['Open']),
                high=float(row['High']),
                low=float(row['Low']),
                close=float(row['Close']),
                adj_close=float(row['Adj Close']),
                volume=int(row['Volume']),
            )
            records.append(record)
        return records

    def get_corporate_actions(self, symbol: str) -> list[CorporateActionRecord]:
        ticker = yf.Ticker(symbol)
        actions_df = ticker.actions

        if actions_df.empty:
            return []
        
        records = []
        for idx, row in actions_df.iterrows():
            action_date = idx.date()
            
            if row["Dividends"] > 0:
                records.append(CorporateActionRecord(
                    symbol=symbol,
                    action_date=action_date,
                    action_type='dividend',
                    value=float(row['Dividends']),
                    value_raw=None
                ))

            if row['Stock Splits'] > 0:
                records.append(CorporateActionRecord(
                    symbol=symbol,
                    action_date=action_date,
                    action_type="split",
                    value=float(row['Stock Splits']),
                    value_raw=None
                ))
        return records

    def get_instrument_info(self, symbol: str) -> InstrumentRecord:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}

        return InstrumentRecord(
            symbol=symbol,
            name=info.get('longName') or info.get('shortName') or symbol,
            exchange='HOSE',
            sector=info.get('sector'),
            is_listed=True
        )