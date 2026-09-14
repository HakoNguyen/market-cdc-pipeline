import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import date, timedelta
from ingestion.source.yfinance_source import YFinanceSource
from ingestion.source.loader import PostgresLoader
from ingestion.config import VN30_TICKERS

def main():
    print("Script for testing ingestion")
    source = YFinanceSource()
    loader = PostgresLoader()

    test_symbols = ['FPT.VN', 'VNM.VN', 'HPG.VN']
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    for symbol in test_symbols:
        print(f"Processing {symbol}")

        inst = source.get_instrument_info(symbol)
        print(f"Instrument: {inst.name} | Exchange: {inst.exchange}")

        bars = source.get_daily_bars(symbol, start_date, end_date)
        print(f"Got {len(bars)} daily bars")

        actions = source.get_corporate_actions(symbol)
        print(f"Got {len(actions)} actions")

        loader.save_raw_json(symbol, 'bars', [b.model_dump() for b in bars])
        loader.save_raw_json(symbol, 'actions', [a.model_dump() for a in actions])

        print(f"Saved raw JSON archive for {symbol} to data/raw/")

        try: 
            loader.load_instruments(inst)
            loader.load_daily_bars(bars)
            loader.load_corporate_actions(actions)

            print(f"Loaded {symbol} into Postgres")
        except Exception as e:
            print(f"Failed to load {symbol} into Postgres: {e}")

if __name__ == "__main__":
    main()