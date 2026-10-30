import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ingestion.source.loader import PostgresLoader
from ingestion.source.yfinance_source import YFinanceSource
from ingestion.config import VN30_TICKERS
import argparse
from datetime import date, datetime, timedelta

def run_backfill(symbols=None, start_date=None, end_date=None, auto_catchup=True):
    source = YFinanceSource()
    loader = PostgresLoader()

    target_symbols = symbols or VN30_TICKERS
    today = date.today()
    target_end_date = end_date or today

    print("Starting backfill for {} symbols, range {} to {} ")

    for symbol in target_symbols:
        inst = source.get_instrument_info(symbol)
        loader.load_instruments(inst)

        # Dynamic Date range
        if auto_catchup and not start_date:
            max_date = loader.get_max_trade_date(symbol)
            if max_date:
                calc_start_date = max_date - timedelta(days=5)
                print(f"Symbol {symbol}: max_date={max_date} -> calc_start_date={calc_start_date}")
            else:
                calc_start_date = target_end_date - timedelta(days=365)
                print(f"Symbol {symbol}: No data found -> calc_start_date={calc_start_date}")
        else:
            calc_start_date = start_date or (target_end_date - timedelta(days=30))
            print(f"Symbol {symbol}: User specified range -> calc_start_date={calc_start_date}")
        
        bars = source.get_daily_bars(symbol, calc_start_date, target_end_date)
        actions = source.get_corporate_actions(symbol)

        if bars: 
            loader.save_raw_json(symbol, 'bars', [b.model_dump() for b in bars])
            loader.load_daily_bars(bars)
            print(f"[{symbol}] Successfully loaded {len(bars)} daily bars into Postgres.")
        
        if actions:
            loader.save_raw_json(symbol, 'actions', [a.model_dump() for a in actions])
            loader.load_corporate_actions(actions)
            print(f"[{symbol}] Successfully loaded {len(actions)} corporate actions into Postgres.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Backfill market daily bar data into Postgres.")
    parser.add_argument("--symbols", nargs="+", help="List of symbols to backfill, e.g. FPT.VN VNM.VN")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--no-catchup", action="store_true", help="Disable auto catchup based on MAX trade_date")
    args = parser.parse_args()
    s_date = datetime.strptime(args.start_date, "%Y-%m-%d").date() if args.start_date else None
    e_date = datetime.strptime(args.end_date, "%Y-%m-%d").date() if args.end_date else None
    run_backfill(
        symbols=args.symbols,
        start_date=s_date,
        end_date=e_date,
        auto_catchup=not args.no_catchup
    )