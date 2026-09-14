import os 
import json
import psycopg2
from datetime import date
from ingestion.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, DATA_RAW_DIR
from ingestion.source.schemas import DailyBarRecord, CorporateActionRecord, InstrumentRecord

class PostgresLoader: 
    def __init__(self):
        self.conn_params = {
            "host": DB_HOST,
            "port": DB_PORT,
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASS
        }

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def save_raw_json(self, symbol: str, data_type: str, data: list[dict]):
        today_str = date.today().strftime("%Y-%m-%d")
        target_dir = os.path.join(DATA_RAW_DIR, today_str)
        os.makedirs(target_dir, exist_ok=True)

        file_path = os.path.join(target_dir, f"{symbol}_{data_type}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    
    def load_daily_bars(self, records: list[DailyBarRecord]):
        if not records: 
            return 
        
        sql = """
        INSERT INTO daily_bar (symbol, trade_date, open, high, low, close, adj_close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        adj_close = EXCLUDED.adj_close,
        volume = EXCLUDED.volume;
        """
        data_typles = [(
            r.symbol, r.trade_date, r.open, r.high, r.low, r.close, r.adj_close, r.volume
        ) for r in records]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, data_typles)
                conn.commit()

    def load_corporate_actions(self, records: list[CorporateActionRecord]):
        if not records: return 
        
        sql = """
        INSERT INTO corporate_action (symbol, action_date, action_type, value, value_raw)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol, action_date, action_type) DO UPDATE SET value = EXCLUDED.value, value_raw = EXCLUDED.value_raw;
        """
        date_str = date.today().strftime('%Y-%m-%d')
        data_tuples = [(r.symbol, r.action_date, r.action_type, r.value, r.value_raw) for r in records]

        with self.get_connection() as conn:
            with conn.cursor() as cur: 
                cur.executemany(sql, data_tuples)
            conn.commit()

    def load_instruments(self, record: InstrumentRecord):
        sql = """
        INSERT INTO instrument (symbol, name, exchange, sector, is_listed)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol)
        DO UPDATE SET 
            name = EXCLUDED.name,
            exchange = EXCLUDED.exchange,
            sector = EXCLUDED.sector,
            is_listed = EXCLUDED.is_listed;
        """
        data_tuples = [(record.symbol, record.name, record.exchange, record.sector, record.is_listed)]
        with self.get_connection() as conn:
            with conn.cursor() as cur: 
                cur.executemany(sql, data_tuples)
            conn.commit()
