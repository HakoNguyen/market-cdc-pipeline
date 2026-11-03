import os
import json
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values
from datetime import date
from ingestion.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS, DATA_RAW_DIR, DATA_QUARANTINE_DIR
from ingestion.source.schemas import DailyBarRecord, CorporateActionRecord, InstrumentRecord

class PostgresLoader:
    """Dùng trong `with PostgresLoader() as loader:` -> 1 connection cho cả run, đóng khi ra khỏi khối.
    Mỗi lần load_* là 1 transaction: lỗi thì rollback riêng lần đó, connection vẫn dùng tiếp được."""

    def __init__(self):
        self.conn_params = {
            "host": DB_HOST,
            "port": DB_PORT,
            "dbname": DB_NAME,
            "user": DB_USER,
            "password": DB_PASS
        }
        self.conn = None

    def __enter__(self):
        self.conn = psycopg2.connect(**self.conn_params)
        return self

    def __exit__(self, *exc):
        self.conn.close()
        self.conn = None

    def _execute_batch(self, sql: str, rows: list[tuple]):
        # `with conn` = 1 transaction (commit khi thành công, rollback khi lỗi), KHÔNG đóng connection.
        # execute_values gộp các dòng thành 1 câu INSERT ... VALUES (..), (..) thay vì N lượt gọi.
        with self.conn, self.conn.cursor() as cur:
            execute_values(cur, sql, rows)

    @staticmethod
    def _write_json(base_dir: str, file_name: str, data: list[dict]):
        """Ghi ra <base_dir>/<hôm nay>/<file_name>."""
        target_dir = os.path.join(base_dir, date.today().strftime("%Y-%m-%d"))
        os.makedirs(target_dir, exist_ok=True)
        with open(os.path.join(target_dir, file_name), 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    def save_raw_json(self, symbol: str, data_type: str, data: list[dict]):
        self._write_json(DATA_RAW_DIR, f"{symbol}_{data_type}.json", data)

    def save_quarantine(self, symbol: str, rows: list[dict]):
        """Dòng nguồn không parse/validate được: [{'row': dòng gốc từ API, 'error': lý do}]."""
        self._write_json(DATA_QUARANTINE_DIR, f"{symbol}.json", rows)

    def load_daily_bars(self, records: list[DailyBarRecord]):
        if not records:
            return

        sql = """
            INSERT INTO daily_bar (symbol, trade_date, open, high, low, close, volume,
                                   reference_price, ceiling_price, floor_price)
            VALUES %s
            ON CONFLICT (symbol, trade_date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                reference_price = EXCLUDED.reference_price,
                ceiling_price = EXCLUDED.ceiling_price,
                floor_price = EXCLUDED.floor_price
            -- Chỉ UPDATE khi giá trị thực sự đổi, tránh sinh CDC event rác
            WHERE (daily_bar.open, daily_bar.high, daily_bar.low, daily_bar.close, daily_bar.volume,
                   daily_bar.reference_price, daily_bar.ceiling_price, daily_bar.floor_price)
                IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume,
                                  EXCLUDED.reference_price, EXCLUDED.ceiling_price, EXCLUDED.floor_price);
        """
        data_typles = [(
            r.symbol, r.trade_date, r.open, r.high, r.low, r.close, r.volume,
            r.reference_price, r.ceiling_price, r.floor_price
        ) for r in records]
        self._execute_batch(sql, data_typles)

    def load_corporate_actions(self, records: list[CorporateActionRecord]):
        if not records: return

        sql = """
        INSERT INTO corporate_action (symbol, ex_date, action_type, cash_amount, ratio, subscription_price, announce_date)
        VALUES %s
        ON CONFLICT (symbol, ex_date, action_type) DO UPDATE SET
            cash_amount = EXCLUDED.cash_amount,
            ratio = EXCLUDED.ratio,
            subscription_price = EXCLUDED.subscription_price,
            announce_date = EXCLUDED.announce_date
        WHERE (corporate_action.cash_amount, corporate_action.ratio, corporate_action.subscription_price, corporate_action.announce_date)
            IS DISTINCT FROM (EXCLUDED.cash_amount, EXCLUDED.ratio, EXCLUDED.subscription_price, EXCLUDED.announce_date);
        """
        data_tuples = [
            (r.symbol, r.ex_date, r.action_type, r.cash_amount, r.ratio, r.subscription_price, r.announce_date)
            for r in records]
        self._execute_batch(sql, data_tuples)

    def load_instruments(self, record: InstrumentRecord):
        sql = """
        INSERT INTO instrument (symbol, name, exchange, sector, is_listed)
        VALUES %s
        ON CONFLICT (symbol)
        DO UPDATE SET
            name = EXCLUDED.name,
            exchange = EXCLUDED.exchange,
            sector = EXCLUDED.sector,
            is_listed = EXCLUDED.is_listed
        WHERE (instrument.name, instrument.exchange, instrument.sector, instrument.is_listed)
            IS DISTINCT FROM (EXCLUDED.name, EXCLUDED.exchange, EXCLUDED.sector, EXCLUDED.is_listed);
        """
        self._execute_batch(sql, [(record.symbol, record.name, record.exchange, record.sector, record.is_listed)])

    def get_max_trade_date(self, symbol: str) -> Optional[date]:
        sql = "SELECT MAX(trade_date) FROM daily_bar WHERE symbol = %s"
        with self.conn, self.conn.cursor() as cur:
            cur.execute(sql, (symbol,))
            res = cur.fetchone()
            return res[0] if res and res[0] else None
