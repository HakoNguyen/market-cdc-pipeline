import mysql.connector
import psycopg2

from ingestion.config import (DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER, STARROCKS_HOST, STARROCKS_PASSWORD, STARROCKS_PORT, STARROCKS_USER)

TABLES = ('daily_bar', 'instrument', 'corporate_action')
ROUTINE_LOAD_JOBS = ('rl_daily_bar', 'rl_instrument', 'rl_corporate_action')

def routine_load_states(sr_cur) -> dict[str, str]:
    """{tên job: trạng thái} của mọi job chưa bị STOP."""
    sr_cur.execute("SHOW ROUTINE LOAD FROM market_db")
    columns = [d[0] for d in sr_cur.description]
    name_idx, state_idx = columns.index("Name"), columns.index("State")
    return {row[name_idx]: row[state_idx] for row in sr_cur.fetchall()}

def cdc_caught_up() -> bool:
    """True: StarRocks đã khớp Postgres. False: chưa khớp, sensor thử lại sau.
    Raise RuntimeError: pipeline hỏng (job Routine Load không RUNNING) -> chờ thêm cũng vô ích."""
    pg = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    sr = mysql.connector.connect(host=STARROCKS_HOST, port=STARROCKS_PORT,
                                 user=STARROCKS_USER, password=STARROCKS_PASSWORD)
    try:
        pg_cur, sr_cur = pg.cursor(), sr.cursor()

        states = routine_load_states(sr_cur)

        not_running = [j for j in ROUTINE_LOAD_JOBS if states.get(j) != "RUNNING"]
        if not_running:
            raise RuntimeError(f"Job Routine Load không RUNNING: {not_running}")

        for table in TABLES:
            # 'at time zone UTC' -> datetime KHÔNG kèm múi giờ, mới so sánh được với StarRocks
            pg_cur.execute(f"select count(*), max(updated_at at time zone 'UTC') from {table}")
            sr_cur.execute(f"select count(*), max(updated_at) from market_db.raw_{table}")
            pg_snapshot, sr_snapshot = pg_cur.fetchone(), sr_cur.fetchone()
            if pg_snapshot != sr_snapshot:
                print(f"{table}: postgres={pg_snapshot} starrocks={sr_snapshot}")
                return False

        return True

    finally:
        pg.close()
        sr.close()


# # test
# if __name__ == "__main__":
#     print(cdc_caught_up())