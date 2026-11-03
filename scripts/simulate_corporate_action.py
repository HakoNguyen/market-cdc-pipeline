"""Demo: một sự kiện quyền thật đi qua CDC và viết lại lịch sử giá điều chỉnh.

FPT thưởng cổ phiếu 15%, GDKHQ 2025-07-21 (giá thô: 126,030 -> 110,300).
  1. Xoá sự kiện ở Postgres -> CDC xoá ở StarRocks -> dbt run: return giả ~ -12.5% tại GDKHQ,
     và dbt test assert_reference_price_gaps_have_events báo thiếu sự kiện.
  2. Nạp lại sự kiện -> CDC -> dbt build: gap biến mất, mọi test xanh.
  3. In độ trễ CDC đo được (commit ở Postgres -> thấy được ở StarRocks) và lưu chart before/after.

Chạy từ thư mục gốc, venv đã activate, stack đang chạy (make init) và FPT đã có dữ liệu 07-08/2025:
    python scripts/simulate_corporate_action.py
Sự kiện luôn được nạp lại (finally), kể cả khi script lỗi giữa chừng.
"""
import shutil
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import matplotlib
matplotlib.use("Agg")  # chỉ ghi file, không mở cửa sổ
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import mysql.connector
import psycopg2

from ingestion.config import (DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER, STARROCKS_HOST, STARROCKS_PASSWORD,
                              STARROCKS_PORT, STARROCKS_USER)

SYMBOL, EX_DATE, ACTION_TYPE = "FPT", date(2025, 7, 21), "bonus_share"
WINDOW = (date(2025, 7, 1), date(2025, 8, 29))
GAP_TEST = "assert_reference_price_gaps_have_events"
CHART = ROOT / "assets" / "demo_fpt_bonus_share.png"
CDC_TIMEOUT_S = 120

COLUMNS = ("symbol", "ex_date", "action_type", "cash_amount", "ratio", "subscription_price", "announce_date")
KEY = "symbol = %s and ex_date = %s and action_type = %s"


def step(msg):
    print(f"\n==> {msg}", flush=True)


def dbt(*args) -> bool:
    exe = shutil.which("dbt", path=str(Path(sys.executable).parent)) or "dbt"
    result = subprocess.run([exe, "--no-use-colors", *args, "--profiles-dir", "."], cwd=ROOT / "market_cdc",
                            capture_output=True, text=True, encoding="utf-8", errors="replace")
    summary = [line for line in result.stdout.splitlines() if "Done. PASS=" in line or " FAIL " in line]
    print("    " + "\n    ".join(summary or result.stdout.splitlines()[-3:]))
    return result.returncode == 0


def commit_and_wait_for_cdc(pg, sr, sql, params, expect_visible: bool) -> float:
    """Chạy sql ở Postgres, trả số giây từ lúc COMMIT tới lúc StarRocks thấy thay đổi."""
    with pg.cursor() as cur:
        cur.execute(sql, params)
    pg.commit()
    committed = time.perf_counter()

    sr_cur = sr.cursor()
    while True:
        sr_cur.execute(f"select count(*) from market_db.raw_corporate_action where {KEY}",
                       (SYMBOL, EX_DATE, ACTION_TYPE))
        if (sr_cur.fetchone()[0] == 1) == expect_visible:
            return time.perf_counter() - committed
        if time.perf_counter() - committed > CDC_TIMEOUT_S:
            raise TimeoutError(f"StarRocks chưa nhận thay đổi sau {CDC_TIMEOUT_S}s (make status để xem CDC)")
        time.sleep(0.1)


def adjusted_series(sr) -> list[tuple]:
    cur = sr.cursor()
    cur.execute(
        "select trade_date, close, adj_close, daily_return_pct from analytics.fct_daily_bars "
        "where symbol = %s and trade_date between %s and %s order by trade_date",
        (SYMBOL, *WINDOW),
    )
    return [(d, float(c), float(a), float(r) if r is not None else None) for d, c, a, r in cur.fetchall()]


def ex_date_return(series) -> float:
    return next(r for d, _, _, r in series if d == EX_DATE)


def plot(before, after):
    dates = [d for d, *_ in after]
    fig, (ax_price, ax_ret) = plt.subplots(2, 1, figsize=(10, 6.5), sharex=True,
                                           gridspec_kw={"height_ratios": [3, 2]})

    ax_price.plot(dates, [c for _, c, _, _ in after], color="#9aa0a6", lw=1.2, label="Raw close")
    ax_price.plot(dates, [a for _, _, a, _ in before], color="#d93025", lw=1.8, label="Adjusted, event missing")
    ax_price.plot(dates, [a for _, _, a, _ in after], color="#1a73e8", lw=1.8, label="Adjusted, event applied")
    ax_price.set_ylabel("Price (VND)")
    ax_price.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    ax_price.set_title(f"{SYMBOL}: 15% bonus shares, ex-date {EX_DATE}")
    ax_price.legend(frameon=False)

    # Cột đỏ rộng (thiếu sự kiện) nằm sau cột xanh hẹp (có sự kiện): chỉ khác nhau ở ngày GDKHQ
    ax_ret.bar(dates, [r or 0 for *_, r in before], width=0.8, color="#d93025", alpha=0.35, label="Event missing")
    ax_ret.bar(dates, [r or 0 for *_, r in after], width=0.4, color="#1a73e8", label="Event applied")
    ax_ret.axhline(0, color="#5f6368", lw=0.8)
    ax_ret.set_ylabel("Daily return (%)")
    ax_ret.set_ylim(min(r or 0 for *_, r in before) * 1.25, None)
    ax_ret.annotate(f"{ex_date_return(before):.1f}% fake drop", xy=(EX_DATE, ex_date_return(before)),
                    xytext=(8, 0), textcoords="offset points", color="#d93025", va="center")
    ax_ret.annotate(f"{ex_date_return(after):+.1f}% actual", xy=(EX_DATE, ex_date_return(after)),
                    xytext=(8, 6), textcoords="offset points", color="#1a73e8", va="bottom")
    ax_ret.legend(frameon=False, loc="lower right")

    for ax in (ax_price, ax_ret):
        ax.axvline(EX_DATE, color="#5f6368", ls="--", lw=0.8)
        ax.spines[["top", "right"]].set_visible(False)
    fig.autofmt_xdate()
    fig.tight_layout()
    CHART.parent.mkdir(exist_ok=True)
    fig.savefig(CHART, dpi=150)


def main():
    pg = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    sr = mysql.connector.connect(host=STARROCKS_HOST, port=STARROCKS_PORT, user=STARROCKS_USER,
                                 password=STARROCKS_PASSWORD, autocommit=True)
    try:
        with pg.cursor() as cur:
            cur.execute(f"select {', '.join(COLUMNS)} from corporate_action where {KEY}",
                        (SYMBOL, EX_DATE, ACTION_TYPE))
            event = cur.fetchone()
        if event is None:
            sys.exit(f"Không thấy {SYMBOL} {ACTION_TYPE} {EX_DATE} ở Postgres: chạy backfill trước "
                     f"(python ingestion/batch/backfill_ingest.py --symbols {SYMBOL} --start-date {WINDOW[0]})")
        pg.rollback()  # kết thúc transaction đọc

        try:
            step(f"1. DELETE {SYMBOL} {ACTION_TYPE} {EX_DATE} ở Postgres, chờ CDC")
            delete_latency = commit_and_wait_for_cdc(
                pg, sr, f"delete from corporate_action where {KEY}", (SYMBOL, EX_DATE, ACTION_TYPE),
                expect_visible=False)
            print(f"    StarRocks đã xoá sau {delete_latency:.1f}s")

            step("   dbt run (không có sự kiện)")
            dbt("run", "--select", "int_adjustment_factor+")
            before = adjusted_series(sr)
            print(f"    Return tại GDKHQ: {ex_date_return(before):+.2f}%")

            step(f"   dbt test {GAP_TEST} (phải FAIL: sàn đổi giá tham chiếu mà không có sự kiện)")
            caught = not dbt("test", "--select", GAP_TEST)
            print(f"    {'Đã phát hiện thiếu sự kiện' if caught else 'KHÔNG phát hiện được, kiểm tra lại test'}")
        finally:
            step(f"2. INSERT lại sự kiện ở Postgres, chờ CDC")
            insert_latency = commit_and_wait_for_cdc(
                pg, sr,
                f"insert into corporate_action ({', '.join(COLUMNS)}) values ({', '.join(['%s'] * len(COLUMNS))}) "
                "on conflict do nothing",
                event, expect_visible=True)
            print(f"    StarRocks đã thấy sau {insert_latency:.1f}s")

        step("   dbt build (có sự kiện)")
        build_ok = dbt("build")
        after = adjusted_series(sr)
        print(f"    Return tại GDKHQ: {ex_date_return(after):+.2f}%")

        plot(before, after)
        step("3. Kết quả")
        print(f"    Return {SYMBOL} {EX_DATE}: thiếu sự kiện {ex_date_return(before):+.2f}% -> "
              f"có sự kiện {ex_date_return(after):+.2f}%")
        print(f"    Độ trễ CDC Postgres -> StarRocks: DELETE {delete_latency:.1f}s, INSERT {insert_latency:.1f}s")
        print(f"    Test thiếu sự kiện: {'bắt được' if caught else 'KHÔNG bắt được'}; dbt build sau cùng: "
              f"{'xanh' if build_ok else 'LỖI'}")
        print(f"    Chart: {CHART.relative_to(ROOT)}")
        if not (caught and build_ok):
            sys.exit(1)
    finally:
        pg.close()
        sr.close()


if __name__ == "__main__":
    main()
