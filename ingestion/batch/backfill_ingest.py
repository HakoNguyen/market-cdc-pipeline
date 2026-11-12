import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from ingestion.source.loader import PostgresLoader
from ingestion.source.vndirect_source import VNDirectSource
from ingestion.config import VN30_TICKERS
import argparse
from datetime import date, datetime, timedelta
import logging

# DAG hằng ngày nạp lại N ngày gần nhất: phiên bị lỡ / dữ liệu nguồn sửa muộn tự được lấp
# (upsert chỉ UPDATE khi giá trị đổi nên nạp lại không sinh CDC event thừa)
DAILY_LOOKBACK_DAYS = 5
# Quá 1% dòng nguồn bị loại -> nhiều khả năng nguồn đổi định dạng, dừng lại cho người xem
MAX_REJECT_RATE = 0.01

log = logging.getLogger(__name__)

def normalize_symbols(symbols) -> list[str]:
    """[' fpt', 'HPG', 'FPT', ''] -> ['FPT', 'HPG']; rỗng -> cả rổ VN30."""
    cleaned = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
    return list(dict.fromkeys(cleaned)) or list(VN30_TICKERS)


def daily_window(end_date: date, lookback_days: int = DAILY_LOOKBACK_DAYS) -> tuple[date, date]:
    return end_date - timedelta(days=lookback_days), end_date


def check_quality(summary: dict, failed: dict, max_reject_rate: float = MAX_REJECT_RATE) -> None:
    """Raise RuntimeError khi: có mã lỗi, có mã bị loại toàn bộ dòng, hoặc tỉ lệ loại của cả run > ngưỡng."""
    failed = dict(failed)
    for symbol, s in summary.items():
        if s["bars"] == 0 and s["rejected"] > 0:
            failed[symbol] = f"all {s['rejected']} rows rejected"

    rejected = sum(s["rejected"] for s in summary.values())
    total = rejected + sum(s["bars"] for s in summary.values())
    rate = rejected / total if total else 0.0  # cửa sổ rơi vào kỳ nghỉ: 0 dòng, không phải lỗi

    problems = []
    if failed:
        problems.append(f"{len(failed)} symbol(s) failed: {failed}")
    if rate > max_reject_rate:
        problems.append(f"{rejected}/{total} rows rejected ({rate:.1%} > {max_reject_rate:.0%})")
    if problems:
        raise RuntimeError("; ".join(problems))


def run_backfill(symbols=None, start_date=None, end_date=None, auto_catchup=True) -> dict:
    """Nạp instrument + daily bar + corporate action vào Postgres. Dùng chung cho CLI và 2 DAG.
    Có start_date -> nạp đúng cửa sổ đó. Không có -> mỗi mã nạp tiếp từ max(trade_date) - 5 ngày
    (chưa có dữ liệu thì 1 năm), trừ khi auto_catchup=False (khi đó 30 ngày).
    Trả {symbol: {'bars', 'actions', 'rejected'}} (Airflow đẩy vào XCom).
    Mã lỗi không chặn các mã sau; cuối run raise RuntimeError nếu check_quality không qua."""
    target_symbols = normalize_symbols(symbols)
    target_end_date = end_date or date.today()
    if start_date and start_date > target_end_date:
        raise ValueError(f"start_date {start_date} sau end_date {target_end_date}")

    source = VNDirectSource()
    with PostgresLoader() as loader:
        return _load_symbols(source, loader, target_symbols, start_date, target_end_date, auto_catchup)


def _load_symbols(source, loader, target_symbols, start_date, target_end_date, auto_catchup) -> dict:
    log.info("Starting backfill for %d symbols, end date %s", len(target_symbols), target_end_date)
    summary, failed = {}, {}

    for symbol in target_symbols:
        # Ranh giới 1 mã: lỗi gì cũng ghi lại rồi chạy tiếp mã sau; check_quality raise ở cuối
        try:
            summary[symbol] = _load_one(source, loader, symbol, start_date, target_end_date, auto_catchup)
        except Exception as e:
            failed[symbol] = repr(e)
            log.exception("[%s] failed", symbol)

    log.info("Backfill done: %d ok, %d failed", len(summary), len(failed))
    check_quality(summary, failed)
    return summary


def _load_one(source, loader, symbol, start_date, target_end_date, auto_catchup) -> dict:
    inst = source.get_instrument_info(symbol)
    loader.load_instruments(inst)

    if auto_catchup and not start_date:
        max_date = loader.get_max_trade_date(symbol)
        if max_date:
            calc_start_date = max_date - timedelta(days=5)
            log.info("[%s] max_date=%s -> start_date=%s", symbol, max_date, calc_start_date)
        else:
            calc_start_date = target_end_date - timedelta(days=365)
            log.info("[%s] no data yet -> start_date=%s", symbol, calc_start_date)
    else:
        calc_start_date = start_date or (target_end_date - timedelta(days=30))
        log.info("[%s] given range -> start_date=%s", symbol, calc_start_date)

    bars, rejected_bars = source.get_daily_bars(symbol, calc_start_date, target_end_date)
    actions = source.get_corporate_actions(symbol)

    if bars:
        loader.save_raw_json(symbol, 'bars', [b.model_dump() for b in bars])
        loader.load_daily_bars(bars)
        log.info("[%s] loaded %d daily bars", symbol, len(bars))

    if actions:
        loader.save_raw_json(symbol, 'actions', [a.model_dump() for a in actions])
        loader.load_corporate_actions(actions)
        log.info("[%s] loaded %d corporate actions", symbol, len(actions))

    if rejected_bars:
        loader.save_quarantine(symbol, rejected_bars)
        log.warning("[%s] %d rows rejected -> quarantine", symbol, len(rejected_bars))

    return {"bars": len(bars), "actions": len(actions), "rejected": len(rejected_bars)}

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Backfill market daily bar data into Postgres.")
    parser.add_argument("--symbols", nargs="+", help="List of symbols to backfill, e.g. FPT VNM")
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
