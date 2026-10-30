-- CDC topic market.public.daily_bar -> raw_daily_bar
-- Debezium gửi DATE dạng số ngày kể từ epoch -> đổi lại bằng DATE_ADD
CREATE ROUTINE LOAD market_db.rl_daily_bar ON raw_daily_bar
COLUMNS(symbol, raw_trade_date, open, high, low, close, volume, updated_at, op, op_ts, trade_date = DATE_ADD('1970-01-01', INTERVAL raw_trade_date DAY))
PROPERTIES(
    "format" = "json",
    "max_error_number" = "1000",
    "jsonpaths" = "[\"$.payload.after.symbol\",\"$.payload.after.trade_date\",\"$.payload.after.open\",\"$.payload.after.high\",\"$.payload.after.low\",\"$.payload.after.close\",\"$.payload.after.volume\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.daily_bar",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);
