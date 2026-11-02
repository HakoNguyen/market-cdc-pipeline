-- CDC topic market.public.daily_bar -> raw_daily_bar
-- Message đã được Debezium làm phẳng (ExtractNewRecordState): {"symbol": ..., "__op": "c|u|d|r", "__deleted": "true|false", ...}
-- DELETE: __deleted = true -> __op = 1 (cột đặc biệt của bảng Primary Key StarRocks) -> xoá dòng
-- Debezium gửi DATE dạng số ngày kể từ epoch -> đổi lại bằng DATE_ADD
CREATE ROUTINE LOAD market_db.rl_daily_bar ON raw_daily_bar
COLUMNS(symbol, raw_trade_date, open, high, low, close, volume, updated_at, op, op_ts, is_deleted,
        trade_date = DATE_ADD('1970-01-01', INTERVAL raw_trade_date DAY),
        __op = if(is_deleted = 'true', 1, 0))
PROPERTIES(
    "format" = "json",
    "max_error_number" = "0",
    "jsonpaths" = "[\"$.symbol\",\"$.trade_date\",\"$.open\",\"$.high\",\"$.low\",\"$.close\",\"$.volume\",\"$.updated_at\",\"$.__op\",\"$.__source_ts_ms\",\"$.__deleted\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.daily_bar",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
