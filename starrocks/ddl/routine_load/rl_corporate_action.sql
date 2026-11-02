-- CDC topic market.public.corporate_action -> raw_corporate_action
-- Debezium gửi DATE dạng số ngày kể từ epoch -> đổi lại bằng DATE_ADD (announce_date có thể NULL)
CREATE ROUTINE LOAD market_db.rl_corporate_action ON raw_corporate_action
COLUMNS(symbol, raw_ex_date, action_type, cash_amount, ratio, subscription_price, raw_announce_date, updated_at, op, op_ts,
        ex_date = DATE_ADD('1970-01-01', INTERVAL raw_ex_date DAY),
        announce_date = DATE_ADD('1970-01-01', INTERVAL raw_announce_date DAY))
PROPERTIES
(
    "format" = "json",
    "max_error_number" = "1000",
    "jsonpaths" = "[\"$.payload.after.symbol\",\"$.payload.after.ex_date\",\"$.payload.after.action_type\",\"$.payload.after.cash_amount\",\"$.payload.after.ratio\",\"$.payload.after.subscription_price\",\"$.payload.after.announce_date\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.corporate_action",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);
