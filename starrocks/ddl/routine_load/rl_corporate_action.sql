-- CDC topic market.public.corporate_action -> raw_corporate_action
CREATE ROUTINE LOAD market_db.rl_corporate_action ON raw_corporate_action
COLUMNS(symbol, raw_action_date, action_type, value, value_raw, updated_at, op, op_ts, action_date = DATE_ADD('1970-01-01', INTERVAL raw_action_date DAY))
PROPERTIES
(
    "format" = "json",
    "max_error_number" = "1000",
    "jsonpaths" = "[\"$.payload.after.symbol\",\"$.payload.after.action_date\",\"$.payload.after.action_type\",\"$.payload.after.value\",\"$.payload.after.value_raw\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.corporate_action",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);
