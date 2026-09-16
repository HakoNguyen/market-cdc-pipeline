USE market_db;

-- 1. Hủy các routine load job cũ
STOP ROUTINE LOAD FOR market_db.rl_daily_bar;
STOP ROUTINE LOAD FOR market_db.rl_corporate_action;
STOP ROUTINE LOAD FOR market_db.rl_instrument;

-- 2. Tạo lại các Routine Load job mới với hàm ép kiểu DATE
CREATE ROUTINE LOAD market_db.rl_daily_bar ON raw_daily_bar
COLUMNS(symbol, raw_trade_date, open, high, low, close, adj_close, volume, updated_at, op, op_ts, trade_date = DATE_ADD('1970-01-01', INTERVAL raw_trade_date DAY))
PROPERTIES(
    "format" = "json",
    "max_error_number" = "1000",
    "jsonpaths" = "[\"$.payload.after.symbol\",\"$.payload.after.trade_date\",\"$.payload.after.open\",\"$.payload.after.high\",\"$.payload.after.low\",\"$.payload.after.close\",\"$.payload.after.adj_close\",\"$.payload.after.volume\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "market.public.daily_bar",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);

CREATE ROUTINE LOAD market_db.rl_instrument ON raw_instrument
COLUMNS(symbol, name, exchange, sector, is_listed, updated_at, op, op_ts)
PROPERTIES(
    "format" = "json",
    "max_error_number" = "1000",
    "jsonpaths" = "[\"$.payload.after.symbol\",\"$.payload.after.name\",\"$.payload.after.exchange\",\"$.payload.after.sector\",\"$.payload.after.is_listed\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "market.public.instrument",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);

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
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "market.public.corporate_action",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);
