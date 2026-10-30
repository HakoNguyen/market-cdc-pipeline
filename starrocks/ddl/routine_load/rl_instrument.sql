-- CDC topic market.public.instrument -> raw_instrument
CREATE ROUTINE LOAD market_db.rl_instrument ON raw_instrument
COLUMNS(symbol, name, exchange, sector, is_listed, updated_at, op, op_ts)
PROPERTIES(
    "format" = "json",
    "max_error_number" = "1000",
    "jsonpaths" = "[\"$.payload.after.symbol\",\"$.payload.after.name\",\"$.payload.after.exchange\",\"$.payload.after.sector\",\"$.payload.after.is_listed\",\"$.payload.after.updated_at\",\"$.payload.op\",\"$.payload.ts_ms\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.instrument",
    "kafka_partitions" = "0",
    "kafka_offsets" = "0"
);
