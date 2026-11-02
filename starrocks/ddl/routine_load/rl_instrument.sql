-- CDC topic market.public.instrument -> raw_instrument
-- DELETE: __deleted = true -> __op = 1 -> xoá dòng (xem rl_daily_bar.sql)
CREATE ROUTINE LOAD market_db.rl_instrument ON raw_instrument
COLUMNS(symbol, name, exchange, sector, is_listed, updated_at, op, op_ts, is_deleted,
        __op = if(is_deleted = 'true', 1, 0))
PROPERTIES(
    "format" = "json",
    "max_error_number" = "0",
    "jsonpaths" = "[\"$.symbol\",\"$.name\",\"$.exchange\",\"$.sector\",\"$.is_listed\",\"$.updated_at\",\"$.__op\",\"$.__source_ts_ms\",\"$.__deleted\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.instrument",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
