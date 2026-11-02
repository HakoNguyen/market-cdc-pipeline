-- CDC topic market.public.corporate_action -> raw_corporate_action
-- DELETE: __deleted = true -> __op = 1 -> xoá dòng (xem rl_daily_bar.sql). Huỷ một sự kiện quyền ở Postgres
-- phải làm nó biến mất ở StarRocks, nếu không giá điều chỉnh vẫn trừ một sự kiện không còn tồn tại.
-- Debezium gửi DATE dạng số ngày kể từ epoch -> đổi lại bằng DATE_ADD (announce_date có thể NULL)
CREATE ROUTINE LOAD market_db.rl_corporate_action ON raw_corporate_action
COLUMNS(symbol, raw_ex_date, action_type, cash_amount, ratio, subscription_price, raw_announce_date, updated_at, op, op_ts, is_deleted,
        ex_date = DATE_ADD('1970-01-01', INTERVAL raw_ex_date DAY),
        announce_date = DATE_ADD('1970-01-01', INTERVAL raw_announce_date DAY),
        __op = if(is_deleted = 'true', 1, 0))
PROPERTIES
(
    "format" = "json",
    "max_error_number" = "0",
    "jsonpaths" = "[\"$.symbol\",\"$.ex_date\",\"$.action_type\",\"$.cash_amount\",\"$.ratio\",\"$.subscription_price\",\"$.announce_date\",\"$.updated_at\",\"$.__op\",\"$.__source_ts_ms\",\"$.__deleted\"]"
)
FROM KAFKA
(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic" = "market.public.corporate_action",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
