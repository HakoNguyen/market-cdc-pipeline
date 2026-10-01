with source as (
    select * from market_db.raw_binance_kline
),

renamed as (
    select
        symbol,
        start_time,
        close_time,
        cast(open as decimal(18, 4)) as open,
        cast(high as decimal(18, 4)) as high,
        cast(low as decimal(18, 4)) as low,
        cast(close as decimal(18, 4)) as close,
        cast(volume as decimal(18, 4)) as volume,
        trades_count,
        is_closed,
        _ingest_at
    from source
)

select * from renamed
