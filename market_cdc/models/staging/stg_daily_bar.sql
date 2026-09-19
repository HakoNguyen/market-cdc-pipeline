with source as (
    select * from market_db.raw_daily_bar
),

renamed as (
    select 
        symbol, 
        trade_date,
        cast(open as decimal(18,4)) as open, 
        cast(high as decimal(18,4)) as high, 
        cast(low as decimal(18,4)) as low,
        cast(close as decimal(18,4)) as close,
        cast(adj_close as decimal(18,4)) as adj_close,
        cast(volume as bigint) as volume,
        updated_at,
        op as cdc_op,
        op_ts as cdc_op_ts,
        _ingest_at
    from source
    where op != 'd' or op is null
)

select * from renamed