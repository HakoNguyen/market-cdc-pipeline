with source as (
    select * from {{ source('market_db', 'raw_daily_bar') }}
),

renamed as (
    select 
        symbol, 
        trade_date,
        cast(open as decimal(18,4)) as open, 
        cast(high as decimal(18,4)) as high, 
        cast(low as decimal(18,4)) as low,
        cast(close as decimal(18,4)) as close,
        cast(volume as bigint) as volume,
        cast(reference_price as decimal(18,4)) as reference_price,
        cast(ceiling_price as decimal(18,4)) as ceiling_price,
        cast(floor_price as decimal(18,4)) as floor_price,
        updated_at,
        op as cdc_op,
        op_ts as cdc_op_ts,
        _ingest_at
    from source
)

select * from renamed