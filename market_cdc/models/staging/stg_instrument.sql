with source as (
    select * from market_db.raw_instrument
),

renamed as (
    select 
        symbol, 
        name,
        coalesce(exchange, 'HOSE') as exchange,
        sector,
        is_listed,
        updated_at,
        op as cdc_op,
        op_ts as cdc_op_ts,
        _ingest_at
    from source
    where op != 'd' or op is null
)

select * from renamed