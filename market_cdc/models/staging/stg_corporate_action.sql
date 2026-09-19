with source as (
    select * from market_db.raw_corporate_action
),

renamed as (
    select 
        symbol, 
        action_date,
        action_type,
        cast(value as decimal(18,6)) as value,
        cast(value_raw as decimal(18,6)) as value_raw,
        updated_at,
        op as cdc_op,
        op_ts as cdc_op_ts,
        _ingest_at
    from source
    where op != 'd' or op is null
)

select * from renamed