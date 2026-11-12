with source as (
    select * from {{ source('market_db', 'raw_corporate_action') }}
),

renamed as (
    select 
        symbol, 
        ex_date,
        action_type,
        cash_amount,
        ratio,
        subscription_price,
        announce_date,
        updated_at,
        op as cdc_op,
        op_ts as cdc_op_ts,
        _ingest_at
    from source
)

select * from renamed