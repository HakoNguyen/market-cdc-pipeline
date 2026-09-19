with stg as (
    select * from {{ ref('stg_daily_bar') }}
),

indicators as (
    select 
        symbol, 
        trade_date,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        avg(close) over (partition by symbol order by trade_date 
        rows between 19 preceding and current row) as sma_20,
        avg(close) over (partition by symbol order by trade_date
        rows between 49 preceding and current row) as sma_50,
        avg(volume) over (partition by symbol order by trade_date
        rows between 24 preceding and current row) as sma_25,
        lag(close, 1) over (partition by symbol order by trade_date) as prev_close,
        round(
    (close - lag(close, 1) over (partition by symbol order by trade_date)) 
    / nullif(lag(close, 1) over (partition by symbol order by trade_date), 0) * 100, 
    4
) as daily_return_pct
    from stg
)
select *
from indicators
order by symbol, trade_date
