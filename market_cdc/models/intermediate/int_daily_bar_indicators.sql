with stg as (
    select * from {{ ref('fct_adjusted_price') }}
),

indicators as (
    select 
    symbol, 
    trade_date, 
    raw_open as open, 
    raw_high as high, 
    raw_low as low, 
    raw_close as close, 
    adj_close, 
    avg(adj_close) over (partition by symbol order by trade_date rows between 19
    preceding and current row) as sma_20,
    avg(adj_close) over (partition by symbol order by trade_date rows between 49
    preceding and current row) as sma_50,
    avg(adj_close) over (partition by symbol order by trade_date rows between 24
    preceding and current row) as sma_25,
    lag(adj_close, 1) over (partition by symbol order by trade_date) as prev_close,
    round(
        (adj_close - lag(adj_close, 1) over (partition by symbol order by trade_date)) 
        / nullif(lag(adj_close, 1) over (partition by symbol order by trade_date), 0)
        * 100, 4
    ) as daily_return_pct
    from stg
)
select * from indicators
order by symbol, trade_date;
