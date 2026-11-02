with bars as (
    select 
        symbol, 
        trade_date, 
        lag(close) over (partition by symbol order by trade_date) as prev_close
    from {{ ref('stg_daily_bar') }}
),

events_per_day as (
    select
        symbol, 
        ex_date, 
        coalesce(sum(case when action_type = 'cash_dividend' then cash_amount end), 0) as total_cash, 
        coalesce(sum(case when action_type = 'rights_issue' then ratio * subscription_price end), 0) as rights_value, 
        coalesce(sum(case when action_type in ('stock_dividend', 'bonus_share', 'rights_issue') then ratio end), 0) as total_ratio
    from {{ ref('stg_corporate_action') }}      
    where ex_date <= current_date()
    group by symbol, ex_date
),

ref_prices as (
    select
        e.symbol,
        e.ex_date,
        b.prev_close,
        (b.prev_close - e.total_cash + e.rights_value) / (1 + e.total_ratio) as ref_price
    from events_per_day e
    join bars b
      on b.symbol = e.symbol
     and b.trade_date = e.ex_date      -- dòng của chính ngày GDKHQ mang prev_close = P_prev
)

select
    symbol,
    ex_date,
    prev_close,
    ref_price,
    ref_price / prev_close as factor
from ref_prices