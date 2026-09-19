select
    symbol,
    trade_date,
    open, 
    high,
    low,
    close
from {{ ref('fct_daily_bars') }}
where open <= 0 or close <= 0 or high < low
