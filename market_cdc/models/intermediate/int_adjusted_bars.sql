with daily_bars as (
    select * from {{ ref('stg_daily_bar') }}
),

-- 1 hệ số / (symbol, ex_date) theo công thức giá tham chiếu gộp của sàn
action_factors as (
    select symbol, ex_date, factor
    from {{ ref('int_adjustment_factor') }}
),

-- Join daily bars with corporate actions that happened AFTER trade_date (ex_date > trade_date)
bars_with_factors as (
    select
        b.symbol,
        b.trade_date,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        coalesce(exp(sum(ln(a.factor))), 1.0) as cum_adjustment_factor
    from daily_bars b
    left join action_factors a
        on b.symbol = a.symbol
       and a.ex_date > b.trade_date
    group by
        b.symbol,
        b.trade_date,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume
)

select
    symbol,
    trade_date,
    open as raw_open,
    high as raw_high,
    low as raw_low,
    close as raw_close,
    volume,
    cast(cum_adjustment_factor as decimal(18,6)) as cum_adjustment_factor,
    cast(open * cum_adjustment_factor as decimal(18,4)) as adj_open,
    cast(high * cum_adjustment_factor as decimal(18,4)) as adj_high,
    cast(low * cum_adjustment_factor as decimal(18,4)) as adj_low,
    cast(close * cum_adjustment_factor as decimal(18,4)) as adj_close
from bars_with_factors
