with daily_bars as (
    select * from {{ ref('stg_daily_bar') }}
),

corporate_actions as (
    select * from {{ ref('stg_corporate_action') }}
),

-- Calculate adjustment factor per corporate action event
action_factors as (
    select
        symbol,
        action_date,
        action_type,
        value,
        case 
            when action_type = 'split' and value > 0 then (1.0 / value)
            else 1.0
        end as factor
    from corporate_actions
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
        coalesce(exp(sum(log(a.factor))), 1.0) as cum_adjustment_factor
    from daily_bars b
    left join action_factors a
        on b.symbol = a.symbol
       and a.action_date > b.trade_date
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
