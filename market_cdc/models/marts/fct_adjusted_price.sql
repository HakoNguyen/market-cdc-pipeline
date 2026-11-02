with daily_bars as (
    select * from {{ ref('stg_daily_bar') }}
),

corporate_actions as (
    select * from {{ ref('stg_corporate_action') }}
),

-- Adjustment factor per ex-date.
-- TẠM (1.2): chỉ sự kiện chia thêm cổ phiếu. Nhiều đợt cùng ngày thì sàn CỘNG tỷ lệ (vd BSR
-- 2025-10-29: thưởng 31.5% + cổ tức cp 30% -> 1/1.615), không nhân từng factor.
-- Cổ tức tiền + quyền mua theo công thức gộp của sàn làm ở 1.3.
action_factors as (
    select
        symbol,
        ex_date,
        1.0 / (1 + sum(ratio)) as factor
    from corporate_actions
    where action_type in ('stock_dividend', 'bonus_share')
      -- Sự kiện được công bố trước ngày GDKHQ; chỉ điều chỉnh khi ngày GDKHQ đã tới
      and ex_date <= current_date()
    group by symbol, ex_date
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
