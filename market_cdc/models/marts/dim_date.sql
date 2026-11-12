-- Lịch ngày cho khoảng có dữ liệu.
-- Ngày giao dịch = ngày có ít nhất 1 phiên trong stg_daily_bar.
-- Ngày nghỉ lễ = ngày thường (T2-T6) mà sàn không mở cửa: Tết, 30/4-1/5, 2/9...
-- trade_day_no: số thứ tự phiên, phiên trước/sau = ±1 kể cả khi cách nhau kỳ nghỉ.
with trading_days as (
    select distinct trade_date
    from {{ ref('stg_daily_bar')}}
),

bounds as (
    select min(trade_date) as first_day, 
            datediff(max(trade_date), min(trade_date)) as n_days
    from trading_days
),

spine as (
    select date_add(b.first_day, interval s.generate_series day) as date_day
    from bounds b, table(generate_series(0, b.n_days)) s
),

flagged as (
    select
        s.date_day,
        dayofweek_iso(s.date_day) as day_of_week,
        t.trade_date is not null as is_trading_day
    from spine s
    left join trading_days t on t.trade_date = s.date_day
)

select 
    date_day, 
    year(date_day) as year, 
    quarter(date_day) as quarter, 
    month(date_day) as month, 
    day_of_week, 
    day_of_week >= 6 as is_weekend, 
    is_trading_day, 
    not is_trading_day and day_of_week <= 5 as is_holiday,
    case when is_trading_day
        then row_number() over (partition by is_trading_day order by date_day)
    end as trade_day_no
from flagged