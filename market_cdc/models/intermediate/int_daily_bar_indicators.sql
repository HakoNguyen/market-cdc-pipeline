with bars as (
    select * from {{ ref('int_adjusted_bars') }}
),

windows as (
    select
        symbol,
        trade_date,
        raw_open,
        raw_high,
        raw_low,
        raw_close,
        adj_close,
        volume,
        row_number() over (partition by symbol order by trade_date) as session_no,
        avg(adj_close) over (partition by symbol order by trade_date rows between 19 preceding and current row) as avg_close_20,
        avg(adj_close) over (partition by symbol order by trade_date rows between 49 preceding and current row) as avg_close_50,
        avg(volume) over (partition by symbol order by trade_date rows between 19 preceding and current row) as avg_volume_20,
        lag(adj_close) over (partition by symbol order by trade_date) as prev_adj_close
    from bars
)

select
    symbol,
    trade_date,
    raw_open as open,
    raw_high as high,
    raw_low as low,
    raw_close as close,
    adj_close,
    volume,
    -- Chỉ báo tính trên giá điều chỉnh. Chưa đủ số phiên của cửa sổ thì trả NULL,
    -- không trả trung bình của ít phiên hơn (con số sai nhưng trông như đúng).
    case when session_no >= 20 then avg_close_20 end as sma_20,
    case when session_no >= 50 then avg_close_50 end as sma_50,
    -- Khối lượng thô: hệ số gộp cả cổ tức tiền, không áp cho khối lượng được
    case when session_no >= 20 then avg_volume_20 end as vol_sma_20,
    prev_adj_close,
    round((adj_close / nullif(prev_adj_close, 0) - 1) * 100, 4) as daily_return_pct
from windows
