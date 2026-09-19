--- Bảng Fact chính lưu giá nến ngày OHLCV kèm 
--- các chỉ báo kỹ thuật (sma_20, sma_50, vol_sma_20, % return).
with indicators as (
    select * from {{ ref('int_daily_bar_indicators') }}
)
select 
    symbol,
    trade_date,
    open, 
    high, 
    low,
    close,
    adj_close,
    volume,
    sma_20, 
    sma_50, 
    sma_25, 
    prev_close, 
    daily_return_pct
from indicators;