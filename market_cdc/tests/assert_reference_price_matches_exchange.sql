-- Giá tham chiếu tự tính (int_adjustment_factor) tại mỗi ngày GDKHQ phải khớp
-- giá tham chiếu do sàn công bố (stg_daily_bar.reference_price), lệch tối đa 0.5%.
-- Lệch nhỏ là do sàn làm tròn theo bước giá; MCH thời UPCoM lệch ~0.24% vì UPCoM lấy giá bình quân.
select
    f.symbol,
    f.ex_date,
    f.ref_price,
    b.reference_price,
    abs(f.ref_price / b.reference_price - 1) as rel_diff
from {{ ref('int_adjustment_factor') }} f
join {{ ref('stg_daily_bar') }} b
    on f.symbol = b.symbol
    and f.ex_date = b.trade_date
where abs(f.ref_price / b.reference_price - 1) > 0.005
