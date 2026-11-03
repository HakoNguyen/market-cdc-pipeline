-- Phát hiện sự kiện quyền BỊ THIẾU (nguồn bỏ sót, hoặc bị xoá nhầm ở Postgres).
-- HOSE/HNX: giá tham chiếu = giá đóng cửa phiên trước, trừ ngày GDKHQ (sàn điều chỉnh giảm).
-- => tham chiếu lệch giá đóng cửa hôm trước mà không có corporate action nào ngày đó = thiếu sự kiện,
--    và giá điều chỉnh trước ngày đó đang sai.
-- Bỏ qua phiên có biên độ > 10%: UPCoM (±15%, tham chiếu = giá bình quân, không phải giá đóng cửa)
-- và phiên đầu niêm yết (±20%+). Giá trị factor của sự kiện CÓ tồn tại do
-- assert_reference_price_matches_exchange kiểm tra.
with bars as (
    select
        symbol,
        trade_date,
        reference_price,
        ceiling_price,
        lag(close) over (partition by symbol order by trade_date) as prev_close
    from {{ ref('stg_daily_bar') }}
)

select
    b.symbol,
    b.trade_date,
    b.prev_close,
    b.reference_price,
    b.reference_price / b.prev_close - 1 as gap
from bars b
left join (select distinct symbol, ex_date from {{ ref('stg_corporate_action') }}) e
    on e.symbol = b.symbol
    and e.ex_date = b.trade_date
where b.prev_close is not null
  and b.ceiling_price / b.reference_price - 1 <= 0.10
  and abs(b.reference_price / b.prev_close - 1) > 0.001
  and e.symbol is null
