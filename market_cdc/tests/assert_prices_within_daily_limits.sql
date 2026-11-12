-- Giá trong phiên phải nằm trong biên độ trần/sàn do sàn công bố cho CHÍNH phiên đó.
-- Dùng giá trần/sàn thật thay vì tự suy ra ±7%: tự đúng cho HOSE ±7%, HNX ±10%, UPCoM ±15%,
-- cả khi một mã chuyển sàn (MCH: UPCoM -> HOSE) và khi sàn làm tròn trần/sàn theo bước giá.
-- high/low nằm trong biên độ thì open/close cũng vậy (assert_positive_prices đã kiểm high >= low).
select
    symbol,
    trade_date,
    low,
    high,
    floor_price,
    ceiling_price
from {{ ref('stg_daily_bar') }}
where high > ceiling_price
   or low < floor_price
