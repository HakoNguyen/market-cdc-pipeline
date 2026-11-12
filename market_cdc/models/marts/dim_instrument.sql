-- SCD2: mỗi lần thông tin mã đổi (tên, sàn, ngành, trạng thái niêm yết) có thêm 1 dòng.
-- Bản hiện tại: is_current. Tra theo thời điểm t: valid_from <= t and (valid_to is null or t < valid_to).
select
    symbol,
    name,
    exchange,
    sector,
    is_listed,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_instrument') }}
