--- Bảng Dimension chứa thông tin các mã cổ phiếu / crypto, sàn giao dịch và ngành nghề.

with stg as (
    select * from {{ ref('stg_instrument') }}
)

select 
    symbol, 
    name,
    exchange,
    sector,
    is_listed,
    updated_at
from stg;