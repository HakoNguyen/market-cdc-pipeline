--- Bảng Fact lưu các sự kiện chia cổ tức, thưởng cổ phiếu, chia tách doanh nghiệp.
with stg as (
    select * from {{ ref('stg_corporate_action') }}
)

select 
    symbol,
    action_date,
    action_type,
    value, 
    value_raw,
    updated_at
from stg;