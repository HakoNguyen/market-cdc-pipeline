--- Bảng Fact lưu các sự kiện quyền: cổ tức tiền, cổ tức cổ phiếu, thưởng cổ phiếu, phát hành quyền mua.
with stg as (
    select * from {{ ref('stg_corporate_action') }}
)

select
    symbol,
    ex_date,
    action_type,
    cash_amount,
    ratio,
    subscription_price,
    announce_date,
    updated_at
from stg