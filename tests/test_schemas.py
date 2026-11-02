from datetime import date

import pytest
from pydantic import ValidationError

from ingestion.source.schemas import CorporateActionRecord


def make(**fields):
    return CorporateActionRecord(symbol="FPT", ex_date=date(2025, 7, 21), **fields)


@pytest.mark.parametrize("fields", [
    dict(action_type="cash_dividend", cash_amount=1000),
    dict(action_type="stock_dividend", ratio=0.15),
    dict(action_type="bonus_share", ratio=0.15),
    dict(action_type="rights_issue", ratio=0.2, subscription_price=15000, announce_date=date(2025, 11, 20)),
])
def test_valid_corporate_actions(fields):
    make(**fields)


@pytest.mark.parametrize("fields", [
    dict(action_type="cash_dividend"),                            # thiếu tiền
    dict(action_type="cash_dividend", cash_amount=0),             # tiền = 0
    dict(action_type="rights_issue", ratio=0.2),                  # thiếu giá mua
    dict(action_type="bonus_share", ratio=0.1, cash_amount=500),  # thừa tiền
    dict(action_type="split", ratio=2),                           # sai loại
    dict(action_type="stock_dividend", ratio=0),                  # tỷ lệ = 0
])
def test_invalid_corporate_actions(fields):
    with pytest.raises(ValidationError):
        make(**fields)
