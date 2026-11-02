from datetime import date
from typing import Optional, Literal
from pydantic import BaseModel, Field, model_validator

class DailyBarRecord(BaseModel):
    symbol: str
    trade_date: date
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: int = Field(ge=0)

    @model_validator(mode='after') 
    def check_high_low(self) -> 'DailyBarRecord':
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must be >= low ({self.low})")
        return self

REQUIRED_FIELDS = {
    "cash_dividend": {"cash_amount"},
    "stock_dividend": {"ratio"},
    "bonus_share": {"ratio"},
    "rights_issue": {"ratio", "subscription_price"},
}
NUMERIC_FIELDS = ("cash_amount", "ratio", "subscription_price")

class CorporateActionRecord(BaseModel):
    symbol: str
    ex_date: date
    action_type: Literal['cash_dividend', 'stock_dividend', 'bonus_share', 'rights_issue']
    cash_amount: Optional[float] = None
    ratio: Optional[float] = None
    subscription_price: Optional[float] = None
    announce_date: Optional[date] = None

    @model_validator(mode='after')
    def check_fields_by_type(self) -> 'CorporateActionRecord':
        required = REQUIRED_FIELDS[self.action_type]
        for field in NUMERIC_FIELDS:
            value = getattr(self, field)
            if field in required:
                if value is None or value <= 0:
                    raise ValueError(f"{self.action_type} requires {field} > 0")
            else:
                if value is not None:
                    raise ValueError(f"{self.action_type} must not have {field}")
        return self


class InstrumentRecord(BaseModel):
    symbol: str
    name: Optional[str] = None
    exchange: str = "HOSE"
    sector: Optional[str] = None
    is_listed: bool = True