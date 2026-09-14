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
    adj_close: float = Field(gt=0)
    volume: int = Field(ge=0)

    @model_validator(mode='after') 
    def check_high_low(self) -> 'DailyBarRecord':
        if self.high < self.low:
            raise ValueError(f"high ({self.high}) must be >= low ({self.low})")
        return self

class CorporateActionRecord(BaseModel):
    symbol: str
    action_date: date
    action_type: Literal['dividend', 'split']
    value: float = Field(gt=0)
    value_raw: Optional[float] = None

class InstrumentRecord(BaseModel):
    symbol: str
    name: Optional[str] = None
    exchange: str = "HOSE"
    sector: Optional[str] = None
    is_listed: bool = True