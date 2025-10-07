# shared/schemas.py
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator
from typing import Literal, List, Union

def convert_timestamp_to_datetime(value: int) -> datetime:
    """ 
    Upbit에서 제공하는 millisecond 단위의 Unix timestamp를
    Python의 표준 datetime 객체(UTC)로 변환하는 공통 함수
    """
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)

## 호가(Orderbook) 데이터의 중첩된 구조를 위한 하위 스키마 ##
class UpbitOrderbookUnitSchema(BaseModel):
    """
    호가 정보(매수/매도 각 1개 라인)를 나타내는 가장 작은 단위의 스키마
    """
    ask_price: float = Field(alias="ap")
    ask_size: float = Field(alias="as")
    bid_price: float = Field(alias="bp")
    bid_size: float = Field(alias="bs")

## 메인 데이터 타입별 스키마 ##
 
class UpbitTradeSchema(BaseModel):
    """
    실시간 체결(Trade) 데이터 스키마
    """
    type: Literal["trade"] = Field(alias="ty")
    symbol: str = Field(alias="cd")
    price: float = Field(alias="tp")
    volume: float = Field(alias="tv")
    side: Literal["ASK", "BID"] = Field(alias="ab")
    timestamp: datetime = Field(alias="tms")

    # Pydantic v2 스타일의 field_validator
    _validate_timestamp = field_validator("timestamp", mode="before")(convert_timestamp_to_datetime)
    
    class Config:
        populate_by_name = True

class UpbitTickerSchema(BaseModel):
    """
    실시간 현재가(Ticker) 데이터 스키마
    """
    type: Literal["ticker"] = Field(alias="ty")
    symbol: str = Field(alias="cd")
    open_price: float = Field(alias="op")
    high_price: float = Field(alias="hp")
    low_price: float = Field(alias="lp")
    trade_price: float = Field(alias="tp") # 현재가
    change: Literal["RISE", "EVEN", "FALL"] = Field(alias="c")
    change_price: float = Field(alias="cp")
    signed_change_rate: float = Field(alias="scr")
    acc_trade_volume_24h: float = Field(alias="atv24h")
    timestamp: datetime = Field(alias="tms")

    _validate_timestamp = field_validator("timestamp", mode="before")(convert_timestamp_to_datetime)

    class Config:
        populate_by_name = True

class UpbitOrderbookSchema(BaseModel):
    """
    실시간 호가(Orderbook) 데이터 스키마
    """

    type: Literal["orderbook"] = Field(alias="ty")
    symbol: str = Field(alias="cd")
    total_ask_size: float = Field(alias="tas")
    total_bid_size: float = Field(alias="tbs")
    orderbook_units: List[UpbitOrderbookUnitSchema] = Field(alias="obu")
    timestamp: datetime = Field(alias="tms")

    _validate_timestamp = field_validator("timestamp", mode="before")(convert_timestamp_to_datetime)

    class Config:
        populate_by_name = True

# --- 모든 웹소켓 데이터 타입을 아우르는 Union 타입 ---

UpbitWebsocketData = Union[UpbitTradeSchema, UpbitTickerSchema, UpbitOrderbookSchema]
