from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator
from typing import List, Literal, Union

# --- Reusable Smart Timestamp Validator ---
def smart_timestamp_converter(value: Union[int, str]) -> datetime:
    """
    Handles both integer/string millisecond timestamps and ISO 8601 formatted strings.
    """
    if isinstance(value, str) and not value.isdigit():
        # Handle ISO 8601 format like '2025-10-09T08:28:29.337000Z'
        # Pydantic v2 automatically handles standard ISO 8601 strings if the type hint is datetime.
        # We'll rely on that parsing and just ensure it's timezone-aware.
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc)
    else:
        # Handle millisecond timestamps (as int or string)
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)

# --- Sub-schema for nested structure in Orderbook data ---

class UpbitOrderbookUnitSchema(BaseModel):
    """Represents the smallest unit of an order book (one ask/bid line)."""
    ask_price: float = Field(alias="ask_price")
    bid_price: float = Field(alias="bid_price")
    ask_size: float = Field(alias="ask_size")
    bid_size: float = Field(alias="bid_size")

# --- Main Schemas for each data type ---

class UpbitTradeSchema(BaseModel):
    """Schema for real-time trade data."""
    type: str = Field(alias="type")
    symbol: str = Field(alias="code")
    trade_price: float = Field(alias="trade_price")
    trade_volume: float = Field(alias="trade_volume")
    side: Literal["ASK", "BID"] = Field(alias="ask_bid")
    timestamp: datetime = Field(alias="trade_timestamp")

    _validate_timestamp = field_validator("timestamp", mode="before")(smart_timestamp_converter)
    
    class Config:
        populate_by_name = True

class UpbitTickerSchema(BaseModel):
    """Schema for real-time ticker data."""
    type: str = Field(alias="type")
    symbol: str = Field(alias="code")
    open_price: float = Field(alias="opening_price")
    high_price: float = Field(alias="high_price")
    low_price: float = Field(alias="low_price")
    trade_price: float = Field(alias="trade_price")
    change: Literal["RISE", "EVEN", "FALL"] = Field(alias="change")
    change_price: float = Field(alias="change_price")
    signed_change_rate: float = Field(alias="signed_change_rate")
    acc_trade_volume_24h: float = Field(alias="acc_trade_volume_24h")
    timestamp: datetime = Field(alias="timestamp")

    _validate_timestamp = field_validator("timestamp", mode="before")(smart_timestamp_converter)

    class Config:
        populate_by_name = True
        
class UpbitOrderbookSchema(BaseModel):
    """Schema for real-time order book data."""
    type: str = Field(alias="type")
    symbol: str = Field(alias="code")
    total_ask_size: float = Field(alias="total_ask_size")
    total_bid_size: float = Field(alias="total_bid_size")
    orderbook_units: List[UpbitOrderbookUnitSchema] = Field(alias="orderbook_units")
    timestamp: datetime = Field(alias="timestamp")

    _validate_timestamp = field_validator("timestamp", mode="before")(smart_timestamp_converter)

    class Config:
        populate_by_name = True