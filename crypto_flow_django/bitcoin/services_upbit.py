# bitcoin/services_upbit.py

import requests
import datetime as dt
from decimal import Decimal
from django.utils import timezone as tz
from .models import RawIngest, SpotSnapshot

UPBIT_BASE = "https://api.upbit.com/v1"
DEFAULT_HEADERS = {"Accept": "application/json", "User-Agent": "CryptoFlow/1.0"}

def fetch_upbit_ticker(market: str = "KRW-BTC") -> dict:
    url = f"{UPBIT_BASE}/ticker"
    resp = requests.get(url, params={"markets": market}, headers=DEFAULT_HEADERS, timeout=5)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError("Unexpected Upbit response format or empty list")
    return payload[0]

def _utc_from_ms(ms: int) -> dt.datetime:
    # Upbit trade_timestamp is milliseconds since epoch in UTC
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=tz.utc)

def save_upbit_ticker(market: str = "KRW-BTC") -> dict:
    data = fetch_upbit_ticker(market)
    event_dt_utc = _utc_from_ms(int(data["trade_timestamp"]))

    RawIngest.objects.create(
        source="upbit",
        endpoint="ticker",
        symbol=market,
        payload=data,
        ts_event=event_dt_utc,
    )

    base, quote = market.split("-", 1) if "-" in market else (market, "KRW")
    SpotSnapshot.objects.create(
        symbol=market,
        price=Decimal(str(data["trade_price"])),
        quote_ccy=quote,
        source="upbit",
        ts_event=event_dt_utc,
    )
    return data