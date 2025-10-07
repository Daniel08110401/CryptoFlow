# bitcoin/services_upbit.py

import requests
import datetime as dt
import time
from decimal import Decimal
from django.utils import timezone as tz
from .models import (
    RawIngest,
    SpotSnapshot,
    Market24hStat,
    Crypto24hRawIngestLanding,
    Crypto24hRawIngestEvent,
)

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

def fetch_upbit_24h_data(trading_pairs: str) -> dict:
    """Fetch 24h trading data for given pairs, returns dict mapping market->acc_trade_price_24h"""
    url = f"{UPBIT_BASE}/ticker"
    params = {"markets": trading_pairs}
    headers = {"Content-Type": "application/json"}
    
    response = requests.get(url, headers=headers, params=params, timeout=5)
    response.raise_for_status()
    data = response.json()
    
    return {item.get("market"): item.get("acc_trade_price_24h") for item in data}

def save_upbit_24h_stats(trading_pairs: str) -> dict:
    """Fetch 24h stats and persist them per market into Market24hStat and RawIngest"""
    url = f"{UPBIT_BASE}/ticker"
    params = {"markets": trading_pairs}
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=5)
    response.raise_for_status()
    items = response.json()

    results = {}
    for item in items:
        market = item.get("market")
        if not market:
            continue
        trade_ts_ms = int(item.get("trade_timestamp")) if item.get("trade_timestamp") else None
        event_dt_utc = _utc_from_ms(trade_ts_ms) if trade_ts_ms else tz.now()

        # Store raw payload for audit/debug
        RawIngest.objects.create(
            source="upbit",
            endpoint="ticker",
            symbol=market,
            payload=item,
            ts_event=event_dt_utc,
        )

        # Persist selected 24h metrics
        Market24hStat.objects.create(
            symbol=market,
            acc_trade_price_24h=Decimal(str(item.get("acc_trade_price_24h", 0))),
            acc_trade_volume_24h=Decimal(str(item.get("acc_trade_volume_24h", 0))) if item.get("acc_trade_volume_24h") is not None else None,
            source="upbit",
            ts_event=event_dt_utc,
        )
        results[market] = {
            "acc_trade_price_24h": item.get("acc_trade_price_24h"),
            "acc_trade_volume_24h": item.get("acc_trade_volume_24h"),
        }

    return results

def save_upbit_ticker_batch(trading_pairs: str) -> dict:
    """
    Data-lake landing: fetch the entire ticker list for the given markets and
    write ONE landing row capturing request params, HTTP status, response headers,
    full JSON payload, received timestamp, and duration.
    """

    url = f"{UPBIT_BASE}/ticker"
    params = {"markets": trading_pairs}
    started = time.monotonic()
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=5)
    duration_ms = int((time.monotonic() - started) * 1000)
    # Do not raise first; we want to capture failures, too
    payload = None
    try:
        payload = response.json()
    except Exception:
        payload = {"parse_error": True, "text": response.text[:2000]}

    received_at = tz.now()

    landing = Crypto24hRawIngestLanding.objects.create(
        source="upbit",
        endpoint="ticker",
        request_params=params,
        http_status=response.status_code,
        headers=dict(response.headers),
        payload=payload,
        received_at=received_at,
        duration_ms=duration_ms,
    )

    # If 2xx, return a simple summary to caller
    if 200 <= response.status_code < 300 and isinstance(payload, list):
        markets = []
        for it in payload:
            if not isinstance(it, dict):
                continue
            market = it.get("market") or "UNKNOWN"
            markets.append(market)
            ts_ms = it.get("trade_timestamp")
            ts_event = _utc_from_ms(int(ts_ms)) if ts_ms else received_at
            Crypto24hRawIngestEvent.objects.create(
                source="upbit",
                symbol=market,
                ts_event=ts_event,
                payload=it,
                call_id=landing.id,
            )
        return {"count": len(markets), "markets": markets[:50]}
    else:
        # For non-2xx, surface status to caller
        return {"status": response.status_code}

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