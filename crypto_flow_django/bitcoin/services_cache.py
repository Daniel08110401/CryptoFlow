# bitcoin/services_cache.py
import json
from django.core.cache import cache  # Django-Redis 설정 필요
from .services_upbit import fetch_upbit_ticker

def get_realtime_price_cached(market="KRW-BTC", ttl=3):
    key = f"price:{market}"
    val = cache.get(key)
    if val:
        return json.loads(val)
    data = fetch_upbit_ticker(market)
    cache.set(key, json.dumps(data), ttl)
    return data
