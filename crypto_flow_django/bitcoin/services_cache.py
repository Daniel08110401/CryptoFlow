# crypto_flow_django/bitcoin/services_cache.py
import json
import redis
import os

# Docker Compose 환경의 Redis 서비스에 직접 연결
# 'redis'는 docker-compose.yml에 정의된 서비스 이름
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

#redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_realtime_ticker_from_redis(market="KRW-BTC"):
    """
    stream-app이 저장한 Redis Hash에서 실시간 Ticker 데이터를 직접 읽어옴
    """
    hash_key = f"ticker:{market}"
    field_key = "data"
    
    # Redis의 HGET 명령어로 Hash에서 특정 필드의 값을 가져옴
    raw_data = redis_client.hget(hash_key, field_key)
    
    if raw_data:
        # JSON 문자열을 파이썬 딕셔너리로 변환하여 반환
        return json.loads(raw_data)
    
    # Redis에 데이터가 없을 경우 None을 반환
    return None