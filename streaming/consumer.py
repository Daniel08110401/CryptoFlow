## Kafka에 데이터를 읽을 때 이 형식에 맞춰서 보내야 함


# 체결(Trade) 데이터: 계속 쌓이는 이벤트이므로 Redis Streams에 기록

# 현재가(Ticker) & 호가(Orderbook) 데이터: 특정 시점의 상태(Snapshot)이므로 Redis Hash에 최신 값으로 계속 덮어씀

import asyncio
import json
from datetime import datetime, timezone
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from streaming.config import settings
from shared.schemas import (
    UpbitWebsocketData, 
    UpbitTradeSchema, 
    UpbitTickerSchema, 
    UpbitOrderbookSchema
)

class TradeDataConsumer:
    """
    Kafka 토픽에서 데이터를 소비하여 Redis에 저장하는 Consumer 클래스.
    """
    def __init__(self):
        # 구독할 모든 토픽 리스트
        topics = [
            settings.KAFKA_TRADE_TOPIC,
            settings.KAFKA_TICKER_TOPIC,
            settings.KAFKA_ORDERBOOK_TOPIC,
        ]
        self._consumer = AIOKafkaConsumer(
            *topics, # 여러 토픽을 동시에 구독
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="crypto-data-group" # 컨슈머 그룹 ID
        )
        self._redis_client = redis.from_url(
            f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}"
        )

    async def _process_trade(self, data: UpbitTradeSchema):
        """체결 데이터를 Redis Streams에 저장합니다."""
        stream_key = f"trades:{data.symbol}"
        stream_data = {
            "price": data.price,
            "volume": data.volume,
            "side": data.side,
            "timestamp": data.timestamp.isoformat(),
        }
        # MAXLEN ~ 10000 : 스트림의 길이를 대략 10000개로 유지
        await self._redis_client.xadd(stream_key, stream_data, maxlen=10000, approximate=True)

    async def _process_ticker(self, data: UpbitTickerSchema):
        """현재가 데이터를 Redis Hash로 저장합니다 (최신 상태 덮어쓰기)."""
        hash_key = f"ticker:{data.symbol}"
        # Pydantic 모델 전체를 JSON 문자열로 저장하여 한 번에 관리
        await self._redis_client.hset(
            hash_key,
            mapping={
                "data": data.model_dump_json(),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        )
        
    async def _process_orderbook(self, data: UpbitOrderbookSchema):
        """호가 데이터를 Redis Hash로 저장합니다 (최신 상태 덮어쓰기)."""
        hash_key = f"orderbook:{data.symbol}"
        await self._redis_client.hset(
            hash_key,
            mapping={
                "data": data.model_dump_json(),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        )

    async def run(self):
        """Kafka Consumer를 시작하고 메시지를 지속적으로 처리합니다."""
        await self._consumer.start()
        print("Kafka consumer started.")
        try:
            async for msg in self._consumer:
                try:
                    # Producer와 동일한 Union 스키마로 데이터 검증
                    data = UpbitWebsocketData.model_validate_json(msg.value)
                    
                    # 데이터 타입에 따라 적절한 처리 함수 호출
                    if isinstance(data, UpbitTradeSchema):
                        await self._process_trade(data)
                    elif isinstance(data, UpbitTickerSchema):
                        await self._process_ticker(data)
                    elif isinstance(data, UpbitOrderbookSchema):
                        await self._process_orderbook(data)

                except ValidationError as e:
                    print(f"Validation Error in consumer: {e}")
                except Exception as e:
                    print(f"Error processing message: {e}")
        finally:
            await self.close()

    async def close(self):
        """Consumer와 Redis 클라이언트를 안전하게 종료합니다."""
        print("Stopping Kafka consumer...")
        await self._consumer.stop()
        await self._redis_client.close()
        print("Consumer and Redis client stopped.")