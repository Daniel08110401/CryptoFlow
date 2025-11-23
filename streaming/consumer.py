## Kafka에 데이터를 읽을 때 이 형식에 맞춰서 보내야 함

# 체결(Trade) 데이터: 계속 쌓이는 이벤트이므로 Redis Streams에 기록

# 현재가(Ticker) & 호가(Orderbook) 데이터: 특정 시점의 상태(Snapshot)이므로 Redis Hash에 최신 값으로 계속 덮어씀

import redis.asyncio as redis
import json
import logging
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError
from streaming.config import settings
from shared.schemas import (
    UpbitTradeSchema, 
    UpbitTickerSchema, 
    UpbitOrderbookSchema
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TradeDataConsumer:
    """
    Kafka 토픽에서 데이터를 소비하여 Redis에 저장하는 Consumer 클래스
    """
    def __init__(self):
        # 1. 구독할 토픽 리스트
        topics = [
            settings.KAFKA_TRADE_TOPIC,
            settings.KAFKA_TICKER_TOPIC,
            settings.KAFKA_ORDERBOOK_TOPIC,
        ]
        
        # 2. Kafka Consumer 설정 (Group ID 지정으로 확장성 확보)
        self._consumer = AIOKafkaConsumer(
            *topics, 
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="crypto-data-group",
            auto_offset_reset='earliest'
        )
        
        # 3. Redis 클라이언트 설정
        self._redis_client = redis.from_url(
            f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}"
        )
        
        # 4. 메시지 타입별 스키마 매핑 (Strategy Pattern)
        self._schema_map = {
            "trade": UpbitTradeSchema,
            "ticker": UpbitTickerSchema,
            "orderbook": UpbitOrderbookSchema,
        }
    
    async def _process_trade(self, data: UpbitTradeSchema):
        """
        체결(Trade) 데이터: 시계열(Time-series) 데이터이므로 Redis Streams에 순차적으로 기록
        """
        try:
            stream_key = f"trades:{data.symbol}"
            
            # Redis Streams에 저장하기 위해 dict 형태로 변환
            # timestamp 등 복잡한 객체는 문자열로 변환 필요하지만, 
            # model_dump(mode='json')을 쓰면 Pydantic이 알아서 처리해줌
            stream_data = data.model_dump(mode='json')
            
            # XADD: 스트림에 이벤트 추가 (maxlen으로 데이터 양 조절하여 메모리 관리)
            await self._redis_client.xadd(stream_key, stream_data, maxlen=10000, approximate=True)
            
        except redis.RedisError as e:
            logging.error(f"Redis error while processing trade: {e}")

    async def _process_ticker(self, data: UpbitTickerSchema):
        """
        현재가(Ticker) 데이터: 최신 상태(Snapshot)가 중요하므로 Redis Hash에 덮어쓰기
        """
        try:
            hash_key = f"ticker:{data.symbol}"
            await self._redis_client.hset(
                hash_key,
                mapping={
                    "data": data.model_dump_json(), # 데이터를 통째로 JSON 문자열로 저장
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
            )
        except redis.RedisError as e:
            logging.error(f"Redis error while processing ticker: {e}")
        
    async def _process_orderbook(self, data: UpbitOrderbookSchema):
        """
        호가(Orderbook) 데이터: 최신 상태(Snapshot)가 중요하므로 Redis Hash에 덮어쓰기
        """
        try:
            hash_key = f"orderbook:{data.symbol}"
            await self._redis_client.hset(
                hash_key,
                mapping={
                    "data": data.model_dump_json(),
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
            )
        except redis.RedisError as e:
            logging.error(f"Redis error while processing orderbook: {e}")

    async def run(self):
        """
        Kafka Consumer를 시작하고 메시지를 지속적으로 처리
        """
        logging.info("Starting Kafka consumer...")
        await self._consumer.start()
        
        try:
            # [Optimization] async for 루프 사용
            # AIOKafka가 내부적으로 Prefetching을 수행하여 처리량(Throughput)을 극대화함
            async for msg in self._consumer:
                try:
                    # 1. 메시지 파싱 (Bytes -> JSON)
                    raw_data = json.loads(msg.value.decode('utf-8'))
                    
                    # 2. 메시지 타입 확인 및 스키마 매핑
                    msg_type = raw_data.get("type")
                    SchemaModel = self._schema_map.get(msg_type)
                    
                    if not SchemaModel:
                        # 알 수 없는 메시지 타입은 무시
                        continue
                        
                    # 3. 데이터 검증 (Pydantic)
                    data = SchemaModel.model_validate(raw_data)
                    
                    # 4. 타입별 처리 로직 분기
                    if isinstance(data, UpbitTradeSchema):
                        await self._process_trade(data)
                    elif isinstance(data, UpbitTickerSchema):
                        await self._process_ticker(data)
                    elif isinstance(data, UpbitOrderbookSchema):
                        await self._process_orderbook(data)

                except ValidationError as e:
                    logging.error(f"Validation Error in consumer: {e}")
                except json.JSONDecodeError:
                    logging.error(f"Failed to decode JSON message: {msg.value}")
                except Exception as e:
                    logging.error(f"Error processing message: {e.__class__.__name__} - {e}", exc_info=True)
                    
        finally:
            # 루프가 종료되면(예: CancelledError) 리소스 정리
            await self.close()

    async def close(self):
        """
        Consumer와 Redis 클라이언트를 안전하게 종료
        """
        logging.info("Stopping Kafka consumer...")
        await self._consumer.stop()
        await self._redis_client.close()
        logging.info("Consumer and Redis client stopped")