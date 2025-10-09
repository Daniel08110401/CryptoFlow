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
        # 구독할 모든 토픽 리스트
        topics = [
            settings.KAFKA_TRADE_TOPIC,
            settings.KAFKA_TICKER_TOPIC,
            settings.KAFKA_ORDERBOOK_TOPIC,
        ]
        self._consumer = AIOKafkaConsumer(
            *topics, 
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id="crypto-data-group",
            auto_offset_reset='earliest'
        )
        self._redis_client = redis.from_url(
            f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}"
        )
    
    async def _process_trade(self, data: UpbitTradeSchema):
        try:
            stream_key = f"trades:{data.symbol}"
            # stream_data 생성
            stream_data = {
                "price": data.price,
                "volume": data.volume,
                "side": data.side,
                "timestamp": data.timestamp.isoformat(),
            }
            await self._redis_client.xadd(stream_key, stream_data, maxlen=10000, approximate=True)
        except redis.RedisError as e:
            logging.error(f"Redis error while processing trade: {e}")

    async def _process_ticker(self, data: UpbitTickerSchema):
        """
        현재가 데이터를 Redis Hash로 저장(최신 상태 덮어쓰기)
        """
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
        """
        호가 데이터를 Redis Hash로 저장 (최신 상태 덮어쓰기)
        """
        hash_key = f"orderbook:{data.symbol}"
        await self._redis_client.hset(
            hash_key,
            mapping={
                "data": data.model_dump_json(),
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        )

    # async def run(self):
    #     """
    #     Kafka Consumer를 시작하고 메시지를 지속적으로 처리
    #     """
    #     logging.info("Starting Kafka consumer...")
    #     await self._consumer.start()
        
    #     schema_map = {
    #         "trade": UpbitTradeSchema,
    #         "ticker": UpbitTickerSchema,
    #         "orderbook": UpbitOrderbookSchema,
    #     }

    #     try:
    #         # 이제 메시지를 처리하는 부분만 루프를 돕니다.
    #         async for msg in self._consumer:
    #             try:
    #                 raw_data = json.loads(msg.value.decode('utf-8'))
    #                 msg_type = raw_data.get("type")
    #                 SchemaModel = schema_map.get(msg_type)
                    
    #                 if not SchemaModel:
    #                     continue
                        
    #                 data = SchemaModel.model_validate(raw_data)
                    
    #                 if isinstance(data, UpbitTradeSchema):
    #                     await self._process_trade(data)
    #                 elif isinstance(data, UpbitTickerSchema):
    #                     await self._process_ticker(data)
    #                 elif isinstance(data, UpbitOrderbookSchema):
    #                     await self._process_orderbook(data)

    #             except ValidationError as e:
    #                 logging.error(f"Validation Error in consumer: {e}")
    #             except Exception as e:
    #                 logging.error(f"Error processing message in consumer: {e.__class__.__name__} - {e}", exc_info=True)
    #     finally:
    #         # 루프가 어떤 이유로든 종료되면 close 호출
    #         await self.close()
    async def run(self):
        """
        Kafka Consumer를 시작하고 메시지를 지속적으로 처리
        """
        logging.info("Starting Kafka consumer...")
        await self._consumer.start()
        
        schema_map = {
            "trade": UpbitTradeSchema,
            "ticker": UpbitTickerSchema,
            "orderbook": UpbitOrderbookSchema,
        }

        try:
            # async for 대신 while 루프와 getone() 사용
            # 새로운 메시지가 도착할 때까지 기다렸다가 메시지 하나를 가져오는 역할
            while True:
                msg = await self._consumer.getone()
                try:
                    raw_data = json.loads(msg.value.decode('utf-8'))
                    msg_type = raw_data.get("type")
                    SchemaModel = schema_map.get(msg_type)
                    
                    if not SchemaModel:
                        continue
                        
                    data = SchemaModel.model_validate(raw_data)
                    
                    if isinstance(data, UpbitTradeSchema):
                        await self._process_trade(data)
                    elif isinstance(data, UpbitTickerSchema):
                        await self._process_ticker(data)
                    elif isinstance(data, UpbitOrderbookSchema):
                        await self._process_orderbook(data)

                except ValidationError as e:
                    logging.error(f"Validation Error in consumer: {e}")
                except Exception as e:
                    logging.error(f"Error processing message in consumer: {e.__class__.__name__} - {e}", exc_info=True)
        finally:
            # 루프가 어떤 이유로든 종료되면 close 호출
            await self.close()


    async def close(self):
        """
        Consumer와 Redis 클라이언트를 안전하게 종료
        """
        logging.info("Stopping Kafka consumer...")
        await self._consumer.stop()
        await self._redis_client.close()
        logging.info("Consumer and Redis client stopped")