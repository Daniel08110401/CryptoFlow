## Kafka에 데이터를 쓸 때 이 형식에 맞춰서 보내야 함

# 1. Upbit WebSocket 서버에 접속

# 2. 우리가 원하는 데이터(체결, 티커, 호가)를 구독 요청

# 3. 메시지를 받으면 shared/schemas.py의 모델로 유효성을 검사

# 4. 검증된 데이터를 적절한 Kafka 토픽으로 전송

# 5. 연결이 끊기면 자동으로 재접속을 시도

import asyncio
import json
import uuid
import websockets
import logging
from websockets.exceptions import ConnectionClosed
from aiokafka import AIOKafkaProducer
from pydantic import ValidationError
from streaming.config import settings
from shared.schemas import (
    UpbitTradeSchema, 
    UpbitTickerSchema, 
    UpbitOrderbookSchema
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class UpbitWebsocketProducer:
    """
    Upbit WebSocket에 연결하여 데이터를 수신하고 Kafka로 전송하는 Producer 클래스
    """
    def __init__(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )
        # 데이터 타입과 Kafka 토픽을 매핑
        self._topic_map = {
            UpbitTradeSchema: settings.KAFKA_TRADE_TOPIC,
            UpbitTickerSchema: settings.KAFKA_TICKER_TOPIC,
            UpbitOrderbookSchema: settings.KAFKA_ORDERBOOK_TOPIC,
        }
        # message type과 Pydantic 스키마를 매핑
        self._schema_map = {
            "trade": UpbitTradeSchema,
            "ticker": UpbitTickerSchema,
            "orderbook": UpbitOrderbookSchema,
        }

    # async def _handle_message(self, message: str):
    #     """
    #     수신된 메시지를 파싱, 검증하고 적절한 Kafka 토픽으로 전송
    #     """
    #     try:
    #         logging.info("--> STEP 1: Received message from Upbit.")

    #         # logging.info(f"RAW DATA: {message}") 

    #         # 1.  JSON 딕셔너리로 파싱
    #         raw_data = json.loads(message)
            
    #         # 2. 'ty' 필드로 메시지 타입 확인
    #         msg_type = raw_data.get("ty")
            
    #         # 3. 타입에 맞는 스키마를 맵에서 가져오기
    #         SchemaModel = self._schema_map.get(msg_type)
            
    #         if not SchemaModel:
    #             # 처리하기로 한 타입이 아니면 무시
    #             return

    #         # 4. 해당 스키마로 데이터 검증
    #         data = SchemaModel.model_validate(raw_data)
            
    #         # 데이터 타입에 맞는 토픽을 가져옴
    #         topic = self._topic_map.get(type(data))
    #         if not topic:
    #             logging.warning(f"No topic mapping for data type: {type(data)}")
    #             return
            
    #         logging.info(f"--> STEP 2: Sending validated '{data.type}' data for {data.symbol} to Kafka topic '{topic}'.")

    #         await self._producer.send_and_wait(
    #             topic, 
    #             data.model_dump_json().encode('utf-8')
    #         )

    #         logging.info("--> STEP 3: Successfully sent message to Kafka.")
            
    #     except ValidationError as e:
    #         #print(f"Validation Error: {e}\nRaw Message: {message[:200]}...")
    #         logging.error(f"Validation Error: {e}")
    #     except Exception as e:
    #         # 더 자세한 에러를 볼 수 있도록 수정
    #         logging.error(f"An unexpected error occurred in producer: {e.__class__.__name__} - {e}", exc_info=True)
    async def _handle_message(self, message: str):
        """
        수신된 메시지를 파싱, 검증하고 적절한 Kafka 토픽으로 전송
        """
        try:
            logging.info("--> STEP 1: Received message from Upbit.")
            
            raw_data = json.loads(message)
            msg_type = raw_data.get("type") # 'ty'가 아니라 'type'일 수 있으므로 확인
            
            SchemaModel = self._schema_map.get(msg_type)
            
            if SchemaModel:
                # 스키마가 존재하면 검증 및 처리 진행
                data = SchemaModel.model_validate(raw_data)
                
                topic = self._topic_map.get(type(data))
                if not topic:
                    logging.warning(f"No topic mapping for data type: {type(data)}")
                    return
                
                logging.info(f"--> STEP 2: Sending validated '{data.type}' data for {data.symbol} to Kafka topic '{topic}'.")
                
                await self._producer.send_and_wait(
                    topic, 
                    data.model_dump_json().encode('utf-8')
                )
                
                logging.info("--> STEP 3: Successfully sent message to Kafka.")
            else:
                # 스키마가 없는 메시지는 로그를 남기고 무시
                logging.warning(f"--> SKIPPING: No schema found for message type '{msg_type}'. Raw data: {message[:300]}")
            
        except ValidationError as e:
            logging.error(f"Validation Error: {e}\nRaw Message: {message[:300]}...")
        except Exception as e:
            logging.error(f"An unexpected error occurred in producer: {e.__class__.__name__} - {e}", exc_info=True)

    async def run(self):
        """
        WebSocket 연결, 메시지 수신 및 재연결 로직을 포함한 메인 실행 루프
        """
        await self._producer.start()
        
        subscribe_message = [
            {"ticket": str(uuid.uuid4())},
            {"type": "trade", "codes": ["KRW-BTC"]},
            {"type": "ticker", "codes": ["KRW-BTC"]},
            {"type": "orderbook", "codes": ["KRW-BTC"]},
        ]
        
        while True:
            try:
                async with websockets.connect(settings.UPBIT_WEBSOCKET_URL) as websocket:
                    #print("WebSocket connected. Sending subscription request...")
                    logging.info("WebSocket connected, Sending subscription request")
                    await websocket.send(json.dumps(subscribe_message))
                    logging.info("Subscription request sent.")
                    
                    async for message in websocket:
                        await self._handle_message(message)
                        
            except ConnectionClosed as e:
                logging.error(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(f"An error occurred in run loop: {e.__class__.__name__} - {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
                
    async def close(self):
        """
        Kafka Producer를 종료
        """
        print("Stopping Kafka producer...")
        await self._producer.stop()
        print("Producer stopped.")