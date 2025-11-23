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
        # linger_ms: 메시지를 즉시 보내지 않고 설정한 시간(ms)만큼 모았다가 보냄 (처리량 증대)
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            linger_ms=10  
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

    
    async def _handle_message(self, message: str):
        """
        수신된 메시지를 파싱, 검증하고 지정한 Kafka 토픽으로 전송
        """
        try:
            #logging.info("--> STEP 1: Received message from Upbit.")
            
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
                
                logging.info(f"Sending validated '{data.type}' data for {data.symbol} to Kafka topic '{topic}'.")
                
                # await self._producer.send_and_wait(
                #     topic, 
                #     data.model_dump_json(by_alias=False).encode('utf-8') # 내부 필드명(symbol, trade_price)으로 통일해서 전송
                # )

                # send_and_wait -> send 메서드로 변경
                # 브로커의 Ack를 기다리지 않고 내부 버퍼에 넣고 즉시 리턴 (Throughput 향상)
                # 데이터 정합성보다 실시간 처리량이 중요할 때 사용
                await self.producer.send(
                    topic,
                    data.model_dump_json(by_alias=False).encode('utf-8')
                )

                logging.info("Successfully sent message to Kafka.")
            else:
                # 스키마가 없는 메시지는 로그를 남기고 무시
                pass
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

        retry_delay = 1
        max_retry_delay = 60 # 최대 60초까지만 늘어남
        
        while True: # to maintain persistent connection even after a connection failure
            try:
                async with websockets.connect(settings.UPBIT_WEBSOCKET_URL) as websocket:
                    
                    logging.info("WebSocket connected, Sending subscription request")
                    await websocket.send(json.dumps(subscribe_message))

                    retry_delay = 1
                    logging.info("Subscription request sent.")
                    
                    async for message in websocket:
                        await self._handle_message(message)
                        
            except (ConnectionClosed, Exception) as e:
                # 연결이 끊기거나 에러 발생 시 로그 출력
                logging.error(f"Connection lost or error: {e}. Reconnecting in {retry_delay} seconds...")
                
                # 지수 백오프 적용
                await asyncio.sleep(retry_delay)
                
                # 다음 대기 시간은 현재의 2배 (최대값 max_retry_delay 제한)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                
    async def close(self):
        """
        Kafka Producer를 종료
        """
        logging.info("Stopping Kafka producer...")
        # 종료 시에는 남아있는 메시지를 모두 보내기 위해 flush()가 내부적으로 호출됨
        await self._producer.stop()
        logging.info("Producer stopped.")