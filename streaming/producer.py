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
from websockets.exceptions import ConnectionClosed
from aiokafka import AIOKafkaProducer
from pydantic import ValidationError

from streaming.config import settings
from shared.schemas import (
    UpbitWebsocketData, 
    UpbitTradeSchema, 
    UpbitTickerSchema, 
    UpbitOrderbookSchema
)

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

    async def _handle_message(self, message: str):
        """
        수신된 메시지를 파싱, 검증하고 적절한 Kafka 토픽으로 전송
        """
        try:
            # Union 타입을 사용하여 어떤 데이터 타입이든 한 번에 검증
            # Union 타입을 통해 어떤 종류(체결, 티커, 호가)의 메시지가 들어와도 Pydantic이 맞는 스키마를 찾아 파싱하고 유효성을 검사
            data = UpbitWebsocketData.model_validate_json(message)
            
            # 데이터 타입에 맞는 토픽을 가져옴
            topic = self._topic_map.get(type(data))
            if not topic:
                print(f"No topic mapping for data type: {type(data)}")
                return
            
            # Pydantic 모델을 JSON 바이트로 직렬화하여 Kafka에 전송
            await self._producer.send_and_wait(
                topic, 
                data.model_dump_json().encode('utf-8')
            )
            print(f"Sent to {topic}: {data.symbol}") # 동작 확인용 로그
            
        except ValidationError as e:
            # Pydantic 유효성 검사 실패 시
            print(f"Validation Error: {e}\nRaw Message: {message[:200]}...")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

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
                    print("WebSocket connected. Sending subscription request...")
                    await websocket.send(json.dumps(subscribe_message))
                    print("Subscription request sent.")
                    
                    async for message in websocket:
                        await self._handle_message(message)
                        
            except ConnectionClosed as e:
                print(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"An error occurred: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
                
    async def close(self):
        """Kafka Producer를 안전하게 종료합니다."""
        print("Stopping Kafka producer...")
        await self._producer.stop()
        print("Producer stopped.")