# streaming/main.py

import asyncio
import signal
from typing import Any

from streaming.producer import UpbitWebsocketProducer
from streaming.consumer import TradeDataConsumer

async def main():
    """
    Producer와 Consumer를 생성하고 비동기적으로 실행하는 메인 함수
    """
    # 1. Producer와 Consumer 인스턴스 생성
    producer = UpbitWebsocketProducer()
    consumer = TradeDataConsumer()

    # 2. 종료 신호(SIGINT, SIGTERM) 처리를 위한 이벤트 객체 생성
    shutdown_event = asyncio.Event()

    def _signal_handler(*_: Any) -> None:
        """
        종료 신호를 받으면 이벤트 객체를 세팅하여 main loop를 중단시키는 핸들러
        """
        print("Shutdown signal received, cleaning up")
        shutdown_event.set()

    # 운영체제 신호에 핸들러 연결
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # 3. Producer와 Consumer를 백그라운드 태스크로 실행
    producer_task = asyncio.create_task(producer.run())
    consumer_task = asyncio.create_task(consumer.run())
    print("Producer and Consumer tasks started")

    # 4. 종료 신호가 들어올 때까지 대기
    await shutdown_event.wait()
    
    # 5. 종료 신호 수신 후, 모든 태스크를 안전하게 취소하고 정리
    print("Shutting down producer and consumer")
    producer_task.cancel()
    consumer_task.cancel()

    # 모든 리소스가 정리될 때까지 기다림
    await asyncio.gather(producer.close(), consumer.close(), return_exceptions=True)
    print("Application shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application interrupted.")