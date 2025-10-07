# streaming/config.py

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    스트리밍 애플리케이션의 설정을 관리하는 클래스
    .env 파일 또는 환경 변수로부터 설정값을 로드
    """
    
    # Upbit WebSocket 설정
    UPBIT_WEBSOCKET_URL: str = "wss://api.upbit.com/websocket/v1"

    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TRADE_TOPIC: str = "upbit-trades"
    KAFKA_TICKER_TOPIC: str = "upbit-tickers"
    KAFKA_ORDERBOOK_TOPIC: str = "upbit-orderbooks"

    # Redis 설정
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    # Pydantic-settings의 설정 클래스
    # .env 파일을 읽어서 환경 변수를 로드하도록 설정
    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding='utf-8',
        extra='ignore' # .env 파일에 추가적인 변수가 있어도 무시
    )

# 설정 객체를 인스턴스화하여 다른 파일에서 import하여 사용 가능
settings = Settings()