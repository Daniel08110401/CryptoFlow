from __future__ import annotations
import json
import time
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Upbit 종가는 한국 시간 오전 9시에 갱신됨
UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
UPBIT_CANDLE_URL = "https://api.upbit.com/v1/candles/days"
HEADERS = {"accept": "application/json"}

@task(task_id="collect_daily_candles_200d")
def collect_daily_candles_200d():
    """
    모든 KRW 마켓의 최근 200일 치 일봉을 수집하여 Raw Data 테이블에 적재
    """
    # 1. 마켓 조회
    resp = requests.get(UPBIT_MARKET_URL, headers=HEADERS)
    markets = [m['market'] for m in resp.json() if m['market'].startswith('KRW-')]
    
    buffer = []
    print(f"Fetching candles for {len(markets)} markets...")
    
    for market in markets:
        # MA 120, 200 계산 등을 위해 넉넉히 200일 치 수집
        params = {"market": market, "count": 200}
        try:
            res = requests.get(UPBIT_CANDLE_URL, headers=HEADERS, params=params)
            res.raise_for_status()
            data = res.json()
            
            # payload, endpoint, ingested_at 등 표준 메타데이터 포맷 준수
            for row in data:
                buffer.append((json.dumps(row), '/v1/candles/days'))
            
            time.sleep(0.05) # Rate Limit 조절
        except Exception as e:
            print(f"Failed {market}: {e}")

    # 2. DB 적재 (upbit_api_raw_data_landing 재사용 혹은 별도 테이블 사용)
    # 여기서는 기존 Raw 테이블을 재사용하되 endpoint로 구분한다고 가정
    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    
    if buffer:
        print(f"Inserting {len(buffer)} rows to Lake...")
        pg_hook.insert_rows(
            table="upbit_api_raw_data_landing",
            rows=buffer,
            target_fields=["payload", "endpoint"]
        )

with DAG(
    dag_id="L1_upbit_candle_daily", # 기존 01과 구분
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule="10 9 * * *", # 매일 09:10 실행 (종가 확정 후)
    catchup=False,
    tags=["lake", "daily", "EL"],
) as dag:
    
    collect_daily_candles_200d()