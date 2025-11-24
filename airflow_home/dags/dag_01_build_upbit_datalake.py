from __future__ import annotations

import json
import pendulum
import requests
import time

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task(task_id="extract_upbit_ticker_data")
def extract_upbit_data():
    """
    Extract: Upbit API에서 KRW 마켓 Ticker 데이터를 Chunk 단위로 가져와 병합
    """
    # 1. 모든 마켓 조회
    krw_markets_url = "https://api.upbit.com/v1/market/all"
    markets_resp = requests.get(krw_markets_url, timeout=10) # Timeout 추가
    markets_resp.raise_for_status()
    
    all_markets = markets_resp.json()
    krw_market_codes = [m['market'] for m in all_markets if m['market'].startswith('KRW-')]
    
    # 2. Chunking (한 번에 50개씩 요청)
    chunk_size = 50
    endpoint = "/v1/ticker"
    url = f"https://api.upbit.com{endpoint}"
    headers = {"accept": "application/json"}
    
    all_data = []
    
    for i in range(0, len(krw_market_codes), chunk_size):
        chunk = krw_market_codes[i:i + chunk_size]
        markets_param = ",".join(chunk)
        
        params = {"markets": markets_param}
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            all_data.extend(response.json())
            
            # API 부하 방지를 위한 약간의 대기 (선택 사항)
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error fetching chunk {chunk}: {e}")
            # 일부 청크 실패 시 전체를 실패처리할지, 로그만 남기고 진행할지 결정 필요
            # 여기서는 에러를 발생시켜 재시도(Retry) 유도
            raise e
            
    # 다음 Task로 전체 데이터 전달
    return {"endpoint": endpoint, "data": all_data}

@task(task_id="load_raw_data_to_postgres")
def load_raw_data_to_postgres(api_response: dict):
    """
    Load: 추출된 원본 데이터를 PostgreSQL의 JSONB 컬럼에 적재
    """
    data_list = api_response.get("data")
    endpoint = api_response.get("endpoint")

    if not data_list:
        print("No data to load.")
        return

    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    
    target_table = "upbit_api_raw_data_landing"
    target_fields = ["payload", "endpoint"]
    
    # JSON 직렬화
    rows_to_insert = [
        (json.dumps(item), endpoint) for item in data_list
    ]
    
    # 대량 데이터 Insert 시 commit_every 옵션 고려 가능
    pg_hook.insert_rows(
        table=target_table,
        rows=rows_to_insert,
        target_fields=target_fields,
    )

with DAG(
    dag_id="dag_01_build_upbit_datalake", # DAG ID 파일명과 일치 권장
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule="*/10 * * * *",  # 10분마다 실행
    catchup=False,
    tags=["upbit", "api", "ELT", "bronze"],
) as dag:
    
    api_data = extract_upbit_data()
    load_raw_data_to_postgres(api_data)