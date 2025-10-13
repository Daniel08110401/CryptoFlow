from __future__ import annotations

import json
import pendulum
import requests

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task(task_id="extract_upbit_ticker_data")
def extract_upbit_data():
    """
    Extract: Upbit API에서 KRW 마켓 Ticker 데이터 전체를 가져와 JSON 리스트로 반환
    """
    endpoint = "/v1/ticker"
    url = f"https://api.upbit.com{endpoint}"
    
    krw_markets_url = "https://api.upbit.com/v1/market/all"
    markets_resp = requests.get(krw_markets_url)
    markets_resp.raise_for_status()
    all_markets = markets_resp.json()
    
    krw_market_codes = [m['market'] for m in all_markets if m['market'].startswith('KRW-')]
    markets_param = ",".join(krw_market_codes)
    
    headers = {"accept": "application/json"}
    params = {"markets": markets_param}
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    # 다음 Task로 원본 데이터와 엔드포인트 정보를 함께 전달
    return {"endpoint": endpoint, "data": response.json()}

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
    # pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    
    target_table = "upbit_api_raw_data_landing"
    target_fields = ["payload", "endpoint"]
    
    # 각 JSON 객체를 json.dumps를 사용하여 문자열로 변환한 후 튜플로 만듭니다.
    rows_to_insert = [
        (json.dumps(item), endpoint) for item in data_list
    ]
    
    pg_hook.insert_rows(
        table=target_table,
        rows=rows_to_insert,
        target_fields=target_fields,
    )

with DAG(
    dag_id="dag_02_upbit_api_to_datalake",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule="*/10 * * * *",  # 10분마다 실행
    catchup=False,
    tags=["upbit", "api", "ELT"],
    doc_md="""
    ### Upbit 24시간 Ticker 데이터 수집 및 데이터 레이크 적재 DAG
    - **Extract**: Upbit REST API에서 KRW 마켓의 24시간 Ticker 데이터를 추출합니다.
    - **Load**: 추출된 원본 데이터를 `upbit_api_raw_data_landing` 테이블의 JSONB 컬럼에 적재합니다.
    """,
) as dag:
    # Task 의존성 설정
    api_data = extract_upbit_data()
    load_raw_data_to_postgres(api_data)