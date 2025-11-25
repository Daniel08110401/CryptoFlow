from __future__ import annotations
import json
import pendulum
import requests
import time
from datetime import datetime, timedelta, timezone

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

PG_CONN_ID = "crypto_flow_postgres"

# --- 1. Upbit 일일 캔들 추출 Task ---
@task(task_id="extract_upbit_daily_candle")
def extract_upbit_daily_candle(market="KRW-BTC"):
    """
    [E] Upbit '일별 캔들' API에서 '어제' 하루치 데이터 1개를 가져옴
    """
    print(f"Fetching daily candle for {market} for yesterday...")
    url = "https://api.upbit.com/v1/candles/days"
    
    try:
        # 'count=1'로 가장 최근(어제) 캔들 1개만 요청
        params = {"market": market, "count": 1}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            raise AirflowFailException(f"No data returned from Upbit for {market}")

        print(f"Fetched 1 candle for {market}")
        return {"market": market, "data": data} # data는 [item] 형태의 리스트

    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        raise AirflowFailException(f"API request failed: {e}")

# --- 2. F&G Index 일일 데이터 추출 Task ---
@task(task_id="extract_fng_daily_data")
def extract_fng_daily_data():
    """
    [E] alternative.me API에서 '어제' 하루치 F&G 지수를 가져옵니다.
    """
    print("Fetching daily F&G Index...")
    # limit=1은 가장 최근(어제) 데이터 1개를 의미
    fng_url = "https://api.alternative.me/fng/?limit=1" 
    try:
        response = requests.get(fng_url)
        response.raise_for_status()
        data = response.json()
        fng_data = data.get('data', [])
        
        if not fng_data:
             raise AirflowFailException("No data returned from F&G API")

        print(f"Fetched 1 F&G record")
        return fng_data # data는 [item] 형태의 리스트

    except requests.exceptions.RequestException as e:
        print(f"Error during F&G API request: {e}")
        raise AirflowFailException(f"API request failed: {e}")

# --- 3. Upbit 데이터 적재 Task ---
@task(task_id="load_upbit_candle_to_datalake")
def load_upbit_candle_to_datalake(api_response: dict):
    """
    [L] 추출된 Upbit 일별 캔들 데이터를 Data Lake 테이블에 추가
    """
    market = api_response.get("market")
    data_list = api_response.get("data")
    
    if not data_list:
        print("No Upbit candle data to load.")
        return

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    target_table = "upbit_daily_candles_landing"
    target_fields = ["market", "payload", "candle_date_time_utc"]
    
    rows_to_insert = [
        (
            item.get('market'),
            json.dumps(item), # 원본 JSONB
            item.get('candle_date_time_utc')
        )
        for item in data_list
    ]
    
    print(f"Loading {len(rows_to_insert)} row into {target_table} for {market}...")
    # insert_rows는 기본적으로 APPEND (추가) 동작을 합니다.
    pg_hook.insert_rows(
        table=target_table,
        rows=rows_to_insert,
        target_fields=target_fields,
    )

# --- 4. F&G 데이터 적재 Task ---
@task(task_id="load_fng_data_to_datalake")
def load_fng_data_to_datalake(api_response: list):
    """
    [L] 추출된 F&G Index 데이터를 Data Lake 테이블에 '추가'합니다.
    """
    if not api_response:
        print("No F&G data to load.")
        return

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    target_table = "fng_index_landing"
    target_fields = ["payload", "value_classification", "timestamp"]

    rows_to_insert = []
    for item in api_response:
        ts_epoch = int(item.get('timestamp', 0))
        ts_utc = datetime.fromtimestamp(ts_epoch, timezone.utc).isoformat()
        
        rows_to_insert.append((
            json.dumps(item), # 원본 JSONB
            item.get('value_classification'),
            ts_utc
        ))

    print(f"Loading {len(rows_to_insert)} row into {target_table}...")
    pg_hook.insert_rows(
        table=target_table,
        rows=rows_to_insert,
        target_fields=target_fields,
    )

# --- DAG 정의 ---
with DAG(
    dag_id="L1_ml_collect_daily_updates",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    # 매일 자정(00:00)에 실행되도록 설정
    schedule="@daily", 
    catchup=False,
    tags=["upbit", "fng", "daily", "ELT"],
    doc_md="""
    ### ML 모델 학습용 일일 신규 데이터 수집 DAG
    - 이 DAG는 **매일 자정**에 실행되어 '어제' 하루치 Upbit 캔들 데이터와
      F&G 지수 데이터를 수집하여 데이터 레이크 테이블에 **Append**합니다.
    - **Extract**: Upbit(count=1) 및 F&G(limit=1) API에서 최신 데이터 추출.
    - **Load**: 추출된 원본 JSON을 각각의 Data Lake 테이블에 적재.
    """,
) as dag:
    
    # 1. BTC 데이터 추출 및 적재
    btc_data = extract_upbit_daily_candle(market="KRW-BTC")
    load_upbit_candle_to_datalake(btc_data)
    
    # 2. ETH 데이터 추출 및 적재
    eth_data = extract_upbit_daily_candle(market="KRW-ETH")
    load_upbit_candle_to_datalake(eth_data)
    
    # 3. F&G 데이터 추출 및 적재
    fng_data = extract_fng_daily_data()
    load_fng_data_to_datalake(fng_data)

    # (참고: 이 DAG 역시 Task 간 의존성 없이 병렬로 실행됩니다.)