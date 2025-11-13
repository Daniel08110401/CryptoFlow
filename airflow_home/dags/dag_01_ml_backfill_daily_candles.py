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

# --- PostgresHook을 사용하기 위한 설정 ---
# 이 DAG는 Airflow 환경에서 실행되므로, PostgresHook 사용 가능
PG_CONN_ID = "crypto_flow_postgres"

# --- 1. Upbit 과거 데이터 추출 Task ---
@task(task_id="extract_upbit_historical_candles")
def extract_upbit_historical_candles(market="KRW-BTC", days_back=1095):
    """
    [E] Upbit '일별 캔들' API에서 과거 데이터를 backfill
    (count=200 제한을 우회하기 위해 루프 사용)
    """
    print(f"Starting backfill for {market} covering last {days_back} days...")
    url = "https://api.upbit.com/v1/candles/days"
    all_data = []
    end_date = datetime.now()
    num_requests = (days_back // 200) + 1

    for i in range(num_requests):
        try:
            params = {
                "market": market,
                "count": 200,
                "to": end_date.strftime("%Y-%m-%d %H:%M:%S")
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print(f"Request {i+1}/{num_requests}: No more data returned. Stopping.")
                break

            all_data.extend(data)
            
            # 다음 요청을 위해 'to' 날짜 업데이트
            oldest_timestamp_utc = data[-1]['candle_date_time_utc']
            # Upbit API는 1초의 중복을 피하기 위해 1초를 빼줌
            end_date = datetime.fromisoformat(oldest_timestamp_utc.replace('Z', '+00:00')) - timedelta(seconds=1)

            print(f"Request {i+1}/{num_requests}: Fetched {len(data)} candles. Next 'to' date: {end_date}")
            
            # API Rate Limit을 피하기 위한 짧은 대기
            time.sleep(0.3) 

        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
            # 백필 실패 시 Task를 실패 처리
            raise AirflowFailException(f"API request failed: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    print(f"Total candles fetched for {market}: {len(all_data)}")
    return {"market": market, "data": all_data}

# --- 2. F&G Index 과거 데이터 추출 Task ---
@task(task_id="extract_fng_historical_data")
def extract_fng_historical_data():
    """
    [E] alternative.me API에서 모든 과거 Fear & Greed Index 데이터를 backfill
    """
    print("Starting backfill for Fear & Greed Index...")
    # limit=0은 모든 데이터를 의미
    fng_url = "https://api.alternative.me/fng/?limit=0"
    try:
        response = requests.get(fng_url)
        response.raise_for_status()
        data = response.json()
        fng_data = data.get('data', [])
        print(f"Total F&G Index records fetched: {len(fng_data)}")
        return fng_data
    except requests.exceptions.RequestException as e:
        print(f"Error during F&G API request: {e}")
        raise AirflowFailException(f"API request failed: {e}")

# --- 3. Upbit 데이터 적재 Task ---
@task(task_id="load_upbit_candles_to_datalake")
def load_upbit_candles_to_datalake(api_response: dict):
    """
    [L] 추출된 Upbit 일별 캔들 데이터를 Data Lake 테이블에 적재합니다.
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
    
    print(f"Loading {len(rows_to_insert)} rows into {target_table} for {market}...")
    pg_hook.insert_rows(
        table=target_table,
        rows=rows_to_insert,
        target_fields=target_fields,
    )

# --- 4. F&G 데이터 적재 Task ---
@task(task_id="load_fng_data_to_datalake")
def load_fng_data_to_datalake(api_response: list):
    """
    [L] 추출된 F&G Index 데이터를 Data Lake 테이블에 적재합니다.
    """
    if not api_response:
        print("No F&G data to load.")
        return

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    target_table = "fng_index_landing"
    target_fields = ["payload", "value_classification", "timestamp"]

    rows_to_insert = []
    for item in api_response:
        # F&G API는 timestamp가 초 단위 문자열
        ts_epoch = int(item.get('timestamp', 0))
        ts_utc = datetime.fromtimestamp(ts_epoch, timezone.utc).isoformat()
        
        rows_to_insert.append((
            json.dumps(item), # 원본 JSONB
            item.get('value_classification'),
            ts_utc
        ))

    print(f"Loading {len(rows_to_insert)} rows into {target_table}...")
    pg_hook.insert_rows(
        table=target_table,
        rows=rows_to_insert,
        target_fields=target_fields,
    )

# --- DAG 정의 ---
with DAG(
    dag_id="dag_01_ml_backfill_historical_data",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    # 이 DAG는 수동 실행을 위한 것이므로, 스케줄을 'None'으로 설정
    schedule=None,
    catchup=False,
    tags=["upbit", "fng", "backfill", "ELT"],
    doc_md="""
    ### ML 모델 학습용 과거 데이터 백필 DAG
    - 이 DAG는 **수동으로 1회 실행**하여 Transformer 모델 학습에 필요한
      과거 3년치 일별 캔들 데이터와 F&G 지수 데이터를 수집합니다.
    - **Extract**: Upbit 및 F&G API에서 전체 과거 데이터를 추출합니다.
    - **Load**: 추출된 원본 JSON을 각각의 Data Lake 테이블에 적재합니다.
    """,
) as dag:
    
    # 1. BTC 데이터 추출 및 적재
    btc_data = extract_upbit_historical_candles(market="KRW-BTC", days_back=1095)
    load_upbit_candles_to_datalake(btc_data)
    
    # 2. ETH 데이터 추출 및 적재 (논문이 ETH도 다루므로)
    eth_data = extract_upbit_historical_candles(market="KRW-ETH", days_back=1095)
    load_upbit_candles_to_datalake(eth_data)
    
    # 3. F&G 데이터 추출 및 적재
    fng_data = extract_fng_historical_data()
    load_fng_data_to_datalake(fng_data)

    # (참고: 이 DAG는 Task 간 의존성이 없습니다. 모든 Task가 병렬로 실행됩니다.)