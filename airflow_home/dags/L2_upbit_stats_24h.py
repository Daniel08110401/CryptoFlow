from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task(task_id="transform_and_load_datamart")
def transform_and_load_datamart():
    """
    Transform: 데이터 레이크의 원본 데이터를 가공하여 데이터 마트에 적재
    Truncate-Load 패턴을 사용하여 일 단위 마켓별 최신 데이터만 적재
    """
    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    
    # SQL을 사용하여 DB 내부에서 모든 ETL 작업을 수행
    transform_sql = """
    -- 1. 기존 데이터 마트 테이블을 비움
    TRUNCATE TABLE market_stats_24h;

    -- 2. 데이터 레이크에서 데이터를 읽어와 가공 후 데이터 마트에 적재(LOAD)
    INSERT INTO market_stats_24h (
        symbol, 
        acc_trade_price_24h, 
        acc_trade_volume_24h, 
        source, 
        ts_event
    )
    SELECT DISTINCT ON (payload ->> 'market')
        payload ->> 'market' AS symbol,
        (payload ->> 'acc_trade_price_24h')::NUMERIC AS acc_trade_price_24hr,
        (payload ->> 'acc_trade_volume_24h')::NUMERIC AS acc_trade_volume_24hr,
        'upbit' AS source,
        -- Upbit timestamp는 millisecond 단위이므로 1000으로 나눠서 초 단위로 변환
        TO_TIMESTAMP((payload ->> 'timestamp')::BIGINT / 1000) AT TIME ZONE 'UTC' AS ts_event,
        NOW() as created_at
    FROM 
        upbit_api_raw_data_landing
    WHERE 
        endpoint = '/v1/ticker'; -- Ticker 데이터만 선택
    ORDER BY 
        (PAYLOAD ->> 'market'), 
        created_at DESC;
    
    COMMIT;
    """
    # PostgresHook을 사용하여 SQL 실행
    pg_hook.run(transform_sql)

with DAG(
    dag_id="L2_upbit_stats_24h",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    # 이전 DAG가 10분마다 실행되므로, 5분 뒤에 실행하여 데이터 정합성을 맞춤
    schedule="5-59/10 * * * *", 
    catchup=False,
    tags=["upbit", "datamart", "ELT"],
    doc_md="""
    ### 데이터 마트 구축 DAG
    - Transform : `upbit_api_raw_data_landing` 테이블의 원본 데이터를 가공 
      분석에 용이한 `market_stats_24h` 테이블에 최종 적재
    """,
) as dag:
    transform_and_load_datamart()