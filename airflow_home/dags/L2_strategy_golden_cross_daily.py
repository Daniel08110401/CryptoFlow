from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.datasets import Dataset

UPBIT_CANDLE_DATASET = Dataset("upbit://daily_candles")

@task(task_id="transform_golden_cross")
def transform_golden_cross():
    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    
    # [수정됨] CREATE TABLE 구문을 트랜잭션 맨 앞에 추가했습니다.
    sql = """
    BEGIN;
    
    -- 1. 테이블이 없으면 생성 (Schema Definition)
    CREATE TABLE IF NOT EXISTS market_trend_scouter (
        symbol VARCHAR(32) PRIMARY KEY,
        current_price DECIMAL,
        ma_5 DECIMAL,
        ma_20 DECIMAL,
        ma_60 DECIMAL,
        is_golden_cross BOOLEAN,
        trend_score INT,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- 2. 기존 데이터 비우기 (Snapshot 갱신)
    TRUNCATE TABLE market_trend_scouter;

    -- 3. 데이터 분석 및 적재
    INSERT INTO market_trend_scouter 
    (symbol, current_price, ma_5, ma_20, ma_60, is_golden_cross, trend_score)
    
    WITH latest_raw_data AS (
        -- 오늘 수집된 Candle 데이터만 조회 (endpoint 확인!)
        SELECT DISTINCT ON (payload->>'market', payload->>'candle_date_time_utc')
            payload->>'market' as symbol,
            (payload->>'trade_price')::NUMERIC as close_price,
            (payload->>'candle_date_time_utc')::DATE as candle_date
        FROM upbit_api_raw_data_landing
        WHERE endpoint = '/v1/candles/days'
          AND ingested_at >= NOW() - INTERVAL '24 hours'
    ),
    moving_averages AS (
        SELECT 
            symbol, candle_date, close_price,
            AVG(close_price) OVER (PARTITION BY symbol ORDER BY candle_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma_5,
            AVG(close_price) OVER (PARTITION BY symbol ORDER BY candle_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma_20,
            AVG(close_price) OVER (PARTITION BY symbol ORDER BY candle_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma_60
        FROM latest_raw_data
    ),
    cross_signal AS (
        SELECT *,
            LAG(ma_5) OVER (PARTITION BY symbol ORDER BY candle_date) as prev_ma_5,
            LAG(ma_20) OVER (PARTITION BY symbol ORDER BY candle_date) as prev_ma_20
        FROM moving_averages
    )
    SELECT 
        symbol, close_price, 
        ROUND(ma_5, 2), ROUND(ma_20, 2), ROUND(ma_60, 2),
        CASE WHEN prev_ma_5 <= prev_ma_20 AND ma_5 > ma_20 THEN TRUE ELSE FALSE END as is_golden_cross,
        CASE WHEN ma_5 > ma_20 AND ma_20 > ma_60 THEN 100 WHEN ma_5 > ma_20 THEN 50 ELSE 0 END as trend_score
    FROM cross_signal
    WHERE candle_date = (SELECT MAX(candle_date) FROM latest_raw_data)
    ORDER BY trend_score DESC;

    COMMIT;
    """
    pg_hook.run(sql)

with DAG(
    dag_id="L2_strategy_golden_cross_daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule=[UPBIT_CANDLE_DATASET],
    catchup=False,
    tags=["mart", "strategy", "transform", "Layer2", "consumer"],
) as dag:

    transform_golden_cross()