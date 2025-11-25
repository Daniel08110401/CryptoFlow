from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor # [중요]

@task(task_id="transform_golden_cross")
def transform_golden_cross():
    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    
    # API 호출 없이, DB 내부에서만 도는 쿼리
    sql = """
    BEGIN;
    
    TRUNCATE TABLE market_trend_scouter;

    INSERT INTO market_trend_scouter 
    (symbol, current_price, ma_5, ma_20, ma_60, is_golden_cross, trend_score)
    
    WITH latest_raw_data AS (
        -- [최적화] 오늘 수집된 데이터(방금 dag_01_b가 넣은 것)만 가져오기
        SELECT 
            payload->>'market' as symbol,
            (payload->>'trade_price')::NUMERIC as close_price,
            (payload->>'candle_date_time_utc')::DATE as candle_date
        FROM upbit_api_raw_data_landing
        WHERE endpoint = '/v1/candles/days'
          AND created_at >= NOW() - INTERVAL '1 hour' -- 최근 1시간 내 수집된 따끈한 데이터
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
        symbol, close_price, ma_5, ma_20, ma_60,
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
    schedule="30 9 * * *", # 09:30 실행 (수집 DAG가 09:10이니까 그 이후)
    catchup=False,
    tags=["mart", "strategy", "transform"],
) as dag:

    # (옵션) 수집 DAG가 끝났는지 확인하고 시작하는 센서 추가 가능
    # wait_for_ingest = ExternalTaskSensor(
    #     task_id="wait_for_ingest",
    #     external_dag_id="dag_01_b_collect_daily_candles",
    #     ...
    # )
    
    transform_golden_cross()