from __future__ import annotations
import pendulum
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

# [설정] 메이저 7대장 리스트 (L2와 동일)
TARGET_SYMBOLS = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL', 'KRW-DOGE', 'KRW-ADA', 'KRW-AVAX']

# [Dataset] L1이 끝나면 실행 (L2와 병렬 실행 가능)
UPBIT_CANDLE_DATASET = Dataset("upbit://daily_candles")

@task(task_id="calc_baseline_stats")
def calc_baseline_stats():
    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    engine = pg_hook.get_sqlalchemy_engine()
    
    # 1. 데이터 로드 (최근 60일치 - 30일 이동평균 계산을 위해 넉넉히)
    symbols_str = "', '".join(TARGET_SYMBOLS)
    query = f"""
        SELECT 
            payload->>'market' as symbol,
            (payload->>'trade_price')::FLOAT as close,
            (payload->>'high_price')::FLOAT as high,
            (payload->>'low_price')::FLOAT as low,
            (payload->>'candle_acc_trade_volume')::FLOAT as volume,
            (payload->>'candle_date_time_utc')::DATE as date
        FROM upbit_api_raw_data_landing
        WHERE endpoint = '/v1/candles/days'
          AND payload->>'market' IN ('{symbols_str}')
          AND ingested_at >= NOW() - INTERVAL '60 days'
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No data found.")
        return

    results = []
    for symbol in TARGET_SYMBOLS:
        sub_df = df[df['symbol'] == symbol].sort_values('date').copy()
        
        # 데이터가 너무 적으면 통계 신뢰도가 없으므로 스킵
        if len(sub_df) < 30: 
            print(f"Skipping {symbol}: Not enough data for baseline.")
            continue
        
        # --- [핵심 로직] Baseline 통계 계산 ---
        
        # 1. 30일 평균 거래량 (Volume Baseline)
        # -> 나중에 실시간 거래량이 이 값의 5배가 넘으면 알람 발송
        avg_volume_30d = sub_df['volume'].rolling(window=30).mean().iloc[-1]
        
        # 2. ATR (Average True Range) - 변동성 기준
        # -> 가격이 급등락할 때 "평소 변동폭의 몇 배냐?"를 판단하는 기준
        sub_df['prev_close'] = sub_df['close'].shift(1)
        sub_df['tr1'] = sub_df['high'] - sub_df['low'] # 당일 고가-저가
        sub_df['tr2'] = (sub_df['high'] - sub_df['prev_close']).abs() # 당일 고가-전일 종가
        sub_df['tr3'] = (sub_df['low'] - sub_df['prev_close']).abs() # 당일 저가-전일 종가
        
        # 세 값 중 가장 큰 값이 True Range
        sub_df['tr'] = sub_df[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # 14일 평균 ATR 계산
        atr_14d = sub_df['tr'].rolling(window=14).mean().iloc[-1]
        
        results.append({
            'symbol': symbol,
            'avg_volume_30d': round(avg_volume_30d, 2),
            'atr_14d': round(atr_14d, 2),
            'updated_at': pd.Timestamp.now()
        })

    # 3. DB 적재 (market_anomaly_baseline 테이블)
    if results:
        res_df = pd.DataFrame(results)
        
        with engine.connect() as conn:
            with conn.begin():
                # 테이블 생성
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS market_anomaly_baseline (
                        symbol VARCHAR(20) PRIMARY KEY,
                        avg_volume_30d DECIMAL, -- 30일 평균 거래량
                        atr_14d DECIMAL,        -- 14일 평균 변동폭
                        updated_at TIMESTAMP
                    )
                """)
                # 덮어쓰기 (항상 최신 기준값만 유지)
                conn.execute("TRUNCATE TABLE market_anomaly_baseline")
        
        res_df.to_sql('market_anomaly_baseline', engine, if_exists='append', index=False)
        print(f"Baseline stats updated for {len(results)} symbols.")

with DAG(
    dag_id="L3_anomaly_baseline_daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule=[UPBIT_CANDLE_DATASET], # L1 끝나면 L2와 같이 실행됨
    catchup=False,
    tags=["mart", "ml", "pandas", "L3"],
) as dag:

    calc_baseline_stats()