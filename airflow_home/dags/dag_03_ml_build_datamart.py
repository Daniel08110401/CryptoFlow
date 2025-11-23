from __future__ import annotations
import pendulum
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN_ID = "crypto_flow_postgres"

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(series, fast=12, slow=26, signal=9):
    exp1 = series.ewm(span=fast, adjust=False).mean()
    exp2 = series.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    return macd, signal_line

def calculate_bollinger_bands(series, window=20, num_std=2):
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)
    return upper_band, lower_band

@task(task_id="build_features_and_load")
def build_features_and_load():
    """
    1. Data Lake에서 Upbit 캔들과 F&G 지수를 가져옵니다.
    2. 두 데이터를 날짜 기준으로 병합(Merge)합니다.
    3. 기술적 지표(RSI, MACD, BB, MA)를 계산합니다.
    4. 최종 결과를 ml_feature_mart_btc 테이블에 덮어씁니다 (Truncate & Load).
    """
    # 1. DB 연결
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    print(">>> [1/4] Fetching data from Data Lake...")
    
    # Upbit 데이터 가져오기 (JSONB 파싱)
    # payload 내부의 trade_price(종가) 등을 꺼냅니다.
    sql_candles = """
        SELECT 
            (payload->>'candle_date_time_utc')::DATE as date,
            (payload->>'opening_price')::NUMERIC as open,
            (payload->>'high_price')::NUMERIC as high,
            (payload->>'low_price')::NUMERIC as low,
            (payload->>'trade_price')::NUMERIC as close,
            (payload->>'candle_acc_trade_volume')::NUMERIC as volume
        FROM upbit_daily_candles_landing
        WHERE market = 'KRW-BTC'
        ORDER BY date ASC;
    """
    df_price = pd.read_sql(sql_candles, engine)
    
    # F&G 데이터 가져오기
    sql_fng = """
        SELECT 
            timestamp::DATE as date,
            (payload->>'value')::INT as fng_value,
            (payload->>'value_classification') as fng_class
        FROM fng_index_landing
        ORDER BY date ASC;
    """
    df_fng = pd.read_sql(sql_fng, engine)

    print(f"Fetched {len(df_price)} price records and {len(df_fng)} F&G records.")

    # 2. 데이터 병합 (Merge)
    print(">>> [2/4] Merging datasets...")
    # 날짜를 기준으로 병합 (Inner Join: 둘 다 있는 날짜만 사용)
    df_merged = pd.merge(df_price, df_fng, on='date', how='inner')
    df_merged = df_merged.sort_values('date').set_index('date')
    
    # 3. 피처 엔지니어링 (Feature Engineering)
    print(">>> [3/4] Calculating Technical Indicators...")
    
    close = df_merged['close']
    
    # RSI
    df_merged['rsi_14'] = calculate_rsi(close, 14)
    
    # MACD
    macd, signal = calculate_macd(close)
    df_merged['macd'] = macd
    df_merged['macd_signal'] = signal
    
    # Bollinger Bands
    bb_up, bb_low = calculate_bollinger_bands(close)
    df_merged['bb_upper'] = bb_up
    df_merged['bb_lower'] = bb_low
    
    # Moving Averages
    df_merged['ma_7'] = close.rolling(window=7).mean()
    df_merged['ma_30'] = close.rolling(window=30).mean()

    # 결측치 제거 (지표 계산 초기에 발생하는 NaN 제거)
    df_final = df_merged.dropna().reset_index()
    
    print(f"Final dataset shape: {df_final.shape}")
    print(df_final.tail())

    # 4. 데이터 적재 (Truncate & Load)
    print(">>> [4/4] Loading to Data Mart (ml_feature_mart_btc)...")
    
    # 기존 데이터 삭제 (Truncate) -> 새로운 학습용 셋으로 갱신
    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE ml_feature_mart_btc;")
    
    # 데이터 삽입 using sqlalchemy (bulk insert)
    df_final['updated_at'] = pd.Timestamp.now(tz='UTC')
    df_final.to_sql('ml_feature_mart_btc', engine, if_exists='append', index=False)
    
    print("Done.")

with DAG(
    dag_id="dag_03_ml_build_datamart",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule="30 0 * * *", # 매일 00:30 (수집 DAG보다 30분 뒤 실행)
    catchup=False,
    tags=["upbit", "ml", "datamart", "feature_engineering"],
) as dag:
    
    build_features_and_load()