from __future__ import annotations
import pendulum
import pandas as pd
import numpy as np
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

# [설정] 메이저 7대장 리스트 (여기에만 집중합니다)
TARGET_SYMBOLS = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL', 'KRW-DOGE', 'KRW-ADA', 'KRW-AVAX']

# [Dataset] L1(수집)이 끝나면 이 DAG가 실행되도록 신호를 받습니다.
UPBIT_CANDLE_DATASET = Dataset("upbit://daily_candles")

def calculate_rsi(series, period=14):
    """
    RSI(상대강도지수) 계산 함수
    """
    delta = series.diff()
    
    # 상승분과 하락분 분리
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    
    # 14일 이동평균 (Wilder's Smoothing 대신 단순 이동평균 사용 - 속도 최적화)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # RS 계산 (Division by zero 방지)
    rs = avg_gain / avg_loss.replace(0, np.nan)
    
    # RSI 도출
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50) # 데이터 부족 시 중립(50) 처리

@task(task_id="analyze_trend_scanner")
def analyze_trend_scanner():
    pg_hook = PostgresHook(postgres_conn_id="crypto_flow_postgres")
    # Pandas to_sql 사용을 위해 SQLAlchemy Engine 가져오기
    engine = pg_hook.get_sqlalchemy_engine()
    
    # 1. DB에서 최근 200일 치 데이터 가져오기 (메이저 7종목만 필터링)
    symbols_str = "', '".join(TARGET_SYMBOLS)
    
    # JSONB payload 내부 필드 추출
    query = f"""
        SELECT 
            payload->>'market' as symbol,
            (payload->>'trade_price')::FLOAT as close,
            (payload->>'candle_date_time_utc')::DATE as date
        FROM upbit_api_raw_data_landing
        WHERE endpoint = '/v1/candles/days'
          AND payload->>'market' IN ('{symbols_str}')
          AND ingested_at >= NOW() - INTERVAL '200 days'
    """
    
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No data found. Skipping analysis.")
        return

    # 2. 종목별 지표 계산 (Pandas)
    results = []
    
    for symbol in TARGET_SYMBOLS:
        # 날짜순 정렬
        sub_df = df[df['symbol'] == symbol].sort_values('date').copy()
        
        # 데이터가 최소 60일(MA60 계산용) 이상 있어야 함
        if len(sub_df) < 60: 
            print(f"Skipping {symbol}: Not enough data ({len(sub_df)} rows)")
            continue

        # 이동평균선(MA) 계산
        sub_df['ma5'] = sub_df['close'].rolling(5).mean()
        sub_df['ma20'] = sub_df['close'].rolling(20).mean()
        sub_df['ma60'] = sub_df['close'].rolling(60).mean()
        
        # RSI (14일) 계산
        sub_df['rsi'] = calculate_rsi(sub_df['close'], 14)

        # 분석 기준일: 가장 최근 데이터 (오늘 아침 마감된 봉)
        latest = sub_df.iloc[-1]
        prev = sub_df.iloc[-2] # 어제 데이터 (골든크로스 비교용)
        
        # 골든 크로스 여부: 어제는 5일선이 20일선 아래, 오늘은 위
        is_golden_cross = (prev['ma5'] <= prev['ma20']) and (latest['ma5'] > latest['ma20'])
        
        # [Trend Score 로직] - 총 100점 만점
        trend_score = 0
        
        # 1. 단기 추세 (30점): 5일선이 20일선 위에 있는가?
        if latest['ma5'] > latest['ma20']: 
            trend_score += 30
            
        # 2. 중기 추세 (30점): 20일선이 60일선 위에 있는가? (정배열의 완성)
        if latest['ma20'] > latest['ma60']: 
            trend_score += 30
            
        # 3. 모멘텀 (20점): RSI가 50 이상인가? (매수세 우위)
        if latest['rsi'] > 50: 
            trend_score += 20
            
        # 4. 시그널 (20점): 방금 골든 크로스가 떴는가? (진입 타이밍)
        if is_golden_cross: 
            trend_score += 20

        # 결과 리스트에 추가
        results.append({
            'symbol': symbol,
            'close_price': latest['close'],
            'ma_5': round(latest['ma5'], 2),
            'ma_20': round(latest['ma20'], 2),
            'ma_60': round(latest['ma60'], 2),
            'rsi_14': round(latest['rsi'], 2),
            'is_golden_cross': is_golden_cross,
            'trend_score': trend_score,
            'created_at': pd.Timestamp.now()
        })
    
    # 3. 결과 저장 (daily_market_trend 테이블)
    if results:
        result_df = pd.DataFrame(results)
        
        with engine.connect() as conn:
            # 트랜잭션 시작
            with conn.begin():
                # (1) 테이블이 없으면 생성
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS daily_market_trend (
                        symbol VARCHAR(20) PRIMARY KEY,
                        close_price DECIMAL,
                        ma_5 DECIMAL,
                        ma_20 DECIMAL,
                        ma_60 DECIMAL,
                        rsi_14 DECIMAL,
                        is_golden_cross BOOLEAN,
                        trend_score INT,
                        created_at TIMESTAMP
                    )
                """)
                # (2) 기존 데이터 비우기 (Daily Snapshot이므로 덮어쓰기)
                conn.execute("TRUNCATE TABLE daily_market_trend")
                
        # (3) 새 데이터 적재
        result_df.to_sql('daily_market_trend', engine, if_exists='append', index=False)
        print(f"Successfully updated trend report for {len(results)} symbols.")
    else:
        print("No valid data to insert.")

with DAG(
    dag_id="L2_trend_scanner_daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    schedule=[UPBIT_CANDLE_DATASET], # L1이 끝나면 실행됨 (Event Driven)
    catchup=False,
    tags=["mart", "strategy", "pandas", "L2"],
) as dag:

    analyze_trend_scanner()