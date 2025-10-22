import pandas as pd
from celery import shared_task
from django.db import connection # Django ORM 대신 직접 SQL 실행 시 사용 가능
import logging

logger = logging.getLogger(__name__)

@shared_task
def check_moving_average_cross(symbol="KRW-BTC", short_window=5, long_window=20):
    """
    PostgreSQL에서 특정 심볼의 최근 데이터를 읽어와
    단기 이동평균선과 장기 이동평균선의 교차(cross)를 확인하는 Celery Task.
    """
    logger.info(f"Checking MA cross for {symbol}...")

    # 이동 평균 계산에 필요한 최소 데이터 개수 + 비교용 1개 = long_window + 1
    required_rows = long_window + 1

    try:
        # Airflow Hook 대신 Django의 connection 사용
        with connection.cursor() as cursor:
            # SQL 쿼리 (psycopg2의 %s 플레이스홀더 사용)
            sql = f"""
            SELECT ts_event, acc_trade_price_24h 
            FROM market_stats_24h 
            WHERE symbol = %s 
            ORDER BY ts_event DESC 
            LIMIT %s; 
            """
            cursor.execute(sql, [symbol, required_rows])
            # fetchall()은 튜플의 리스트를 반환
            records = cursor.fetchall()

        if not records or len(records) < required_rows:
            logger.warning(f"Not enough data for {symbol} to calculate MA cross. Found {len(records)} records.")
            return

        # Pandas DataFrame으로 변환 (최신 데이터가 위쪽에 있음)
        df = pd.DataFrame(records, columns=['ts_event', 'price'])
        # 시간순으로 다시 정렬 (오래된 데이터가 위로 가도록)
        df = df.sort_values('ts_event').reset_index(drop=True)

        # 이동 평균 계산
        df[f'MA{short_window}'] = df['price'].rolling(window=short_window).mean()
        df[f'MA{long_window}'] = df['price'].rolling(window=long_window).mean()

        # NaN 값이 없는 마지막 두 행만 선택하여 교차 확인
        df_check = df.dropna().tail(2)

        if len(df_check) < 2:
            logger.info(f"Not enough valid MA data points for {symbol} yet.")
            return

        # 마지막 두 시점의 이동 평균 값 가져오기
        prev_ma_short = df_check.iloc[0][f'MA{short_window}']
        prev_ma_long = df_check.iloc[0][f'MA{long_window}']
        curr_ma_short = df_check.iloc[1][f'MA{short_window}']
        curr_ma_long = df_check.iloc[1][f'MA{long_window}']

        # 교차(Cross) 확인 로직
        golden_cross = (prev_ma_short <= prev_ma_long) and (curr_ma_short > curr_ma_long)
        dead_cross = (prev_ma_short >= prev_ma_long) and (curr_ma_short < curr_ma_long)

        if golden_cross:
            logger.warning(f"📈 GOLDEN CROSS DETECTED for {symbol} at {df_check.iloc[1]['ts_event']}!")
            # 여기에 실제 알림 로직 추가 가능 (예: 이메일 발송, 슬랙 메시지 등)
        elif dead_cross:
            logger.warning(f"📉 DEAD CROSS DETECTED for {symbol} at {df_check.iloc[1]['ts_event']}!")
            # 여기에 실제 알림 로직 추가
        else:
            logger.info(f"No MA cross detected for {symbol}.")

    except Exception as e:
        logger.error(f"Error checking MA cross for {symbol}: {e}", exc_info=True)