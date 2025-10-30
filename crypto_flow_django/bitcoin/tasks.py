import pandas as pd
from celery import shared_task
from django.db import connection
from django.core.mail import send_mail
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

def send_notification_email(subject, message_body):
    """
    알림 메일 보내는 function
    """
    try:
        send_mail(
            subject,
            message_body,
            settings.DEFAULT_FROM_EMAIL,
            ['hairu2908@gmail.com'],
            fail_silently=False,
        )
        logger.info(f"Successfully sent email notification: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)

@shared_task
def check_moving_average_cross(symbol="KRW-BTC", short_window=5, long_window=20):
    """
    PostgreSQL에서 특정 심볼의 최근 데이터를 읽어와
    단기 이동평균선과 장기 이동평균선의 교차(cross)를 확인하는 Celery Task
    """
    logger.info(f"Checking MA cross for {symbol}...")

    # 이동 평균 계산에 필요한 최소 데이터 개수 + 비교용 1개 = long_window + 1
    required_rows = long_window + 1

    try:
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

        df = pd.DataFrame(records, columns=['ts_event', 'price'])
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
            #logger.warning(f"📈 GOLDEN CROSS DETECTED for {symbol} at {df_check.iloc[1]['ts_event']}!")
            event_time = df_check.iloc[1]['ts_event']
            subject = f"📈 골든 크로스 발생! ({symbol})"
            message = f"{symbol}의 단기 이동평균선({short_window}p)이 장기 이동평균선({long_window}p)을 상향 돌파했습니다.\n\n이벤트 발생 시각: {event_time}\n현재 단기MA: {curr_ma_short:,.0f}\n현재 장기MA: {curr_ma_long:,.0f}"
            logger.warning(subject)
            send_notification_email(subject, message)
        elif dead_cross:
            #logger.warning(f"📉 DEAD CROSS DETECTED for {symbol} at {df_check.iloc[1]['ts_event']}!")
            event_time = df_check.iloc[1]['ts_event']
            subject = f"📉 데드 크로스 발생! ({symbol})"
            message = f"{symbol}의 단기 이동평균선({short_window}p)이 장기 이동평균선({long_window}p)을 하향 돌파했습니다.\n\n이벤트 발생 시각: {event_time}\n현재 단기MA: {curr_ma_short:,.0f}\n현재 장기MA: {curr_ma_long:,.0f}"
            logger.warning(subject)
            send_notification_email(subject, message)
        else:
            logger.info(f"No MA cross detected for {symbol}.")

    except Exception as e:
        logger.error(f"Error checking MA cross for {symbol}: {e}", exc_info=True)