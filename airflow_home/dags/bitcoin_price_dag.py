from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

# 비트코인 가격 데이터를 가져오는 함수

# 배치 단위
def fetch_bitcoin_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice/BTC.json" ## api 수정 예정
    response = requests.get(url)
    data = response.json()
    price = data['bpi']['USD']['rate']
    print(f"Current Bitcoin price: ${price}")
    # 여기서 DB에 저장하거나 다른 작업을 할 수 있어.


# DAG 설정
with DAG(
    'bitcoin_price_dag',
    description='A simple DAG to fetch Bitcoin price',
    schedule_interval='@hourly',  # 매 시간마다 실행
    start_date=datetime(2025, 9, 25),
    catchup=False,
) as dag:
    fetch_price_task = PythonOperator(
        task_id='fetch_bitcoin_price',
        python_callable=fetch_bitcoin_price,
    )
