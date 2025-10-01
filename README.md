# CryptoFlow: Real-Time Cryptocurrency Data Pipeline & Analytics

## 1. 프로젝트 개요

본 프로젝트는 가상화폐(비트코인)를 대상으로 **실시간 + 배치 데이터 파이프라인**을 구축하여 가격 데이터를 자동으로 수집, 처리, 저장, 제공하는 시스템입니다.

Airflow를 활용한 배치 수집, Kafka/Redis 기반의 실시간 스트리밍, PostgreSQL 저장, Django REST Framework API 및 대시보드 시각화까지 포함된 **엔드투엔드 데이터 엔지니어링 파이프라인**입니다.

이 프로젝트는 데이터 엔지니어링의 핵심 요소인 **데이터 수집, 처리, 저장, 제공** 전 과정을 자동화하고, 금융 데이터의 실시간 활용 시나리오를 학습하는 것을 목표로 합니다.

---

## 2. 주요 기능 및 수집 데이터

* **자동화된 데이터 수집**: Airflow DAG를 통해 1시간 단위로 배치 API 호출 (24h 캔들/시세 데이터)
* **실시간 데이터 스트리밍**: Kafka Producer가 업비트 WebSocket 체결 데이터를 Redis에 적재
* **정규화된 데이터 저장**: Raw JSON → Bronze 레이어 적재 → Silver 변환 → Gold 분석/예측 데이터 생성
* **REST API 제공**: Django REST Framework로 최신 가격, 트렌드, 예측 결과를 API로 제공
* **대시보드 시각화**: Plotly/Dash 기반 시각화 대시보드 제공

#### 수집 항목 (예시: Upbit `KRW-BTC`)

* `market`: 마켓 코드 (예: KRW-BTC)
* `trade_timestamp`: 거래 발생 시각
* `opening_price`: 시가
* `high_price`: 고가
* `low_price`: 저가
* `trade_price`: 종가(체결가)
* `acc_trade_volume_24h`: 24시간 누적 거래량
* `acc_trade_price_24h`: 24시간 누적 거래대금
* `change_rate`: 전일 대비 변동률
* `timestamp`: 수집 시각

---

## 3. 기술 스택 및 아키텍처

* **언어**: Python 3.10
* **의존성 관리**: Poetry
* **컨테이너화**: Docker, Docker Compose
* **워크플로우 관리 (Orchestration)**: Apache Airflow 2.7.3
* **데이터베이스**: PostgreSQL 16 (Docker 기반)
* **웹 프레임워크**: Django 4.2, Django REST Framework
* **실시간 처리**: Kafka 3.5, Redis 7
* **시각화/대시보드**: Plotly/Dash
* **머신러닝**: Scikit-learn, XGBoost

### 아키텍처 개요

* **Docker Compose**가 PostgreSQL, Airflow, Kafka, Redis, Django 서비스를 실행
* **Airflow DAG**이 Upbit API 호출 → PostgreSQL Bronze 레이어 저장
* **Kafka Producer**가 실시간 체결 데이터 스트리밍 → Redis 적재
* **Django API**가 PostgreSQL/Redis 데이터를 읽어 REST API 응답 제공
* **Plotly/Dash**가 실시간 대시보드를 제공

---

## 4. 프로젝트 구조

```
CryptoFlow/
├── crypto_flow_django/
│   ├── manage.py                     # Django 관리 명령어 파일
│   ├── bitcoin/                      # 비트코인 데이터 앱
│   └── ...
├── airflow_home/
│   ├── dags/
│   │   └── bitcoin_price_dag.py      # Airflow DAG 정의
│   ├── logs/
│   └── plugins/
├── scripts/
│   └── producer.py                   # Kafka Producer (실시간 데이터 수집)
├── docker-compose.yml                 # 전체 서비스 정의 (Airflow, Postgres, Kafka, Redis, Django)
├── dockerfile.airflow                 # Airflow 커스텀 Dockerfile
├── pyproject.toml                     # Poetry 의존성 정의
├── poetry.lock                        # 의존성 버전 고정
└── README.md                          # 프로젝트 설명서
```

---

## 5. 데이터베이스 스키마

데이터는 **Bronze → Silver → Gold** 3단계로 관리됩니다.

#### 1. `crypto_24hr_rawingest_landing` (Bronze: API 호출 원본 저장)

| 컬럼명           | 타입           | 설명                |
| :------------ | :----------- | :---------------- |
| `id`          | BIGSERIAL PK | 호출 고유 ID          |
| `source`      | TEXT         | 데이터 소스 (예: upbit) |
| `endpoint`    | TEXT         | API 엔드포인트         |
| `payload`     | JSONB        | 원본 응답 JSON 전체     |
| `call_ts`     | TIMESTAMPTZ  | API 호출 시각         |
| `received_at` | TIMESTAMPTZ  | 수집 완료 시각          |

#### 2. `crypto_24hr_rawingest_events` (Bronze: 이벤트 단위 확장 저장)

| 컬럼명           | 타입           | 설명                 |
| :------------ | :----------- | :----------------- |
| `id`          | BIGSERIAL PK | 이벤트 고유 ID          |
| `source`      | TEXT         | 데이터 소스             |
| `symbol`      | TEXT         | 마켓 코드 (예: KRW-BTC) |
| `ts_event`    | TIMESTAMPTZ  | 거래소 이벤트 시각         |
| `payload`     | JSONB        | 원본 이벤트 JSON        |
| `call_id`     | BIGINT FK    | landing.id 참조      |
| `received_at` | TIMESTAMPTZ  | 이벤트 수신 시각          |

#### 3. Silver/Gold 레이어

* Silver: 캔들, 체결, 통계 데이터로 정규화
* Gold: 트렌드, 변동성, ML 기반 예측 결과 저장

---

## 6. 설치 및 실행 방법

### 사전 준비

* Docker 및 Docker Compose 설치
* Poetry 설치

### 1단계: 프로젝트 클론 및 의존성 설치

```bash
git clone https://github.com/Daniel08110401/CryptoFlow.git
cd CryptoFlow
poetry install
```

### 2단계: Docker 서비스 실행

```bash
docker compose up -d --build
```

### 3단계: Django 초기화

```bash
poetry run python manage.py migrate
poetry run python manage.py runserver
```

### 4단계: Airflow DAG 실행

* `http://localhost:8080` 접속
* 계정: `admin / admin`
* `bitcoin_price_dag` 활성화 및 실행

---

## 7. 데이터 확인 및 산출물

### PostgreSQL 접속

```bash
docker compose exec postgres psql -U crypto -d cryptoflow
```

### 데이터 확인 예시

```sql
SELECT symbol, ts_event, payload->>'trade_price' AS trade_price
FROM crypto_24hr_rawingest_events
ORDER BY ts_event DESC LIMIT 10;
```

### 대시보드

* 추후 Plotly/Dash 기반으로 실시간 차트 제공

---

## 8. 프로젝트 로드맵

* [x] Docker 기반 Postgres, Airflow, Django 연결
* [x] Bronze 테이블 설계
* [ ] Airflow DAG 구현 (배치 수집)
* [ ] Kafka Producer & Redis Consumer 구축 (실시간 수집)
* [ ] Silver → Gold ETL 파이프라인
* [ ] Django REST API 완성
* [ ] 대시보드 시각화
* [ ] Slack/Web 알림 이벤트 추가
