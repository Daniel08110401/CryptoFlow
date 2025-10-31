# CryptoFlow: Real-Time Cryptocurrency Data Pipeline & Analytics

## 1\. 프로젝트 개요

본 프로젝트는 Upbit 암호화폐 거래소 데이터를 대상으로 **실시간(Hot Path)**과 **배치(Cold Path)** 데이터 파이프라인을 분리하여 설계, 구축한 엔드투엔드(End-to-End) 데이터 플랫폼입니다.

실시간 데이터는 `WebSocket`과 `Kafka`를 통해 `Redis`에 적재되며, 배치 데이터는 `Airflow`를 통해 `PostgreSQL` 데이터 레이크(ELT)에 저장됩니다. `Docker Compose`를 통해 전체 인프라를 컨테이너화했으며, `Django`는 수집된 데이터를 활용하여 API 제공 및 비동기 알림(Celery)을 처리하는 서비스 계층을 담당합니다.

이 프로젝트는 대용량 데이터 처리 아키텍처, 데이터 계약의 중요성, 서비스 간 역할 분리(Decoupling) 등 현대 데이터 엔지니어링의 핵심 요소를 학습하고 구현하는 것을 목표로 합니다.

-----

## 2\. 주요 기능 및 아키텍처

본 프로젝트의 핵심은 **듀얼 파이프라인 아키텍처**입니다.

### ⚡️ 실시간 경로 (Hot Path)

  * **목적**: 현재 시장 상황을 가장 낮은 지연 시간(Low Latency)으로 처리하여 실시간 API 및 대시보드에 제공합니다.
  * **데이터 흐름**: `Upbit WebSocket` → `Python Producer (Asyncio)` → `Kafka` → `Python Consumer (Asyncio)` → `Redis`
  * **핵심 기능**:
      * `aiokafka` 기반 비동기 Producer/Consumer로 초당 수십 건의 체결/호가/시세 데이터 안정적 처리.
      * `Redis Streams` (체결 이력) 및 `Hashes` (최신 시세)를 사용한 목적별 데이터 저장.
      * `Django API`가 Redis 데이터를 직접 조회하여 수십 ms 이내의 빠른 응답 제공.

### 📚 배치 경로 (Cold Path)

  * **목적**: 과거 데이터를 **누락 없이 영구적으로 저장**하고, 분석 및 ML 모델 학습을 위한 정제된 데이터 마트를 구축합니다.
  * **데이터 흐름**: `Upbit REST API` → `Airflow DAG (EL)` → `PostgreSQL (Data Lake)` → `Airflow DAG (T)` → `PostgreSQL (Data Mart)`
  * **핵심 기능**:
      * **ELT(Extract-Load-Transform) 패턴** 적용: Airflow가 10분마다 200개+ 마켓 데이터를 수집하여 원본 그대로 \*\*데이터 레이크(`JSONB`)\*\*에 먼저 적재(Load)합니다.
      * **데이터 마트 구축**: 별도의 Airflow DAG가 SQL을 실행하여 데이터 레이크의 원본 데이터를 **DB 내부에서** 가공(Transform), 분석에 최적화된 `market_stats_24h` 테이블을 생성합니다.

### 🛎️ 서비스 및 자동화 (Application Layer)

  * **Django REST API**: `Redis`(실시간)와 `PostgreSQL`(배치)의 데이터를 조합하여 `/api/realtime-price/`, `/api/market-stats/` 등 RESTful API 제공. (DRF 고급 기능: Pagination, Filtering 적용)
  * **비동기 알림 시스템**: **Celery** 및 **Celery Beat**를 도입하여, Airflow가 생성한 데이터 마트를 주기적으로 분석하고(예: 이동 평균선 교차 확인), 특정 조건 충족 시 알림(로그, 향후 이메일/Slack)을 보내는 백그라운드 작업 자동화.

-----

## 3\. 기술 스택

  * **Language**: Python 3.11+
  * **Dependency Management**: Poetry
  * **Infra & DevOps**: Docker, Docker Compose
  * **Orchestration (Batch)**: Apache Airflow
  * **Database**: PostgreSQL 16 (Data Lake & Mart), Redis 7 (Real-time Cache & Broker)
  * **Streaming**: Apache Kafka 3.5
  * **Backend & API**: Django, Django REST Framework
  * **Async & Task Queue**: Celery, asyncio
  * **Data Handling**: Pandas, Pydantic (데이터 계약 및 유효성 검증)
  * **(예정) ML**: Scikit-learn, XGBoost

-----

## 4\. 설치 및 실행

### 사전 준비

  * Docker 및 Docker Compose 설치
  * Poetry 설치
  * 프로젝트 루트에 `.env` 파일 생성 (DB 및 Airflow 설정)

### 1단계: 프로젝트 클론 및 가상 환경 설정

```bash
git clone https://github.com/Daniel08110401/CryptoFlow.git
cd CryptoFlow
# pyproject.toml의 Python 버전에 맞는 로컬 Python 환경 설정 (예: pyenv)
poetry install # 가상 환경 생성 및 모든 의존성 설치
```

### 2단계: Docker 서비스 실행

전체 시스템(실시간 + 배치 + API)을 실행합니다.

```bash
docker-compose up --build
```

*`--build` 옵션은 `Dockerfile`이나 의존성 변경 시에만 필요합니다.*

### 3단계: Airflow & API 확인

  * **Airflow UI**: `http://localhost:8081`
  * **Django API (example: 실시간)**: `http://localhost:8000/api/realtime-price/`
  * **Django API (example: 배치)**: `http://localhost:8000/api/market-stats/?symbol=KRW-BTC`

-----

## 5\. 프로젝트 로드맵

  * [x] **Hot Path**: 실시간 스트리밍 파이프라인 (`WebSocket` → `Kafka` → `Redis`) 구축
  * [x] **Cold Path (ELT)**: Airflow 기반 데이터 레이크 및 데이터 마트 구축
  * [x] **Docker**: 전체 서비스 컨테이너화 및 `docker-compose` 환경 구축
  * [x] **API (기본)**: `Redis` 및 `PostgreSQL` 연동 API 엔드포인트 구현 (DRF)
  * [x] **API (고도화)**: 페이지네이션 및 필터링 기능 적용
  * [x] **비동기 작업**: `Celery` 및 `Celery Beat` 연동 (주기적 MA Cross 분석)
  * [x] **알림 기능**: Celery Task와 이메일/Slack을 연동하여 실제 알림 발송
  * [ ] **ML 파이프라인**: (EDA 완료) ML 모델 학습 및 `/api/predict` 엔드포인트 구현
  * [ ] **대시보드**: Django Template 또는 `Streamlit`/`Dash`를 이용한 시각화
  * [x] **테스트**: Django 단위 테스트 및 통합 테스트 코드 작성
