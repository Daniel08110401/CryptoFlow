# Dockerfile

# 1. 베이스 이미지 선택 (경량화된 Python 3.10)
FROM python:3.10-slim

# 2. 작업 디렉토리 설정
WORKDIR /app

# 3. 시스템 패키지 업데이트 및 git 설치 (필요시)
# RUN apt-get update && apt-get install -y git

# 4. Poetry 설치
RUN pip install poetry

# 5. 의존성 파일만 먼저 복사하여 Docker 캐시 활용
COPY pyproject.toml poetry.lock* ./

# 6. Poetry를 사용해 의존성 설치 (가상환경 생성 없이)
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --only main

# 7. 프로젝트 소스 코드 전체 복사
COPY ./streaming ./streaming
COPY ./shared ./shared

# 8. 애플리케이션 실행 명령어
CMD ["python", "-m", "streaming.main"]