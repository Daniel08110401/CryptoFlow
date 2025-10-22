import os
from celery import Celery

# Django settings 모듈을 Celery의 기본 설정으로 사용하도록 설정
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_setup.settings')

# Celery 애플리케이션 인스턴스 생성
app = Celery('django_setup')

# Django settings.py 파일로부터 설정을 로드
# namespace='CELERY'는 settings.py 안에서 CELERY_ 로 시작하는 모든 설정을 Celery 설정으로 인식하라는 (예: CELERY_BROKER_URL)
app.config_from_object('django.conf:settings', namespace='CELERY')

# Django 앱 설정 파일들에서 task 모듈을 자동으로 로드
# 각 앱(예: bitcoin 앱)의 tasks.py 파일에 정의된 Task들을 Celery가 자동으로 조회
app.autodiscover_tasks()

@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')