from pathlib import Path

import os
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-rht6c+*0q*u0=7ds60sac8tu$pm-2o@j9*m3p*55gmp(h=yeg&'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_celery_beat',
    'django_filters',
    'rest_framework',
    'bitcoin',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'django_setup.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'django_setup.wsgi.application'


# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ.get('POSTGRES_DB', 'cryptoflow'),
        'USER': os.environ.get('POSTGRES_USER', 'crypto'),
        'PASSWORD': os.environ.get('POSTGRES_PASSWORD', 'crypto'),
        'HOST': os.environ.get('POSTGRES_HOST', 'localhost'),
        'PORT': '5432',
    }
}

# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# CELERY
# Redis를 메시지 브로커로 사용
# Docker 환경에서는 서비스 이름 'redis'를 사용
CELERY_REDIS_DB = '1'
CELERY_BROKER_URL = f"redis://{os.environ.get('REDIS_HOST', 'localhost')}:6379/{CELERY_REDIS_DB}"
# 작업 결과를 Redis에 저장 (상태 추적에 유용)
CELERY_RESULT_BACKEND = CELERY_BROKER_URL
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'Asia/Seoul' # 작업 시간 기준을 서울 시간으로 설정

# CELERY BEAT SETTINGS (주기적 작업 스케줄)
CELERY_BEAT_SCHEDULER = 'django_celery_beat.schedulers:DatabaseScheduler'

CELERY_BEAT_SCHEDULE = {
    'check-ma-cross-every-10-minutes': {
        'task': 'bitcoin.tasks.check_moving_average_cross', # 실행할 Task 경로
        'schedule': 600.0,  # 실행 간격 (초 단위, 600초 = 10분)
        'args': ('KRW-BTC', 5, 20), # Task에 전달할 인자 (symbol, short, long)
        # 'kwargs': {'symbol': 'KRW-BTC'} # 또는 키워드 argument 전달
    },
    # 다른 주기적 Task가 있다면 여기에 추가
}

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
CACHE_REDIS_DB = '2'

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": f"redis://{REDIS_HOST}:6379/{CACHE_REDIS_DB}",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
        }
    }
}

REST_FRAMEWORK = {
    # 기본 페이지네이션 클래스 설정
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.PageNumberPagination',
    # 페이지당 보여줄 기본 항목 개수 설정
    'PAGE_SIZE': 100,
    'DEFAULT_FILTER_BACKENDS': [
        'django_filters.rest_framework.DjangoFilterBackend'
    ],

    'DEFAULT_THROTTLE_CLASSES': [
        # 인증된 사용자는 유저별로, 익명 사용자는 IP별로 제한
        'rest_framework.throttling.AnonRateThrottle',
        'rest_framework.throttling.UserRateThrottle'
    ],
    'DEFAULT_THROTTLE_RATES': {
        'anon': '50/minute',  # 익명 사용자: 50requests/min
        'user': '100/minute' # 인증된 사용자: 100 requests/min
    }
}

# EMAIL SETTINGS (for MailHog in Docker)
# 이메일 백엔드를 콘솔 대신 SMTP로 설정
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
# MailHog 컨테이너의 서비스 이름과 포트
EMAIL_HOST = 'mailhog'
EMAIL_PORT = 1025
EMAIL_USE_TLS = False
EMAIL_USE_SSL = False
DEFAULT_FROM_EMAIL = 'admin@cryptoflow.com' # 기본 발신자 주소 

# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = 'static/'

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
