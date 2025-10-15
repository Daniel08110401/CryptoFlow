from django.contrib import admin
from django.urls import path
from bitcoin import views  # 1. bitcoin 앱의 views.py 파일을 가져옴

urlpatterns = [
    path('admin/', admin.site.urls),

    # 2. 새로운 경로 추가
    path('realtime-price/', views.RealtimePriceView.as_view(), name='realtime-price'),
]