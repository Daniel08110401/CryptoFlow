from django.contrib import admin
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from bitcoin import views  # views 모듈 전체를 import하여 네임스페이스 활용

# Router 설정
router = DefaultRouter()
# views.TrendReportViewSet 형태로 사용하여 import 구문을 줄이고 출처를 명확히 함
router.register(r'trend-report', views.TrendReportViewSet, basename='trend-report')

urlpatterns = [
    # 1. Django Admin
    path('admin/', admin.site.urls),

    # 2. DRF Router URLs (자동 생성된 API)
    # -> /api/trend-report/ 및 /api/trend-report/top-picks/ 등
    path('api/', include(router.urls)),

    # 3. Custom API Views (수동 정의된 API)
    # Real-time data
    path('api/realtime-price/', views.RealtimePriceView.as_view(), name='realtime-price'),

    # Batch data (Market Stats)
    path('api/market-stats/', views.MarketStatsView.as_view(), name='market-stats-list'),
    path('api/market-stats/<str:symbol>/', views.MarketStatsSpecificMarketView.as_view(), name='market-stats-detail'),
]