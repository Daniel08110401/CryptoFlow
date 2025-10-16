from django.contrib import admin
from django.urls import path
from bitcoin import views

urlpatterns = [
    path('admin/', admin.site.urls),

    ## Real-time data api ##
    path('api/realtime-price/', views.RealtimePriceView.as_view(), name='realtime-price'),

    ## Batch data api ##
    path('api/market-stats/', views.MarketStatsView.as_view(), name='market-stats-list'),

    # Batch data for specific crypto market
    path('api/market-stats/<str:market_symbol>/', views.MarketStatsDetailView.as_view(), name='market-stats-detail'),
]