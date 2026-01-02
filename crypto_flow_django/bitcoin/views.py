from rest_framework import viewsets, filters
from rest_framework.decorators import action
from rest_framework.views import APIView
from rest_framework.generics import ListAPIView, RetrieveAPIView
from rest_framework.response import Response
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from .services_cache import get_realtime_ticker_from_redis
from .models import MarketStats24h, DailyMarketTrend
from .serializers import MarketStats24hSerializer, DailyTrendSerializer
from .filters import MarketStatsFilter

# Streaming data views
class RealtimePriceView(APIView):
    def get(self, request, market="KRW-BTC"):
        # 새로운 서비스 함수를 호출
        data = get_realtime_ticker_from_redis(market)
        
        if data:
            # Redis에서 받은 데이터 구조에 맞게 응답을 구성
            return Response({
                "symbol": data.get("symbol"),
                "price": data.get("trade_price"),
                "timestamp": data.get("timestamp")
            })
        else:
            # 데이터가 없을 경우 에러 메시지를 반환
            return Response({"error": f"No realtime data for {market}"}, status=404)


# Batch data views (cache logic is applied)
@method_decorator(cache_page(60 * 10), name='get')
class MarketStatsView(ListAPIView):
    """
    market_stats_24h 테이블의 모든 데이터를 조회하는 API view
    """
    # 이 뷰가 사용할 데이터셋(모든 MarketStats24h 객체)을 지정
    queryset = MarketStats24h.objects.all().order_by('symbol')
    # 이 뷰가 데이터를 JSON으로 변환할 때 사용할 Serializer를 지정
    serializer_class = MarketStats24hSerializer
    filterset_class = MarketStatsFilter

# Speciic market data views
@method_decorator(cache_page(60 * 10), name='get')
class MarketStatsSpecificMarketView(RetrieveAPIView):
    """
    특정 마켓(symbol) 하나의 24시간 통계 데이터를 조회하는 API view
    """
    queryset = MarketStats24h.objects.all()
    serializer_class = MarketStats24hSerializer
    lookup_field = 'symbol'

class TrendReportViewSet(viewsets.ReadOnlyModelViewSet):
    """
    오늘의 추세 리포트를 조회하는 API
    GET /api/trend-report/
    """
    queryset = DailyMarketTrend.objects.all()
    serializer_class = DailyTrendSerializer

    # 검색 필터 활성화, ex) /api/trend-report/?search=BTC
    filter_backends = [filters.SearchFilter]
    search_fields = ['symbol'] # 'symbol' 컬럼에서 검색

    # [추가 기능] 점수가 높은 순으로 정렬해서 Top 3만 뽑아주는 API
    # GET /api/trend-report/top-picks/
    @action(detail=False, methods=['get'], url_path='top-picks')
    def top_picks(self, request):
        top_coins = self.queryset.order_by('-trend_score')[:3]
        serializer = self.get_serializer(top_coins, many=True)
        return Response(serializer.data)