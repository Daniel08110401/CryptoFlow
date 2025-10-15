# crypto_flow_django/bitcoin/views.py

from rest_framework.views import APIView
from rest_framework.response import Response
from .services_cache import get_realtime_ticker_from_redis

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