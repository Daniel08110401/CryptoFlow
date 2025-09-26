# Django REST Framework를 사용하여 RESTful API를 만들어서 비트코인 데이터를 클라이언트에게 전달

from rest_framework.views import APIView
from rest_framework.response import Response
from .services_cache import get_realtime_price_cached

class RealtimePriceView(APIView):
    def get(self, request):
        data = get_realtime_price_cached("KRW-BTC", ttl=3)
        return Response({
            "market": data["market"],
            "price": data["trade_price"],
            "ts": data["trade_timestamp"]
        })
