# crypto_flow_django/bitcoin/serializers.py

from rest_framework import serializers
from .models import MarketStats24h

class MarketStats24hSerializer(serializers.ModelSerializer):
    """
    MarketStats24h 모델을 JSON으로 변환하기 위한 Serializer
    """
    class Meta:
        model = MarketStats24h
        # API 응답에 포함될 필드들을 지정
        fields = [
            'symbol', 
            'acc_trade_price_24h', 
            'acc_trade_volume_24h', 
            'source', 
            'ts_event', 
            'created_at'
        ]