#데이터베이스 테이블과 매핑되는 파이썬 클래스를 정의

from django.db import models

class RawIngest(models.Model):
    source = models.CharField(max_length=32)            # 'upbit' | 'coinpaprika'
    endpoint = models.CharField(max_length=64)          # 'ticker' | 'ohlcv'
    symbol = models.CharField(max_length=32, db_index=True)  # 'KRW-BTC' | 'BTC-USD'
    payload = models.JSONField()                        # raw data JSON
    ts_event = models.DateTimeField(null=True, blank=True)   # 이벤트 기준 시각
    ts_ingested = models.DateTimeField(auto_now_add=True)

class SpotSnapshot(models.Model):
    symbol = models.CharField(max_length=32, db_index=True)
    price = models.DecimalField(max_digits=20, decimal_places=8)   # KRW/BTC면 KRW 단위
    quote_ccy = models.CharField(max_length=8, default="KRW")      # 'KRW' | 'USD'
    source = models.CharField(max_length=32, default="upbit")
    ts_event = models.DateTimeField(db_index=True)                 # 거래소/응답 기준 시각
    ts_ingested = models.DateTimeField(auto_now_add=True)

class Candle(models.Model):
    symbol = models.CharField(max_length=32, db_index=True)        # 'BTC-USD' 등
    interval = models.CharField(max_length=8)                      # '1h','1d'...
    open = models.DecimalField(max_digits=20, decimal_places=8)
    high = models.DecimalField(max_digits=20, decimal_places=8)
    low = models.DecimalField(max_digits=20, decimal_places=8)
    close = models.DecimalField(max_digits=20, decimal_places=8)
    volume = models.DecimalField(max_digits=24, decimal_places=8, null=True)
    ts_open = models.DateTimeField(db_index=True)
    source = models.CharField(max_length=32, default="coinpaprika")
    ts_ingested = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("symbol", "interval", "ts_open", "source")
    


