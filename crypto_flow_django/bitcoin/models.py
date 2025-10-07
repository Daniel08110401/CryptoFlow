#데이터베이스 테이블과 매핑되는 파이썬 클래스 정의
#model을 migration을 통해 DB 스키마 생성

from django.db import models

class RawIngest(models.Model):
    source = models.CharField(max_length=32)                    # 'upbit' | 'coinpaprika'
    endpoint = models.CharField(max_length=64)                  # 'ticker' | 'ohlcv'
    symbol = models.CharField(max_length=32, db_index=True)     # 'KRW-BTC' | 'BTC-USD'
    payload = models.JSONField()                                # raw data JSON
    ts_event = models.DateTimeField(null=True, blank=True)      # 이벤트 기준 시각
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
    interval = models.CharField(max_length=8)                      # '1h','1d'
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
    
class Market24hStat(models.Model):
    symbol = models.CharField(max_length=32, db_index=True)
    acc_trade_price_24h = models.DecimalField(max_digits=30, decimal_places=8)
    acc_trade_volume_24h = models.DecimalField(max_digits=30, decimal_places=8, null=True)
    source = models.CharField(max_length=32, default="upbit")
    ts_event = models.DateTimeField(db_index=True)
    ts_ingested = models.DateTimeField(auto_now_add=True)

    class Meta:
        index_together = ("symbol", "ts_event")

class Crypto24hRawIngestLanding(models.Model):
    source = models.CharField(max_length=32)
    endpoint = models.CharField(max_length=64)
    request_params = models.JSONField(null=True, blank=True)
    http_status = models.IntegerField()
    headers = models.JSONField()
    payload = models.JSONField()
    received_at = models.DateTimeField(db_index=True)
    duration_ms = models.IntegerField()

    class Meta:
        db_table = "crypto_24hr_rawingest_landing"

class Crypto24hRawIngestEvent(models.Model):
    source = models.CharField(max_length=32)
    symbol = models.CharField(max_length=128)
    ts_event = models.DateTimeField(db_index=True)
    payload = models.JSONField()
    call = models.ForeignKey(Crypto24hRawIngestLanding, on_delete=models.CASCADE, db_column="call_id")
    received_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "crypto_24hr_rawingest_events"



