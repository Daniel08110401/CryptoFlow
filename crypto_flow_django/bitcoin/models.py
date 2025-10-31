from django.db import models
import sys

class MarketStats24h(models.Model):
    """
    Airflow DAG가 생성한 `market_stats_24h` 테이블을
    읽기 전용으로 참조하기 위한 Django 모델
    """
    # DB에 생성된 컬럼과 동일한 필드를 정의합니다.
    symbol = models.CharField(max_length=32, primary_key=True) # symbol을 기본 키로 사용하면 조회가 편함
    acc_trade_price_24h = models.DecimalField(max_digits=30, decimal_places=8)
    acc_trade_volume_24h = models.DecimalField(max_digits=30, decimal_places=8, null=True)
    source = models.CharField(max_length=32)
    ts_event = models.DateTimeField()
    created_at = models.DateTimeField()

    class Meta:
        managed = False  # False -> 이미 존재하는 테이블을 읽기만 진행
        if 'test' in sys.argv:
            managed = True
        db_table = 'market_stats_24h'  # 실제 DB 테이블 이름 명시
        verbose_name = '24시간 마켓 통계 (Airflow)'
        verbose_name_plural = verbose_name

# class SpotSnapshot(models.Model):
#     symbol = models.CharField(max_length=32, db_index=True)
#     price = models.DecimalField(max_digits=20, decimal_places=8)   # KRW/BTC면 KRW 단위
#     quote_ccy = models.CharField(max_length=8, default="KRW")      # 'KRW' | 'USD'
#     source = models.CharField(max_length=32, default="upbit")
#     ts_event = models.DateTimeField(db_index=True)                 # 거래소/응답 기준 시각
#     ts_ingested = models.DateTimeField(auto_now_add=True)

# class Candle(models.Model):
#     symbol = models.CharField(max_length=32, db_index=True)        # 'BTC-USD' 등
#     interval = models.CharField(max_length=8)                      # '1h','1d'
#     open = models.DecimalField(max_digits=20, decimal_places=8)
#     high = models.DecimalField(max_digits=20, decimal_places=8)
#     low = models.DecimalField(max_digits=20, decimal_places=8)
#     close = models.DecimalField(max_digits=20, decimal_places=8)
#     volume = models.DecimalField(max_digits=24, decimal_places=8, null=True)
#     ts_open = models.DateTimeField(db_index=True)
#     source = models.CharField(max_length=32, default="coinpaprika")
#     ts_ingested = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         unique_together = ("symbol", "interval", "ts_open", "source")


# class Crypto24hRawIngestLanding(models.Model):
#     source = models.CharField(max_length=32)
#     endpoint = models.CharField(max_length=64)
#     request_params = models.JSONField(null=True, blank=True)
#     http_status = models.IntegerField()
#     headers = models.JSONField()
#     payload = models.JSONField()
#     received_at = models.DateTimeField(db_index=True)
#     duration_ms = models.IntegerField()

#     class Meta:
#         db_table = "crypto_24hr_rawingest_landing"

# class Crypto24hRawIngestEvent(models.Model):
#     source = models.CharField(max_length=32)
#     symbol = models.CharField(max_length=128)
#     ts_event = models.DateTimeField(db_index=True)
#     payload = models.JSONField()
#     call = models.ForeignKey(Crypto24hRawIngestLanding, on_delete=models.CASCADE, db_column="call_id")
#     received_at = models.DateTimeField(auto_now_add=True)

#     class Meta:
#         db_table = "crypto_24hr_rawingest_events"



