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

class DailyMarketTrend(models.Model):
    """
    Airflow L2 DAG가 매일 아침 생성하는 '오늘의 추세 리포트' 테이블
    managed = False 옵션을 사용하여 Django가 테이블을 새로 만들거나 건드리지 않게 합니다.
    """
    symbol = models.CharField(max_length=20, primary_key=True)
    close_price = models.DecimalField(max_digits=20, decimal_places=4)
    ma_5 = models.DecimalField(max_digits=20, decimal_places=4)
    ma_20 = models.DecimalField(max_digits=20, decimal_places=4)
    ma_60 = models.DecimalField(max_digits=20, decimal_places=4)
    rsi_14 = models.DecimalField(max_digits=10, decimal_places=2) # 새로 추가됨
    is_golden_cross = models.BooleanField()
    trend_score = models.IntegerField()
    created_at = models.DateTimeField()

    class Meta:
        managed = False  # [중요] Airflow가 만든 테이블이므로 Django는 읽기만 함 (No Migration)
        db_table = 'daily_market_trend' # 실제 DB 테이블 이름
        ordering = ['-trend_score'] # 조회 시 점수 높은 순으로 자동 정렬
        verbose_name = "Daily Trend Report"