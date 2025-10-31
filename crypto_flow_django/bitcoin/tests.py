from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from unittest.mock import patch
from .models import MarketStats24h
import pendulum

class MarketStatsAPITests(APITestCase):
    """
    /api/market-stats/ (배치 데이터 API)에 대한 테스트
    """

    def setUp(self):
        """
        모든 테스트가 실행되기 전에 필요한 '가짜 데이터'를 DB에 생성
        테스트용 DB는 실제 DB와 분리된 임시 DB에서 실행
        """
        MarketStats24h.objects.create(
            symbol="KRW-BTC",
            acc_trade_price_24h="1000000",
            acc_trade_volume_24h="10",
            source="upbit",
            ts_event=pendulum.now('UTC'),
            created_at=pendulum.now('UTC')
        )
        MarketStats24h.objects.create(
            symbol="KRW-ETH",
            acc_trade_price_24h="500000",
            acc_trade_volume_24h="50",
            source="upbit",
            ts_event=pendulum.now('UTC'),
            created_at=pendulum.now('UTC')
        )
        self.list_url = reverse('market-stats-list') # '/api/market-stats/'

    
    def test_get_market_stats_list(self):
        """
        GET /api/market-stats/ (목록 조회)가 페이지네이션 응답을 반환하는지 테스트
        """
        response = self.client.get(self.list_url)
        
        # 1. 응답 상태 코드가 200 (OK)인지 확인
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # 2. 페이지네이션 구조(count, results)가 맞는지 확인
        self.assertIn('count', response.data)
        self.assertIn('results', response.data)
        
        # 3. 데이터가 2개 들어있는지 확인
        self.assertEqual(response.data['count'], 2)
        self.assertEqual(len(response.data['results']), 2)

    def test_filter_market_stats_by_symbol(self):
        """
        GET /api/market-stats/?symbol=KRW-BTC (필터링)가 KRW-BTC 데이터만 반환하는지 테스트
        """
        url = f"{self.list_url}?symbol=KRW-BTC"
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # 1. 결과가 1개만 필터링되었는지 확인
        self.assertEqual(response.data['count'], 1)
        
        # 2. 그 1개의 심볼이 KRW-BTC가 맞는지 확인
        self.assertEqual(response.data['results'][0]['symbol'], 'KRW-BTC')

    def test_get_market_stats_detail(self):
        """
        GET /api/market-stats/KRW-ETH/ (상세 조회)가 KRW-ETH 데이터만 반환하는지 테스트
        """
        # 'name'을 사용하여 URL을 생성 (urls.py의 name='market-stats-detail')
        url = reverse('market-stats-detail', kwargs={'symbol': 'KRW-ETH'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # 1. 반환된 심볼이 KRW-ETH가 맞는지 확인
        self.assertEqual(response.data['symbol'], 'KRW-ETH')
        self.assertEqual(response.data['acc_trade_price_24h'], '500000.00000000')

    def test_get_market_stats_detail_not_found(self):
        """
        존재하지 않는 마켓 조회 시 404 에러가 발생하는지 테스트
        """
        url = reverse('market-stats-detail', kwargs={'symbol': 'KRW-DOGE'})
        response = self.client.get(url)
        
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class RealtimePriceAPITests(APITestCase):
    """
    /api/realtime-price/ (실시간 데이터 API)에 대한 테스트
    """

    # @patch를 사용하여 'bitcoin.views.get_realtime_ticker_from_redis' 함수를 'mock_get_redis_data'라는 가짜 객체로 대체
    @patch('bitcoin.views.get_realtime_ticker_from_redis')
    def test_get_realtime_price_success(self, mock_get_redis_data):
        """
        Redis에 데이터가 있을 경우, API가 200 (OK)와 올바른 데이터를 반환하는지 테스트
        """
        # 가짜 함수가 반환할 가짜 데이터를 정의합니다.
        fake_data = {
            "symbol": "KRW-BTC",
            "trade_price": 999999.0,
            "timestamp": "2025-10-10T10:10:10Z"
        }
        mock_get_redis_data.return_value = fake_data
        
        url = reverse('realtime-price') # '/api/realtime-price/'
        response = self.client.get(url)
        
        # 1. 응답 상태 코드가 200 (OK)인지 확인
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        
        # 2. 반환된 데이터가 우리가 정의한 가짜 데이터와 일치하는지 확인
        self.assertEqual(response.data['symbol'], 'KRW-BTC')
        self.assertEqual(response.data['price'], 999999.0)

    @patch('bitcoin.views.get_realtime_ticker_from_redis')
    def test_get_realtime_price_no_data(self, mock_get_redis_data):
        """
        Redis에 데이터가 없을 경우(None 반환), API가 404 (Not Found)를 반환하는지 테스트
        """
        # 가짜 함수가 None을 반환하도록 설정합니다.
        mock_get_redis_data.return_value = None
        
        url = reverse('realtime-price')
        response = self.client.get(url)
        
        # 1. 응답 상태 코드가 404 (Not Found)인지 확인
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        
        # 2. 에러 메시지가 올바르게 포함되어 있는지 확인
        self.assertIn('error', response.data)