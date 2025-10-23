from django_filters import rest_framework as filters
from .models import MarketStats24h

class MarketStatsFilter(filters.FilterSet):
    """
    FilterSet for the MarketStats24h model.
    Allows filtering by market symbol.
    """
    class Meta:
        model = MarketStats24h
        # Define fields available for filtering via URL parameters
        fields = {
            'symbol': ['exact'], # Allows filtering like ?symbol=KRW-BTC
        }