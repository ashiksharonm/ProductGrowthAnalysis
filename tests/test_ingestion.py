import pytest
from unittest.mock import Mock, patch
from src.ingestion.wiki_client import WikiClient

def test_fetch_pageviews_success():
    client = WikiClient()
    mock_response = Mock()
    mock_response.json.return_value = {
        "items": [
            {"timestamp": "2023010100", "views": 100}
        ]
    }
    mock_response.raise_for_status.return_value = None
    
    with patch('requests.get', return_value=mock_response) as mock_get:
        data = client.fetch_pageviews("Test_Page", "20230101", "20230101")
        assert data is not None
        assert data['items'][0]['views'] == 100
        mock_get.assert_called_once()

def test_fetch_pageviews_failure():
    client = WikiClient()
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = Exception("API Error")
    
    with patch('requests.get', return_value=mock_response):
        data = client.fetch_pageviews("Test_Page", "20230101", "20230101")
        assert data is None
