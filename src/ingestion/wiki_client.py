import requests
import datetime
from typing import Dict, Any, Optional

class WikiClient:
    """Client for fetching Wikipedia pageview data."""
    
    BASE_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    
    def __init__(self, user_agent: str = "ProductGrowthAnalytics/1.0 (me@example.com)"):
        self.headers = {
            "User-Agent": user_agent
        }

    def fetch_pageviews(
        self, 
        article: str, 
        start_date: str, 
        end_date: str, 
        project: str = "en.wikipedia",
        access: str = "all-access",
        agent: str = "user",
        granularity: str = "daily"
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch daily pageviews for a specific article.
        
        Args:
            article: The title of the article (e.g., "Artificial_intelligence")
            start_date: YYYYMMDD format
            end_date: YYYYMMDD format
        """
        # Ensure article title is URL-safe (replace spaces with underscores usually needed, 
        # but the API expects exact matching, often casing matters)
        safe_article = article.replace(" ", "_")
        
        url = (
            f"{self.BASE_URL}/{project}/{access}/{agent}/{safe_article}/{granularity}/{start_date}/{end_date}"
        )
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching data for {article}: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error for {article}: {e}")
            return None

if __name__ == "__main__":
    # Quick test
    client = WikiClient()
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=7)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    data = client.fetch_pageviews("Artificial_intelligence", start, end)
    print(data)
