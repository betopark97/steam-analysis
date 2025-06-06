import os
from typing import Optional
import random
import logging
import requests
import time
from json.decoder import JSONDecodeError


logger = logging.getLogger("SteamAPIManager")

class SteamAPIManager:
    """Handle Steam API interactions"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('STEAM_API_KEY')
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.64",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Android 10; Mobile; rv:85.0) Gecko/85.0 Firefox/85.0"
        ]

    def _make_request(self, url: str, params: dict = None, return_type: str = 'json'):
        max_retries = 5
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json"
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=10)

                if response.status_code == 200:
                    try:
                        time.sleep(3)
                        return response.json() if return_type == 'json' else response.text
                    except JSONDecodeError as e:
                        logger.warning(f"JSONDecodeError for URL {url}: {e}")
                        return None

                if response.status_code in {429} or 500 <= response.status_code < 600:
                    time.sleep(30)
                    raise requests.HTTPError(f"Retryable HTTP error: {response.status_code}", response=response)

                else:
                    logger.error(f"Non-retryable error: {response.status_code} - {response.text}")
                    raise Exception(f"Non-retryable error: {response.status_code} - {response.text}")

            except (requests.RequestException, requests.Timeout) as e:
                if attempt == max_retries:
                    logger.critical(f"All {max_retries} retries failed for URL: {url}")
                    raise

                backoff = 2 ** attempt + random.uniform(0, 0.5)
                logger.warning(f"[Attempt {attempt}/{max_retries}] Error: {e}. Retrying in {backoff:.2f}s")
                time.sleep(backoff)

    # -----------------------
    # Async API Methods
    # -----------------------
    def get_app_names(self):
        url = "https://api.steampowered.com/ISteamApps/GetAppList/v0002"
        return self._make_request(url)

    def get_app_details(self, app_id: int):
        url = "https://store.steampowered.com/api/appdetails"
        params = {"appids": app_id}
        return self._make_request(url, params)

    def get_app_reviews(self, app_id: int):
        url = f"https://store.steampowered.com/appreviews/{app_id}"
        return self._make_request(url)

    def get_app_tags(self, app_id: int):
        url = "https://steamspy.com/api.php"
        params = {"request": "appdetails", "appid": app_id}
        return self._make_request(url, params)

    # ---------------------------------------------------------------
    # Data enrichment
    # ---------------------------------------------------------------
    def get_app_html(self, app_id: int):
        url = f"https://store.steampowered.com/app/{app_id}"
        return self._make_request(url, return_type='text')