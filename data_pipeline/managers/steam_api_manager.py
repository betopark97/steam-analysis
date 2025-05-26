import json
import os
from pathlib import Path
from typing import Optional
import random
import requests
from dotenv import load_dotenv
import time
from datetime import datetime
# from .postgres_manager import PostgresManager

load_dotenv()


class SteamAPIManager:
    """Handle Steam API interactions and data management"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('STEAM_API_KEY')
        self.output_dir = Path(__file__).cwd().parent / 'data'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        
    def _make_request(self, url: str, params: dict = None, return_type: str = 'json') -> dict:
        """Make a request to the Steam API"""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.64",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Android 10; Mobile; rv:85.0) Gecko/85.0 Firefox/85.0"
        ]

        # Randomly select a User-Agent header
        headers = {
            "User-Agent": random.choice(user_agents)
        }

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                time.sleep(random.uniform(2, 4))
                response = requests.get(url=url, params=params, headers=headers)
                response.raise_for_status()
                if return_type == 'json':
                    return response.json()
                if return_type == 'text':
                    return response.text  # Return raw response if not JSON
            except requests.exceptions.RequestException as e:
                print(f"[Attempt {attempt}] Request failed: {e}")
                if attempt == max_retries:
                    raise RuntimeError(f"Failed to fetch data from Steam API after {max_retries} attempts: {e}")
                backoff = 2 ** attempt
                print(f"Retrying in {backoff}s...")
                time.sleep(backoff)
    
    def get_app_names(self) -> dict:
        """Get app names from Steam API"""
        url = "https://api.steampowered.com/ISteamApps/GetAppList/v0002"
        response = self._make_request(url)
        
        return response
    
    def get_app_details(self, app_id: int) -> dict:
        """Get game info from Steam API"""
        url = "https://store.steampowered.com/api/appdetails"
        params = {"appids": app_id}
        response = self._make_request(url, params)
        
        return response
    
    def get_app_tags(self, app_id: int) -> dict:
        """Get app tags from Steam Spy"""
        url = "https://steamspy.com/api.php"
        params = {"request": "appdetails", "appid": app_id}
        response = self._make_request(url, params)

        return response
    
    def get_app_reviews(self, app_id: int) -> dict:
        """Get app reviews from Steam API"""
        url = f"https://store.steampowered.com/appreviews/{app_id}"
        response = self._make_request(url)
        
        return response
    
    # ---------------------------------------------------------------
    # Data enrichment
    # ---------------------------------------------------------------
    def get_app_html(self, app_id: int) -> dict:
        """Get app description from Steam Website"""
        url = f"https://store.steampowered.com/app/{app_id}"
        response = self._make_request(url, return_type='text')
        
        return response