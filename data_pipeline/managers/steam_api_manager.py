import os
from typing import Optional
import random
import logging
import aiohttp
import asyncio


logger = logging.getLogger("AsyncSteamAPIManager")

class AsyncSteamAPIManager:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('STEAM_API_KEY')
        self.last_request_time = {}

        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.64",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Android 10; Mobile; rv:85.0) Gecko/85.0 Firefox/85.0"
        ]
        
    async def _rate_limited_request(self, url: str, params: dict = None, return_type: str = 'json'):
        max_retries = 5
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json"
        }

        for attempt in range(1, max_retries + 1):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 200:
                            await asyncio.sleep(3)
                            return await resp.json() if return_type == 'json' else await resp.text()

                        if resp.status in {429} or 500 <= resp.status < 600:
                            await asyncio.sleep(30)
                            raise aiohttp.ClientResponseError(
                                request_info=resp.request_info,
                                history=resp.history,
                                status=resp.status,
                                message=f"Retryable HTTP error: {resp.status}",
                                headers=resp.headers,
                            )
                        else:
                            error_text = await resp.text()
                            logger.error(f"Non-retryable error: {resp.status} - {error_text}")
                            raise Exception(f"Non-retryable error: {resp.status} - {error_text}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == max_retries:
                    logger.critical(f"All {max_retries} retries failed for URL: {url}")
                    raise

                backoff = 2 ** attempt + random.uniform(0, 0.5)
                logger.warning(f"[Attempt {attempt}/{max_retries}] Error: {e}. Retrying in {backoff:.2f}s")
                await asyncio.sleep(backoff)

    # -----------------------
    # Async API Methods
    # -----------------------
    async def get_app_names(self):
        url = "https://api.steampowered.com/ISteamApps/GetAppList/v0002"
        return await self._rate_limited_request(url)

    async def get_app_details(self, app_id: int):
        url = "https://store.steampowered.com/api/appdetails"
        params = {"appids": app_id}
        return await self._rate_limited_request(url, params)

    async def get_app_reviews(self, app_id: int):
        url = f"https://store.steampowered.com/appreviews/{app_id}"
        return await self._rate_limited_request(url)

    async def get_app_tags(self, app_id: int):
        url = "https://steamspy.com/api.php"
        params = {"request": "appdetails", "appid": app_id}
        return await self._rate_limited_request(url, params)
    
    # ---------------------------------------------------------------
    # Data enrichment
    # ---------------------------------------------------------------
    async def get_app_html(self, app_id: int):
        url = f"https://store.steampowered.com/app/{app_id}"
        return await self._rate_limited_request(url, return_type='text')