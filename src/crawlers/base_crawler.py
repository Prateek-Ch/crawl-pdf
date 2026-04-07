from abc import ABC, abstractmethod
import time
import requests


class BaseCrawler(ABC):

    BASE_DELAY = 1 # seconds between requests to avoid rate limiting
    MAX_RETRIES = 3
    TIMEOUT = 20

    def __init__(self, topic, max_docs):
        self.topic = topic
        self.max_docs = max_docs

        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://duckduckgo.com/",
            "Origin": "https://duckduckgo.com",
            "Connection": "keep-alive"
        }

    def safe_request(self, url, params=None):
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=self.headers,
                    timeout=self.TIMEOUT
                )

                if response.status_code != 200:
                    print(f"[{self.__class__.__name__}] Bad status: {response.status_code}")
                    return None

                if not response.content or not response.content.strip():
                    print(f"[{self.__class__.__name__}] Empty response")
                    return None

                return response

            except requests.exceptions.ReadTimeout:
                print(f"[{self.__class__.__name__}] Timeout (attempt {attempt + 1})")
                time.sleep(2 * (attempt + 1))

            except requests.exceptions.RequestException as e:
                print(f"[{self.__class__.__name__}] Request failed: {e}")
                return None

        print(f"[{self.__class__.__name__}] Max retries reached. Skipping request.")
        return None

    def throttle(self):
        time.sleep(self.BASE_DELAY)

    @abstractmethod
    def fetch_pdf_links(self) -> list:
        pass

    @abstractmethod
    def fetch_pdf_links_batch(self, max_results, start=0) -> list:
        pass