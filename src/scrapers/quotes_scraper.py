"""
Scraper pour quotes.toscrape.com

Fonctionnalités :
- Scraping des citations (texte, auteur, tags)
- Pagination
"""

import time
import hashlib
from typing import Generator, Optional
from urllib.parse import urljoin
from dataclasses import dataclass
import os
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

from config import scraper_config

@dataclass
class Quote:
    text: str
    author: str
    tags: list

    @property
    def id(self) -> str:
        return hashlib.md5(self.text.encode()).hexdigest()[:12].upper()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "text": self.text,
            "author": self.author,
            "tags": self.tags
        }


try:
    BASE_URL = getattr(scraper_config, "quotes_url", os.getenv("QUOTES_BASE_URL", "https://quotes.toscrape.com/"))
except ImportError:
    BASE_URL = os.getenv("QUOTES_BASE_URL", "https://quotes.toscrape.com/")

class QuotesScraper:

    def __init__(self):
        self.base_url = BASE_URL
        self.delay = scraper_config.delay
        self.session = requests.Session()
        self.ua = UserAgent()
        self.session.headers.update({"User-Agent": self.ua.random})

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        response = self.session.get(url, timeout=scraper_config.timeout)
        response.raise_for_status()
        time.sleep(self.delay)
        return BeautifulSoup(response.content, "lxml")

    def scrape_quotes(self, max_pages: int = None) -> Generator[Quote, None, None]:
        url = self.base_url
        page = 1
        while url and (not max_pages or page <= max_pages):
            soup = self._fetch(url)
            for quote_div in soup.select("div.quote"):
                text = quote_div.find("span", class_="text").text
                author = quote_div.find("small", class_="author").text
                tags = [tag.text for tag in quote_div.select(".tags .tag")]
                yield Quote(text, author, tags)
            next_btn = soup.select_one("li.next > a")
            if next_btn:
                url = urljoin(url, next_btn["href"])
                page += 1
            else:
                break

    def close(self):
        self.session.close()

if __name__ == "__main__":
    scraper = QuotesScraper()
    quotes = list(scraper.scrape_quotes(max_pages=1))
    for quote in quotes:
        print(f"{quote.text} - {quote.author} [{', '.join(quote.tags)}]")
    print(f"Total: {len(quotes)} citations scrapees")
    scraper.close()
