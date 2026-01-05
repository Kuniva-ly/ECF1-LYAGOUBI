"""
Scraper pour books.toscrape.com

Fonctionnalités :
- Scraping des livres (titre, prix, note, catégorie, image)
- Gestion de la pagination
- Délai entre requêtes
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
class Book:
    title: str
    price: float
    rating: int
    category: str
    image_url: str
    product_url: str

    @property
    def sku(self) -> str:
        return hashlib.md5(self.title.encode()).hexdigest()[:12].upper()

    def to_dict(self) -> dict:
        return {
            "sku": self.sku,
            "title": self.title,
            "price": self.price,
            "rating": self.rating,
            "category": self.category,
            "image_url": self.image_url,
            "product_url": self.product_url
        }

class BooksScraper:
    try:
        from config import scraper_config
        BASE_URL = getattr(scraper_config, "books_url", os.getenv("BOOKS_BASE_URL", "https://books.toscrape.com/"))
    except ImportError:
        BASE_URL = os.getenv("BOOKS_BASE_URL", "https://books.toscrape.com/")

    def __init__(self):
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

    def _parse_rating(self, element) -> int:
        rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
        classes = element.get("class", [])
        for c in classes:
            if c in rating_map:
                return rating_map[c]
        return 0

    def _extract_category(self, product_url: str) -> str:
        try:
            soup = self._fetch(product_url)
            items = soup.select("ul.breadcrumb li")
            if len(items) >= 2:
                return items[-2].get_text(strip=True)
        except Exception:
            return "Unknown"
        return "Unknown"

    def scrape_books(self, max_pages: int = None) -> Generator[Book, None, None]:
        url = self.BASE_URL + "catalogue/page-1.html"
        page = 1
        while url and (not max_pages or page <= max_pages):
            soup = self._fetch(url)
            for article in soup.select("article.product_pod"):
                title = article.h3.a["title"]
                price = float(article.select_one(".price_color").text[1:])
                rating = self._parse_rating(article.p)
                product_url = urljoin(url, article.h3.a["href"])
                image_url = urljoin(url, article.img["src"])
                category = self._extract_category(product_url)
                yield Book(title, price, rating, category, image_url, product_url)
            next_btn = soup.select_one("li.next > a")
            if next_btn:
                url = urljoin(url, next_btn["href"])
                page += 1
            else:
                break

    def close(self):
        self.session.close()

if __name__ == "__main__":
    scraper = BooksScraper()
    books = list(scraper.scrape_books(max_pages=1))
    for book in books:
        print(f"{book.title} - {book.price}€ - {book.rating}★")
    print(f"Total: {len(books)} livres scrapés")
    scraper.close()
