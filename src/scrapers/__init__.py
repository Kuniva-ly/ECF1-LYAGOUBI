"""Package des scrapers multi-sources."""

from .book_scraper import BooksScraper
from .quotes_scraper import QuotesScraper
from .api_adresse_scraper import APIAdresseScraper
# from .ecommerce_scraper import EcommerceScraper  # décommente si tu as ce scraper

__all__ = ["BooksScraper", "QuotesScraper", "APIAdresseScraper"]  # Ajoute EcommerceScraper si besoin
