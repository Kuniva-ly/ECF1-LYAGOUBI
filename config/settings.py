"""
Configuration centralisée du projet DataPipeline Multi-Sources.

Ce fichier contient toutes les configurations pour :
- MinIO (stockage objet)
- PostgreSQL (base de données analytique)
- Scraper (paramètres de scraping)
- API Adresse (paramètres d'appel)
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

@dataclass
class MinIOConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    secure: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"
    bucket_raw: str = "raw-data"
    bucket_processed: str = "processed-data"
    bucket_images: str = "product-images"
    bucket_exports: str = "data-exports"

@dataclass
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    user: str = os.getenv("POSTGRES_USER", "tpuser")
    password: str = os.getenv("POSTGRES_PASSWORD", "tppassword")
    database: str = os.getenv("POSTGRES_DB", "tpdatabase")

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class ScraperConfig:
    delay: float = 1.0
    timeout: int = 30
    max_retries: int = 3
    max_pages: int = 10
    books_url: str = os.getenv("BOOKS_BASE_URL", "https://books.toscrape.com/")
    quotes_url: str = os.getenv("QUOTES_BASE_URL", "https://quotes.toscrape.com/")
    ecommerce_url: str = os.getenv("ECOMMERCE_BASE_URL", "https://webscraper.io/test-sites/e-commerce/allinone")

@dataclass
class APIAdresseConfig:
    base_url: str = "https://api-adresse.data.gouv.fr/search/"
    rate_limit: int = 50  # requêtes/seconde

# Instances globales
minio_config = MinIOConfig()
postgres_config = PostgresConfig()
scraper_config = ScraperConfig()
api_adresse_config = APIAdresseConfig()

if __name__ == "__main__":
    print("=== Configuration MinIO ===")
    print(minio_config)
    print("\n=== Configuration PostgreSQL ===")
    print(postgres_config)
    print("\n=== Configuration Scraper ===")
    print(scraper_config)
    print("\n=== Configuration API Adresse ===")
    print(api_adresse_config)
