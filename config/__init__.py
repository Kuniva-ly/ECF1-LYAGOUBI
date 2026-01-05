"""Package de configuration."""

from .settings import (
    minio_config,
    scraper_config,
    postgres_config,
    api_adresse_config,
)

__all__ = [
    "minio_config",
    "scraper_config",
    "postgres_config",
    "api_adresse_config",
]
