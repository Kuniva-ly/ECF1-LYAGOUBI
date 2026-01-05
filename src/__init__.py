"""
Package principal du projet DataPulse Multi-Sources.

Ce package contient :
- scrapers : Collecte multi-sources (books, quotes, api adresse)
- storage : Clients de stockage (PostgreSQL, MinIO)
- pipeline : Pipeline ETL multi-sources
"""

from .pipeline import MultiSourcePipeline

__all__ = ["MultiSourcePipeline"]
