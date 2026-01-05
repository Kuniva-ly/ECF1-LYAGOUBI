"""
Pipeline ETL multi-sources (livres, citations, API adresse).

Ce module applique des transformations (normalisation, deduplication,
conversion de devises) et charge les donnees dans PostgreSQL.

Usage:
    python -m src.pipeline --source books --pages 3
    python -m src.pipeline --source quotes --pages 2
    python -m src.pipeline --source api --query "10 rue de la paix" --limit 5
    python -m src.pipeline --source all --pages 3 --query "Paris" --limit 5
"""

from datetime import datetime
from typing import Optional
import argparse
import hashlib
import sys
import os

from tqdm import tqdm
import structlog
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.scrapers import BooksScraper, QuotesScraper, APIAdresseScraper
from src.scrapers.book_scraper import Book
from src.scrapers.quotes_scraper import Quote
from src.scrapers.api_adresse_scraper import AdresseResult
from src.storage import PostgresClient, MinIOStorage

logger = structlog.get_logger()


class MultiSourcePipeline:
    """
    Pipeline ETL multi-sources (livres, citations, API adresse).
    """

    def __init__(self):
        self.books_scraper = None
        self.quotes_scraper = None
        self.api_scraper = None
        self.pg = None
        self.minio = None
        self.partners_columns = [
            "nom_librairie",
            "adresse",
            "code_postal",
            "ville",
            "contact_nom",
            "contact_email",
            "contact_telephone",
            "ca_annuel",
            "date_partenariat",
            "specialite",
        ]
        self.gbp_to_eur = float(os.getenv("GBP_EUR_RATE", "1.17"))
        self.max_price_gbp = float(os.getenv("MAX_BOOK_PRICE_GBP", "500"))
        self._seen_books = set()
        self._seen_quotes = set()
        self._seen_addresses = set()
        self._seen_partners = set()
        self.export_data = {
            "books": [],
            "quotes": [],
            "api": [],
            "partners": [],
        }
        self.stats = {
            "books_scraped": 0,
            "books_loaded": 0,
            "book_images_uploaded": 0,
            "quotes_scraped": 0,
            "quotes_loaded": 0,
            "api_addresses_scraped": 0,
            "api_addresses_loaded": 0,
            "partners_loaded": 0,
            "errors": [],
        }

    @staticmethod
    def _normalize_text(value: Optional[str]) -> str:
        if not value:
            return ""
        return " ".join(str(value).strip().split())

    @staticmethod
    def _normalize_tags(tags: list) -> list:
        clean = []
        for tag in tags or []:
            normalized = MultiSourcePipeline._normalize_text(tag).lower()
            if normalized:
                clean.append(normalized)
        return sorted(set(clean))

    @staticmethod
    def _hash_id(text: str) -> str:
        # ID stable pour deduplication sans exposer de donnees sensibles (RGPD).
        return hashlib.md5(text.encode()).hexdigest()[:12].upper()

    @staticmethod
    def _hash_pii(value: str) -> Optional[str]:
        # Pseudonymisation des donnees personnelles (RGPD).
        if not value:
            return None
        return hashlib.sha256(value.encode()).hexdigest()

    def _ensure_books(self) -> None:
        if not self.books_scraper:
            self.books_scraper = BooksScraper()

    def _ensure_quotes(self) -> None:
        if not self.quotes_scraper:
            self.quotes_scraper = QuotesScraper()

    def _ensure_api(self) -> None:
        if not self.api_scraper:
            self.api_scraper = APIAdresseScraper()

    def _ensure_pg(self) -> None:
        if not self.pg:
            self.pg = PostgresClient()
            self._init_schema()

    def _ensure_minio(self) -> None:
        if not self.minio:
            self.minio = MinIOStorage()

    def _init_schema(self) -> None:
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                sku TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                price_gbp NUMERIC(10, 2),
                price_eur NUMERIC(10, 2),
                rating INTEGER,
                category TEXT,
                image_url TEXT,
                minio_image_ref TEXT,
                product_url TEXT,
                scraped_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        self.pg.execute("ALTER TABLE books ADD COLUMN IF NOT EXISTS minio_image_ref TEXT;")
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                id TEXT PRIMARY KEY,
                text TEXT NOT NULL,
                author TEXT,
                tags TEXT[],
                text_normalized TEXT,
                tags_normalized TEXT[],
                scraped_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS api_addresses (
                id TEXT PRIMARY KEY,
                label TEXT,
                score NUMERIC(6, 4),
                type TEXT,
                city TEXT,
                postcode TEXT,
                context TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                query TEXT,
                scraped_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        self.pg.execute(
            """
            CREATE TABLE IF NOT EXISTS partners (
                id TEXT PRIMARY KEY,
                nom_librairie TEXT NOT NULL,
                adresse TEXT,
                code_postal TEXT,
                ville TEXT,
                contact_nom_hash TEXT,
                contact_email_hash TEXT,
                contact_telephone_hash TEXT,
                ca_annuel NUMERIC(14, 2),
                date_partenariat DATE,
                specialite TEXT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                scraped_at TIMESTAMP DEFAULT NOW()
            );
            """
        )
        self.pg.execute("CREATE INDEX IF NOT EXISTS idx_books_category ON books (category);")
        self.pg.execute("CREATE INDEX IF NOT EXISTS idx_quotes_author ON quotes (author);")
        self.pg.execute("CREATE INDEX IF NOT EXISTS idx_api_postcode ON api_addresses (postcode);")
        self.pg.execute("CREATE INDEX IF NOT EXISTS idx_partners_ville ON partners (ville);")
        self.pg.execute("CREATE INDEX IF NOT EXISTS idx_partners_postal ON partners (code_postal);")

    def _transform_book(self, product: "Book") -> Optional[dict]:
        price_gbp = float(product.price)
        if price_gbp <= 0 or price_gbp > self.max_price_gbp:
            return None
        rating = product.rating if 1 <= int(product.rating) <= 5 else None
        price_eur = round(price_gbp * self.gbp_to_eur, 2)
        return {
            "sku": product.sku,
            "title": self._normalize_text(product.title),
            "price_gbp": price_gbp,
            "price_eur": price_eur,
            "rating": rating,
            "category": self._normalize_text(product.category).lower() or None,
            "image_url": self._normalize_text(product.image_url),
            "product_url": self._normalize_text(product.product_url),
        }

    def _transform_quote(self, quote: "Quote") -> Optional[dict]:
        text_norm = self._normalize_text(quote.text)
        if not text_norm:
            return None
        tags_norm = self._normalize_tags(quote.tags)
        return {
            "id": quote.id,
            "text": quote.text,
            "author": self._normalize_text(quote.author),
            "tags": quote.tags,
            "text_normalized": text_norm,
            "tags_normalized": tags_norm,
        }

    def _transform_address(self, result: "AdresseResult", query: str) -> Optional[dict]:
        label = self._normalize_text(result.label)
        if not label:
            return None
        record_id = result.id or self._hash_id(f"{label}-{result.postcode}-{result.city}")
        return {
            "id": record_id,
            "label": label,
            "score": float(result.score),
            "type": self._normalize_text(result.type),
            "city": self._normalize_text(result.city or ""),
            "postcode": self._normalize_text(result.postcode or ""),
            "context": self._normalize_text(result.context or ""),
            "latitude": result.latitude,
            "longitude": result.longitude,
            "query": self._normalize_text(query),
        }

    def _transform_partner(self, row: dict, latitude: Optional[float], longitude: Optional[float]) -> Optional[dict]:
        name = self._normalize_text(row.get("nom_librairie", ""))
        if not name:
            return None
        adresse = self._normalize_text(row.get("adresse", ""))
        raw_postal = row.get("code_postal", "")
        code_postal = "" if pd.isna(raw_postal) else self._normalize_text(str(raw_postal))
        ville = self._normalize_text(row.get("ville", ""))
        ca_annuel = row.get("ca_annuel")
        ca_annuel = None if pd.isna(ca_annuel) else float(ca_annuel)
        record_id = self._hash_id(f"{name}-{adresse}-{code_postal}-{ville}")
        return {
            "id": record_id,
            "nom_librairie": name,
            "adresse": adresse,
            "code_postal": code_postal,
            "ville": ville,
            "contact_nom_hash": self._hash_pii(self._normalize_text(row.get("contact_nom", ""))),
            "contact_email_hash": self._hash_pii(self._normalize_text(row.get("contact_email", ""))),
            "contact_telephone_hash": self._hash_pii(self._normalize_text(row.get("contact_telephone", ""))),
            "ca_annuel": ca_annuel,
            "date_partenariat": row.get("date_partenariat"),
            "specialite": self._normalize_text(row.get("specialite", "")),
            "latitude": latitude,
            "longitude": longitude,
        }

    def _load_book(self, data: dict) -> None:
        self.pg.execute(
            """
            INSERT INTO books (
                sku, title, price_gbp, price_eur, rating, category, image_url, minio_image_ref, product_url
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sku) DO UPDATE SET
                title = EXCLUDED.title,
                price_gbp = EXCLUDED.price_gbp,
                price_eur = EXCLUDED.price_eur,
                rating = EXCLUDED.rating,
                category = EXCLUDED.category,
                image_url = EXCLUDED.image_url,
                minio_image_ref = EXCLUDED.minio_image_ref,
                product_url = EXCLUDED.product_url;
            """,
            (
                data["sku"],
                data["title"],
                data["price_gbp"],
                data["price_eur"],
                data["rating"],
                data["category"],
                data["image_url"],
                data.get("minio_image_ref"),
                data["product_url"],
            ),
        )

    def _load_quote(self, data: dict) -> None:
        self.pg.execute(
            """
            INSERT INTO quotes (
                id, text, author, tags, text_normalized, tags_normalized
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                text = EXCLUDED.text,
                author = EXCLUDED.author,
                tags = EXCLUDED.tags,
                text_normalized = EXCLUDED.text_normalized,
                tags_normalized = EXCLUDED.tags_normalized;
            """,
            (
                data["id"],
                data["text"],
                data["author"],
                data["tags"],
                data["text_normalized"],
                data["tags_normalized"],
            ),
        )

    def _load_address(self, data: dict) -> None:
        self.pg.execute(
            """
            INSERT INTO api_addresses (
                id, label, score, type, city, postcode, context, latitude, longitude, query
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                label = EXCLUDED.label,
                score = EXCLUDED.score,
                type = EXCLUDED.type,
                city = EXCLUDED.city,
                postcode = EXCLUDED.postcode,
                context = EXCLUDED.context,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                query = EXCLUDED.query;
            """,
            (
                data["id"],
                data["label"],
                data["score"],
                data["type"],
                data["city"],
                data["postcode"],
                data["context"],
                data["latitude"],
                data["longitude"],
                data["query"],
            ),
        )

    def _load_partner(self, data: dict) -> None:
        self.pg.execute(
            """
            INSERT INTO partners (
                id, nom_librairie, adresse, code_postal, ville,
                contact_nom_hash, contact_email_hash, contact_telephone_hash,
                ca_annuel, date_partenariat, specialite, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                nom_librairie = EXCLUDED.nom_librairie,
                adresse = EXCLUDED.adresse,
                code_postal = EXCLUDED.code_postal,
                ville = EXCLUDED.ville,
                contact_nom_hash = EXCLUDED.contact_nom_hash,
                contact_email_hash = EXCLUDED.contact_email_hash,
                contact_telephone_hash = EXCLUDED.contact_telephone_hash,
                ca_annuel = EXCLUDED.ca_annuel,
                date_partenariat = EXCLUDED.date_partenariat,
                specialite = EXCLUDED.specialite,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude;
            """,
            (
                data["id"],
                data["nom_librairie"],
                data["adresse"],
                data["code_postal"],
                data["ville"],
                data["contact_nom_hash"],
                data["contact_email_hash"],
                data["contact_telephone_hash"],
                data["ca_annuel"],
                data["date_partenariat"],
                data["specialite"],
                data["latitude"],
                data["longitude"],
            ),
        )

    def _download_image(self, url: str) -> Optional[bytes]:
        if not url:
            return None
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.warning("image_download_failed", url=url, error=str(e))
            return None

    def _generate_image_path(self, product: "Book") -> str:
        category = self._normalize_text(product.category).lower() or "other"
        return f"{category}/{product.sku}.jpg"

    def _run_books(self, max_pages: int, show_progress: bool, load_sql: bool) -> None:
        self._ensure_books()
        if load_sql:
            self._ensure_pg()
        books = list(self.books_scraper.scrape_books(max_pages=max_pages))
        iterator = tqdm(books, desc="Processing books", unit="book") if show_progress else books
        for book in iterator:
            if book.sku in self._seen_books:
                continue
            self._seen_books.add(book.sku)
            data = self._transform_book(book)
            if not data:
                continue
            if self.minio:
                image_data = self._download_image(book.image_url)
                if image_data:
                    object_name = self._generate_image_path(book)
                    image_ref = self.minio.upload_image(image_data, object_name)
                    if image_ref:
                        data["minio_image_ref"] = image_ref
                        self.stats["book_images_uploaded"] += 1
            self.stats["books_scraped"] += 1
            if load_sql:
                self._load_book(data)
                self.stats["books_loaded"] += 1
            self.export_data["books"].append(data)

    def _run_quotes(self, max_pages: int, show_progress: bool, load_sql: bool) -> None:
        self._ensure_quotes()
        if load_sql:
            self._ensure_pg()
        quotes = list(self.quotes_scraper.scrape_quotes(max_pages=max_pages))
        iterator = tqdm(quotes, desc="Processing quotes", unit="quote") if show_progress else quotes
        for quote in iterator:
            if quote.id in self._seen_quotes:
                continue
            self._seen_quotes.add(quote.id)
            data = self._transform_quote(quote)
            if not data:
                continue
            self.stats["quotes_scraped"] += 1
            if load_sql:
                self._load_quote(data)
                self.stats["quotes_loaded"] += 1
            self.export_data["quotes"].append(data)

    def _run_api(self, query: str, limit: int, show_progress: bool, load_sql: bool) -> None:
        self._ensure_api()
        if load_sql:
            self._ensure_pg()
        results = list(self.api_scraper.search(query=query, limit=limit))
        iterator = (
            tqdm(results, desc="Processing api results", unit="result")
            if show_progress
            else results
        )
        for result in iterator:
            data = self._transform_address(result, query)
            if not data:
                continue
            if data["id"] in self._seen_addresses:
                continue
            self._seen_addresses.add(data["id"])
            self.stats["api_addresses_scraped"] += 1
            if load_sql:
                self._load_address(data)
                self.stats["api_addresses_loaded"] += 1
            self.export_data["api"].append(data)

    def _run_partners(self, filepath: str, geocode: bool, show_progress: bool, load_sql: bool) -> None:
        if not filepath or not os.path.exists(filepath):
            logger.warning("partners_file_missing", filepath=filepath)
            return
        if load_sql:
            self._ensure_pg()
        df = pd.read_excel(filepath)
        missing = [col for col in self.partners_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns in partners file: {missing}")

        df = df[self.partners_columns].copy()
        df["date_partenariat"] = pd.to_datetime(df["date_partenariat"], errors="coerce").dt.date
        rows = df.to_dict(orient="records")
        iterator = tqdm(rows, desc="Processing partners", unit="row") if show_progress else rows

        if geocode:
            self._ensure_api()

        for row in iterator:
            latitude = None
            longitude = None
            if geocode:
                query_parts = [
                    row.get("adresse", ""),
                    "" if pd.isna(row.get("code_postal")) else row.get("code_postal", ""),
                    row.get("ville", ""),
                ]
                query = " ".join([str(p) for p in query_parts if p])
                if query:
                    results = list(self.api_scraper.search(query=query, limit=1))
                    if results:
                        latitude = results[0].latitude
                        longitude = results[0].longitude
            data = self._transform_partner(row, latitude, longitude)
            if not data:
                continue
            if data["id"] in self._seen_partners:
                continue
            self._seen_partners.add(data["id"])
            if load_sql:
                self._load_partner(data)
                self.stats["partners_loaded"] += 1
            self.export_data["partners"].append(data)

    def _export_to_minio(self, export_prefix: str = "export") -> None:
        if not self.minio:
            return
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        for key, records in self.export_data.items():
            if not records:
                continue
            df = pd.DataFrame(records)
            csv_name = f"{export_prefix}_{key}_{timestamp}.csv"
            json_name = f"{export_prefix}_{key}_{timestamp}.json"
            self.minio.upload_csv(df.to_csv(index=False), csv_name)
            self.minio.upload_json(records, json_name)

    def run(
        self,
        source: str = "books",
        max_pages: int = 5,
        show_progress: bool = True,
        query: Optional[str] = None,
        limit: int = 5,
        load_sql: bool = True,
        minio_enabled: bool = True,
        export_minio: bool = True,
        partners_file: Optional[str] = None,
        geocode_partners: bool = False,
    ) -> dict:
        start_time = datetime.utcnow()
        sources = ["books", "quotes", "api", "partners"] if source == "all" else [source]

        if "api" in sources and not query:
            raise ValueError("API source requires --query.")

        logger.info("pipeline_started", source=source, max_pages=max_pages, load_sql=load_sql)

        try:
            if minio_enabled:
                self._ensure_minio()
            if "books" in sources:
                self._run_books(max_pages, show_progress, load_sql)
            if "quotes" in sources:
                self._run_quotes(max_pages, show_progress, load_sql)
            if "api" in sources:
                self._run_api(query=query, limit=limit, show_progress=show_progress, load_sql=load_sql)
            if "partners" in sources:
                self._run_partners(
                    filepath=partners_file,
                    geocode=geocode_partners,
                    show_progress=show_progress,
                    load_sql=load_sql,
                )
            if export_minio and minio_enabled:
                self._export_to_minio()
        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            self.stats["errors"].append(str(e))
        finally:
            end_time = datetime.utcnow()
            self.stats["duration_seconds"] = (end_time - start_time).total_seconds()
            self.stats["start_time"] = start_time.isoformat()
            self.stats["end_time"] = end_time.isoformat()

        logger.info("pipeline_completed", **self.stats)
        return self.stats

    def close(self) -> None:
        if self.books_scraper:
            self.books_scraper.close()
        if self.quotes_scraper:
            self.quotes_scraper.close()
        if self.api_scraper:
            self.api_scraper.close()
        if self.pg:
            self.pg.close()
        logger.info("pipeline_closed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline multi-sources (books, quotes, api adresse)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python -m src.pipeline --source books --pages 3
  python -m src.pipeline --source quotes --pages 2
  python -m src.pipeline --source api --query "Paris" --limit 5
  python -m src.pipeline --source all --pages 3 --query "Paris" --limit 5
        """,
    )

    parser.add_argument(
        "--source",
        choices=["books", "quotes", "api", "partners", "all"],
        default="books",
        help="Source a executer (defaut: books)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Nombre de pages a scraper (defaut: 3)",
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Requete pour l'API adresse",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Nombre de resultats pour l'API adresse (defaut: 5)",
    )
    parser.add_argument(
        "--partners-file",
        type=str,
        default="data/partenaire_librairies.xlsx",
        help="Chemin du fichier Excel des partenaires",
    )
    parser.add_argument(
        "--geocode-partners",
        action="store_true",
        help="Geocoder les adresses partenaires via l'API Adresse",
    )
    parser.add_argument(
        "--no-sql",
        action="store_true",
        help="Ne pas charger dans PostgreSQL",
    )
    parser.add_argument(
        "--no-minio",
        action="store_true",
        help="Ne pas utiliser MinIO (images et exports)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Mode silencieux (pas de barre de progression)",
    )

    args = parser.parse_args()

    pipeline = MultiSourcePipeline()
    try:
        print("\nDemarrage du pipeline de scraping...")
        stats = pipeline.run(
            source=args.source,
            max_pages=args.pages,
            show_progress=not args.quiet,
            query=args.query,
            limit=args.limit,
            load_sql=not args.no_sql,
            minio_enabled=not args.no_minio,
            export_minio=not args.no_minio,
            partners_file=args.partners_file,
            geocode_partners=args.geocode_partners,
        )
        print("\n" + "=" * 50)
        print("PIPELINE TERMINE")
        print("=" * 50)
        print(f"   Livres scrapes      : {stats['books_scraped']}")
        print(f"   Livres charges      : {stats['books_loaded']}")
        print(f"   Images MinIO        : {stats['book_images_uploaded']}")
        print(f"   Citations scrapees  : {stats['quotes_scraped']}")
        print(f"   Citations chargees  : {stats['quotes_loaded']}")
        print(f"   API adresses        : {stats['api_addresses_scraped']}")
        print(f"   API adresses charge : {stats['api_addresses_loaded']}")
        print(f"   Partenaires charges : {stats['partners_loaded']}")
        print(f"   Duree               : {stats['duration_seconds']:.2f}s")
        print(f"   Erreurs             : {len(stats['errors'])}")
        if stats["errors"]:
            print("\nErreurs rencontrees:")
            for error in stats["errors"][:5]:
                print(f"   - {error}")
    except KeyboardInterrupt:
        print("\nInterruption par l'utilisateur")
    except Exception as e:
        print(f"\nErreur: {str(e)}")
        raise
    finally:
        pipeline.close()
        print("\nPipeline ferme.")


if __name__ == "__main__":
    main()
