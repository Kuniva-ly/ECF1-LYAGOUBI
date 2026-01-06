"""
Client MinIO pour le stockage d'objets.

Ce module fournit une interface simplifiée pour interagir avec MinIO :
- Upload/download d'images
- Export de fichiers (CSV, JSON, Parquet)
- Gestion des backups
- Génération d'URLs présignées

MinIO est un serveur de stockage objet compatible S3.
"""

import io
import json
from datetime import datetime, timedelta
from typing import Optional
from minio import Minio
from minio.error import S3Error
import structlog

# Import de la configuration
from config import minio_config

logger = structlog.get_logger()


class MinIOStorage:
    """
    Gestionnaire de stockage MinIO.
    
    Cette classe encapsule toutes les opérations de stockage objet :
    - Gestion des buckets
    - Upload/download de fichiers
    - Génération d'URLs temporaires
    
    Attributes:
        client: Client MinIO
    
    Example:
        >>> storage = MinIOStorage()
        >>> storage.upload_image(image_bytes, "products/laptop.jpg")
        'minio://product-images/products/laptop.jpg'
    """
    
    def __init__(self):
        """Initialise le client MinIO et crée les buckets nécessaires."""
        self.client = Minio(
            endpoint=minio_config.endpoint,
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
            secure=minio_config.secure  # IMPORTANT: False pour HTTP local
        )
        self._ensure_buckets()
    
    def _ensure_buckets(self) -> None:
        """
        Crée les buckets s'ils n'existent pas.
        
        Buckets créés :
        - product-images : Images des produits
        - data-exports : Exports CSV, JSON, Parquet
        """
        buckets = [
            minio_config.bucket_bronze,
            minio_config.bucket_silver,
            minio_config.bucket_gold,
        ]
        
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info("bucket_created", bucket=bucket)
            except S3Error as e:
                logger.error("bucket_creation_failed", bucket=bucket, error=str(e))
    
    # ==================== IMAGES ====================
    
    def upload_image(
        self,
        image_data: bytes,
        object_name: str,
        content_type: str = "image/jpeg"
    ) -> Optional[str]:
        """
        Upload une image dans le bucket bronze.
        
        Args:
            image_data: Contenu binaire de l'image
            object_name: Chemin dans le bucket (ex: "laptops/prod_123.jpg")
            content_type: Type MIME de l'image
            
        Returns:
            URI MinIO (ex: "minio://product-images/laptops/prod_123.jpg")
            None en cas d'erreur
            
        Example:
            >>> with open("image.jpg", "rb") as f:
            ...     uri = storage.upload_image(f.read(), "category/image.jpg")
        """
        try:
            self.client.put_object(
                bucket_name=minio_config.bucket_bronze,
                object_name=object_name,
                data=io.BytesIO(image_data),
                length=len(image_data),
                content_type=content_type
            )
            
            uri = f"minio://{minio_config.bucket_bronze}/{object_name}"
            logger.info("image_uploaded", 
                       object_name=object_name, 
                       size_kb=len(image_data) // 1024)
            return uri
            
        except S3Error as e:
            logger.error("image_upload_failed", object_name=object_name, error=str(e))
            return None
    
    def get_image(self, object_name: str) -> Optional[bytes]:
        """
        Télécharge une image depuis MinIO.
        
        Args:
            object_name: Chemin de l'objet dans le bucket
            
        Returns:
            Contenu binaire de l'image ou None
        """
        try:
            response = self.client.get_object(
                bucket_name=minio_config.bucket_bronze,
                object_name=object_name
            )
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error("image_download_failed", object_name=object_name, error=str(e))
            return None
    
    def delete_image(self, object_name: str) -> bool:
        """Supprime une image."""
        try:
            self.client.remove_object(minio_config.bucket_bronze, object_name)
            logger.info("image_deleted", object_name=object_name)
            return True
        except S3Error as e:
            logger.error("image_delete_failed", object_name=object_name, error=str(e))
            return False
    
    def list_images(self, prefix: str = "") -> list[dict]:
        """
        Liste les images dans le bucket.
        
        Args:
            prefix: Filtre par préfixe (ex: "laptops/")
            
        Returns:
            Liste de dictionnaires avec name, size, modified
        """
        try:
            objects = self.client.list_objects(
                bucket_name=minio_config.bucket_bronze,
                prefix=prefix,
                recursive=True
            )
            return [
                {
                    "name": obj.object_name,
                    "size": obj.size,
                    "modified": obj.last_modified
                }
                for obj in objects
            ]
        except S3Error as e:
            logger.error("list_images_failed", error=str(e))
            return []
    
    # ==================== EXPORTS ====================
    
    def upload_export(
        self,
        data: bytes,
        filename: str,
        content_type: str = "application/octet-stream",
        bucket_name: Optional[str] = None,
    ) -> Optional[str]:
        """
        Upload un fichier d'export (CSV, JSON, Parquet).
        
        Args:
            data: Contenu du fichier
            filename: Nom du fichier
            content_type: Type MIME
            
        Returns:
            URI MinIO ou None
        """
        try:
            bucket_name = bucket_name or minio_config.bucket_silver
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=filename,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type
            )
            
            uri = f"minio://{bucket_name}/{filename}"
            logger.info("export_uploaded", filename=filename, size_kb=len(data) // 1024)
            return uri
            
        except S3Error as e:
            logger.error("export_upload_failed", filename=filename, error=str(e))
            return None
    
    def upload_csv(self, csv_content: str, filename: str, bucket_name: Optional[str] = None) -> Optional[str]:
        """Upload un fichier CSV."""
        return self.upload_export(
            csv_content.encode("utf-8"),
            filename,
            "text/csv",
            bucket_name
        )
    
    def upload_json(self, data: dict, filename: str, bucket_name: Optional[str] = None) -> Optional[str]:
        """Upload un fichier JSON."""
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        return self.upload_export(json_bytes, filename, "application/json", bucket_name)
    
    def get_export(self, filename: str) -> Optional[bytes]:
        """Télécharge un fichier d'export."""
        try:
            response = self.client.get_object(minio_config.bucket_exports, filename)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error:
            return None
    
    def list_exports(self) -> list[dict]:
        """Liste tous les exports (silver)."""
        try:
            objects = self.client.list_objects(minio_config.bucket_silver, recursive=True)
            return [
                {"name": obj.object_name, "size": obj.size, "modified": obj.last_modified}
                for obj in objects
            ]
        except S3Error:
            return []
    
    # ==================== URLS PRÉSIGNÉES ====================
    
    def get_presigned_url(
        self,
        object_name: str,
        bucket: str = None,
        expires_hours: int = 24
    ) -> Optional[str]:
        """
        Génère une URL temporaire pour accès direct.
        
        Args:
            object_name: Chemin de l'objet
            bucket: Nom du bucket (défaut: images)
            expires_hours: Durée de validité en heures
            
        Returns:
            URL présignée ou None
        """
        bucket = bucket or minio_config.bucket_bronze
        
        try:
            url = self.client.presigned_get_object(
                bucket_name=bucket,
                object_name=object_name,
                expires=timedelta(hours=expires_hours)
            )
            return url
        except S3Error as e:
            logger.error("presigned_url_failed", object_name=object_name, error=str(e))
            return None
    
    # ==================== STATISTIQUES ====================
    
    def get_stats(self) -> dict:
        """
        Retourne des statistiques sur le stockage.
        
        Returns:
            Dictionnaire avec les stats de chaque bucket
        """
        stats = {}
        
        for bucket_name in [
            minio_config.bucket_bronze,
            minio_config.bucket_silver,
            minio_config.bucket_gold,
        ]:
            try:
                objects = list(self.client.list_objects(bucket_name, recursive=True))
                stats[bucket_name] = {
                    "count": len(objects),
                    "total_size_mb": sum(o.size for o in objects) / (1024 * 1024)
                }
            except S3Error:
                stats[bucket_name] = {"count": 0, "total_size_mb": 0}
        
        return stats
    
    def get_images_by_category(self) -> dict:
        """
        Groupe les images par catégorie (premier niveau du chemin).
        
        Returns:
            {category: {"count": N, "size_kb": M}}
        """
        images = self.list_images()
        categories = {}
        
        for img in images:
            # Extraire la catégorie du chemin
            parts = img["name"].split("/")
            category = parts[0] if len(parts) > 1 else "root"
            
            if category not in categories:
                categories[category] = {"count": 0, "size_kb": 0}
            
            categories[category]["count"] += 1
            categories[category]["size_kb"] += img["size"] / 1024
        
        return categories


# Test du module
if __name__ == "__main__":
    print("Test du client MinIO...")
    
    storage = MinIOStorage()
    
    # Test upload
    test_data = b"Hello MinIO!"
    uri = storage.upload_export(test_data, "test.txt", "text/plain")
    print(f"Upload test: {uri}")
    
    # Test stats
    stats = storage.get_stats()
    print(f"Stats: {stats}")
    
    print("Tests terminés!")
