# DAT - Dossier d'Architecture Technique

## 1) Choix d'architecture globale

Architecture retenue : hybride Data Lake + Data Warehouse.

Pourquoi ce choix :
- Data Lake (MinIO) pour stocker les fichiers (images, exports) et garder une trace des donnees.
- Data Warehouse (PostgreSQL) pour les donnees structurees et les requetes analytiques SQL.

Avantages :
- Separation entre fichiers et donnees analytiques.
- Requetes SQL rapides sur la partie analytique.
- Stockage objet evolutif pour les fichiers.

Inconvenients :
- Deux stockages a administrer.
- Necessite d'orchestration ETL pour synchroniser les couches.

## 2) Choix des technologies

Stockage donnees brutes et fichiers :
- MinIO (S3 compatible) pour images + exports.
- Alternative : AWS S3 (payant, plus scalable).

Transformations :
- Python (pandas + requests) pour nettoyage et enrichissement.
- Alternative : Apache Spark (utile a grande echelle).

Stockage analytique :
- PostgreSQL pour les tables finales et jointures.
- Alternative : DuckDB (local) ou BigQuery (cloud).

Orchestration :
- Docker Compose pour demarrer l'infrastructure.
- Alternative : Kubernetes (plus complexe, production).

## 3) Organisation des donnees

Convention de nommage :
- Tables : `books`, `quotes`, `api_addresses`, `partners`.
- Colonnes explicites (ex: `price_eur`, `minio_image_ref`).

Couches :
- Fichiers : MinIO (images, exports CSV/JSON).
- Analytique : PostgreSQL (donnees transformees).

## 4) Modelisation des donnees

Tables principales :
- `books` : livres (prix GBP/EUR, rating, categorie).
- `quotes` : citations (texte, auteur, tags).
- `api_addresses` : resultats API adresse (label, lat, lon).
- `partners` : partenaires (donnees nettoyees + champs perso hash).

Cle :
- `books.sku` : identifiant livre.
- `quotes.id` : identifiant citation.
- `api_addresses.id` : identifiant resultat.
- `partners.id` : hash stable du partenaire.

Relation :
- Lien indirect entre `partners` et `api_addresses` via code_postal/ville.
- Lien vers MinIO via `books.minio_image_ref`.

## 5) Conformite RGPD

Donnees personnelles :
- contact_nom, contact_email, contact_telephone (fichier partenaires).

Mesures :
- Pseudonymisation par hash SHA-256.
- Acces restreints via roles SQL.
- Donnees sensibles non exposees en clair.

Suppression :
- Suppression de la ligne partenaire dans PostgreSQL.
- Suppression des exports associes dans MinIO si necessaire.
