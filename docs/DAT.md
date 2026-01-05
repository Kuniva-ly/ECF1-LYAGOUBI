# ECF-1
**1. Type d’architecture**
- Pour cet ECF on va utilisé une architecture hybride (Data Lake + Data Warehouse).
### Pourquoi ce choix:
- Permet de stocker les données brutes et les transformer pour l'analyse.
### Avantages:
- Scalabilité 
#### Faible coût pour le stockage
- Séparation entre données brutes et données nettoyées
### inconvénients:
- Gestion des accées et des transformations

### Collecte multi-source:
- Web scraping,
- API REST,
- Fichier Excel

**2. Choix des technologies**
### Stockage :
- MinIO DataLake : Elle est adaptée à des données multi-sources hétérogènes et répond aux exigences de traçabilité, nettoyage et analyse (Bronze, Silver, Gold)
#### Altérnative:
- Amazon S3 : payant pour le stockage et pour la transformation 

### transformation:
- Les transformations sont réalisées en Python  afin de :
    nettoyer et normaliser les données,
    préparer les jeux de données pour le stockage analytique
#### Altérnative:
- Apache Spark: Adapté pour les Big Data

### Stockage final:
- PostgreSQL : Base relationnelle robuste, facilité de jointures pour analyses croisées. 
#### Altérnative:
- DuckDB si pipeline local et pour les petite dataset local
        
### Orchestration: 
- Docker : Docker pour conteneuriser tous les services,
- Facilité de déploiement.
#### Altérnative:
Kubernetes si auto healing 

## RGPD:
### Données personelles identifié:
- Gestion des droits d'accès
### Mesure de protection:
- séparation des environnements,
- limitation des accès aux données sensibles.
### Gestion du droit à l'effacement:
- Suppression des lignes concernées dans la table “contacts” sur demande.

```
[Web/API/Excel] 
      │
      ▼
 [Python ETL]
      │
      ▼
   [MinIO]
      │
      ▼
 [Transformation]
      │
      ▼
 [PostgreSQL] ←→ [pgAdmin]
```