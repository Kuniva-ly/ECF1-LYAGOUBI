# DataPulse Multi-Sources

## Demarrage rapide

1) Creer un environnement virtuel et installer les dependances :
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) Lancer l'infrastructure (PostgreSQL, MinIO, pgAdmin) :
```bash
docker compose up -d
```

3) Executer le pipeline ETL :
```bash
python -m src.pipeline --source books --pages 1 # Scraping livres 1 page
python -m src.pipeline --source quotes --pages 1 # Scraping citations 1 page
python -m src.pipeline --source api --query "Lille" --limit 3 # API Adresse - 3 resultats
python -m src.pipeline --source partners --partners-file data/partenaire_librairies.xlsx --geocode-partners 
```

Execution rapide sur PowerShell :
```powershell
.\run_project.ps1
```

Execution via Dockerfile :
```bash
docker build -t datapulse-pipeline . # Construire l'image Docker
docker run --rm --network host -v "$PWD/data:/app/data" datapulse-pipeline # Executer le conteneur Docker
```
```docker compose ps``` pour verifier les conteneurs en cours d'execution.

Note : placer le fichier `data/partenaire_librairies.xlsx` avant d'executer le pipeline partners.

## Acces pgAdmin

- URL : http://localhost:5050
- Email : admin@admin.com
- Mot de passe : admin123

Serveur PostgreSQL :
- Host : postgres 
- Port : 5432
- DB : tpdatabase
- User : tpuser
- Password : tppassword
