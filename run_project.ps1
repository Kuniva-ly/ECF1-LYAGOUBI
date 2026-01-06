docker compose up -d

python -m src.pipeline --source books --pages 1 # Scraping livres 1 page
python -m src.pipeline --source quotes --pages 1 # Scraping citations 1 page
python -m src.pipeline --source api --query "Lille" --limit 3 # API Adresse - 3 resultats
python -m src.pipeline --source partners --partners-file data/partenaire_librairies.xlsx --geocode-partners 
