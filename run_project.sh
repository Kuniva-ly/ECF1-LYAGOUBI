#!/usr/bin/env bash
set -euo pipefail

docker compose up -d

python -m src.pipeline --source books --pages 1
python -m src.pipeline --source quotes --pages 1
python -m src.pipeline --source api --query "Lille" --limit 3
python -m src.pipeline --source partners --partners-file data/partenaire_librairies.xlsx --geocode-partners
