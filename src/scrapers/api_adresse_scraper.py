"""
Scraper pour l'API adresse (api-adresse.data.gouv.fr).

Fonctionnalites :
- Recherche d'adresses par requete
- Rate limiting simple
"""

import time
from dataclasses import dataclass
from typing import Generator, Optional

import requests

from config import api_adresse_config


@dataclass
class AdresseResult:
    id: str
    label: str
    score: float
    type: str
    city: Optional[str]
    postcode: Optional[str]
    context: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "label": self.label,
            "score": self.score,
            "type": self.type,
            "city": self.city,
            "postcode": self.postcode,
            "context": self.context,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


class APIAdresseScraper:
    def __init__(self):
        self.base_url = api_adresse_config.base_url
        self.delay = 1.0 / max(api_adresse_config.rate_limit, 1)
        self.session = requests.Session()

    def search(self, query: str, limit: int = 5) -> Generator[AdresseResult, None, None]:
        params = {"q": query, "limit": limit}
        response = self.session.get(self.base_url, params=params, timeout=10)
        response.raise_for_status()
        time.sleep(self.delay)

        data = response.json()
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [None, None])
            yield AdresseResult(
                id=props.get("id", ""),
                label=props.get("label", ""),
                score=float(props.get("score", 0.0)),
                type=props.get("type", ""),
                city=props.get("city"),
                postcode=props.get("postcode"),
                context=props.get("context"),
                latitude=coords[1],
                longitude=coords[0],
            )

    def close(self) -> None:
        self.session.close()
