"""
Client PostgreSQL pour le stockage analytique.

Ce module fournit une interface simplifiée pour interagir avec PostgreSQL :
- Connexion centralisée
- Exécution de requêtes SQL
- Import/export de données
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from config import postgres_config
import structlog

logger = structlog.get_logger()

class PostgresClient:
    """
    Gestionnaire de connexion PostgreSQL.
    Utilise la configuration centralisée.
    """

    def __init__(self):
        self.conn = psycopg2.connect(
            host=postgres_config.host,
            port=postgres_config.port,
            user=postgres_config.user,
            password=postgres_config.password,
            dbname=postgres_config.database
        )
        self.conn.autocommit = True

    def execute(self, query, params=None):
        """
        Exécute une requête SQL (INSERT, UPDATE, DELETE, etc.).
        """
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            logger.info("query_executed", query=query)
    
    def fetchall(self, query, params=None):
        """
        Exécute une requête SELECT et retourne tous les résultats.
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            logger.info("query_fetched", query=query, rows=len(results))
            return results

    def fetchone(self, query, params=None):
        """
        Exécute une requête SELECT et retourne un seul résultat.
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            logger.info("query_fetched_one", query=query)
            return result

    def close(self):
        self.conn.close()
        logger.info("postgres_connection_closed")

# Test du module
if __name__ == "__main__":
    client = PostgresClient()
    client.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);")
    client.execute("INSERT INTO test_table (name) VALUES (%s);", ("Alice",))
    rows = client.fetchall("SELECT * FROM test_table;")
    print(rows)
    client.close()