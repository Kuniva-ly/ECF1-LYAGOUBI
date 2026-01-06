-- a executer avec tpuser
DROP ROLE IF EXISTS user1;
CREATE ROLE user1 LOGIN PASSWORD 'user1';
GRANT CONNECT ON DATABASE tpdatabase TO user1;
GRANT USAGE ON SCHEMA public TO user1;

REVOKE ALL ON books FROM user1;
REVOKE ALL ON partners FROM user1;

-- Books:pour bloquer SELECT *
GRANT SELECT (sku, title, price_gbp, price_eur, rating, category, image_url, minio_image_ref, product_url)
ON books TO user1;

-- Partners: toutes les colonnes sauf ca_annuel
GRANT SELECT (id, nom_librairie, adresse, code_postal, ville,
              contact_nom_hash, contact_email_hash, contact_telephone_hash,
              date_partenariat, specialite, latitude, longitude, scraped_at)
ON partners TO user1;

-- Requetes qui doivent echouer avec user1
SELECT ca_annuel FROM partners LIMIT 5;
SELECT * FROM books LIMIT 5;
SELECT * FROM partners LIMIT 5;

-- Requetes qui doivent fonctionner avec user1
SELECT title, category, price_eur FROM books LIMIT 5;
SELECT nom_librairie, ville FROM partners LIMIT 5;
