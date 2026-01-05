-- a executer avec tpuser
CREATE ROLE admin LOGIN PASSWORD 'admin';
GRANT CONNECT ON DATABASE tpdatabase TO admin;
GRANT USAGE ON SCHEMA public TO admin;

REVOKE ALL ON books FROM admin;
REVOKE ALL ON partners FROM admin;

-- Books: prix inclus, sans scraped_at pour bloquer SELECT *
GRANT SELECT (sku, title, price_gbp, price_eur, rating, category, image_url, minio_image_ref, product_url)
ON books TO admin;

-- Partners: toutes les colonnes sauf ca_annuel
GRANT SELECT (id, nom_librairie, adresse, code_postal, ville,
              contact_nom_hash, contact_email_hash, contact_telephone_hash,
              date_partenariat, specialite, latitude, longitude, scraped_at)
ON partners TO admin;

-- Requetes qui doivent echouer avec admin
SELECT ca_annuel FROM partners LIMIT 5;
SELECT * FROM books LIMIT 5;
SELECT * FROM partners LIMIT 5;

-- Requetes qui doivent fonctionner avec admin
SELECT title, category, price_eur FROM books LIMIT 5;
SELECT nom_librairie, ville FROM partners LIMIT 5;
