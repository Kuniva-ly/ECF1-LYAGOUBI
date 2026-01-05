-- 1) Aggregation simple: nombre de livres et prix moyen par categorie
SELECT
    category,
    COUNT(*) AS books_count,
    ROUND(AVG(price_eur), 2) AS avg_price_eur
FROM books
GROUP BY category
ORDER BY books_count DESC;

-- 2) Jointure: partenaires geocodes 
SELECT    
    p.nom_librairie,
    p.ville,
    p.code_postal,
    a.label AS api_label,
    a.score AS api_score
FROM partners p
JOIN api_addresses a
  ON p.code_postal = a.postcode
 AND p.ville = a.city
ORDER BY a.score DESC
LIMIT 20;

-- 3) Window function: classement des partenaires par CA dans chaque ville
SELECT
    nom_librairie,
    ville,
    ca_annuel,
    RANK() OVER (PARTITION BY ville ORDER BY ca_annuel DESC) AS rank_in_city
FROM partners
ORDER BY ville, rank_in_city;

-- 4) Top N: 5 livres les plus chers (en EUR)
-- on peut egalement utiliser FETCH FIRST 5 ROWS ONLY ou
-- nlargist(5) selon le SGBD
SELECT
    sku,
    title,
    price_eur,
    rating,
    category
FROM books
ORDER BY price_eur DESC
LIMIT 5;

-- 5) Croisement multi-sources: volumes par source (books, quotes, api, partners)
SELECT 'books' AS source, COUNT(*) AS rows_count FROM books
UNION ALL
SELECT 'quotes' AS source, COUNT(*) AS rows_count FROM quotes
UNION ALL
SELECT 'api_addresses' AS source, COUNT(*) AS rows_count FROM api_addresses
UNION ALL
SELECT 'partners' AS source, COUNT(*) AS rows_count FROM partners;




