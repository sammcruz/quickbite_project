-- SILVER: limpa, tipada, sem duplicidade. Quarentenas e relatório de qualidade.
-- Requisitos: veja a seção 3.2 do notebook.

CREATE OR REFRESH MATERIALIZED VIEW quickbite.silver_restaurants AS
SELECT restaurant_id, name, city, cuisine
FROM quickbite.bronze_restaurants
QUALIFY row_number() OVER (PARTITION BY restaurant_id ORDER BY _ingest_ts DESC) = 1