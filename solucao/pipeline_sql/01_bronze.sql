-- ============================================================================
-- BRONZE (SQL): cópia fiel dos arquivos que chegam no volume landing.
-- Sem limpeza. Cada linha guarda o arquivo de origem e o momento da ingestão.
-- Parâmetros do pipeline (Configuration): landing_path, sla_minutes, qb_catalog, qb_schema
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Pedidos crus (JSON Lines) como chegaram no landing'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingest_ts
FROM STREAM read_files(
  '${landing_path}/orders/',
  format => 'json',
  schema => 'order_id STRING, customer STRUCT<id: STRING, email: STRING, city: STRING>, restaurant_id STRING, courier_id STRING, items ARRAY<STRUCT<dish: STRING, qty: INT, price: STRING>>, status STRING, created_at STRING, delivered_at STRING'
);

CREATE OR REFRESH STREAMING TABLE bronze_ratings
COMMENT 'Avaliações cruas (CSV), tudo como texto'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingest_ts
FROM STREAM read_files(
  '${landing_path}/ratings/',
  format => 'csv',
  header => true,
  schema => 'order_id STRING, stars STRING, comment STRING'
);

CREATE OR REFRESH STREAMING TABLE bronze_restaurants
COMMENT 'Cadastro de restaurantes cru (CSV)'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingest_ts
FROM STREAM read_files(
  '${landing_path}/restaurants/',
  format => 'csv',
  header => true,
  schema => 'restaurant_id STRING, name STRING, city STRING, cuisine STRING'
);

CREATE OR REFRESH STREAMING TABLE bronze_couriers
COMMENT 'Cadastro de entregadores cru (CSV)'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingest_ts
FROM STREAM read_files(
  '${landing_path}/couriers/',
  format => 'csv',
  header => true,
  schema => 'courier_id STRING, name STRING, vehicle STRING'
);
