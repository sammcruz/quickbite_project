-- ============================================================================
-- SILVER (SQL): limpa, tipada e sem duplicidade.
-- Pedidos e avaliações: streaming tables (processam só o que chegou) + AUTO CDC para deduplicar.
-- Quarentenas, itens, dimensões e relatório: materialized views (recalculam, então
-- avaliações que chegam antes do pedido são reavaliadas sozinhas).
-- ============================================================================

-- ---------- pedidos: tipagem e regras (expectations descartam o que não presta)
CREATE OR REFRESH STREAMING TABLE silver_orders_clean (
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT not_test_account  EXPECT (customer_email NOT LIKE '%@quickbite.test') ON VIOLATION DROP ROW,
  CONSTRAINT valid_status      EXPECT (status IN ('delivered', 'cancelled')) ON VIOLATION DROP ROW,
  CONSTRAINT has_items         EXPECT (items_count > 0),
  CONSTRAINT positive_total    EXPECT (order_total > 0)
)
COMMENT 'Pedidos tipados e padronizados (ainda pode haver duplicados entre arquivos)'
AS SELECT
  *,
  size(items) AS items_count,
  aggregate(items, 0, (a, i) -> a + i.quantity) AS total_qty,
  aggregate(items, CAST(0 AS DECIMAL(12,2)), (a, i) -> CAST(a + i.quantity * i.unit_price AS DECIMAL(12,2))) AS order_total
FROM (
  SELECT
    trim(order_id) AS order_id,
    customer.id AS customer_id,
    lower(trim(customer.email)) AS customer_email,
    initcap(trim(customer.city)) AS delivery_city,
    restaurant_id,
    courier_id,
    status,
    to_timestamp(created_at) AS created_at,
    to_timestamp(delivered_at) AS delivered_at,
    CAST(to_timestamp(created_at) AS DATE) AS order_date,
    hour(to_timestamp(created_at)) AS order_hour,
    CASE WHEN hour(to_timestamp(created_at)) BETWEEN 11 AND 15 THEN 'lunch'
         WHEN hour(to_timestamp(created_at)) BETWEEN 18 AND 23 THEN 'dinner'
         ELSE 'other' END AS meal_period,
    CASE WHEN status = 'delivered'
         THEN CAST(round((unix_timestamp(to_timestamp(delivered_at)) - unix_timestamp(to_timestamp(created_at))) / 60.0, 1) AS DOUBLE) END AS delivery_minutes,
    transform(items, i -> named_struct(
      'dish', trim(i.dish),
      'quantity', CAST(i.qty AS INT),
      'unit_price', CAST(replace(regexp_replace(i.price, '[^0-9,.]', ''), ',', '.') AS DECIMAL(10,2))
    )) AS items,
    _source_file,
    _ingest_ts
  FROM STREAM(bronze_orders)
);

-- ---------- pedidos: um por order_id (o reenvio de um arquivo não duplica)
CREATE OR REFRESH STREAMING TABLE silver_orders
COMMENT 'Pedidos limpos, um por order_id, sem contas de teste';

CREATE FLOW silver_orders_dedup AS AUTO CDC INTO silver_orders
FROM STREAM(silver_orders_clean)
KEYS (order_id)
SEQUENCE BY struct(_source_file, _ingest_ts)
STORED AS SCD TYPE 1;

-- ---------- avaliações
CREATE OR REFRESH STREAMING TABLE silver_ratings_clean (
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT stars_in_range    EXPECT (stars BETWEEN 1 AND 5) ON VIOLATION DROP ROW
)
COMMENT 'Avaliações tipadas. Nota fora de 1 a 5 é descartada aqui e explicada em quarantine_ratings'
AS SELECT
  trim(order_id) AS order_id,
  try_cast(stars AS INT) AS stars,
  nullif(trim(comment), '') AS comment,
  _source_file,
  _ingest_ts
FROM STREAM(bronze_ratings);

CREATE OR REFRESH STREAMING TABLE silver_ratings
COMMENT 'Avaliações com nota válida, uma por pedido (a ligação com o pedido é feita na gold)';

CREATE FLOW silver_ratings_dedup AS AUTO CDC INTO silver_ratings
FROM STREAM(silver_ratings_clean)
KEYS (order_id)
SEQUENCE BY struct(_source_file, _ingest_ts)
STORED AS SCD TYPE 1;

-- ---------- itens do pedido
CREATE OR REFRESH MATERIALIZED VIEW silver_order_items
COMMENT 'Um registro por item de pedido, com preço numérico em euros'
AS SELECT
  o.order_id,
  pos + 1 AS line_no,
  item.dish AS dish,
  item.quantity AS quantity,
  item.unit_price AS unit_price,
  CAST(item.quantity * item.unit_price AS DECIMAL(12,2)) AS line_total
FROM silver_orders o
LATERAL VIEW posexplode(o.items) x AS pos, item;

-- ---------- cadastros (a última versão de cada id)
CREATE OR REFRESH MATERIALIZED VIEW silver_restaurants
COMMENT 'Restaurantes, com cidade padronizada'
AS SELECT restaurant_id, restaurant_name, city, cuisine FROM (
  SELECT trim(restaurant_id) AS restaurant_id, trim(name) AS restaurant_name, initcap(trim(city)) AS city, trim(cuisine) AS cuisine,
         row_number() OVER (PARTITION BY trim(restaurant_id) ORDER BY _ingest_ts DESC, _source_file DESC) AS _rn
  FROM bronze_restaurants
) WHERE _rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver_couriers
COMMENT 'Entregadores'
AS SELECT courier_id, courier_name, vehicle FROM (
  SELECT trim(courier_id) AS courier_id, trim(name) AS courier_name, lower(trim(vehicle)) AS vehicle,
         row_number() OVER (PARTITION BY trim(courier_id) ORDER BY _ingest_ts DESC, _source_file DESC) AS _rn
  FROM bronze_couriers
) WHERE _rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW silver_customers
COMMENT 'Clientes derivados dos pedidos válidos'
AS SELECT customer_id, max(customer_email) AS customer_email, min(created_at) AS first_order_at,
          max(created_at) AS last_order_at, count(*) AS n_orders
FROM silver_orders GROUP BY customer_id;

-- ---------- quarentenas (nada some em silêncio)
CREATE OR REFRESH MATERIALIZED VIEW quarantine_orders
COMMENT 'Pedidos rejeitados e o motivo'
AS SELECT order_id, customer_id, customer_email, restaurant_id, status, created_at, reason, _source_file FROM (
  SELECT trim(order_id) AS order_id, customer.id AS customer_id, customer.email AS customer_email, restaurant_id, status,
         to_timestamp(created_at) AS created_at, 'test_account' AS reason, _source_file,
         row_number() OVER (PARTITION BY trim(order_id) ORDER BY _ingest_ts DESC, _source_file DESC) AS _rn
  FROM bronze_orders
  WHERE lower(trim(customer.email)) LIKE '%@quickbite.test'
) WHERE _rn = 1;

CREATE OR REFRESH MATERIALIZED VIEW quarantine_ratings
COMMENT 'Avaliações rejeitadas e o motivo. Avaliação de pedido ainda desconhecido sai daqui sozinha quando o pedido chegar'
AS SELECT order_id, stars, comment, reason, _source_file FROM (
  SELECT r.order_id, r.stars, r.comment, r._source_file,
         CASE WHEN r.stars IS NULL OR r.stars NOT BETWEEN 1 AND 5 THEN 'rating_out_of_range'
              WHEN s.order_id IS NOT NULL AND s.status = 'cancelled' THEN 'order_not_delivered'
              WHEN s.order_id IS NOT NULL THEN NULL
              WHEN b.order_id IS NOT NULL THEN 'order_is_test_account'
              ELSE 'unknown_order' END AS reason
  FROM (
    SELECT trim(order_id) AS order_id, try_cast(stars AS INT) AS stars, nullif(trim(comment), '') AS comment, _source_file,
           row_number() OVER (PARTITION BY trim(order_id) ORDER BY _ingest_ts DESC, _source_file DESC) AS _rn
    FROM bronze_ratings
  ) r
  LEFT JOIN silver_orders s ON s.order_id = r.order_id
  LEFT JOIN (SELECT DISTINCT trim(order_id) AS order_id FROM bronze_orders) b ON b.order_id = r.order_id
  WHERE r._rn = 1
) WHERE reason IS NOT NULL;

-- ---------- relatório de qualidade (lido pelo app)
CREATE OR REFRESH MATERIALIZED VIEW silver_quality_report
COMMENT 'Quanto lixo havia nos arquivos e o que foi feito com ele'
AS
SELECT 1 AS step, 'bronze_orders' AS source, 'rows read' AS rule, 'info' AS action, count(*) AS affected_rows FROM bronze_orders
UNION ALL
SELECT 2, 'bronze_orders', 'duplicate rows (same order_id)', 'dropped', count(*) - count(DISTINCT trim(order_id)) FROM bronze_orders
UNION ALL
SELECT 3, 'bronze_orders', 'test-account orders', 'quarantine', count(*) FROM quarantine_orders
UNION ALL
SELECT 4, 'bronze_orders', 'raw item lines with price outside the 0.00 € format', 'fixed',
       count(*) FROM (SELECT explode(items) AS i FROM bronze_orders) WHERE i.price NOT RLIKE '^[0-9]+[.][0-9]{2} €$'
UNION ALL
SELECT 5, 'silver_orders', 'orders loaded', 'info', count(*) FROM silver_orders
UNION ALL
SELECT 6, 'silver_orders', 'cancelled orders (no courier or delivery time)', 'info', count(*) FROM silver_orders WHERE status = 'cancelled'
UNION ALL
SELECT 7, 'bronze_ratings', 'unique ratings read', 'info', count(DISTINCT trim(order_id)) FROM bronze_ratings
UNION ALL
SELECT 7 + row_number() OVER (ORDER BY reason), 'bronze_ratings', concat('rejected: ', reason), 'quarantine', n
FROM (SELECT reason, count(*) AS n FROM quarantine_ratings GROUP BY reason)
UNION ALL
SELECT 20, 'silver_ratings', 'ratings linked to a delivered order', 'info', count(*)
FROM silver_ratings r JOIN silver_orders o ON o.order_id = r.order_id AND o.status = 'delivered';
