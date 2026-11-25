-- ============================================================================
-- GOLD (SQL): modelo dimensional + KPIs, tudo como materialized views.
-- PKs e FKs são informativas (o Unity Catalog não as impõe). As linhas marcadas "-- FK"
-- podem ser removidas se o seu workspace reclamar delas; o resto do pipeline não depende.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_date (
  date_key DATE NOT NULL PRIMARY KEY, year INT, month INT, day INT, dow_num INT, day_name STRING, week INT, is_weekend BOOLEAN
)
COMMENT 'Calendário do período dos pedidos'
AS SELECT d AS date_key, year(d) AS year, month(d) AS month, day(d) AS day, dayofweek(d) AS dow_num,
          date_format(d, 'EEEE') AS day_name, weekofyear(d) AS week, dayofweek(d) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(min(order_date), max(order_date))) AS d FROM silver_orders);

CREATE OR REFRESH MATERIALIZED VIEW dim_restaurant (
  restaurant_id STRING NOT NULL PRIMARY KEY, restaurant_name STRING, city STRING, cuisine STRING
)
COMMENT 'Restaurantes'
AS SELECT restaurant_id, restaurant_name, city, cuisine FROM silver_restaurants;

CREATE OR REFRESH MATERIALIZED VIEW dim_courier (
  courier_id STRING NOT NULL PRIMARY KEY, courier_name STRING, vehicle STRING
)
COMMENT 'Entregadores e tipo de veículo'
AS SELECT courier_id, courier_name, vehicle FROM silver_couriers;

CREATE OR REFRESH MATERIALIZED VIEW dim_customer (
  customer_id STRING NOT NULL PRIMARY KEY, customer_email STRING, first_order_at TIMESTAMP, last_order_at TIMESTAMP, n_orders BIGINT
)
COMMENT 'Clientes reais (sem contas de teste)'
AS SELECT customer_id, customer_email, first_order_at, last_order_at, n_orders FROM silver_customers;

CREATE OR REFRESH MATERIALIZED VIEW dim_dish (
  dish STRING NOT NULL PRIMARY KEY, cuisine STRING, list_price DECIMAL(10,2)
)
COMMENT 'Pratos, culinária e preço de tabela'
AS SELECT i.dish AS dish, max(r.cuisine) AS cuisine, max(i.unit_price) AS list_price
FROM silver_order_items i
JOIN silver_orders o ON o.order_id = i.order_id
JOIN silver_restaurants r ON r.restaurant_id = o.restaurant_id
GROUP BY i.dish;

CREATE OR REFRESH MATERIALIZED VIEW fact_orders (
  order_id STRING NOT NULL PRIMARY KEY,
  order_date DATE, order_hour INT, meal_period STRING,
  customer_id STRING, restaurant_id STRING, courier_id STRING, delivery_city STRING,
  status STRING, created_at TIMESTAMP, delivered_at TIMESTAMP, delivery_minutes DOUBLE,
  items_count INT, total_qty INT, order_total DECIMAL(12,2),
  is_delivered BOOLEAN, is_cancelled BOOLEAN, within_sla BOOLEAN,
  revenue DECIMAL(12,2), lost_revenue DECIMAL(12,2),
  stars INT, comment STRING,
  CONSTRAINT fk_fact_orders_date       FOREIGN KEY (order_date)    REFERENCES ${qb_catalog}.${qb_schema}.dim_date(date_key),          -- FK
  CONSTRAINT fk_fact_orders_restaurant FOREIGN KEY (restaurant_id) REFERENCES ${qb_catalog}.${qb_schema}.dim_restaurant(restaurant_id), -- FK
  CONSTRAINT fk_fact_orders_courier    FOREIGN KEY (courier_id)    REFERENCES ${qb_catalog}.${qb_schema}.dim_courier(courier_id),       -- FK
  CONSTRAINT fk_fact_orders_customer   FOREIGN KEY (customer_id)   REFERENCES ${qb_catalog}.${qb_schema}.dim_customer(customer_id)      -- FK
)
COMMENT 'Um registro por pedido. Receita = pedidos entregues, receita perdida = cancelados, within_sla é nulo para não entregues e stars é nulo sem avaliação'
AS SELECT
  o.order_id, o.order_date, o.order_hour, o.meal_period, o.customer_id, o.restaurant_id, o.courier_id, o.delivery_city,
  o.status, o.created_at, o.delivered_at, o.delivery_minutes, o.items_count, o.total_qty, o.order_total,
  o.status = 'delivered' AS is_delivered,
  o.status = 'cancelled' AS is_cancelled,
  CASE WHEN o.status = 'delivered' THEN o.delivery_minutes <= ${sla_minutes} END AS within_sla,
  CAST(CASE WHEN o.status = 'delivered' THEN o.order_total ELSE 0 END AS DECIMAL(12,2)) AS revenue,
  CAST(CASE WHEN o.status = 'cancelled' THEN o.order_total ELSE 0 END AS DECIMAL(12,2)) AS lost_revenue,
  r.stars, r.comment
FROM silver_orders o
LEFT JOIN silver_ratings r ON r.order_id = o.order_id AND o.status = 'delivered';

CREATE OR REFRESH MATERIALIZED VIEW fact_order_items (
  order_id STRING NOT NULL, line_no INT NOT NULL, dish STRING, quantity INT, unit_price DECIMAL(10,2), line_total DECIMAL(12,2),
  order_date DATE, restaurant_id STRING, status STRING,
  CONSTRAINT pk_fact_order_items PRIMARY KEY (order_id, line_no),
  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES ${qb_catalog}.${qb_schema}.fact_orders(order_id), -- FK
  CONSTRAINT fk_items_dish  FOREIGN KEY (dish)     REFERENCES ${qb_catalog}.${qb_schema}.dim_dish(dish)         -- FK
)
COMMENT 'Um registro por item de pedido'
AS SELECT i.order_id, i.line_no, i.dish, i.quantity, i.unit_price, i.line_total, o.order_date, o.restaurant_id, o.status
FROM silver_order_items i JOIN silver_orders o ON o.order_id = i.order_id;

-- ---------- marts de KPI (consumo do app)
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_kpis
COMMENT 'KPIs por dia e cidade. Guarda somas e contagens para recalcular médias de qualquer recorte (não some médias)'
AS SELECT order_date, delivery_city,
       count(*) AS n_orders,
       sum(CASE WHEN is_delivered THEN 1 ELSE 0 END) AS n_delivered,
       sum(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS n_cancelled,
       sum(revenue) AS revenue, sum(lost_revenue) AS lost_revenue,
       sum(CASE WHEN is_delivered THEN delivery_minutes END) AS delivery_minutes_sum,
       sum(CASE WHEN within_sla THEN 1 ELSE 0 END) AS n_within_sla,
       sum(stars) AS stars_sum, count(stars) AS n_ratings
FROM fact_orders GROUP BY order_date, delivery_city;

CREATE OR REFRESH MATERIALIZED VIEW gold_restaurant_summary
COMMENT 'Ranking de restaurantes'
AS SELECT r.restaurant_id, r.restaurant_name, r.city, r.cuisine, count(*) AS n_orders, sum(f.revenue) AS revenue,
       round(100 * avg(CASE WHEN f.is_cancelled THEN 1.0 ELSE 0.0 END), 1) AS cancel_rate_pct,
       round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes, round(avg(f.stars), 2) AS avg_stars, count(f.stars) AS n_ratings
FROM fact_orders f JOIN dim_restaurant r ON r.restaurant_id = f.restaurant_id
GROUP BY r.restaurant_id, r.restaurant_name, r.city, r.cuisine;

CREATE OR REFRESH MATERIALIZED VIEW gold_courier_summary
COMMENT 'Desempenho dos entregadores (só entregas realizadas)'
AS SELECT c.courier_id, c.courier_name, c.vehicle, count(*) AS n_deliveries,
       round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes,
       round(100 * avg(CASE WHEN f.within_sla THEN 1.0 ELSE 0.0 END), 1) AS within_sla_pct, round(avg(f.stars), 2) AS avg_stars
FROM fact_orders f JOIN dim_courier c ON c.courier_id = f.courier_id
WHERE f.is_delivered GROUP BY c.courier_id, c.courier_name, c.vehicle;

CREATE OR REFRESH MATERIALIZED VIEW gold_dish_ranking
COMMENT 'Pratos mais vendidos (só pedidos entregues)'
AS SELECT d.dish, d.cuisine, sum(i.quantity) AS units, sum(i.line_total) AS revenue, count(DISTINCT i.order_id) AS n_orders
FROM fact_order_items i JOIN dim_dish d ON d.dish = i.dish
WHERE i.status = 'delivered' GROUP BY d.dish, d.cuisine;

CREATE OR REFRESH MATERIALIZED VIEW gold_hourly_demand
COMMENT 'Demanda por dia da semana e hora'
AS SELECT dd.dow_num, dd.day_name, f.order_hour, count(*) AS n_orders, sum(f.revenue) AS revenue
FROM fact_orders f JOIN dim_date dd ON dd.date_key = f.order_date
GROUP BY dd.dow_num, dd.day_name, f.order_hour;

CREATE OR REFRESH MATERIALIZED VIEW gold_rating_reasons
COMMENT 'Comentários das avaliações: quantidade e nota média'
AS SELECT coalesce(comment, '(no comment)') AS comment, count(*) AS n_ratings, round(avg(stars), 2) AS avg_stars
FROM fact_orders WHERE stars IS NOT NULL GROUP BY 1;
