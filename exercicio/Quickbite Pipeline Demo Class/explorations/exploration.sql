-- Databricks notebook source
-- silver_orders: transformação a partir da bronze_orders 
--CREATE OR REPLACE STREAMING TABLE quickbite.silver_orders AS
SELECT
  order_id,
  customer.id AS customer_id,
  customer.email AS customer_email,
  -- padroniza cidade para minúsculas e remove espaços extras
  trim(lower(customer.city)) AS delivery_city,
  restaurant_id,
  courier_id,
  status,
  cast(created_at AS timestamp) AS created_at,
  cast(delivered_at AS timestamp) AS delivered_at,
  cast(created_at AS date) AS order_date,
  hour(cast(created_at AS timestamp)) AS order_hour,
  CASE
    WHEN hour(cast(created_at AS timestamp)) BETWEEN 6 AND 11 THEN 'cafe_manha'
    WHEN hour(cast(created_at AS timestamp)) BETWEEN 12 AND 17 THEN 'almoco'
    WHEN hour(cast(created_at AS timestamp)) BETWEEN 18 AND 23 THEN 'jantar'
    ELSE 'madrugada'
  END AS meal_period,
  CASE WHEN status = 'delivered' AND delivered_at IS NOT NULL THEN
    (unix_timestamp(delivered_at) - unix_timestamp(created_at)) / 60.0 END AS delivery_minutes,
  size(items) AS items_count,
  aggregate(items, 0, (acc, x) -> acc + coalesce(x.qty, 0)) AS total_qty,
  aggregate(items, 0.0, (acc, x) -> acc + coalesce(cast(x.qty as double) * cast(x.price as double), 0.00)) AS order_total -- valor total euro
FROM workspace.quickbite.bronze_orders
WHERE customer.email NOT LIKE '%+test%' -- contas de teste fora
  AND order_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY created_at DESC) = 1;

-- COMMAND ----------

SELECT
    order_id, dish, line_no as item_no, dish as dish_name, quantity, unit_price, line_total
FROM (SELECT
  o.order_id,
  posexplode(items) AS (line_no, item),
  item.dish AS dish,
  cast(item.qty as int) AS quantity,
  round(cast(trim(replace(replace(item.price, '€', ''), ',', '.')) as decimal(10,2)), 2) AS unit_price,
  round(cast(item.qty as decimal(10,2)) * cast(replace(replace(item.price, '€', ''), ',', '.') as decimal(10,2)), 2) AS line_total
FROM workspace.quickbite.bronze_orders o
WHERE o.customer.email NOT LIKE '%+test%'
  AND o.order_id IS NOT NULL);

-- COMMAND ----------

-- DBTITLE 1,PySpark silver_order_items (replace euro/decimal with native functions)
-- MAGIC %python
-- MAGIC # Silver table: silver_order_items (PySpark)
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.types import DecimalType, IntegerType
-- MAGIC
-- MAGIC bronze = spark.table("quickbite.bronze_orders")
-- MAGIC
-- MAGIC # Seleciona pedidos válidos
-- MAGIC orders_valid = bronze \
-- MAGIC     .filter(~F.col("customer.email").like("%+test%")) \
-- MAGIC     .filter(F.col("order_id").isNotNull())
-- MAGIC
-- MAGIC # Explode itens por pedido (posexplode fix)
-- MAGIC items = orders_valid.select(
-- MAGIC     "order_id",
-- MAGIC     F.posexplode("items").alias("line_no", "item")
-- MAGIC )
-- MAGIC
-- MAGIC items_silver = items.select(
-- MAGIC     F.col("order_id"),
-- MAGIC     F.col("line_no").alias("item_no"),
-- MAGIC     F.col("item.dish").alias("dish_name"),
-- MAGIC     F.col("item.dish").alias("dish"),
-- MAGIC     F.col("item.qty").cast(IntegerType()).alias("quantity"),
-- MAGIC     F.round(
-- MAGIC     F.trim(F.regexp_replace(F.regexp_replace(F.col("item.price"), "€", ""), ",", ".")).cast(DecimalType(10, 2)),
-- MAGIC     2
-- MAGIC ).alias("unit_price"),
-- MAGIC     F.round(
-- MAGIC     F.col("item.qty").cast(DecimalType(10,2)) *
-- MAGIC     F.trim(F.regexp_replace(F.regexp_replace(F.col("item.price"), "€", ""), ",", ".")).cast(DecimalType(10,2)),
-- MAGIC     2
-- MAGIC ).alias("line_total")
-- MAGIC )
-- MAGIC
-- MAGIC display(items_silver)
-- MAGIC
-- MAGIC #items_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("quickbite.silver_order_items")
-- MAGIC

-- COMMAND ----------

select restaurant_id, name, city, cuisine from quickbite.bronze_restaurants limit 100